package apply_test

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"letts/internal/apply"
	"letts/internal/eventfile"
	"letts/internal/ids"
	"letts/internal/lane"
	"letts/internal/storage"
)

func newLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func setupDB(t *testing.T) *sql.DB {
	t.Helper()
	dir := t.TempDir()
	db, err := storage.Open(filepath.Join(dir, "state.db"), storage.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := storage.Migrate(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	return db
}

func newManager(t *testing.T, db *sql.DB) *lane.Manager {
	t.Helper()
	ctx := context.Background()
	m := &lane.Manager{
		DB:      db,
		Spawner: func(_ context.Context, _ *storage.Mission, release func()) error { release(); return nil },
		Logger:  newLogger(),
		Ctx:     ctx,
	}
	t.Cleanup(func() { m.StopAll() })
	return m
}

func sortedStrings(ss []string) []string {
	out := make([]string, len(ss))
	copy(out, ss)
	sort.Strings(out)
	return out
}

// TestApplyStartsLanes verifies basic apply starts two lanes.
func TestApplyStartsLanes(t *testing.T) {
	db := setupDB(t)
	mgr := newManager(t, db)

	desired := apply.AppliedState{
		MissionDir: "/missions",
		Lanes: map[string]apply.LaneCfg{
			"fast": {Concurrency: 4},
			"slow": {Concurrency: 2},
		},
	}

	result, err := apply.Apply(context.Background(), db, mgr, desired, apply.Options{})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}

	gotStarted := sortedStrings(result.Started)
	if !reflect.DeepEqual(gotStarted, []string{"fast", "slow"}) {
		t.Errorf("started: got %v, want [fast slow]", gotStarted)
	}
	if len(result.Stopped) != 0 {
		t.Errorf("stopped: want none, got %v", result.Stopped)
	}
}

// TestApplyPersistsState verifies that after apply, GetAppliedConfig returns the state.
func TestApplyPersistsState(t *testing.T) {
	db := setupDB(t)
	mgr := newManager(t, db)

	desired := apply.AppliedState{
		MissionDir: "/data",
		Labels:     []string{"prod"},
		Lanes:      map[string]apply.LaneCfg{"alpha": {Concurrency: 1}},
	}

	if _, err := apply.Apply(context.Background(), db, mgr, desired, apply.Options{Source: "test"}); err != nil {
		t.Fatalf("apply: %v", err)
	}

	cfg, err := storage.GetAppliedConfig(context.Background(), db)
	if err != nil {
		t.Fatalf("GetAppliedConfig: %v", err)
	}
	if cfg.AppliedAt == 0 {
		t.Error("applied_at should be set")
	}
	if cfg.Source.String != "test" {
		t.Errorf("source: got %q, want test", cfg.Source.String)
	}
}

// TestApplyConflictRuntimeChange verifies that changing runtime with active
// missions returns ErrConflict (without Force).
func TestApplyConflictRuntimeChange(t *testing.T) {
	db := setupDB(t)
	mgr := newManager(t, db)

	// First apply sets baseline runtime.
	initial := apply.AppliedState{
		Lanes:   map[string]apply.LaneCfg{"lane1": {Concurrency: 2}},
		Runtime: apply.Runtime{CommandTemplate: []string{"cmd"}},
	}
	if _, err := apply.Apply(context.Background(), db, mgr, initial, apply.Options{}); err != nil {
		t.Fatalf("initial apply: %v", err)
	}

	// Insert a queued mission.
	m := &storage.Mission{
		ID:               "01900000-0000-7000-8000-000000000001",
		Kind:             storage.KindMission,
		Lane:             "lane1",
		MissionName:      "test",
		Status:           storage.StatusQueued,
		InputFingerprint: "fp",
		Input:            []byte("{}"),
		TimeCreatedMs:    1000,
	}
	if err := storage.InsertMission(context.Background(), db, m); err != nil {
		t.Fatalf("insert mission: %v", err)
	}

	// Try to apply with changed runtime without Force.
	changed := apply.AppliedState{
		Lanes:   map[string]apply.LaneCfg{"lane1": {Concurrency: 2}},
		Runtime: apply.Runtime{CommandTemplate: []string{"new-cmd"}},
	}
	_, err := apply.Apply(context.Background(), db, mgr, changed, apply.Options{})
	if !errors.Is(err, apply.ErrConflict) {
		t.Errorf("expected ErrConflict, got %v", err)
	}

	// With Force it should succeed.
	_, err = apply.Apply(context.Background(), db, mgr, changed, apply.Options{Force: true})
	if err != nil {
		t.Errorf("apply with force: %v", err)
	}
}

// TestApplyConflictLaneRemoval verifies that removing a lane with active missions
// returns ErrConflict without ForcePrune.
func TestApplyConflictLaneRemoval(t *testing.T) {
	db := setupDB(t)
	mgr := newManager(t, db)
	dataDir := t.TempDir()

	// First apply sets two lanes — "remove" paused from the start so
	// no lazy-pause race against the runner's first iteration.
	initial := apply.AppliedState{
		Lanes: map[string]apply.LaneCfg{
			"keep":   {Concurrency: 1},
			"remove": {Concurrency: 1, Paused: true},
		},
	}
	if _, err := apply.Apply(context.Background(), db, mgr, initial,
		apply.Options{DataDir: dataDir}); err != nil {
		t.Fatalf("initial apply: %v", err)
	}

	// Insert queued mission in "remove" lane.
	mid := "01900000-0000-7000-8000-000000000002"
	m := &storage.Mission{
		ID:               mid,
		Kind:             storage.KindMission,
		Lane:             "remove",
		MissionName:      "blocked",
		Status:           storage.StatusQueued,
		InputFingerprint: "fp2",
		Input:            []byte("{}"),
		TimeCreatedMs:    2000,
	}
	if err := storage.InsertMission(context.Background(), db, m); err != nil {
		t.Fatalf("insert mission: %v", err)
	}

	// Mimic dispatch so the force-prune intent-journal flow
	// has an events file to append to.
	shard, err := ids.ShardPath(mid)
	if err != nil {
		t.Fatal(err)
	}
	parentDir := filepath.Join(dataDir, "output", shard)
	if err := os.MkdirAll(parentDir, 0o755); err != nil {
		t.Fatal(err)
	}
	ew, err := eventfile.Create(parentDir, mid)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ew.Append(eventfile.KindQueued, map[string]any{"time": 2000}, true); err != nil {
		t.Fatal(err)
	}
	_ = ew.Close()

	// Apply with --prune (lanes actually get removed) but without
	// ForcePrune → conflict because of the queued mission in "remove".
	desired := apply.AppliedState{
		Lanes: map[string]apply.LaneCfg{"keep": {Concurrency: 1}},
	}
	_, err = apply.Apply(context.Background(), db, mgr, desired,
		apply.Options{Prune: true, DataDir: dataDir})
	if !errors.Is(err, apply.ErrConflict) {
		t.Errorf("expected ErrConflict, got %v", err)
	}

	// With Prune and ForcePrune → success.
	result, err := apply.Apply(context.Background(), db, mgr, desired,
		apply.Options{Prune: true, ForcePrune: true, DataDir: dataDir})
	if err != nil {
		t.Fatalf("apply with force_prune: %v", err)
	}
	if len(result.Stopped) != 1 || result.Stopped[0] != "remove" {
		t.Errorf("stopped: want [remove], got %v", result.Stopped)
	}
}

// TestApplyDefaultPreservesUnmentionedLanes verifies the default-
// without-prune behavior: lanes present in current state but absent from
// the desired apply must be PRESERVED, not removed. Operators applying
// partial overlays don't lose lanes they didn't touch.
func TestApplyDefaultPreservesUnmentionedLanes(t *testing.T) {
	db := setupDB(t)
	mgr := newManager(t, db)

	// Initial: two lanes.
	initial := apply.AppliedState{
		Lanes: map[string]apply.LaneCfg{
			"keep":   {Concurrency: 2},
			"forget": {Concurrency: 1},
		},
	}
	if _, err := apply.Apply(context.Background(), db, mgr, initial, apply.Options{}); err != nil {
		t.Fatalf("initial apply: %v", err)
	}

	// Apply a partial overlay that omits "forget". Without --prune the
	// lane should survive untouched.
	partial := apply.AppliedState{
		Lanes: map[string]apply.LaneCfg{"keep": {Concurrency: 2}},
	}
	result, err := apply.Apply(context.Background(), db, mgr, partial, apply.Options{})
	if err != nil {
		t.Fatalf("partial apply: %v", err)
	}
	if len(result.Stopped) != 0 {
		t.Errorf("stopped=%v, want [] (forget should be preserved without --prune)", result.Stopped)
	}

	// Re-applying with --prune actually removes "forget".
	result, err = apply.Apply(context.Background(), db, mgr, partial, apply.Options{Prune: true})
	if err != nil {
		t.Fatalf("prune apply: %v", err)
	}
	if len(result.Stopped) != 1 || result.Stopped[0] != "forget" {
		t.Errorf("stopped=%v, want [forget]", result.Stopped)
	}
}

// TestApplyForcePruneTerminatesQueuedMissions verifies the force-prune
// transition: with --prune and --force-prune, queued missions in the
// removed lane are flipped to done(killed/lane_removed) BEFORE the lane
// runner is stopped. The DB row, the events file terminal `done` and the
// finalize-intent journal must all agree (queued-kill path).
func TestApplyForcePruneTerminatesQueuedMissions(t *testing.T) {
	db := setupDB(t)
	mgr := newManager(t, db)
	dataDir := t.TempDir()

	initial := apply.AppliedState{
		Lanes: map[string]apply.LaneCfg{
			"keep":   {Concurrency: 1},
			"reaped": {Concurrency: 1, Paused: true},
		},
	}
	if _, err := apply.Apply(context.Background(), db, mgr, initial,
		apply.Options{DataDir: dataDir}); err != nil {
		t.Fatalf("initial: %v", err)
	}

	// (initial apply already started "reaped" as paused via LaneCfg.Paused,
	// no need for a lazy PauseLane that can race the runner's first
	// iteration.)

	mid := "01900000-0000-7000-8000-000000000099"
	if err := storage.InsertMission(context.Background(), db, &storage.Mission{
		ID: mid, Kind: storage.KindMission, Lane: "reaped",
		MissionName: "blocked", Status: storage.StatusQueued,
		InputFingerprint: "fp99", Input: []byte("{}"), TimeCreatedMs: 99000,
	}); err != nil {
		t.Fatal(err)
	}

	// Dispatch normally creates the events file with a queued event
	// before INSERT. Replicate that here so the
	// force-prune intent-journal flow has a file to append to.
	shard, err := ids.ShardPath(mid)
	if err != nil {
		t.Fatal(err)
	}
	parentDir := filepath.Join(dataDir, "output", shard)
	if err := os.MkdirAll(parentDir, 0o755); err != nil {
		t.Fatal(err)
	}
	ew, err := eventfile.Create(parentDir, mid)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ew.Append(eventfile.KindQueued, map[string]any{"time": 99000}, true); err != nil {
		t.Fatal(err)
	}
	_ = ew.Close()

	desired := apply.AppliedState{
		Lanes: map[string]apply.LaneCfg{"keep": {Concurrency: 1}},
	}
	if _, err := apply.Apply(context.Background(), db, mgr, desired,
		apply.Options{Prune: true, ForcePrune: true, DataDir: dataDir}); err != nil {
		t.Fatalf("force-prune apply: %v", err)
	}

	got, err := storage.GetMission(context.Background(), db, mid)
	if err != nil {
		t.Fatalf("get mission: %v", err)
	}
	if got.Status != storage.StatusDone {
		t.Errorf("status=%q, want done", got.Status)
	}
	if got.Outcome.String != "killed" || got.FailReason.String != "lane_removed" {
		t.Errorf("outcome=%q reason=%q, want killed/lane_removed",
			got.Outcome.String, got.FailReason.String)
	}

	// Any terminal transition must go through the
	// durable-finalize path. That means the events file must end with
	// a `done` event matching the DB outcome — otherwise live
	// /v1/missions/{id}/events consumers never see done(killed).
	eventsPath := filepath.Join(parentDir, mid+"-events")
	f, err := os.Open(eventsPath)
	if err != nil {
		t.Fatalf("open events file: %v", err)
	}
	defer func() { _ = f.Close() }()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 16<<20)
	var sawDone bool
	var doneOutcome, doneFailReason string
	for scanner.Scan() {
		var ev map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &ev); err != nil {
			t.Fatalf("parse event: %v", err)
		}
		if ev["event"] == "done" {
			sawDone = true
			if s, ok := ev["outcome"].(string); ok {
				doneOutcome = s
			}
			if s, ok := ev["fail_reason"].(string); ok {
				doneFailReason = s
			}
		}
	}
	if !sawDone {
		t.Fatal("events file missing terminal `done` event after force-prune (durable-finalize violation)")
	}
	if doneOutcome != "killed" || doneFailReason != "lane_removed" {
		t.Errorf("events file done: outcome=%q reason=%q; want killed/lane_removed",
			doneOutcome, doneFailReason)
	}

	// Finalize intent must be deleted on successful completion (the
	// row only exists during the journal window).
	intent, err := storage.GetFinalizeIntent(context.Background(), db, mid)
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("get intent: %v", err)
	}
	if intent != nil {
		t.Errorf("finalize intent should be deleted after success, got phase=%q outcome=%q",
			intent.Phase, intent.Outcome)
	}
}

// TestComputeDiff checks the diff calculation.
func TestComputeDiff(t *testing.T) {
	current := apply.AppliedState{
		MissionDir: "/old",
		Labels:     []string{"a"},
		Lanes: map[string]apply.LaneCfg{
			"keep": {Concurrency: 2},
			"old":  {Concurrency: 1},
		},
		Runtime: apply.Runtime{CommandTemplate: []string{"old"}},
	}
	desired := apply.AppliedState{
		MissionDir: "/new",
		Labels:     []string{"a"},
		Lanes: map[string]apply.LaneCfg{
			"keep": {Concurrency: 5},
			"new":  {Concurrency: 3},
		},
		Runtime: apply.Runtime{CommandTemplate: []string{"new"}},
	}
	d := apply.ComputeDiff(current, desired)

	if !d.MissionDirChanged {
		t.Error("expected MissionDirChanged")
	}
	if d.LabelsChanged {
		t.Error("expected LabelsChanged=false")
	}
	if !d.RuntimeChanged {
		t.Error("expected RuntimeChanged")
	}
	if len(d.LanesAdded) != 1 || d.LanesAdded[0] != "new" {
		t.Errorf("LanesAdded: got %v", d.LanesAdded)
	}
	if len(d.LanesRemoved) != 1 || d.LanesRemoved[0] != "old" {
		t.Errorf("LanesRemoved: got %v", d.LanesRemoved)
	}
	if len(d.LanesResized) != 1 || d.LanesResized[0] != "keep" {
		t.Errorf("LanesResized: got %v", d.LanesResized)
	}
}
