package handlers_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"letts/internal/apply"
	"letts/internal/eventfile"
	"letts/internal/ids"
	"letts/internal/lane"
	"letts/internal/server/handlers"
	"letts/internal/storage"
)

// Ensure lane import is used.
var _ = lane.LaneSpec{}

func makeAdminHandler(t *testing.T) (*handlers.Admin, *lane.Manager) {
	t.Helper()
	db := setupDB(t)
	mgr := &lane.Manager{
		DB:      db,
		Spawner: func(_ context.Context, _ *storage.Mission, release func()) error { release(); return nil },
		Logger:  newTestLogger(),
		Ctx:     context.Background(),
	}
	t.Cleanup(func() { mgr.StopAll() })
	h := &handlers.Admin{DB: db, Manager: mgr}
	return h, mgr
}

// TestAdminApplyTwoLanes verifies that POST /v1/admin/apply starts two lanes.
func TestAdminApplyTwoLanes(t *testing.T) {
	h, _ := makeAdminHandler(t)
	mux := http.NewServeMux()
	h.Register(mux)

	desired := apply.AppliedState{
		MissionDir: "/missions",
		Lanes: map[string]apply.LaneCfg{
			"fast": {Concurrency: 4},
			"slow": {Concurrency: 2},
		},
	}
	body, _ := json.Marshal(desired)
	req := httptest.NewRequest("POST", "/v1/admin/apply", bytes.NewReader(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status: got %d, want 200; body=%s", w.Code, w.Body.String())
	}
	var result apply.Result
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(result.Started) != 2 {
		t.Errorf("started: want 2, got %d: %v", len(result.Started), result.Started)
	}
}

// TestAdminApplyConflictLaneRemoval verifies 409 when removing lane with active missions.
func TestAdminApplyConflictLaneRemoval(t *testing.T) {
	db := setupDB(t)
	mgr := &lane.Manager{
		DB:      db,
		Spawner: func(_ context.Context, _ *storage.Mission, release func()) error { release(); return nil },
		Logger:  newTestLogger(),
		Ctx:     context.Background(),
	}
	t.Cleanup(func() { mgr.StopAll() })

	h := &handlers.Admin{DB: db, Manager: mgr}
	mux := http.NewServeMux()
	h.Register(mux)

	// First apply — establish two lanes.
	initial := apply.AppliedState{
		Lanes: map[string]apply.LaneCfg{
			"keep": {Concurrency: 1},
			"drop": {Concurrency: 1},
		},
	}
	body, _ := json.Marshal(initial)
	req := httptest.NewRequest("POST", "/v1/admin/apply", bytes.NewReader(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("initial apply: %d %s", w.Code, w.Body.String())
	}

	// Insert a queued mission in "drop" lane.
	m := &storage.Mission{
		ID:               "01900000-0000-7000-8000-000000000003",
		Kind:             storage.KindMission,
		Lane:             "drop",
		MissionName:      "blocked",
		Status:           storage.StatusQueued,
		InputFingerprint: "fp3",
		Input:            []byte("{}"),
		TimeCreatedMs:    3000,
	}
	if err := storage.InsertMission(context.Background(), db, m); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Second apply removing "drop" with --prune (so lanes actually get
	// removed) but without --force-prune (queued mission blocks removal).
	// Must return 409.
	desired := apply.AppliedState{
		Lanes: map[string]apply.LaneCfg{"keep": {Concurrency: 1}},
	}
	body2, _ := json.Marshal(desired)
	req2 := httptest.NewRequest("POST", "/v1/admin/apply?prune=true", bytes.NewReader(body2))
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, req2)

	if w2.Code != http.StatusConflict {
		t.Errorf("expected 409, got %d: %s", w2.Code, w2.Body.String())
	}
}

// TestAdminApplyForcePrune verifies that ?force_prune=true succeeds despite
// active missions AND that the queued mission goes through the durable
// finalize-intent path (terminal done event in /events file, intent
// inserted/deleted within one writer txn).
func TestAdminApplyForcePrune(t *testing.T) {
	db := setupDB(t)
	mgr := &lane.Manager{
		DB:      db,
		Spawner: func(_ context.Context, _ *storage.Mission, release func()) error { release(); return nil },
		Logger:  newTestLogger(),
		Ctx:     context.Background(),
	}
	t.Cleanup(func() { mgr.StopAll() })

	dataDir := t.TempDir()
	h := &handlers.Admin{DB: db, Manager: mgr, DataDir: dataDir}
	mux := http.NewServeMux()
	h.Register(mux)

	// Establish two lanes — "drop" starts paused so its runner cannot
	// race the test into picking the queued mission before force-prune
	// applies. (mgr.Apply forwards LaneCfg.Paused to runner.Pause at
	// construction time, eliminating the lazy PauseLane window.)
	initial := apply.AppliedState{
		Lanes: map[string]apply.LaneCfg{
			"keep": {Concurrency: 1},
			"drop": {Concurrency: 1, Paused: true},
		},
	}
	body, _ := json.Marshal(initial)
	req := httptest.NewRequest("POST", "/v1/admin/apply", bytes.NewReader(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("initial apply: %d", w.Code)
	}

	// Insert queued mission in "drop".
	mid := "01900000-0000-7000-8000-000000000004"
	m := &storage.Mission{
		ID:               mid,
		Kind:             storage.KindMission,
		Lane:             "drop",
		MissionName:      "blocked",
		Status:           storage.StatusQueued,
		InputFingerprint: "fp4",
		Input:            []byte("{}"),
		TimeCreatedMs:    4000,
	}
	if err := storage.InsertMission(context.Background(), db, m); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Mimic dispatch step 13: create the events file with a queued
	// event so the force-prune finalize path has a file to open.
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
	if _, err := ew.Append(eventfile.KindQueued, map[string]any{"time": 4000}, true); err != nil {
		t.Fatal(err)
	}
	_ = ew.Close()

	// Remove "drop" with force_prune=true.
	desired := apply.AppliedState{
		Lanes: map[string]apply.LaneCfg{"keep": {Concurrency: 1}},
	}
	body2, _ := json.Marshal(desired)
	req2 := httptest.NewRequest("POST", "/v1/admin/apply?force_prune=true", bytes.NewReader(body2))
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, req2)

	if w2.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w2.Code, w2.Body.String())
	}
	var result apply.Result
	if err := json.NewDecoder(w2.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(result.Stopped) != 1 || result.Stopped[0] != "drop" {
		t.Errorf("stopped: want [drop], got %v", result.Stopped)
	}

	// Durable terminal done event must be present.
	f, err := os.Open(filepath.Join(parentDir, mid+"-events"))
	if err != nil {
		t.Fatalf("open events: %v", err)
	}
	defer func() { _ = f.Close() }()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 16<<20)
	var sawDone bool
	var doneOutcome, doneFailReason string
	for scanner.Scan() {
		var ev map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &ev); err == nil && ev["event"] == "done" {
			sawDone = true
			doneOutcome, _ = ev["outcome"].(string)
			doneFailReason, _ = ev["fail_reason"].(string)
		}
	}
	if !sawDone {
		t.Fatal("events file missing terminal done event (durable-finalize violation)")
	}
	if doneOutcome != "killed" || doneFailReason != "lane_removed" {
		t.Errorf("done event: outcome=%q reason=%q; want killed/lane_removed",
			doneOutcome, doneFailReason)
	}
}

// TestAdminPauseLanePersistedAcrossApply enforces:
// POST /v1/admin/lanes/{name}/pause must update the applied config
// row, not just the runtime Manager state. Otherwise a subsequent
// `letts apply` reads stored config (paused=false) and silently
// un-pauses the lane via mgr.Apply → r.Resume(). Same risk on
// daemon restart (replay reads stored config).
func TestAdminPauseLanePersistedAcrossApply(t *testing.T) {
	h, mgr := makeAdminHandler(t)
	mux := http.NewServeMux()
	h.Register(mux)

	// Initial apply: two lanes, both un-paused.
	initial := apply.AppliedState{
		Lanes: map[string]apply.LaneCfg{
			"alpha": {Concurrency: 1},
			"beta":  {Concurrency: 2},
		},
	}
	body, _ := json.Marshal(initial)
	req := httptest.NewRequest("POST", "/v1/admin/apply", bytes.NewReader(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("initial apply: %d", w.Code)
	}

	// Pause "alpha" via the admin endpoint.
	req2 := httptest.NewRequest("POST", "/v1/admin/lanes/alpha/pause", nil)
	req2.SetPathValue("name", "alpha")
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, req2)
	if w2.Code != 200 {
		t.Fatalf("pause: %d %s", w2.Code, w2.Body.String())
	}

	// Re-apply the SAME initial state (paused not mentioned ⇒ false in
	// raw YAML, but we keep "alpha" listed). Without the fix this
	// would silently un-pause. With the fix, the applied state on disk
	// already has alpha.Paused=true, so the diff for "alpha" is no-op
	// and the lane stays paused.
	body3, _ := json.Marshal(initial)
	req3 := httptest.NewRequest("POST", "/v1/admin/apply", bytes.NewReader(body3))
	w3 := httptest.NewRecorder()
	mux.ServeHTTP(w3, req3)
	if w3.Code != 200 {
		t.Fatalf("re-apply: %d %s", w3.Code, w3.Body.String())
	}

	// Inspect applied state: alpha must still be paused.
	req4 := httptest.NewRequest("GET", "/v1/admin/state", nil)
	w4 := httptest.NewRecorder()
	mux.ServeHTTP(w4, req4)
	if w4.Code != 200 {
		t.Fatalf("state: %d", w4.Code)
	}
	var got map[string]any
	_ = json.NewDecoder(w4.Body).Decode(&got)
	state, _ := got["state"].(map[string]any)
	lanes, _ := state["lanes"].(map[string]any)
	alpha, _ := lanes["alpha"].(map[string]any)
	if paused, _ := alpha["paused"].(bool); !paused {
		t.Errorf("alpha.paused not persisted across apply (got %v)", alpha)
	}

	// And the runtime mgr also reports alpha as paused.
	for _, ls := range mgr.CurrentLanes() {
		if ls.Name == "alpha" && !ls.Paused {
			t.Errorf("runtime: alpha not paused after re-apply")
		}
	}
}

// TestAdminPauseLaneRollsBackRuntimeOnPersistFailure: if
// the runtime PauseLane succeeds but setLanePausedInConfig fails, the
// handler must roll the runtime change back so the operator sees a
// coherent 500 instead of a silent runtime-vs-persisted split.
func TestAdminPauseLaneRollsBackRuntimeOnPersistFailure(t *testing.T) {
	h, mgr := makeAdminHandler(t)
	mux := http.NewServeMux()
	h.Register(mux)

	initial := apply.AppliedState{
		Lanes: map[string]apply.LaneCfg{"alpha": {Concurrency: 1}},
	}
	body, _ := json.Marshal(initial)
	req := httptest.NewRequest("POST", "/v1/admin/apply", bytes.NewReader(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("initial apply: %d", w.Code)
	}

	// Close the DB so setLanePausedInConfig fails.
	if err := h.DB.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	req2 := httptest.NewRequest("POST", "/v1/admin/lanes/alpha/pause", nil)
	req2.SetPathValue("name", "alpha")
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, req2)
	if w2.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 on persist failure, got %d", w2.Code)
	}

	// Runtime must be rolled back: alpha NOT paused.
	for _, ls := range mgr.CurrentLanes() {
		if ls.Name == "alpha" && ls.Paused {
			t.Errorf("runtime alpha still paused after persist failure; rollback missing")
		}
	}
}

// TestAdminPauseLane_CtlOriginSurvivesYAMLUnpause:
// pause via the admin endpoint stamps PausedBy="ctl"; a subsequent
// `letts apply` with paused:false (the YAML doesn't know about the ctl
// pause) must preserve the pause. The operator has to issue
// `letts ctl lanes continue` to unpause.
func TestAdminPauseLane_CtlOriginSurvivesYAMLUnpause(t *testing.T) {
	h, mgr := makeAdminHandler(t)
	mux := http.NewServeMux()
	h.Register(mux)

	initial := apply.AppliedState{
		Lanes: map[string]apply.LaneCfg{"alpha": {Concurrency: 1}},
	}
	body, _ := json.Marshal(initial)
	req := httptest.NewRequest("POST", "/v1/admin/apply", bytes.NewReader(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("initial: %d %s", w.Code, w.Body.String())
	}

	// Pause via ctl.
	req2 := httptest.NewRequest("POST", "/v1/admin/lanes/alpha/pause", nil)
	req2.SetPathValue("name", "alpha")
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, req2)
	if w2.Code != 200 {
		t.Fatalf("pause: %d", w2.Code)
	}

	// Persisted state must carry PausedBy="ctl".
	cfg, err := storage.GetAppliedConfig(context.Background(), h.DB)
	if err != nil {
		t.Fatalf("GetAppliedConfig: %v", err)
	}
	var s apply.AppliedState
	if err := json.Unmarshal(cfg.Data, &s); err != nil {
		t.Fatal(err)
	}
	if s.Lanes["alpha"].PausedBy != apply.PausedByCtl {
		t.Errorf("after ctl pause: PausedBy=%q want ctl", s.Lanes["alpha"].PausedBy)
	}

	// Apply with paused:false (operator's YAML doesn't know about the
	// ctl pause). Must preserve.
	yaml := apply.AppliedState{
		Lanes: map[string]apply.LaneCfg{"alpha": {Concurrency: 1, Paused: false}},
	}
	body3, _ := json.Marshal(yaml)
	req3 := httptest.NewRequest("POST", "/v1/admin/apply", bytes.NewReader(body3))
	w3 := httptest.NewRecorder()
	mux.ServeHTTP(w3, req3)
	if w3.Code != 200 {
		t.Fatalf("re-apply: %d %s", w3.Code, w3.Body.String())
	}

	// Still paused, still ctl-origin.
	cfg2, _ := storage.GetAppliedConfig(context.Background(), h.DB)
	var s2 apply.AppliedState
	_ = json.Unmarshal(cfg2.Data, &s2)
	if !s2.Lanes["alpha"].Paused {
		t.Error("ctl pause cleared by YAML re-apply (regression)")
	}
	if s2.Lanes["alpha"].PausedBy != apply.PausedByCtl {
		t.Errorf("PausedBy=%q want ctl preserved", s2.Lanes["alpha"].PausedBy)
	}
	for _, ls := range mgr.CurrentLanes() {
		if ls.Name == "alpha" && !ls.Paused {
			t.Error("runtime alpha unpaused after YAML re-apply")
		}
	}

	// `letts ctl lanes continue` clears the pause and PausedBy.
	req4 := httptest.NewRequest("POST", "/v1/admin/lanes/alpha/continue", nil)
	req4.SetPathValue("name", "alpha")
	w4 := httptest.NewRecorder()
	mux.ServeHTTP(w4, req4)
	if w4.Code != 200 {
		t.Fatalf("continue: %d", w4.Code)
	}
	cfg3, _ := storage.GetAppliedConfig(context.Background(), h.DB)
	var s3 apply.AppliedState
	_ = json.Unmarshal(cfg3.Data, &s3)
	if s3.Lanes["alpha"].Paused || s3.Lanes["alpha"].PausedBy != "" {
		t.Errorf("after continue: Paused=%v PausedBy=%q; want both clear",
			s3.Lanes["alpha"].Paused, s3.Lanes["alpha"].PausedBy)
	}
}

// TestAdminApply_YAMLUnpausesYAMLOrigin: an operator
// editing `paused: true` → `paused: false` and re-applying must actually
// see the lane unpause.
func TestAdminApply_YAMLUnpausesYAMLOrigin(t *testing.T) {
	h, mgr := makeAdminHandler(t)
	mux := http.NewServeMux()
	h.Register(mux)

	// First apply: pause via YAML.
	first := apply.AppliedState{
		Lanes: map[string]apply.LaneCfg{"alpha": {Concurrency: 1, Paused: true}},
	}
	body, _ := json.Marshal(first)
	req := httptest.NewRequest("POST", "/v1/admin/apply", bytes.NewReader(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("first: %d %s", w.Code, w.Body.String())
	}
	// Provenance must be yaml.
	cfg, _ := storage.GetAppliedConfig(context.Background(), h.DB)
	var s apply.AppliedState
	_ = json.Unmarshal(cfg.Data, &s)
	if s.Lanes["alpha"].PausedBy != apply.PausedByYAML {
		t.Errorf("PausedBy=%q want yaml", s.Lanes["alpha"].PausedBy)
	}

	// Second apply: paused:false on a yaml-origin pause.
	second := apply.AppliedState{
		Lanes: map[string]apply.LaneCfg{"alpha": {Concurrency: 1, Paused: false}},
	}
	body2, _ := json.Marshal(second)
	req2 := httptest.NewRequest("POST", "/v1/admin/apply", bytes.NewReader(body2))
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, req2)
	if w2.Code != 200 {
		t.Fatalf("second: %d %s", w2.Code, w2.Body.String())
	}
	cfg2, _ := storage.GetAppliedConfig(context.Background(), h.DB)
	var s2 apply.AppliedState
	_ = json.Unmarshal(cfg2.Data, &s2)
	if s2.Lanes["alpha"].Paused {
		t.Error("YAML paused:false did not unpause yaml-origin lane (regression)")
	}
	for _, ls := range mgr.CurrentLanes() {
		if ls.Name == "alpha" && ls.Paused {
			t.Error("runtime alpha still paused after YAML unpause")
		}
	}
}

// TestAdminState verifies GET /v1/admin/state after apply.
func TestAdminState(t *testing.T) {
	h, _ := makeAdminHandler(t)
	mux := http.NewServeMux()
	h.Register(mux)

	// Apply first.
	desired := apply.AppliedState{
		MissionDir: "/data",
		Lanes:      map[string]apply.LaneCfg{"alpha": {Concurrency: 2}},
	}
	body, _ := json.Marshal(desired)
	req := httptest.NewRequest("POST", "/v1/admin/apply", bytes.NewReader(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("apply: %d", w.Code)
	}

	// Get state.
	req2 := httptest.NewRequest("GET", "/v1/admin/state", nil)
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, req2)

	if w2.Code != http.StatusOK {
		t.Errorf("state: got %d, want 200: %s", w2.Code, w2.Body.String())
	}
	var got map[string]any
	if err := json.NewDecoder(w2.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got["applied_at"] == nil {
		t.Error("applied_at should not be nil")
	}
	state, ok := got["state"].(map[string]any)
	if !ok {
		t.Fatalf("state field missing or wrong type: %T", got["state"])
	}
	if state["mission_dir"] != "/data" {
		t.Errorf("mission_dir: got %v, want /data", state["mission_dir"])
	}
}

// TestAdminStateNoConfig verifies GET /v1/admin/state before any apply returns no applied_at.
func TestAdminStateNoConfig(t *testing.T) {
	h, _ := makeAdminHandler(t)
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest("GET", "/v1/admin/state", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("state: got %d, want 200", w.Code)
	}
	var got map[string]any
	if err := json.NewDecoder(w.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got["applied_at"] != nil {
		t.Errorf("applied_at should be nil when no config; got %v", got["applied_at"])
	}
}
