package handlers_test

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"letts/internal/apply"
	"letts/internal/config"
	"letts/internal/ids"
	"letts/internal/lane"
	"letts/internal/server/handlers"
	"letts/internal/storage"
)

func setupRestartFixture(t *testing.T, status storage.Status) (*handlers.LifecycleHandler, string, *sql.DB, string) {
	t.Helper()
	db := setupDB(t)
	dataDir := t.TempDir()
	id := ids.NewUUIDv7()

	m := storage.Mission{
		ID:               id,
		Lane:             "normal",
		MissionName:      "RestartFixture",
		Status:           status,
		Input:            []byte(`{"k":"v"}`),
		InputFingerprint: "fp",
		TimeCreatedMs:    time.Now().UnixMilli(),
		TimeoutMs:        sql.NullInt64{Int64: 60000, Valid: true},
	}
	if err := storage.InsertMission(context.Background(), db, &m); err != nil {
		t.Fatalf("insert mission: %v", err)
	}
	rt := storage.MissionRuntime{
		MissionID:           id,
		MissionDir:          "/tmp/missions",
		CommandTemplate:     `["sh","{mission_path}"]`,
		MissionPathTemplate: "{mission}.sh",
		ValidateMissionFile: false,
	}
	if err := storage.InsertRuntime(context.Background(), db, &rt); err != nil {
		t.Fatalf("insert runtime: %v", err)
	}
	cfg := &config.DugdaleConfig{
		DataDir: dataDir,
		Limits:  config.LimitsConfig{MaxEventsBuffer: 1024, MaxEventLineSize: 1024},
	}
	h := &handlers.LifecycleHandler{
		DB:          db,
		Cfg:         cfg,
		DataDir:     dataDir,
		LaneManager: &lane.Manager{},
	}
	return h, id, db, dataDir
}

func doRestart(h *handlers.LifecycleHandler, id string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", "/v1/missions/"+id+"/restart", nil)
	r.SetPathValue("id", id)
	w := httptest.NewRecorder()
	h.Restart(w, r)
	return w
}

func TestRestartDoneCreatesNewMission(t *testing.T) {
	h, id, db, dataDir := setupRestartFixture(t, storage.StatusDone)
	w := doRestart(h, id)
	if w.Code != 201 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	got := parseJSON(t, w.Body.Bytes())
	newID, _ := got["mission_id"].(string)
	if newID == "" {
		t.Fatal("mission_id missing")
	}
	if !ids.ValidateUUIDv7(newID) {
		t.Errorf("mission_id not UUIDv7: %q", newID)
	}
	if got["restarted_from"] != id {
		t.Errorf("restarted_from=%v", got["restarted_from"])
	}
	if got["status"] != "queued" {
		t.Errorf("status=%v", got["status"])
	}

	nm, err := storage.GetMission(context.Background(), db, newID)
	if err != nil {
		t.Fatalf("get new mission: %v", err)
	}
	if nm.Status != storage.StatusQueued {
		t.Errorf("new mission status=%q", nm.Status)
	}
	if !nm.RestartedFrom.Valid || nm.RestartedFrom.String != id {
		t.Errorf("RestartedFrom=%+v", nm.RestartedFrom)
	}
	if string(nm.Input) != `{"k":"v"}` {
		t.Errorf("Input=%q", string(nm.Input))
	}
	if !nm.TimeoutMs.Valid || nm.TimeoutMs.Int64 != 60000 {
		t.Errorf("TimeoutMs=%+v", nm.TimeoutMs)
	}

	// Runtime row copied.
	nrt, err := storage.GetRuntime(context.Background(), db, newID)
	if err != nil {
		t.Fatalf("get runtime: %v", err)
	}
	if nrt.MissionDir != "/tmp/missions" {
		t.Errorf("MissionDir=%q", nrt.MissionDir)
	}

	// Events file with queued event exists.
	shard, _ := ids.ShardPath(newID)
	evPath := filepath.Join(dataDir, "output", shard, newID+"-events")
	if _, err := os.ReadFile(evPath); err != nil {
		t.Errorf("events file: %v", err)
	}
}

func TestRestartCopiesInputRefsButNotOutputRefs(t *testing.T) {
	h, id, db, _ := setupRestartFixture(t, storage.StatusDone)

	inID := ids.NewUUIDv7()
	outID := ids.NewUUIDv7()
	for _, sid := range []string{inID, outID} {
		if err := storage.InsertStaging(context.Background(), db, &storage.StagingFile{
			StagingID: sid, State: storage.StagingComplete, Sha256: "x", Size: 1, BytesReceived: 1,
			Path: "p", TimeCreatedMs: 1, TimeUpdatedMs: 1, TimeExpiresMs: 9999999999,
		}); err != nil {
			t.Fatalf("insert staging: %v", err)
		}
	}
	_ = storage.InsertRef(context.Background(), db, storage.StagingRef{
		MissionID: id, StagingID: inID, RefKind: storage.RefInput, Role: "cover",
	})
	_ = storage.InsertRef(context.Background(), db, storage.StagingRef{
		MissionID: id, StagingID: outID, RefKind: storage.RefOutput, Role: "result",
	})

	w := doRestart(h, id)
	if w.Code != 201 {
		t.Fatalf("status=%d", w.Code)
	}
	got := parseJSON(t, w.Body.Bytes())
	newID := got["mission_id"].(string)
	refs, _ := storage.RefsByMission(context.Background(), db, newID)
	if len(refs) != 1 {
		t.Fatalf("len(refs)=%d, want 1 (input only)", len(refs))
	}
	if refs[0].RefKind != storage.RefInput || refs[0].StagingID != inID {
		t.Errorf("ref=%+v", refs[0])
	}
}

// TestRestartRejectsRemovedLane: restart refuses with
// 400 unknown_lane when the source mission's lane was removed via
// `letts apply --prune` since the original dispatch. Without the check
// the new mission would queue into a vanished lane with no runner — a
// sticky orphan.
func TestRestartRejectsRemovedLane(t *testing.T) {
	h, id, db, _ := setupRestartFixture(t, storage.StatusDone)
	// Mission was dispatched into "normal" lane (setupRestartFixture).
	// applied state knows only "other-lane" — restart should refuse.
	h.GetApplied = func() (*apply.AppliedState, bool) {
		return &apply.AppliedState{
			Lanes: map[string]apply.LaneCfg{"other-lane": {Concurrency: 1}},
		}, true
	}

	w := doRestart(h, id)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s, want 400", w.Code, w.Body.String())
	}
	got := parseJSON(t, w.Body.Bytes())
	if got["error"] != "unknown_lane" {
		t.Errorf("error=%v, want unknown_lane", got["error"])
	}

	// And the source mission must not have a new sibling queued.
	rows, _ := db.Query(`SELECT COUNT(*) FROM missions WHERE restarted_from=?`, id)
	defer func() { _ = rows.Close() }()
	rows.Next()
	var n int
	_ = rows.Scan(&n)
	if n != 0 {
		t.Errorf("restart created %d sibling rows even though lane was vanished", n)
	}
}

// TestRestartRecalcsStagingTTLToInfinity: restart never
// called RecalcStagingTTL after InsertRef, so the input staging row's
// time_expires stayed pinned to the OLD (done) mission's
// time_finished + failed_ttl. The new mission was queued referencing a
// staging row about to expire — StagingGC tombstoned it under the live
// queued mission, which then failed input materialization.
//
// After fix: post-InsertRef the row's time_expires lifts to MaxInt64
// (queued/running ref → "infinity").
func TestRestartRecalcsStagingTTLToInfinity(t *testing.T) {
	h, id, db, _ := setupRestartFixture(t, storage.StatusDone)
	// Wire cleanup TTL so RecalcStagingTTL has a non-zero policy.
	h.Cfg.Cleanup.SuccessTTL = 24 * time.Hour
	h.Cfg.Cleanup.FailedTTL = 7 * 24 * time.Hour
	h.Cfg.Cleanup.StagingTTL = time.Hour
	h.Cfg.Cleanup.DownloadedGrace = time.Hour

	// Mark the source mission with a time_finished so the old TTL
	// computation matches production (and would expire before infinity).
	tFinished := time.Now().UnixMilli()
	if _, err := db.Exec(
		`UPDATE missions SET time_finished=?, outcome='failed' WHERE mission_id=?`,
		tFinished, id); err != nil {
		t.Fatalf("update finished: %v", err)
	}

	inID := ids.NewUUIDv7()
	preExpires := tFinished + (7 * 24 * time.Hour).Milliseconds()
	if err := storage.InsertStaging(context.Background(), db, &storage.StagingFile{
		StagingID: inID, State: storage.StagingComplete,
		Sha256: "x", Size: 1, BytesReceived: 1,
		Path:          "p",
		TimeCreatedMs: tFinished, TimeUpdatedMs: tFinished,
		TimeExpiresMs: preExpires,
	}); err != nil {
		t.Fatalf("insert staging: %v", err)
	}
	if err := storage.InsertRef(context.Background(), db, storage.StagingRef{
		MissionID: id, StagingID: inID, RefKind: storage.RefInput, Role: "cover",
	}); err != nil {
		t.Fatalf("insert ref: %v", err)
	}

	w := doRestart(h, id)
	if w.Code != 201 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}

	sf, err := storage.GetStaging(context.Background(), db, inID)
	if err != nil {
		t.Fatalf("get staging: %v", err)
	}
	// New mission is in StatusQueued → time_expires=MaxInt64.
	if sf.TimeExpiresMs <= preExpires {
		t.Errorf("staging.time_expires=%d still pinned to pre-restart value %d — RecalcStagingTTL missing",
			sf.TimeExpiresMs, preExpires)
	}
}

func TestRestartExpiredStagingReturns409(t *testing.T) {
	h, id, db, _ := setupRestartFixture(t, storage.StatusDone)

	deletedID := ids.NewUUIDv7()
	if err := storage.InsertStaging(context.Background(), db, &storage.StagingFile{
		StagingID: deletedID, State: storage.StagingDeleting, Sha256: "x", Size: 1, BytesReceived: 1,
		Path: "p", TimeCreatedMs: 1, TimeUpdatedMs: 1, TimeExpiresMs: 9999999999,
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	_ = storage.InsertRef(context.Background(), db, storage.StagingRef{
		MissionID: id, StagingID: deletedID, RefKind: storage.RefInput, Role: "cover",
	})

	w := doRestart(h, id)
	if w.Code != 409 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	got := parseJSON(t, w.Body.Bytes())
	if got["error"] != "input_artifacts_expired" {
		t.Errorf("code=%v", got["error"])
	}
}

func TestRestartQueuedReturns409(t *testing.T) {
	h, id, _, _ := setupRestartFixture(t, storage.StatusQueued)
	w := doRestart(h, id)
	if w.Code != 409 {
		t.Errorf("status=%d", w.Code)
	}
	got := parseJSON(t, w.Body.Bytes())
	if got["error"] != "mission_not_done" {
		t.Errorf("code=%v", got["error"])
	}
}

func TestRestartRunningReturns409(t *testing.T) {
	h, id, _, _ := setupRestartFixture(t, storage.StatusRunning)
	w := doRestart(h, id)
	if w.Code != 409 || parseJSON(t, w.Body.Bytes())["error"] != "mission_not_done" {
		t.Errorf("status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestRestartDeletingReturns409(t *testing.T) {
	h, id, _, _ := setupRestartFixture(t, storage.StatusDeleting)
	w := doRestart(h, id)
	if w.Code != 409 {
		t.Errorf("status=%d", w.Code)
	}
	got := parseJSON(t, w.Body.Bytes())
	if got["error"] != "mission_deleting" {
		t.Errorf("code=%v", got["error"])
	}
}

func TestRestartUnknownReturns404(t *testing.T) {
	h, _, _, _ := setupRestartFixture(t, storage.StatusDone)
	bogus := ids.NewUUIDv7()
	w := doRestart(h, bogus)
	if w.Code != 404 {
		t.Errorf("status=%d", w.Code)
	}
}

func TestRestartInvalidIDReturns400(t *testing.T) {
	h, _, _, _ := setupRestartFixture(t, storage.StatusDone)
	w := doRestart(h, "bad-uuid")
	if w.Code != 400 {
		t.Errorf("status=%d", w.Code)
	}
}
