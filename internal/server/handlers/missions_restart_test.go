package handlers_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"letts/internal/apply"
	"letts/internal/config"
	"letts/internal/ids"
	"letts/internal/lane"
	"letts/internal/server/handlers"
	"letts/internal/server/middleware"
	"letts/internal/storage"
)

func setupRestartFixture(t *testing.T, status storage.Status) (*handlers.LifecycleHandler, string, *sql.DB, string) {
	t.Helper()
	db := setupDB(t)
	dataDir := t.TempDir()
	id := ids.NewUUIDv7()

	m := storage.Mission{
		ID:               id,
		Kind:             storage.KindMission,
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
	// All restart routes are admin-only in production; tests need the
	// same identity in ctx for the kind-gate to allow the request.
	ctx := context.WithValue(r.Context(),
		middleware.IdentityCtxKey(),
		middleware.Identity{Scope: middleware.ScopeAdmin})
	r = r.WithContext(ctx)
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

// TestRestartKindGateRefusesDispatchScopeOnExecMission asserts that a
// dispatch-token caller hitting /v1/missions/{id}/restart for an exec
// mission gets 403 forbidden_kind. Bulk handlers don't gate
// kind, and the shared restartOne/deleteOne helpers must enforce it
// regardless of how the route is mounted today (admin-only) so future
// per-scope route wiring stays safe by construction.
func TestRestartKindGateRefusesDispatchScopeOnExecMission(t *testing.T) {
	h, _, db, _ := setupRestartFixture(t, storage.StatusDone)
	execID := ids.NewUUIDv7()
	insertDoneExec(t, db, execID, "normal", "{}")

	r := httptest.NewRequest("POST", "/v1/missions/"+execID+"/restart", nil)
	r.SetPathValue("id", execID)
	ctx := context.WithValue(r.Context(),
		middleware.IdentityCtxKey(),
		middleware.Identity{Scope: middleware.ScopeDispatch})
	r = r.WithContext(ctx)
	w := httptest.NewRecorder()
	h.Restart(w, r)
	if w.Code != 403 {
		t.Fatalf("status=%d body=%s, want 403", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "forbidden_kind") {
		t.Errorf("body=%q, want forbidden_kind", w.Body.String())
	}
}

// TestRestartKindGateAdminScopeOK asserts an admin-scope caller restarts
// an exec mission with no 403. Admin is the superset and bypasses kind.
func TestRestartKindGateAdminScopeOK(t *testing.T) {
	_, _, db, dataDir := setupRestartFixture(t, storage.StatusDone)
	h := newExecLifecycleHandler(t, db, dataDir)
	execID := ids.NewUUIDv7()
	insertDoneExec(t, db, execID, "normal", "{}")

	r := httptest.NewRequest("POST", "/v1/missions/"+execID+"/restart", nil)
	r.SetPathValue("id", execID)
	ctx := context.WithValue(r.Context(),
		middleware.IdentityCtxKey(),
		middleware.Identity{Scope: middleware.ScopeAdmin})
	r = r.WithContext(ctx)
	w := httptest.NewRecorder()
	h.Restart(w, r)
	if w.Code == 403 {
		t.Errorf("admin got 403 forbidden_kind; body=%s", w.Body.String())
	}
}

// TestBulkRestartKindGatePerIDForbiddenForDispatchScope asserts a bulk
// restart from a dispatch-scope caller returns per-id forbidden_kind for
// exec missions while still 200 OK on mission-kind ids (so partial
// success works the same way as other per-id errors).
func TestBulkRestartKindGatePerIDForbiddenForDispatchScope(t *testing.T) {
	h, missionID, db, _ := setupRestartFixture(t, storage.StatusDone)
	execID := ids.NewUUIDv7()
	insertDoneExec(t, db, execID, "normal", "{}")

	body := `{"ids":["` + missionID + `","` + execID + `"]}`
	r := httptest.NewRequest("POST", "/v1/missions/bulk-restart", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	ctx := context.WithValue(r.Context(),
		middleware.IdentityCtxKey(),
		middleware.Identity{Scope: middleware.ScopeDispatch})
	r = r.WithContext(ctx)
	w := httptest.NewRecorder()
	h.BulkRestart(w, r)
	if w.Code != 200 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	body2 := w.Body.String()
	if !strings.Contains(body2, "forbidden_kind") {
		t.Errorf("body missing forbidden_kind: %s", body2)
	}
	if !strings.Contains(body2, execID) {
		t.Errorf("body missing execID %s: %s", execID, body2)
	}
}

// --- Exec restart tests ---

// insertExecInStatus inserts an exec mission with the given status and input.
// Returns no value (id is passed in by the caller).
func insertExecInStatus(t *testing.T, db *sql.DB, id, laneName, inputJSON string, status storage.Status) {
	t.Helper()
	now := time.Now().UnixMilli()
	m := storage.Mission{
		ID:               id,
		Kind:             storage.KindExec,
		Lane:             laneName,
		MissionName:      "exec",
		Status:           status,
		Input:            []byte(inputJSON),
		InputFingerprint: "fp-" + id,
		TimeCreatedMs:    now,
	}
	if status == storage.StatusDone {
		m.Outcome = sql.NullString{String: string(storage.OutcomeSuccess), Valid: true}
		m.TimeStartedMs = sql.NullInt64{Int64: now, Valid: true}
		m.TimeFinishedMs = sql.NullInt64{Int64: now, Valid: true}
	}
	if err := storage.InsertMission(context.Background(), db, &m); err != nil {
		t.Fatalf("insert exec mission: %v", err)
	}
}

// insertDoneExec inserts an exec mission in status=done/outcome=success state.
func insertDoneExec(t *testing.T, db *sql.DB, id, laneName, inputJSON string) {
	t.Helper()
	insertExecInStatus(t, db, id, laneName, inputJSON, storage.StatusDone)
}

// insertDoneExecWithGroup inserts an exec mission in done state with the
// given group_id and display_name set.
func insertDoneExecWithGroup(t *testing.T, db *sql.DB, id, laneName, inputJSON, groupID, displayName string) {
	t.Helper()
	now := time.Now().UnixMilli()
	m := storage.Mission{
		ID:               id,
		Kind:             storage.KindExec,
		Lane:             laneName,
		MissionName:      "exec",
		DisplayName:      sql.NullString{String: displayName, Valid: displayName != ""},
		GroupID:          sql.NullString{String: groupID, Valid: groupID != ""},
		Status:           storage.StatusDone,
		Outcome:          sql.NullString{String: string(storage.OutcomeSuccess), Valid: true},
		Input:            []byte(inputJSON),
		InputFingerprint: "fp-" + id,
		TimeCreatedMs:    now,
		TimeStartedMs:    sql.NullInt64{Int64: now, Valid: true},
		TimeFinishedMs:   sql.NullInt64{Int64: now, Valid: true},
	}
	if err := storage.InsertMission(context.Background(), db, &m); err != nil {
		t.Fatalf("insert exec mission with group: %v", err)
	}
}

// insertCompleteStagingFile inserts a staging file in complete state and returns its id.
func insertCompleteStagingFile(t *testing.T, db *sql.DB, content string) string {
	t.Helper()
	sid := ids.NewUUIDv7()
	now := time.Now().UnixMilli()
	st := &storage.StagingFile{
		StagingID:     sid,
		State:         storage.StagingComplete,
		Sha256:        "sha-" + sid,
		Size:          int64(len(content)),
		BytesReceived: int64(len(content)),
		Path:          "/tmp/staging/" + sid,
		TimeCreatedMs: now,
		TimeUpdatedMs: now,
		TimeExpiresMs: now + 86400000,
	}
	if err := storage.InsertStaging(context.Background(), db, st); err != nil {
		t.Fatalf("insert staging: %v", err)
	}
	return sid
}

// insertExecRef is a tiny wrapper for inserting a mission_staging_refs row.
func insertExecRef(t *testing.T, db *sql.DB, missionID, stagingID string, refKind storage.RefKind, role string) {
	t.Helper()
	if err := storage.InsertRef(context.Background(), db, storage.StagingRef{
		MissionID: missionID, StagingID: stagingID, RefKind: refKind, Role: role,
	}); err != nil {
		t.Fatalf("insert ref: %v", err)
	}
}

// newExecLifecycleHandler builds a LifecycleHandler against the given DB and
// dataDir with an empty (no-runners) LaneManager.
func newExecLifecycleHandler(t *testing.T, db *sql.DB, dataDir string) *handlers.LifecycleHandler {
	t.Helper()
	cfg := &config.DugdaleConfig{
		DataDir: dataDir,
		Limits:  config.LimitsConfig{MaxEventsBuffer: 1024, MaxEventLineSize: 1024},
	}
	return &handlers.LifecycleHandler{
		DB:          db,
		Cfg:         cfg,
		DataDir:     dataDir,
		LaneManager: &lane.Manager{},
	}
}

func TestRestartExecBasic(t *testing.T) {
	db := setupDB(t)
	dataDir := t.TempDir()

	oldID := "0192aaaa-0000-7000-8000-000000000001"
	insertDoneExec(t, db, oldID, "light", `{"lane":"light","command":["uptime"]}`)

	h := newExecLifecycleHandler(t, db, dataDir)
	w := doRestart(h, oldID)

	if w.Code != 201 {
		t.Fatalf("status=%d, want 201; body=%s", w.Code, w.Body.String())
	}
	got := parseJSON(t, w.Body.Bytes())
	newID, _ := got["mission_id"].(string)
	if newID == "" {
		t.Fatal("mission_id missing")
	}
	if got["restarted_from"] != oldID {
		t.Errorf("restarted_from=%v, want %s", got["restarted_from"], oldID)
	}
	if got["status"] != "queued" {
		t.Errorf("status=%v, want queued", got["status"])
	}

	nm, err := storage.GetMission(context.Background(), db, newID)
	if err != nil {
		t.Fatalf("get new mission: %v", err)
	}
	if nm.Kind != storage.KindExec {
		t.Errorf("new mission kind=%q, want exec", nm.Kind)
	}
	if !nm.RestartedFrom.Valid || nm.RestartedFrom.String != oldID {
		t.Errorf("RestartedFrom=%+v, want %s", nm.RestartedFrom, oldID)
	}
	if nm.Status != storage.StatusQueued {
		t.Errorf("new mission status=%q, want queued", nm.Status)
	}
}

func TestRestartExecWithStaging(t *testing.T) {
	db := setupDB(t)
	dataDir := t.TempDir()

	scriptSID := insertCompleteStagingFile(t, db, "script-content")
	inSID := insertCompleteStagingFile(t, db, "input-content")

	oldID := "0192aaaa-0000-7000-8000-000000000002"
	insertDoneExec(t, db, oldID, "light", fmt.Sprintf(
		`{"lane":"light","command":["bash","$LETTS_SCRIPT"],"script":{"staging_id":"%s"},"in":[{"key":"pdf","staging_id":"%s"}]}`,
		scriptSID, inSID))
	insertExecRef(t, db, oldID, scriptSID, storage.RefScript, "script")
	insertExecRef(t, db, oldID, inSID, storage.RefInput, "in:pdf")

	h := newExecLifecycleHandler(t, db, dataDir)
	w := doRestart(h, oldID)
	if w.Code != 201 {
		t.Fatalf("status=%d, want 201; body=%s", w.Code, w.Body.String())
	}
	got := parseJSON(t, w.Body.Bytes())
	newID := got["mission_id"].(string)

	newRefs, err := storage.RefsByMission(context.Background(), db, newID)
	if err != nil {
		t.Fatalf("refs: %v", err)
	}
	if len(newRefs) != 2 {
		t.Fatalf("new mission has %d refs, want 2", len(newRefs))
	}
	gotSIDs := []string{newRefs[0].StagingID, newRefs[1].StagingID}
	wantSIDs := []string{scriptSID, inSID}
	sort.Strings(gotSIDs)
	sort.Strings(wantSIDs)
	if !reflect.DeepEqual(gotSIDs, wantSIDs) {
		t.Errorf("ref staging_ids=%v, want %v (shared with old mission)", gotSIDs, wantSIDs)
	}
}

func TestRestartExecStagingGCd(t *testing.T) {
	db := setupDB(t)
	dataDir := t.TempDir()

	scriptSID := insertCompleteStagingFile(t, db, "script-content")
	oldID := "0192aaaa-0000-7000-8000-000000000003"
	insertDoneExec(t, db, oldID, "light", fmt.Sprintf(
		`{"lane":"light","command":["bash","$LETTS_SCRIPT"],"script":{"staging_id":"%s"}}`, scriptSID))
	insertExecRef(t, db, oldID, scriptSID, storage.RefScript, "script")

	// Now mark staging as deleting (simulates GC).
	if err := storage.MarkStagingDeleting(context.Background(), db, scriptSID); err != nil {
		t.Fatalf("mark deleting: %v", err)
	}

	h := newExecLifecycleHandler(t, db, dataDir)
	w := doRestart(h, oldID)
	if w.Code != 409 {
		t.Fatalf("status=%d, want 409; body=%s", w.Code, w.Body.String())
	}
	got := parseJSON(t, w.Body.Bytes())
	if got["error"] != "input_artifacts_expired" {
		t.Errorf("error=%v, want input_artifacts_expired", got["error"])
	}
	details, _ := got["details"].(map[string]any)
	if details == nil {
		t.Fatalf("details missing in body: %s", w.Body.String())
	}
	if details["staging_id"] != scriptSID {
		t.Errorf("details.staging_id=%v, want %s", details["staging_id"], scriptSID)
	}
}

func TestRestartExecNoRuntimeRow(t *testing.T) {
	db := setupDB(t)
	dataDir := t.TempDir()

	oldID := "0192aaaa-0000-7000-8000-000000000004"
	insertDoneExec(t, db, oldID, "light", `{"lane":"light","command":["true"]}`)

	// Pre-check: no runtime row for the original exec.
	if _, err := storage.GetRuntime(context.Background(), db, oldID); err == nil {
		t.Fatal("original exec had a mission_runtime row; exec dispatch must not insert runtime")
	}

	h := newExecLifecycleHandler(t, db, dataDir)
	w := doRestart(h, oldID)
	if w.Code != 201 {
		t.Fatalf("status=%d, want 201; body=%s", w.Code, w.Body.String())
	}
	newID := parseJSON(t, w.Body.Bytes())["mission_id"].(string)

	// Post-check: no runtime row for the new exec either.
	if _, err := storage.GetRuntime(context.Background(), db, newID); err == nil {
		t.Errorf("new exec has a mission_runtime row; restart pipeline copied where it should have skipped")
	}
}

func TestRestartExecPreservesGroupID(t *testing.T) {
	db := setupDB(t)
	dataDir := t.TempDir()

	oldID := "0192aaaa-0000-7000-8000-000000000005"
	gid := "0192bbbb-0000-7000-8000-000000000000"
	insertDoneExecWithGroup(t, db, oldID, "light", `{"lane":"light","command":["true"]}`, gid, "uptime [+2 hosts]")

	h := newExecLifecycleHandler(t, db, dataDir)
	w := doRestart(h, oldID)
	if w.Code != 201 {
		t.Fatalf("status=%d, want 201; body=%s", w.Code, w.Body.String())
	}
	newID := parseJSON(t, w.Body.Bytes())["mission_id"].(string)
	got, err := storage.GetMission(context.Background(), db, newID)
	if err != nil {
		t.Fatalf("get new mission: %v", err)
	}
	if got.GroupID.String != gid {
		t.Errorf("group_id=%q, want %q", got.GroupID.String, gid)
	}
	if got.DisplayName.String != "uptime [+2 hosts]" {
		t.Errorf("display_name=%q, want %q", got.DisplayName.String, "uptime [+2 hosts]")
	}
}

func TestRestartExecNotDoneRejected(t *testing.T) {
	db := setupDB(t)
	dataDir := t.TempDir()

	oldID := "0192aaaa-0000-7000-8000-000000000006"
	insertExecInStatus(t, db, oldID, "light", `{"lane":"light","command":["true"]}`, storage.StatusRunning)

	h := newExecLifecycleHandler(t, db, dataDir)
	w := doRestart(h, oldID)
	if w.Code != 409 {
		t.Fatalf("status=%d, want 409; body=%s", w.Code, w.Body.String())
	}
	got := parseJSON(t, w.Body.Bytes())
	if got["error"] != "mission_not_done" {
		t.Errorf("error=%v, want mission_not_done", got["error"])
	}
}

func TestRestartExecCreatesEventsFile(t *testing.T) {
	db := setupDB(t)
	dataDir := t.TempDir()

	oldID := "0192aaaa-0000-7000-8000-000000000007"
	insertDoneExec(t, db, oldID, "light", `{"lane":"light","command":["true"]}`)

	h := newExecLifecycleHandler(t, db, dataDir)
	w := doRestart(h, oldID)
	if w.Code != 201 {
		t.Fatalf("status=%d, want 201; body=%s", w.Code, w.Body.String())
	}
	newID := parseJSON(t, w.Body.Bytes())["mission_id"].(string)
	shard, err := ids.ShardPath(newID)
	if err != nil {
		t.Fatalf("shard: %v", err)
	}
	eventsPath := filepath.Join(dataDir, "output", shard, newID+"-events")
	data, err := os.ReadFile(eventsPath)
	if err != nil {
		t.Fatalf("events file missing: %v (path %s)", err, eventsPath)
	}
	if !strings.Contains(string(data), `"event":"queued"`) {
		t.Errorf("events file does not contain queued event: %s", data)
	}
	if !strings.Contains(string(data), `"restarted_from":"`+oldID+`"`) {
		t.Errorf("events file does not reference original: %s", data)
	}
}

// TestRestartExecNotifiesLane verifies that the lane runner is woken up after
// the restart completes. We use a real lane.Manager with a counting spawner
// to confirm the queued exec mission is picked up promptly (which only
// happens if Notify wakes the runner from its tick wait).
func TestRestartExecNotifiesLane(t *testing.T) {
	db := setupDB(t)
	dataDir := t.TempDir()

	oldID := "0192aaaa-0000-7000-8000-000000000008"
	insertDoneExec(t, db, oldID, "light", `{"lane":"light","command":["true"]}`)

	picked := make(chan string, 4)
	mgr := &lane.Manager{
		DB: db,
		Spawner: func(_ context.Context, m *storage.Mission, release func()) error {
			picked <- m.ID
			release()
			return nil
		},
		Logger: newTestLogger(),
		Ctx:    context.Background(),
	}
	t.Cleanup(func() { mgr.StopAll() })
	mgr.Apply([]lane.LaneSpec{{Name: "light", Concurrency: 1}})

	cfg := &config.DugdaleConfig{
		DataDir: dataDir,
		Limits:  config.LimitsConfig{MaxEventsBuffer: 1024, MaxEventLineSize: 1024},
	}
	h := &handlers.LifecycleHandler{
		DB:          db,
		Cfg:         cfg,
		DataDir:     dataDir,
		LaneManager: mgr,
	}
	w := doRestart(h, oldID)
	if w.Code != 201 {
		t.Fatalf("status=%d, want 201; body=%s", w.Code, w.Body.String())
	}
	newID := parseJSON(t, w.Body.Bytes())["mission_id"].(string)

	// The runner should pick up the new queued exec promptly. If Notify is
	// missed, the runner falls back to its slow tick (≥ a few hundred ms);
	// a generous 3s timeout still differentiates wake-on-notify from the
	// background pickup loop without flaking.
	select {
	case got := <-picked:
		if got != newID {
			t.Errorf("picked=%q, want %q", got, newID)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("lane runner did not pick up the new exec; Notify not delivered or runner not registered")
	}
}
