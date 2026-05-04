package handlers_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"letts/internal/ids"
	"letts/internal/server/handlers"
	"letts/internal/server/middleware"
	"letts/internal/storage"
)

// withScope returns r with the given Scope injected via Identity in its
// context — equivalent to what middleware.Auth() would do on success.
// Without it, handlers calling RequireKindForScope would 500 on
// "missing identity".
func withScope(r *http.Request, scope middleware.Scope) *http.Request {
	ctx := context.WithValue(r.Context(), middleware.IdentityCtxKey(),
		middleware.Identity{Scope: scope})
	return r.WithContext(ctx)
}

func insertTestMission(t *testing.T, db *sql.DB, status storage.Status, lane string, createdMs int64) string {
	t.Helper()
	id := ids.NewUUIDv7()
	m := storage.Mission{
		ID:               id,
		Kind:             storage.KindMission,
		Lane:             lane,
		MissionName:      "FixtureMission",
		Status:           status,
		Input:            []byte(`{"k":"v"}`),
		InputFingerprint: "fp",
		TimeCreatedMs:    createdMs,
	}
	if err := storage.InsertMission(context.Background(), db, &m); err != nil {
		t.Fatalf("insert: %v", err)
	}
	return id
}

func setupMissionsGet(t *testing.T) (*handlers.MissionsHandler, *sql.DB) {
	t.Helper()
	db := setupDB(t)
	return &handlers.MissionsHandler{DB: db}, db
}

func parseJSON(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return m
}

func TestMissionsGetByIDReturnsResource(t *testing.T) {
	h, db := setupMissionsGet(t)
	id := insertTestMission(t, db, storage.StatusQueued, "normal", time.Now().UnixMilli())

	r := withScope(httptest.NewRequest("GET", "/v1/missions/"+id, nil), middleware.ScopeDispatch)
	r.SetPathValue("id", id)
	w := httptest.NewRecorder()
	h.GetByID(w, r)

	if w.Code != 200 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	got := parseJSON(t, w.Body.Bytes())
	if got["mission_id"] != id {
		t.Errorf("mission_id=%v", got["mission_id"])
	}
	if got["status"] != "queued" {
		t.Errorf("status=%v", got["status"])
	}
	if got["lane"] != "normal" {
		t.Errorf("lane=%v", got["lane"])
	}
	if _, ok := got["inputs"]; !ok {
		t.Errorf("inputs missing")
	}
}

func TestMissionsGetByIDNotFound(t *testing.T) {
	h, _ := setupMissionsGet(t)
	bogus := ids.NewUUIDv7()
	r := withScope(httptest.NewRequest("GET", "/v1/missions/"+bogus, nil), middleware.ScopeDispatch)
	r.SetPathValue("id", bogus)
	w := httptest.NewRecorder()
	h.GetByID(w, r)
	if w.Code != 404 {
		t.Errorf("status=%d", w.Code)
	}
}

func TestMissionsGetByIDInvalidUUID(t *testing.T) {
	h, _ := setupMissionsGet(t)
	r := withScope(httptest.NewRequest("GET", "/v1/missions/notuuid", nil), middleware.ScopeDispatch)
	r.SetPathValue("id", "notuuid")
	w := httptest.NewRecorder()
	h.GetByID(w, r)
	if w.Code != 400 {
		t.Errorf("status=%d", w.Code)
	}
}

// TestMissionsGetByIDDeletingReturns404 verifies that a mission whose row is
// in status='deleting' surfaces as 404 not_found.
// (The events handler returns 410 'gone' for the same case; missions_get
// returns 404 to keep the mission opaque to callers once delete is initiated.)
func TestMissionsGetByIDDeletingReturns404(t *testing.T) {
	h, db := setupMissionsGet(t)
	id := insertTestMission(t, db, storage.StatusDeleting, "normal", time.Now().UnixMilli())

	r := withScope(httptest.NewRequest("GET", "/v1/missions/"+id, nil), middleware.ScopeDispatch)
	r.SetPathValue("id", id)
	w := httptest.NewRecorder()
	h.GetByID(w, r)

	if w.Code != 404 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	got := parseJSON(t, w.Body.Bytes())
	if got["error"] != "not_found" {
		t.Errorf("error=%v, want not_found", got["error"])
	}
}

func TestMissionsGetByIDIncludesOutputsFromRefs(t *testing.T) {
	h, db := setupMissionsGet(t)
	id := insertTestMission(t, db, storage.StatusDone, "normal", time.Now().UnixMilli())

	stagingID := ids.NewUUIDv7()
	if err := storage.InsertStaging(context.Background(), db, &storage.StagingFile{
		StagingID: stagingID, State: storage.StagingComplete,
		Sha256: "abc123", Size: 42, BytesReceived: 42,
		Path: "out/x", TimeCreatedMs: 1, TimeUpdatedMs: 1, TimeExpiresMs: 9999999999,
	}); err != nil {
		t.Fatalf("insert staging: %v", err)
	}
	if err := storage.InsertRef(context.Background(), db, storage.StagingRef{
		MissionID: id, StagingID: stagingID, RefKind: storage.RefOutput, Role: "result",
	}); err != nil {
		t.Fatalf("insert ref: %v", err)
	}

	r := withScope(httptest.NewRequest("GET", "/v1/missions/"+id, nil), middleware.ScopeDispatch)
	r.SetPathValue("id", id)
	w := httptest.NewRecorder()
	h.GetByID(w, r)
	if w.Code != 200 {
		t.Fatalf("status=%d", w.Code)
	}
	got := parseJSON(t, w.Body.Bytes())
	outputs, ok := got["outputs"].(map[string]any)
	if !ok {
		t.Fatalf("outputs missing or wrong type: %T", got["outputs"])
	}
	res, ok := outputs["result"].(map[string]any)
	if !ok {
		t.Fatalf("outputs.result missing")
	}
	if res["staging_id"] != stagingID {
		t.Errorf("staging_id=%v", res["staging_id"])
	}
	if res["sha256"] != "abc123" {
		t.Errorf("sha256=%v", res["sha256"])
	}
}

func TestMissionsListEmpty(t *testing.T) {
	h, _ := setupMissionsGet(t)
	r := httptest.NewRequest("GET", "/v1/missions", nil)
	w := httptest.NewRecorder()
	h.List(w, r)
	if w.Code != 200 {
		t.Fatalf("status=%d", w.Code)
	}
	got := parseJSON(t, w.Body.Bytes())
	missions, _ := got["missions"].([]any)
	if len(missions) != 0 {
		t.Errorf("missions=%v", missions)
	}
	if _, has := got["next_cursor"]; has {
		t.Errorf("next_cursor unexpectedly present")
	}
}

func TestMissionsListReturnsRowsDescending(t *testing.T) {
	h, db := setupMissionsGet(t)
	now := time.Now().UnixMilli()
	id1 := insertTestMission(t, db, storage.StatusDone, "normal", now)
	id2 := insertTestMission(t, db, storage.StatusDone, "normal", now+1)
	id3 := insertTestMission(t, db, storage.StatusDone, "normal", now+2)

	r := httptest.NewRequest("GET", "/v1/missions", nil)
	w := httptest.NewRecorder()
	h.List(w, r)
	got := parseJSON(t, w.Body.Bytes())
	missions, _ := got["missions"].([]any)
	if len(missions) != 3 {
		t.Fatalf("len=%d", len(missions))
	}
	first := missions[0].(map[string]any)
	if first["mission_id"] != id3 {
		t.Errorf("first=%v, want %v", first["mission_id"], id3)
	}
	last := missions[2].(map[string]any)
	if last["mission_id"] != id1 {
		t.Errorf("last=%v, want %v", last["mission_id"], id1)
	}
	_ = id2
}

func TestMissionsListCursorPagination(t *testing.T) {
	h, db := setupMissionsGet(t)
	now := time.Now().UnixMilli()
	id1 := insertTestMission(t, db, storage.StatusDone, "normal", now)
	id2 := insertTestMission(t, db, storage.StatusDone, "normal", now+1)
	id3 := insertTestMission(t, db, storage.StatusDone, "normal", now+2)

	r := httptest.NewRequest("GET", "/v1/missions?limit=2", nil)
	w := httptest.NewRecorder()
	h.List(w, r)
	got := parseJSON(t, w.Body.Bytes())
	missions, _ := got["missions"].([]any)
	if len(missions) != 2 {
		t.Fatalf("page1 len=%d", len(missions))
	}
	if missions[0].(map[string]any)["mission_id"] != id3 {
		t.Errorf("page1[0]=%v", missions[0])
	}
	if missions[1].(map[string]any)["mission_id"] != id2 {
		t.Errorf("page1[1]=%v", missions[1])
	}
	cursor, ok := got["next_cursor"].(string)
	if !ok || cursor == "" {
		t.Fatal("next_cursor missing")
	}

	// Page 2.
	r2 := httptest.NewRequest("GET", "/v1/missions?limit=2&cursor="+cursor, nil)
	w2 := httptest.NewRecorder()
	h.List(w2, r2)
	got2 := parseJSON(t, w2.Body.Bytes())
	missions2, _ := got2["missions"].([]any)
	if len(missions2) != 1 {
		t.Fatalf("page2 len=%d", len(missions2))
	}
	if missions2[0].(map[string]any)["mission_id"] != id1 {
		t.Errorf("page2[0]=%v, want %v", missions2[0], id1)
	}
	if _, has := got2["next_cursor"]; has {
		t.Errorf("next_cursor present on last page")
	}
}

func TestMissionsListFilterByStatus(t *testing.T) {
	h, db := setupMissionsGet(t)
	now := time.Now().UnixMilli()
	insertTestMission(t, db, storage.StatusQueued, "normal", now)
	doneID := insertTestMission(t, db, storage.StatusDone, "normal", now+1)

	r := httptest.NewRequest("GET", "/v1/missions?status=done", nil)
	w := httptest.NewRecorder()
	h.List(w, r)
	got := parseJSON(t, w.Body.Bytes())
	missions, _ := got["missions"].([]any)
	if len(missions) != 1 {
		t.Fatalf("len=%d", len(missions))
	}
	if missions[0].(map[string]any)["mission_id"] != doneID {
		t.Errorf("got=%v", missions[0])
	}
}

func TestMissionsListFilterByGroupID(t *testing.T) {
	h, db := setupMissionsGet(t)

	groupA := "0192aaaa-0000-7000-8000-000000000000"
	groupB := "0192bbbb-0000-7000-8000-000000000000"

	for i, gid := range []string{groupA, groupA, groupA, groupB, groupB} {
		m := &storage.Mission{
			ID:            fmt.Sprintf("0192dddd-0000-7000-8000-00000000000%d", i),
			Kind:          storage.KindExec,
			MissionName:   "exec",
			Lane:          "light",
			Status:        storage.StatusQueued,
			Input:         []byte(`{}`),
			TimeCreatedMs: int64(1700000000000 + i),
			GroupID:       sql.NullString{String: gid, Valid: true},
		}
		if err := storage.InsertMission(context.Background(), db, m); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	r := httptest.NewRequest("GET", "/v1/missions?group_id="+groupA, nil)
	w := httptest.NewRecorder()
	h.List(w, r)

	if w.Code != 200 {
		t.Fatalf("status=%d, want 200; body=%s", w.Code, w.Body.String())
	}
	var resp struct {
		Missions []map[string]any `json:"missions"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v; body=%s", err, w.Body.String())
	}
	if len(resp.Missions) != 3 {
		t.Fatalf("got %d missions, want 3", len(resp.Missions))
	}
}

func TestMissionsListBadCursorReturns400(t *testing.T) {
	h, _ := setupMissionsGet(t)
	r := httptest.NewRequest("GET", "/v1/missions?cursor=NOT-BASE64!!!", nil)
	w := httptest.NewRecorder()
	h.List(w, r)
	if w.Code != 400 {
		t.Errorf("status=%d", w.Code)
	}
}

func TestMissionsListBadLimitReturns400(t *testing.T) {
	h, _ := setupMissionsGet(t)
	r := httptest.NewRequest("GET", "/v1/missions?limit=-1", nil)
	w := httptest.NewRecorder()
	h.List(w, r)
	if w.Code != 400 {
		t.Errorf("status=%d", w.Code)
	}
}

func insertDoneMission(t *testing.T, db *sql.DB, lane string, createdMs, finishedMs int64) string {
	t.Helper()
	id := ids.NewUUIDv7()
	m := storage.Mission{
		ID: id, Kind: storage.KindMission, Lane: lane, MissionName: "Fixture",
		Status: storage.StatusDone, Outcome: sql.NullString{String: "failed", Valid: true},
		Input: []byte(`{}`), InputFingerprint: "fp",
		TimeCreatedMs:  createdMs,
		TimeStartedMs:  sql.NullInt64{Int64: createdMs, Valid: true},
		TimeFinishedMs: sql.NullInt64{Int64: finishedMs, Valid: true},
	}
	if err := storage.InsertMission(context.Background(), db, &m); err != nil {
		t.Fatalf("insert: %v", err)
	}
	return id
}

func TestMissionsListOrderFinished(t *testing.T) {
	h, db := setupMissionsGet(t)
	idLate := insertDoneMission(t, db, "normal", 1000, 5000)
	idMid := insertDoneMission(t, db, "normal", 2000, 3000)
	idEarly := insertDoneMission(t, db, "normal", 3000, 4000)
	_ = idEarly

	r := httptest.NewRequest("GET", "/v1/missions?order=finished", nil)
	w := httptest.NewRecorder()
	h.List(w, r)
	if w.Code != 200 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	got := parseJSON(t, w.Body.Bytes())
	missions, _ := got["missions"].([]any)
	if len(missions) != 3 {
		t.Fatalf("len=%d", len(missions))
	}
	if missions[0].(map[string]any)["mission_id"] != idLate {
		t.Errorf("first=%v want %v (finished 5000)", missions[0], idLate)
	}
	if missions[2].(map[string]any)["mission_id"] != idMid {
		t.Errorf("last=%v want %v (finished 3000)", missions[2], idMid)
	}
}

func TestMissionsListOrderInvalid(t *testing.T) {
	h, db := setupMissionsGet(t)
	_ = insertDoneMission(t, db, "normal", 1000, 2000)
	r := httptest.NewRequest("GET", "/v1/missions?order=bogus", nil)
	w := httptest.NewRecorder()
	h.List(w, r)
	if w.Code != 400 {
		t.Fatalf("status=%d, want 400", w.Code)
	}
}

// insertExecMissionForTest inserts a mission row with kind='exec', used by the
// kind-gating tests across missions_get / events / output.
func insertExecMissionForTest(t *testing.T, db *sql.DB, status storage.Status) string {
	t.Helper()
	id := ids.NewUUIDv7()
	m := storage.Mission{
		ID:               id,
		Kind:             storage.KindExec,
		Lane:             "exec",
		MissionName:      "ExecFixture",
		Status:           status,
		Input:            []byte(`{}`),
		InputFingerprint: "fp",
		TimeCreatedMs:    time.Now().UnixMilli(),
	}
	if err := storage.InsertMission(context.Background(), db, &m); err != nil {
		t.Fatalf("insert exec mission: %v", err)
	}
	return id
}

// TestMissionsGetByIDKindGated covers kind gating: dispatch tokens cannot read
// kind='exec' rows, exec tokens cannot read kind='mission' rows, admin tokens
// see everything. Mismatch returns 403 forbidden_kind.
func TestMissionsGetByIDKindGated(t *testing.T) {
	h, db := setupMissionsGet(t)
	mid := insertTestMission(t, db, storage.StatusDone, "normal", time.Now().UnixMilli())
	xid := insertExecMissionForTest(t, db, storage.StatusDone)

	call := func(id string, scope middleware.Scope) *httptest.ResponseRecorder {
		r := withScope(httptest.NewRequest("GET", "/v1/missions/"+id, nil), scope)
		r.SetPathValue("id", id)
		w := httptest.NewRecorder()
		h.GetByID(w, r)
		return w
	}

	// dispatch on exec → 403
	w := call(xid, middleware.ScopeDispatch)
	if w.Code != 403 {
		t.Errorf("dispatch on exec: code=%d, want 403; body=%s", w.Code, w.Body.String())
	}
	if got := parseJSON(t, w.Body.Bytes())["error"]; got != "forbidden_kind" {
		t.Errorf("error=%v, want forbidden_kind", got)
	}

	// exec on mission → 403
	w = call(mid, middleware.ScopeExec)
	if w.Code != 403 {
		t.Errorf("exec on mission: code=%d, want 403", w.Code)
	}
	if got := parseJSON(t, w.Body.Bytes())["error"]; got != "forbidden_kind" {
		t.Errorf("error=%v, want forbidden_kind", got)
	}

	// admin on either → 200
	if w := call(mid, middleware.ScopeAdmin); w.Code != 200 {
		t.Errorf("admin on mission: code=%d, want 200", w.Code)
	}
	if w := call(xid, middleware.ScopeAdmin); w.Code != 200 {
		t.Errorf("admin on exec: code=%d, want 200", w.Code)
	}

	// dispatch on mission → 200 (sanity)
	if w := call(mid, middleware.ScopeDispatch); w.Code != 200 {
		t.Errorf("dispatch on mission: code=%d, want 200", w.Code)
	}

	// exec on exec → 200 (sanity)
	if w := call(xid, middleware.ScopeExec); w.Code != 200 {
		t.Errorf("exec on exec: code=%d, want 200", w.Code)
	}
}
