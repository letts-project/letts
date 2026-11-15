package handlers_test

import (
	"context"
	"database/sql"
	"net/http/httptest"
	"testing"
	"time"

	"letts/internal/ids"
	"letts/internal/server/handlers"
	"letts/internal/storage"
)

func insertStagingWithRef(t *testing.T, db *sql.DB, missionID string, kind storage.RefKind, role string, createdMs int64) string {
	t.Helper()
	id := ids.NewUUIDv7()
	if err := storage.InsertStaging(context.Background(), db, &storage.StagingFile{
		StagingID: id, State: storage.StagingComplete, Sha256: "sha-" + role, Size: 1, BytesReceived: 1,
		Path: "p", TimeCreatedMs: createdMs, TimeUpdatedMs: createdMs, TimeExpiresMs: createdMs + 60_000,
	}); err != nil {
		t.Fatalf("insert staging: %v", err)
	}
	if err := storage.InsertRef(context.Background(), db, storage.StagingRef{
		MissionID: missionID, StagingID: id, RefKind: kind, Role: role,
	}); err != nil {
		t.Fatalf("insert ref: %v", err)
	}
	return id
}

func doStagingList(h *handlers.StagingHandler, query string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("GET", "/v1/staging?"+query, nil)
	w := httptest.NewRecorder()
	h.List(w, r)
	return w
}

func TestStagingListMissionIDRequired(t *testing.T) {
	h, _, _ := setupStagingPut(t)
	w := doStagingList(h, "")
	if w.Code != 400 {
		t.Errorf("status=%d", w.Code)
	}
}

func TestStagingListMissionIDInvalid(t *testing.T) {
	h, _, _ := setupStagingPut(t)
	w := doStagingList(h, "mission_id=bogus")
	if w.Code != 400 {
		t.Errorf("status=%d", w.Code)
	}
}

func TestStagingListEmpty(t *testing.T) {
	h, _, _ := setupStagingPut(t)
	mID := ids.NewUUIDv7()
	w := doStagingList(h, "mission_id="+mID)
	if w.Code != 200 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	got := parseJSON(t, w.Body.Bytes())
	staging, _ := got["staging"].([]any)
	if len(staging) != 0 {
		t.Errorf("staging=%v", staging)
	}
}

func TestStagingListReturnsAllRefs(t *testing.T) {
	h, db, _ := setupStagingPut(t)
	mID := bulkInsertMission(t, db, h.DataDir, storage.StatusDone)
	now := time.Now().UnixMilli()
	insertStagingWithRef(t, db, mID, storage.RefInput, "input1", now)
	insertStagingWithRef(t, db, mID, storage.RefInput, "input2", now+1)
	insertStagingWithRef(t, db, mID, storage.RefOutput, "result", now+2)

	w := doStagingList(h, "mission_id="+mID)
	if w.Code != 200 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	got := parseJSON(t, w.Body.Bytes())
	staging, _ := got["staging"].([]any)
	if len(staging) != 3 {
		t.Errorf("len=%d, want 3", len(staging))
	}
}

func TestStagingListFilterByRefKind(t *testing.T) {
	h, db, _ := setupStagingPut(t)
	mID := bulkInsertMission(t, db, h.DataDir, storage.StatusDone)
	now := time.Now().UnixMilli()
	insertStagingWithRef(t, db, mID, storage.RefInput, "in", now)
	insertStagingWithRef(t, db, mID, storage.RefOutput, "out", now+1)

	w := doStagingList(h, "mission_id="+mID+"&ref_kind=output")
	if w.Code != 200 {
		t.Fatalf("status=%d", w.Code)
	}
	got := parseJSON(t, w.Body.Bytes())
	staging, _ := got["staging"].([]any)
	if len(staging) != 1 {
		t.Fatalf("len=%d, want 1", len(staging))
	}
	if staging[0].(map[string]any)["ref_kind"] != "output" {
		t.Errorf("kind=%v", staging[0])
	}
}

func TestStagingListBadRefKind(t *testing.T) {
	h, _, _ := setupStagingPut(t)
	w := doStagingList(h, "mission_id="+ids.NewUUIDv7()+"&ref_kind=weird")
	if w.Code != 400 {
		t.Errorf("status=%d", w.Code)
	}
}

func TestStagingListCursorPagination(t *testing.T) {
	h, db, _ := setupStagingPut(t)
	mID := bulkInsertMission(t, db, h.DataDir, storage.StatusDone)
	now := time.Now().UnixMilli()
	insertStagingWithRef(t, db, mID, storage.RefInput, "a", now)
	insertStagingWithRef(t, db, mID, storage.RefInput, "b", now+1)
	insertStagingWithRef(t, db, mID, storage.RefInput, "c", now+2)

	// Page 1.
	w := doStagingList(h, "mission_id="+mID+"&limit=2")
	got := parseJSON(t, w.Body.Bytes())
	staging, _ := got["staging"].([]any)
	if len(staging) != 2 {
		t.Fatalf("page1 len=%d", len(staging))
	}
	cursor, ok := got["next_cursor"].(string)
	if !ok || cursor == "" {
		t.Fatal("missing next_cursor")
	}

	// Page 2.
	w = doStagingList(h, "mission_id="+mID+"&limit=2&cursor="+cursor)
	got = parseJSON(t, w.Body.Bytes())
	staging, _ = got["staging"].([]any)
	if len(staging) != 1 {
		t.Errorf("page2 len=%d", len(staging))
	}
	if _, has := got["next_cursor"]; has {
		t.Errorf("next_cursor on last page")
	}
}

func TestStagingListBadCursorReturns400(t *testing.T) {
	h, _, _ := setupStagingPut(t)
	w := doStagingList(h, "mission_id="+ids.NewUUIDv7()+"&cursor=NOT-VALID-BASE64!")
	if w.Code != 400 {
		t.Errorf("status=%d", w.Code)
	}
}
