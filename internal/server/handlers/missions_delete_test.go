package handlers_test

import (
	"context"
	"database/sql"
	"net/http/httptest"
	"testing"
	"time"

	"letts/internal/config"
	"letts/internal/ids"
	"letts/internal/mission"
	"letts/internal/server/handlers"
	"letts/internal/server/middleware"
	"letts/internal/storage"
)

func setupDeleteFixture(t *testing.T, status storage.Status) (*handlers.LifecycleHandler, string, *sql.DB, *fakeRuntime) {
	t.Helper()
	db := setupDB(t)
	dataDir := t.TempDir()
	id := ids.NewUUIDv7()
	m := storage.Mission{
		ID:               id,
		Kind:             storage.KindMission,
		Lane:             "normal",
		MissionName:      "DeleteFixture",
		Status:           status,
		Input:            []byte(`{}`),
		InputFingerprint: "fp",
		TimeCreatedMs:    time.Now().UnixMilli(),
	}
	if err := storage.InsertMission(context.Background(), db, &m); err != nil {
		t.Fatalf("insert: %v", err)
	}
	cfg := &config.DugdaleConfig{DataDir: dataDir}
	rt := &fakeRuntime{available: true}
	h := &handlers.LifecycleHandler{
		DB: db, Cfg: cfg, DataDir: dataDir, Runtime: rt,
		ForceDeleteTimeout: 1 * time.Second,
		ForceDeletePoll:    20 * time.Millisecond,
	}
	return h, id, db, rt
}

func doDelete(h *handlers.LifecycleHandler, id string, force bool) *httptest.ResponseRecorder {
	url := "/v1/missions/" + id
	if force {
		url += "?force=true"
	}
	r := httptest.NewRequest("DELETE", url, nil)
	r.SetPathValue("id", id)
	// Admin identity for the kind-gate; matches production wiring.
	ctx := context.WithValue(r.Context(),
		middleware.IdentityCtxKey(),
		middleware.Identity{Scope: middleware.ScopeAdmin})
	r = r.WithContext(ctx)
	w := httptest.NewRecorder()
	h.Delete(w, r)
	return w
}

func assertDeleting(t *testing.T, db *sql.DB, id string) {
	t.Helper()
	m, err := storage.GetMission(context.Background(), db, id)
	if err != nil {
		t.Fatalf("get mission: %v", err)
	}
	if m.Status != storage.StatusDeleting {
		t.Errorf("Status=%q, want deleting", m.Status)
	}
}

func TestDeleteQueuedMarksDeleting(t *testing.T) {
	h, id, db, _ := setupDeleteFixture(t, storage.StatusQueued)
	w := doDelete(h, id, false)
	if w.Code != 202 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	assertDeleting(t, db, id)
	if parseJSON(t, w.Body.Bytes())["status"] != "deletion_pending" {
		t.Errorf("body=%s", w.Body.String())
	}
}

func TestDeleteDoneMarksDeleting(t *testing.T) {
	h, id, db, _ := setupDeleteFixture(t, storage.StatusDone)
	w := doDelete(h, id, false)
	if w.Code != 202 {
		t.Fatalf("status=%d", w.Code)
	}
	assertDeleting(t, db, id)
}

func TestDeleteDeletingIsIdempotent(t *testing.T) {
	h, id, _, _ := setupDeleteFixture(t, storage.StatusDeleting)
	w := doDelete(h, id, false)
	if w.Code != 202 {
		t.Errorf("status=%d", w.Code)
	}
	if parseJSON(t, w.Body.Bytes())["status"] != "deletion_pending" {
		t.Errorf("body=%s", w.Body.String())
	}
}

func TestDeleteRunningWithoutForceReturns409(t *testing.T) {
	h, id, _, rt := setupDeleteFixture(t, storage.StatusRunning)
	w := doDelete(h, id, false)
	if w.Code != 409 {
		t.Errorf("status=%d", w.Code)
	}
	if parseJSON(t, w.Body.Bytes())["error"] != "mission_running" {
		t.Errorf("body=%s", w.Body.String())
	}
	rt.mu.Lock()
	if len(rt.calls) != 0 {
		t.Errorf("SignalKill called without force: %v", rt.calls)
	}
	rt.mu.Unlock()
}

func TestDeleteRunningWithForceWaitsAndMarksDeleting(t *testing.T) {
	h, id, db, rt := setupDeleteFixture(t, storage.StatusRunning)
	// Simulate runtime: when SignalKill fires, mark mission done after a short delay.
	rt.onSignal = func(missionID string, _ mission.ExternalKillReason) {
		go func() {
			time.Sleep(60 * time.Millisecond)
			_, _ = db.ExecContext(context.Background(),
				`UPDATE missions SET status='done', outcome='killed', fail_reason='force_delete', exit_code=0 WHERE mission_id=?`, missionID)
		}()
	}
	w := doDelete(h, id, true)
	if w.Code != 202 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	assertDeleting(t, db, id)
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if len(rt.calls) != 1 {
		t.Fatalf("SignalKill calls=%d", len(rt.calls))
	}
	if rt.calls[0].reason != mission.KillForceDelete {
		t.Errorf("reason=%q, want force_delete", rt.calls[0].reason)
	}
}

func TestDeleteRunningWithForceTimeout(t *testing.T) {
	h, id, _, _ := setupDeleteFixture(t, storage.StatusRunning)
	h.ForceDeleteTimeout = 100 * time.Millisecond
	w := doDelete(h, id, true)
	if w.Code != 504 {
		t.Errorf("status=%d", w.Code)
	}
	if parseJSON(t, w.Body.Bytes())["error"] != "force_delete_timeout" {
		t.Errorf("body=%s", w.Body.String())
	}
}

func TestDeleteRunningWithForceWithoutRuntimeReturns500(t *testing.T) {
	h, id, _, _ := setupDeleteFixture(t, storage.StatusRunning)
	h.Runtime = nil
	w := doDelete(h, id, true)
	if w.Code != 500 {
		t.Errorf("status=%d", w.Code)
	}
}

func TestDeleteUnknownReturns404(t *testing.T) {
	h, _, _, _ := setupDeleteFixture(t, storage.StatusQueued)
	bogus := ids.NewUUIDv7()
	w := doDelete(h, bogus, false)
	if w.Code != 404 {
		t.Errorf("status=%d", w.Code)
	}
}

func TestDeleteInvalidIDReturns400(t *testing.T) {
	h, _, _, _ := setupDeleteFixture(t, storage.StatusQueued)
	w := doDelete(h, "bad", false)
	if w.Code != 400 {
		t.Errorf("status=%d", w.Code)
	}
}
