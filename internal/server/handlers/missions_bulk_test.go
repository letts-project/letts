package handlers_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"letts/internal/config"
	"letts/internal/eventfile"
	"letts/internal/ids"
	"letts/internal/mission"
	"letts/internal/server/handlers"
	"letts/internal/server/middleware"
	"letts/internal/storage"
)

func bulkSetupHandler(t *testing.T) (*handlers.LifecycleHandler, *sql.DB, string, *fakeRuntime) {
	t.Helper()
	db := setupDB(t)
	dataDir := t.TempDir()
	cfg := &config.DugdaleConfig{
		DataDir: dataDir,
		Limits:  config.LimitsConfig{MaxEventsBuffer: 1024, MaxEventLineSize: 1024},
	}
	rt := &fakeRuntime{available: true}
	h := &handlers.LifecycleHandler{
		DB: db, Cfg: cfg, DataDir: dataDir, Runtime: rt,
		ForceDeleteTimeout: 1 * time.Second,
		ForceDeletePoll:    20 * time.Millisecond,
	}
	return h, db, dataDir, rt
}

func bulkInsertMission(t *testing.T, db *sql.DB, dataDir string, status storage.Status) string {
	t.Helper()
	id := ids.NewUUIDv7()
	m := storage.Mission{
		ID: id, Kind: storage.KindMission, Lane: "normal",
		MissionName: "BulkFixture", Status: status,
		Input: []byte(`{}`), InputFingerprint: "fp",
		TimeCreatedMs: time.Now().UnixMilli(),
	}
	if err := storage.InsertMission(context.Background(), db, &m); err != nil {
		t.Fatalf("insert: %v", err)
	}
	rt := storage.MissionRuntime{
		MissionID: id, MissionDir: "/tmp", CommandTemplate: `["true"]`,
	}
	if err := storage.InsertRuntime(context.Background(), db, &rt); err != nil {
		t.Fatalf("insert runtime: %v", err)
	}
	// Pre-create events file (dispatch normally does this).
	shard, _ := ids.ShardPath(id)
	parentDir := filepath.Join(dataDir, "output", shard)
	_ = os.MkdirAll(parentDir, 0o755)
	w, err := eventfile.Create(parentDir, id)
	if err != nil {
		t.Fatalf("event create: %v", err)
	}
	_, _ = w.Append(eventfile.KindQueued, map[string]any{"time": time.Now().UnixMilli()}, true)
	_ = w.Close()
	return id
}

// asAdminReq injects an admin Identity into req's context, matching what
// middleware.Auth does in production. Required since the kind-gate
// consults the Identity in restartOne/deleteOne.
func asAdminReq(req *http.Request) *http.Request {
	return req.WithContext(context.WithValue(req.Context(),
		middleware.IdentityCtxKey(),
		middleware.Identity{Scope: middleware.ScopeAdmin}))
}

func bulkBody(t *testing.T, ids []string, force bool) *bytes.Buffer {
	t.Helper()
	body := map[string]any{"ids": ids}
	if force {
		body["force"] = true
	}
	b, _ := json.Marshal(body)
	return bytes.NewBuffer(b)
}

func bulkResults(t *testing.T, body []byte) []map[string]any {
	t.Helper()
	var resp map[string]any
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	results, _ := resp["results"].([]any)
	out := make([]map[string]any, 0, len(results))
	for _, r := range results {
		out = append(out, r.(map[string]any))
	}
	return out
}

func TestBulkRestartMixedStatuses(t *testing.T) {
	h, db, dataDir, _ := bulkSetupHandler(t)
	doneID := bulkInsertMission(t, db, dataDir, storage.StatusDone)
	queuedID := bulkInsertMission(t, db, dataDir, storage.StatusQueued)
	bogusID := ids.NewUUIDv7()

	body := bulkBody(t, []string{doneID, queuedID, bogusID}, false)
	r := asAdminReq(httptest.NewRequest("POST", "/v1/missions/bulk-restart", body))
	w := httptest.NewRecorder()
	h.BulkRestart(w, r)
	if w.Code != 200 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	results := bulkResults(t, w.Body.Bytes())
	if len(results) != 3 {
		t.Fatalf("results len=%d", len(results))
	}
	if results[0]["id"] != doneID || results[0]["ok"] != true {
		t.Errorf("results[0]=%v", results[0])
	}
	if results[0]["mission_id"] == "" {
		t.Errorf("missing mission_id for restart success")
	}
	if results[1]["id"] != queuedID || results[1]["ok"] != false || results[1]["error"] != "mission_not_done" {
		t.Errorf("results[1]=%v", results[1])
	}
	if results[2]["id"] != bogusID || results[2]["ok"] != false || results[2]["error"] != "not_found" {
		t.Errorf("results[2]=%v", results[2])
	}
}

func TestBulkDeleteSkipsRunningWithoutForce(t *testing.T) {
	h, db, dataDir, rt := bulkSetupHandler(t)
	doneID := bulkInsertMission(t, db, dataDir, storage.StatusDone)
	runningID := bulkInsertMission(t, db, dataDir, storage.StatusRunning)

	body := bulkBody(t, []string{doneID, runningID}, false)
	r := asAdminReq(httptest.NewRequest("POST", "/v1/missions/bulk-delete", body))
	w := httptest.NewRecorder()
	h.BulkDelete(w, r)
	if w.Code != 200 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	results := bulkResults(t, w.Body.Bytes())
	if results[0]["ok"] != true || results[0]["status"] != "deletion_pending" {
		t.Errorf("results[0]=%v", results[0])
	}
	if results[1]["ok"] != false || results[1]["error"] != "mission_running" {
		t.Errorf("results[1]=%v", results[1])
	}

	// Without force, runtime should not be signaled at all.
	rt.mu.Lock()
	if len(rt.calls) != 0 {
		t.Errorf("SignalKill called without force: %v", rt.calls)
	}
	rt.mu.Unlock()
}

func TestBulkDeleteForceTrueAppliesToRunning(t *testing.T) {
	h, db, dataDir, rt := bulkSetupHandler(t)
	doneID := bulkInsertMission(t, db, dataDir, storage.StatusDone)
	runningID := bulkInsertMission(t, db, dataDir, storage.StatusRunning)

	rt.onSignal = func(missionID string, _ mission.ExternalKillReason) {
		go func() {
			time.Sleep(40 * time.Millisecond)
			_, _ = db.ExecContext(context.Background(),
				`UPDATE missions SET status='done', outcome='killed', fail_reason='force_delete' WHERE mission_id=?`, missionID)
		}()
	}

	body := bulkBody(t, []string{doneID, runningID}, true)
	r := asAdminReq(httptest.NewRequest("POST", "/v1/missions/bulk-delete", body))
	w := httptest.NewRecorder()
	h.BulkDelete(w, r)
	if w.Code != 200 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	results := bulkResults(t, w.Body.Bytes())
	if results[0]["ok"] != true || results[1]["ok"] != true {
		t.Errorf("results=%v", results)
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if len(rt.calls) != 1 {
		t.Errorf("SignalKill calls=%d, want 1", len(rt.calls))
	}
}

func TestBulkRestartEmptyIDsReturns400(t *testing.T) {
	h, _, _, _ := bulkSetupHandler(t)
	body := bytes.NewBufferString(`{"ids":[]}`)
	r := asAdminReq(httptest.NewRequest("POST", "/v1/missions/bulk-restart", body))
	w := httptest.NewRecorder()
	h.BulkRestart(w, r)
	if w.Code != 400 {
		t.Errorf("status=%d", w.Code)
	}
}

func TestBulkDeleteEmptyIDsReturns400(t *testing.T) {
	h, _, _, _ := bulkSetupHandler(t)
	body := bytes.NewBufferString(`{"ids":[]}`)
	r := asAdminReq(httptest.NewRequest("POST", "/v1/missions/bulk-delete", body))
	w := httptest.NewRecorder()
	h.BulkDelete(w, r)
	if w.Code != 400 {
		t.Errorf("status=%d", w.Code)
	}
}

func TestBulkRestartBadJSONReturns400(t *testing.T) {
	h, _, _, _ := bulkSetupHandler(t)
	body := bytes.NewBufferString(`{"ids":notarray}`)
	r := asAdminReq(httptest.NewRequest("POST", "/v1/missions/bulk-restart", body))
	w := httptest.NewRecorder()
	h.BulkRestart(w, r)
	if w.Code != 400 {
		t.Errorf("status=%d", w.Code)
	}
}
