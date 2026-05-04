package handlers_test

import (
	"context"
	"database/sql"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
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

type fakeRuntime struct {
	mu        sync.Mutex
	calls     []signalCall
	reject    bool
	available bool
	onSignal  func(id string, reason mission.ExternalKillReason)
}

type signalCall struct {
	id     string
	reason mission.ExternalKillReason
}

func (f *fakeRuntime) SignalKill(id string, reason mission.ExternalKillReason) bool {
	f.mu.Lock()
	f.calls = append(f.calls, signalCall{id: id, reason: reason})
	cb := f.onSignal
	rej := f.reject
	f.mu.Unlock()
	if cb != nil {
		cb(id, reason)
	}
	return !rej
}

func (f *fakeRuntime) IsRunning(_ string) bool { return f.available }

func setupKillFixture(t *testing.T, status storage.Status) (*handlers.LifecycleHandler, string, *sql.DB, string, *fakeRuntime) {
	t.Helper()
	db := setupDB(t)
	dataDir := t.TempDir()
	id := ids.NewUUIDv7()

	m := storage.Mission{
		ID:               id,
		Kind:             storage.KindMission,
		Lane:             "normal",
		MissionName:      "KillFixture",
		Status:           status,
		Input:            []byte(`{}`),
		InputFingerprint: "fp",
		TimeCreatedMs:    time.Now().UnixMilli(),
	}
	if err := storage.InsertMission(context.Background(), db, &m); err != nil {
		t.Fatalf("insert mission: %v", err)
	}
	rt := storage.MissionRuntime{
		MissionID: id, MissionDir: "/tmp", CommandTemplate: `["true"]`,
	}
	if err := storage.InsertRuntime(context.Background(), db, &rt); err != nil {
		t.Fatalf("insert runtime: %v", err)
	}

	// Pre-create events file with a queued event (dispatch normally does this).
	shard, _ := ids.ShardPath(id)
	parentDir := filepath.Join(dataDir, "output", shard)
	if err := os.MkdirAll(parentDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	w, err := eventfile.Create(parentDir, id)
	if err != nil {
		t.Fatalf("eventfile.Create: %v", err)
	}
	_, _ = w.Append(eventfile.KindQueued, map[string]any{"time_created": time.Now().UnixMilli()}, true)
	_ = w.Close()

	cfg := &config.DugdaleConfig{
		DataDir: dataDir,
		Limits:  config.LimitsConfig{MaxEventsBuffer: 1024, MaxEventLineSize: 1024},
	}
	rt2 := &fakeRuntime{available: true}
	h := &handlers.LifecycleHandler{
		DB: db, Cfg: cfg, DataDir: dataDir, Runtime: rt2,
	}
	return h, id, db, dataDir, rt2
}

func doKill(h *handlers.LifecycleHandler, id string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", "/v1/missions/"+id+"/kill", nil)
	r.SetPathValue("id", id)
	// Admin identity matches production wiring (all kill routes are
	// admin-only). Required by the kind-vs-scope gate.
	ctx := context.WithValue(r.Context(),
		middleware.IdentityCtxKey(),
		middleware.Identity{Scope: middleware.ScopeAdmin})
	r = r.WithContext(ctx)
	w := httptest.NewRecorder()
	h.Kill(w, r)
	return w
}

func TestKillQueuedFinalizesAsKilledByAPI(t *testing.T) {
	h, id, db, dataDir, _ := setupKillFixture(t, storage.StatusQueued)
	w := doKill(h, id)
	if w.Code != 200 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}

	got, err := storage.GetMission(context.Background(), db, id)
	if err != nil {
		t.Fatalf("get mission: %v", err)
	}
	if got.Status != storage.StatusDone {
		t.Errorf("Status=%q", got.Status)
	}
	if got.Outcome.String != "killed" || got.FailReason.String != "killed_by_api" {
		t.Errorf("Outcome=%q FailReason=%q", got.Outcome.String, got.FailReason.String)
	}

	// Events file must have terminal done event.
	shard, _ := ids.ShardPath(id)
	evPath := filepath.Join(dataDir, "output", shard, id+"-events")
	b, err := os.ReadFile(evPath)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	if !strings.Contains(string(b), `"event":"done"`) {
		t.Errorf("events missing done: %s", string(b))
	}
	if !strings.Contains(string(b), `"outcome":"killed"`) {
		t.Errorf("done missing killed outcome: %s", string(b))
	}

	// Intent should be deleted.
	if _, err := storage.GetFinalizeIntent(context.Background(), db, id); err == nil {
		t.Errorf("finalize intent still present")
	}
}

func TestKillRunningSendsToRuntime(t *testing.T) {
	h, id, _, _, rt := setupKillFixture(t, storage.StatusRunning)
	w := doKill(h, id)
	if w.Code != 200 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	got := parseJSON(t, w.Body.Bytes())
	if got["status"] != "kill_sent" {
		t.Errorf("body=%v", got)
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if len(rt.calls) != 1 {
		t.Fatalf("calls=%d", len(rt.calls))
	}
	if rt.calls[0].id != id || rt.calls[0].reason != mission.KillByAPI {
		t.Errorf("call=%+v", rt.calls[0])
	}
}

func TestKillRunningRejectedReturns500(t *testing.T) {
	h, id, _, _, rt := setupKillFixture(t, storage.StatusRunning)
	rt.reject = true
	w := doKill(h, id)
	if w.Code != 500 {
		t.Errorf("status=%d", w.Code)
	}
}

func TestKillDoneReturns409(t *testing.T) {
	h, id, _, _, _ := setupKillFixture(t, storage.StatusDone)
	w := doKill(h, id)
	if w.Code != 409 {
		t.Fatalf("status=%d", w.Code)
	}
	if parseJSON(t, w.Body.Bytes())["error"] != "mission_done" {
		t.Errorf("body=%s", w.Body.String())
	}
}

func TestKillDeletingReturns409(t *testing.T) {
	h, id, _, _, _ := setupKillFixture(t, storage.StatusDeleting)
	w := doKill(h, id)
	if w.Code != 409 {
		t.Fatalf("status=%d", w.Code)
	}
	if parseJSON(t, w.Body.Bytes())["error"] != "mission_deleting" {
		t.Errorf("body=%s", w.Body.String())
	}
}

func TestKillUnknownReturns404(t *testing.T) {
	h, _, _, _, _ := setupKillFixture(t, storage.StatusQueued)
	bogus := ids.NewUUIDv7()
	w := doKill(h, bogus)
	if w.Code != 404 {
		t.Errorf("status=%d", w.Code)
	}
}

func TestKillInvalidIDReturns400(t *testing.T) {
	h, _, _, _, _ := setupKillFixture(t, storage.StatusQueued)
	w := doKill(h, "bad-uuid")
	if w.Code != 400 {
		t.Errorf("status=%d", w.Code)
	}
}

func TestKillRunningWithoutRuntimeReturns500(t *testing.T) {
	h, id, _, _, _ := setupKillFixture(t, storage.StatusRunning)
	h.Runtime = nil
	w := doKill(h, id)
	if w.Code != 500 {
		t.Errorf("status=%d", w.Code)
	}
}
