package handlers_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"letts/internal/apply"
	"letts/internal/lane"
	"letts/internal/server/handlers"
	"letts/internal/server/middleware"
	"letts/internal/storage"
)

// testMissionID returns a synthetic UUIDv7-looking ID for tests.
func testMissionID(n int) string {
	return fmt.Sprintf("01900000-0000-7000-8000-%012x", n)
}

func makeInspectHandler(t *testing.T) (*handlers.Inspect, *handlers.Admin, *lane.Manager) {
	t.Helper()
	db := setupDB(t)
	mgr := &lane.Manager{
		DB:      db,
		Spawner: func(_ context.Context, _ *storage.Mission, release func()) error { release(); return nil },
		Logger:  newTestLogger(),
		Ctx:     context.Background(),
	}
	t.Cleanup(func() { mgr.StopAll() })
	adminH := &handlers.Admin{DB: db, Manager: mgr}
	inspectH := &handlers.Inspect{
		DB:        db,
		Manager:   mgr,
		StartedAt: time.Now().Add(-5 * time.Second),
	}
	return inspectH, adminH, mgr
}

// TestInspectLanesQueuedCounts verifies that GET /v1/lanes shows correct queued counts.
func TestInspectLanesQueuedCounts(t *testing.T) {
	inspectH, adminH, _ := makeInspectHandler(t)
	mux := http.NewServeMux()
	adminH.Register(mux)
	inspectH.Register(mux)
	db := inspectH.DB

	// Apply two lanes. Pause alpha so its runner does not pick up the
	// queued rows we insert below before the GET observes them.
	desired := apply.AppliedState{
		Lanes: map[string]apply.LaneCfg{
			"alpha": {Concurrency: 2, Paused: true},
			"beta":  {Concurrency: 1},
		},
	}
	body, _ := json.Marshal(desired)
	req := httptest.NewRequest("POST", "/v1/admin/apply", bytes.NewReader(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("apply: %d %s", w.Code, w.Body.String())
	}

	// Insert 3 queued missions in "alpha".
	for i := 0; i < 3; i++ {
		id := testMissionID(i + 10)
		m := &storage.Mission{
			ID:               id,
			Kind:             storage.KindMission,
			Lane:             "alpha",
			MissionName:      "m",
			Status:           storage.StatusQueued,
			InputFingerprint: "fp",
			Input:            []byte("{}"),
			TimeCreatedMs:    int64(i + 100),
		}
		if err := storage.InsertMission(context.Background(), db, m); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	// GET /v1/lanes.
	req2 := httptest.NewRequest("GET", "/v1/lanes", nil)
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, req2)

	if w2.Code != http.StatusOK {
		t.Errorf("lanes: got %d, want 200: %s", w2.Code, w2.Body.String())
	}

	var lanes []map[string]any
	if err := json.NewDecoder(w2.Body).Decode(&lanes); err != nil {
		t.Fatalf("decode: %v", err)
	}

	alphaQueued := 0
	for _, l := range lanes {
		if l["name"] == "alpha" {
			alphaQueued = int(l["queued"].(float64))
		}
	}
	if alphaQueued != 3 {
		t.Errorf("alpha queued: want 3, got %d", alphaQueued)
	}
}

// TestInspectLanesFiltersByKindForNonAdmin verifies dispatch-scope
// callers must NOT see exec mission counts in /v1/lanes, and exec-scope
// callers must NOT see kind=mission counts. The previous SELECT was
// kind-agnostic, leaking cross-kind enumeration through aggregate counts.
func TestInspectLanesFiltersByKindForNonAdmin(t *testing.T) {
	inspectH, adminH, _ := makeInspectHandler(t)
	mux := http.NewServeMux()
	adminH.Register(mux)
	inspectH.Register(mux)
	db := inspectH.DB

	desired := apply.AppliedState{
		Lanes: map[string]apply.LaneCfg{
			"shared": {Concurrency: 2, Paused: true},
		},
	}
	body, _ := json.Marshal(desired)
	req := httptest.NewRequest("POST", "/v1/admin/apply", bytes.NewReader(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("apply: %d %s", w.Code, w.Body.String())
	}

	// Insert 2 mission-kind and 3 exec-kind queued rows in the same lane.
	for i := 0; i < 2; i++ {
		_ = storage.InsertMission(context.Background(), db, &storage.Mission{
			ID: testMissionID(i + 10), Kind: storage.KindMission, Lane: "shared",
			MissionName: "m", Status: storage.StatusQueued,
			InputFingerprint: "fp", Input: []byte("{}"), TimeCreatedMs: int64(100 + i),
		})
	}
	for i := 0; i < 3; i++ {
		_ = storage.InsertMission(context.Background(), db, &storage.Mission{
			ID: testMissionID(i + 20), Kind: storage.KindExec, Lane: "shared",
			MissionName: "exec", Status: storage.StatusQueued,
			InputFingerprint: "fp", Input: []byte("{}"), TimeCreatedMs: int64(200 + i),
		})
	}

	// Call /v1/lanes with a dispatch-scope identity in ctx (handler
	// reads it through kindFilterForCtx).
	req2 := httptest.NewRequest("GET", "/v1/lanes", nil)
	req2 = req2.WithContext(context.WithValue(req2.Context(),
		middleware.IdentityCtxKey(),
		middleware.Identity{Scope: middleware.ScopeDispatch}))
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, req2)

	var lanes []map[string]any
	_ = json.NewDecoder(w2.Body).Decode(&lanes)
	if len(lanes) == 0 {
		t.Fatalf("no lanes returned; body=%s", w2.Body.String())
	}
	for _, l := range lanes {
		if l["name"] == "shared" {
			got := int(l["queued"].(float64))
			if got != 2 {
				t.Errorf("dispatch scope queued count = %d, want 2 (kind=mission only); exec rows leaked", got)
			}
		}
	}

	// Same call with exec scope: must see only the 3 exec rows.
	req3 := httptest.NewRequest("GET", "/v1/lanes", nil)
	req3 = req3.WithContext(context.WithValue(req3.Context(),
		middleware.IdentityCtxKey(),
		middleware.Identity{Scope: middleware.ScopeExec}))
	w3 := httptest.NewRecorder()
	mux.ServeHTTP(w3, req3)

	var lanes2 []map[string]any
	_ = json.NewDecoder(w3.Body).Decode(&lanes2)
	for _, l := range lanes2 {
		if l["name"] == "shared" {
			got := int(l["queued"].(float64))
			if got != 3 {
				t.Errorf("exec scope queued count = %d, want 3 (kind=exec only); mission rows leaked", got)
			}
		}
	}
}

// TestInspectDugdale verifies GET /v1/dugdale returns version and applied_at.
func TestInspectDugdale(t *testing.T) {
	inspectH, adminH, _ := makeInspectHandler(t)
	mux := http.NewServeMux()
	adminH.Register(mux)
	inspectH.Register(mux)

	// Apply first.
	desired := apply.AppliedState{
		Lanes: map[string]apply.LaneCfg{"main": {Concurrency: 1}},
	}
	body, _ := json.Marshal(desired)
	req := httptest.NewRequest("POST", "/v1/admin/apply", bytes.NewReader(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("apply: %d", w.Code)
	}

	req2 := httptest.NewRequest("GET", "/v1/dugdale", nil)
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, req2)

	if w2.Code != http.StatusOK {
		t.Errorf("dugdale: got %d, want 200: %s", w2.Code, w2.Body.String())
	}
	var got map[string]any
	if err := json.NewDecoder(w2.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got["version"] == nil {
		t.Error("version missing")
	}
	if got["applied_at"] == nil {
		t.Error("applied_at missing after apply")
	}
	if _, ok := got["uptime_seconds"]; !ok {
		t.Error("uptime_seconds missing")
	}
}
