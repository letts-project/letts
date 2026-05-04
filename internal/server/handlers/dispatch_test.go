package handlers_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"letts/internal/apply"
	"letts/internal/config"
	"letts/internal/fingerprint"
	"letts/internal/ids"
	"letts/internal/lane"
	"letts/internal/server/handlers"
	"letts/internal/server/middleware"
	"letts/internal/storage"
)

// defaultCfg returns a minimal DugdaleConfig for tests. It sets sensible
// defaults without loading a YAML file.
func defaultCfg() *config.DugdaleConfig {
	cfg, _ := config.LoadDugdaleBytes([]byte(`
listen: ":9000"
data_dir: "/tmp"
`))
	return cfg
}

// makeDispatchHandler creates a DispatchHandler wired to a fresh in-memory DB
// and a stub lane manager. Returns the handler, the data dir, and a cleanup func.
func makeDispatchHandler(t *testing.T, cfg *config.DugdaleConfig, getApplied func() (*apply.AppliedState, bool)) (*handlers.DispatchHandler, string) {
	t.Helper()
	db := setupDB(t)
	dataDir := t.TempDir()
	mgr := &lane.Manager{
		DB:      db,
		Spawner: func(_ context.Context, _ *storage.Mission, release func()) error { release(); return nil },
		Logger:  newTestLogger(),
		Ctx:     context.Background(),
	}
	t.Cleanup(func() { mgr.StopAll() })

	if cfg == nil {
		cfg = defaultCfg()
	}
	if getApplied == nil {
		getApplied = func() (*apply.AppliedState, bool) { return nil, false }
	}

	h := &handlers.DispatchHandler{
		DB:          db,
		Cfg:         cfg,
		DataDir:     dataDir,
		LaneManager: mgr,
		KeyMu:       handlers.NewKeyMutex(),
		GetApplied:  getApplied,
	}
	return h, dataDir
}

// dispatchReq is a helper that builds and sends a dispatch POST request.
func dispatchReq(t *testing.T, mux *http.ServeMux, idemKey string, body any) *httptest.ResponseRecorder {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	req := httptest.NewRequest("POST", "/v1/dispatch", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	return w
}

// TestDispatchDuplicateFileRole: two files sharing a role must be
// rejected with 400 duplicate_role in validation, not pass through to the
// writer tx where the msr_unique_role UNIQUE constraint produces a 500 — which
// the PHP client treats as a sticky-retryable server error, storming
// the daemon with a deterministically-malformed request.
func TestDispatchDuplicateFileRole(t *testing.T) {
	applied := &apply.AppliedState{
		MissionDir: "/tmp/missions",
		Lanes:      map[string]apply.LaneCfg{"normal": {Concurrency: 5}},
	}
	h, _ := makeDispatchHandler(t, nil, func() (*apply.AppliedState, bool) { return applied, true })
	mux := http.NewServeMux()
	h.Register(mux)

	var ids2 [2]string
	for i := range ids2 {
		id := ids.NewUUIDv7()
		ids2[i] = id
		if err := storage.InsertStaging(context.Background(), h.DB, &storage.StagingFile{
			StagingID: id, State: storage.StagingComplete, Sha256: "abc", Size: 1,
			Path: "/staging/" + id, TimeCreatedMs: 1000, TimeUpdatedMs: 1000,
			TimeExpiresMs: 9999999999999,
		}); err != nil {
			t.Fatalf("insert staging: %v", err)
		}
	}

	w := dispatchReq(t, mux, ids.NewUUIDv7(), map[string]any{
		"mission": "my_mission",
		"lane":    "normal",
		"files": []map[string]any{
			{"role": "data", "staging_id": ids2[0]},
			{"role": "data", "staging_id": ids2[1]},
		},
	})
	if w.Code != http.StatusBadRequest {
		t.Errorf("want 400, got %d: %s", w.Code, w.Body.String())
	}
	assertErrorCode(t, w, "duplicate_role")
}

// TestDispatchMissingIdempotencyKey verifies 400 when header is absent.
func TestDispatchMissingIdempotencyKey(t *testing.T) {
	h, _ := makeDispatchHandler(t, nil, nil)
	mux := http.NewServeMux()
	h.Register(mux)

	body := map[string]any{"mission": "run", "lane": "normal", "input": nil}
	b, _ := json.Marshal(body)
	req := httptest.NewRequest("POST", "/v1/dispatch", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("want 400, got %d: %s", w.Code, w.Body.String())
	}
	assertErrorCode(t, w, "bad_request")
}

// TestDispatchInvalidIdempotencyKey verifies 400 when key is not UUIDv7.
func TestDispatchInvalidIdempotencyKey(t *testing.T) {
	h, _ := makeDispatchHandler(t, nil, nil)
	mux := http.NewServeMux()
	h.Register(mux)

	w := dispatchReq(t, mux, "not-a-uuid", map[string]any{"mission": "run", "lane": "normal"})
	if w.Code != http.StatusBadRequest {
		t.Errorf("want 400, got %d: %s", w.Code, w.Body.String())
	}
	assertErrorCode(t, w, "bad_request")
}

// TestDispatchInvalidJSON verifies 400 when body is not valid JSON.
func TestDispatchInvalidJSON(t *testing.T) {
	h, _ := makeDispatchHandler(t, nil, nil)
	mux := http.NewServeMux()
	h.Register(mux)

	key := ids.NewUUIDv7()
	req := httptest.NewRequest("POST", "/v1/dispatch", bytes.NewReader([]byte("{bad json}")))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", key)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("want 400, got %d: %s", w.Code, w.Body.String())
	}
	assertErrorCode(t, w, "bad_request")
}

// TestDispatchRefuses503DuringDrain verifies the graceful-shutdown
// gate: once the shutdown coordinator's IsDraining returns true, the
// dispatch handler must short-circuit with 503 draining and Retry-After,
// regardless of body validity or applied-config state.
func TestDispatchRefuses503DuringDrain(t *testing.T) {
	h, _ := makeDispatchHandler(t, nil, nil)
	draining := true
	h.IsDraining = func() bool { return draining }
	mux := http.NewServeMux()
	h.Register(mux)

	key := ids.NewUUIDv7()
	w := dispatchReq(t, mux, key, map[string]any{"mission": "m", "lane": "normal"})
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("want 503, got %d: %s", w.Code, w.Body.String())
	}
	if got := w.Header().Get("Retry-After"); got != "30" {
		t.Errorf("Retry-After=%q, want 30", got)
	}
	assertErrorCode(t, w, "draining")

	// Once the gate flips off, normal validation resumes (no applied state
	// here → 412 awaiting_apply, NOT 503 draining — proves we're past the
	// gate).
	draining = false
	w = dispatchReq(t, mux, ids.NewUUIDv7(), map[string]any{"mission": "m", "lane": "normal"})
	if w.Code == http.StatusServiceUnavailable && w.Body.String() != "" &&
		bytes.Contains(w.Body.Bytes(), []byte(`"draining"`)) {
		t.Errorf("still gated after IsDraining flipped: %s", w.Body.String())
	}
}

// TestDispatchOversizeBody413 verifies that a body larger than
// max_dispatch_body_size surfaces as 413 payload_too_large (not 400
// bad_request). middleware.BodyLimit wraps r.Body in
// http.MaxBytesReader; the decode path must recognize the resulting
// *http.MaxBytesError and map it to the correct status code.
func TestDispatchOversizeBody413(t *testing.T) {
	h, _ := makeDispatchHandler(t, nil, nil)
	// Wrap in BodyLimit middleware exactly like cmd/dugdale/main.go does.
	bodyLimit := int64(1024)
	wrapped := middleware.BodyLimit(bodyLimit, h.Dispatch)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/dispatch", wrapped)

	key := ids.NewUUIDv7()
	// Build a syntactically valid JSON body whose total length exceeds the
	// limit. With Content-Length=-1 the BodyLimit middleware can't reject
	// up-front; instead MaxBytesReader trips during the json.Decoder read
	// and surfaces *http.MaxBytesError — the path under test.
	padding := bytes.Repeat([]byte("y"), int(bodyLimit)+128)
	big := append([]byte(`{"input":{"k":"`), padding...)
	big = append(big, []byte(`"}}`)...)
	req := httptest.NewRequest("POST", "/v1/dispatch", bytes.NewReader(big))
	req.ContentLength = -1
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", key)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("want 413, got %d: %s", w.Code, w.Body.String())
	}
	assertErrorCode(t, w, "payload_too_large")
}

// TestDispatchNewMission202 verifies that a new (first-time) mission_id
// with a valid applied config returns 202 queued.
func TestDispatchNewMission202(t *testing.T) {
	applied := &apply.AppliedState{
		MissionDir: "/tmp/missions",
		Lanes:      map[string]apply.LaneCfg{"normal": {Concurrency: 5}},
	}
	getApplied := func() (*apply.AppliedState, bool) { return applied, true }

	h, _ := makeDispatchHandler(t, nil, getApplied)
	mux := http.NewServeMux()
	h.Register(mux)

	key := ids.NewUUIDv7()
	w := dispatchReq(t, mux, key, map[string]any{
		"mission": "my_mission",
		"lane":    "normal",
		"input":   map[string]any{"x": 1},
	})

	if w.Code != http.StatusAccepted {
		t.Errorf("want 202, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	_ = json.NewDecoder(w.Body).Decode(&resp)
	if resp["status"] != "queued" {
		t.Errorf("status: want queued, got %v", resp["status"])
	}
}

// TestDispatchIdempotencyReplay200 verifies 200 replay when same fingerprint exists.
func TestDispatchIdempotencyReplay200(t *testing.T) {
	applied := &apply.AppliedState{
		MissionDir: "/tmp/missions",
		Lanes:      map[string]apply.LaneCfg{"normal": {Concurrency: 5}},
	}
	h, _ := makeDispatchHandler(t, nil, func() (*apply.AppliedState, bool) { return applied, true })
	mux := http.NewServeMux()
	h.Register(mux)

	missionID := ids.NewUUIDv7()

	// Compute the fingerprint for the body we'll send.
	canonInput, _ := fingerprint.CanonicalizeInput(json.RawMessage(`{"key":"val"}`))
	fp, err := fingerprint.Mission(fingerprint.MissionInput{
		Lane:           "normal",
		Mission:        "my_mission",
		InputCanonical: canonInput,
	})
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}

	// Pre-insert the mission.
	m := &storage.Mission{
		ID:               missionID,
		Kind:             storage.KindMission,
		Lane:             "normal",
		MissionName:      "my_mission",
		Status:           storage.StatusQueued,
		Input:            canonInput,
		InputFingerprint: fp,
		TimeCreatedMs:    1000,
	}
	if err := storage.InsertMission(context.Background(), h.DB, m); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Dispatch with same body — should get 200 replay.
	w := dispatchReq(t, mux, missionID, map[string]any{
		"mission": "my_mission",
		"lane":    "normal",
		"input":   map[string]any{"key": "val"},
	})

	if w.Code != http.StatusOK {
		t.Errorf("want 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	_ = json.NewDecoder(w.Body).Decode(&resp)
	if resp["mission_id"] != missionID {
		t.Errorf("mission_id: want %q, got %v", missionID, resp["mission_id"])
	}
	if resp["status"] != "queued" {
		t.Errorf("status: want queued, got %v", resp["status"])
	}
}

// TestDispatchIdempotencyConflict409 verifies 409 when same key with different body.
func TestDispatchIdempotencyConflict409(t *testing.T) {
	applied := &apply.AppliedState{
		MissionDir: "/tmp/missions",
		Lanes:      map[string]apply.LaneCfg{"normal": {Concurrency: 5}},
	}
	h, _ := makeDispatchHandler(t, nil, func() (*apply.AppliedState, bool) { return applied, true })
	mux := http.NewServeMux()
	h.Register(mux)

	missionID := ids.NewUUIDv7()

	// Compute fingerprint for original body (no timeout).
	canonInput, _ := fingerprint.CanonicalizeInput(json.RawMessage(`{"key":"val"}`))
	fp, _ := fingerprint.Mission(fingerprint.MissionInput{
		Lane:           "normal",
		Mission:        "my_mission",
		InputCanonical: canonInput,
	})

	if err := storage.InsertMission(context.Background(), h.DB, &storage.Mission{
		ID:               missionID,
		Kind:             storage.KindMission,
		Lane:             "normal",
		MissionName:      "my_mission",
		Status:           storage.StatusQueued,
		Input:            canonInput,
		InputFingerprint: fp,
		TimeCreatedMs:    1000,
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Replay with different timeout — fingerprint will differ → 409.
	w := dispatchReq(t, mux, missionID, map[string]any{
		"mission": "my_mission",
		"lane":    "normal",
		"input":   map[string]any{"key": "val"},
		"timeout": "30s", // was absent before
	})

	if w.Code != http.StatusConflict {
		t.Errorf("want 409, got %d: %s", w.Code, w.Body.String())
	}
	assertErrorCode(t, w, "idempotency_conflict")
}

// TestDispatchDeleting410 verifies 410 when mission_id's status is deleting.
func TestDispatchDeleting410(t *testing.T) {
	applied := &apply.AppliedState{
		MissionDir: "/tmp/missions",
		Lanes:      map[string]apply.LaneCfg{"normal": {Concurrency: 5}},
	}
	h, _ := makeDispatchHandler(t, nil, func() (*apply.AppliedState, bool) { return applied, true })
	mux := http.NewServeMux()
	h.Register(mux)

	missionID := ids.NewUUIDv7()

	canonInput, _ := fingerprint.CanonicalizeInput(json.RawMessage(`{"k":"v"}`))
	fp, _ := fingerprint.Mission(fingerprint.MissionInput{
		Lane:           "normal",
		Mission:        "my_mission",
		InputCanonical: canonInput,
	})

	if err := storage.InsertMission(context.Background(), h.DB, &storage.Mission{
		ID:               missionID,
		Kind:             storage.KindMission,
		Lane:             "normal",
		MissionName:      "my_mission",
		Status:           storage.StatusDeleting,
		Input:            canonInput,
		InputFingerprint: fp,
		TimeCreatedMs:    1000,
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	w := dispatchReq(t, mux, missionID, map[string]any{
		"mission": "my_mission",
		"lane":    "normal",
		"input":   map[string]any{"k": "v"},
	})

	if w.Code != http.StatusGone {
		t.Errorf("want 410, got %d: %s", w.Code, w.Body.String())
	}
	assertErrorCode(t, w, "mission_deleting")
}

// TestDispatchNoAppliedConfig412 verifies 412 when no lanes have been applied.
func TestDispatchNoAppliedConfig412(t *testing.T) {
	// getApplied returns false (no config applied).
	h, _ := makeDispatchHandler(t, nil, func() (*apply.AppliedState, bool) { return nil, false })
	mux := http.NewServeMux()
	h.Register(mux)

	key := ids.NewUUIDv7()
	w := dispatchReq(t, mux, key, map[string]any{
		"mission": "my_mission",
		"lane":    "normal",
	})

	if w.Code != http.StatusPreconditionFailed {
		t.Errorf("want 412, got %d: %s", w.Code, w.Body.String())
	}
	assertErrorCode(t, w, "no_lanes_configured")
}

// TestDispatchInvalidMissionName400 verifies 400 for a bad mission name.
func TestDispatchInvalidMissionName400(t *testing.T) {
	applied := &apply.AppliedState{
		MissionDir: "/tmp/missions",
		Lanes:      map[string]apply.LaneCfg{"normal": {Concurrency: 2}},
	}
	h, _ := makeDispatchHandler(t, nil, func() (*apply.AppliedState, bool) { return applied, true })
	mux := http.NewServeMux()
	h.Register(mux)

	key := ids.NewUUIDv7()
	w := dispatchReq(t, mux, key, map[string]any{
		"mission": "INVALID MISSION NAME WITH SPACES",
		"lane":    "normal",
	})

	if w.Code != http.StatusBadRequest {
		t.Errorf("want 400, got %d: %s", w.Code, w.Body.String())
	}
	assertErrorCode(t, w, "bad_request")
}

// Mission name validation must precede staging-ref resolution
// and fingerprint compute. Order matters because all three return 400, but:
//   - resolveStagingMetadata hits the DB (one row per FileRef).
//   - fingerprint.Mission runs JCS over the canonical input.
//
// A bad mission name should short-circuit both. The strongest observable
// fingerprint of "validated first" is: the response details mention the
// mission name regex, not "unknown_staging_ref".
func TestDispatchValidatesMissionNameBeforeStagingRefs(t *testing.T) {
	applied := &apply.AppliedState{
		MissionDir: "/tmp/missions",
		Lanes:      map[string]apply.LaneCfg{"normal": {Concurrency: 2}},
	}
	h, _ := makeDispatchHandler(t, nil, func() (*apply.AppliedState, bool) { return applied, true })
	mux := http.NewServeMux()
	h.Register(mux)

	// Bad mission name and non-existent staging id. Pre-fix: staging
	// resolve fails first → "unknown_staging_ref". Post-fix: name
	// validation fails first → "invalid mission name".
	bogusStagingID := ids.NewUUIDv7()
	key := ids.NewUUIDv7()
	w := dispatchReq(t, mux, key, map[string]any{
		"mission": "INVALID NAME WITH SPACES",
		"lane":    "normal",
		"files":   []map[string]any{{"role": "data", "staging_id": bogusStagingID}},
	})

	if w.Code != http.StatusBadRequest {
		t.Errorf("want 400, got %d: %s", w.Code, w.Body.String())
	}
	body := w.Body.String()
	if !bytes.Contains([]byte(body), []byte("invalid mission name")) {
		t.Errorf("expected 'invalid mission name' in details, got: %s", body)
	}
	if bytes.Contains([]byte(body), []byte("unknown_staging_ref")) {
		t.Errorf("staging-ref check fired first; body=%s", body)
	}
}

// TestDispatchUnknownLane400 verifies 400 when lane is not in applied config.
func TestDispatchUnknownLane400(t *testing.T) {
	applied := &apply.AppliedState{
		MissionDir: "/tmp/missions",
		Lanes:      map[string]apply.LaneCfg{"normal": {Concurrency: 2}},
	}
	h, _ := makeDispatchHandler(t, nil, func() (*apply.AppliedState, bool) { return applied, true })
	mux := http.NewServeMux()
	h.Register(mux)

	key := ids.NewUUIDv7()
	w := dispatchReq(t, mux, key, map[string]any{
		"mission": "my_mission",
		"lane":    "nonexistent",
	})

	if w.Code != http.StatusBadRequest {
		t.Errorf("want 400, got %d: %s", w.Code, w.Body.String())
	}
	assertErrorCode(t, w, "unknown_lane")
}

// TestDispatchLaneQueueFull503 verifies 503 with Retry-After when per-lane limit hit.
func TestDispatchLaneQueueFull503(t *testing.T) {
	cfg := defaultCfg()
	cfg.Limits.MaxQueuePerLane = 2

	applied := &apply.AppliedState{
		MissionDir: "/tmp/missions",
		Lanes:      map[string]apply.LaneCfg{"normal": {Concurrency: 5}},
	}
	h, _ := makeDispatchHandler(t, cfg, func() (*apply.AppliedState, bool) { return applied, true })
	mux := http.NewServeMux()
	h.Register(mux)

	// Fill the queue to the limit.
	for i := 0; i < 2; i++ {
		mid := ids.NewUUIDv7()
		fp := "fp-lane-" + mid
		if err := storage.InsertMission(context.Background(), h.DB, &storage.Mission{
			ID:               mid,
			Kind:             storage.KindMission,
			Lane:             "normal",
			MissionName:      "filler",
			Status:           storage.StatusQueued,
			Input:            []byte("null"),
			InputFingerprint: fp,
			TimeCreatedMs:    int64(i + 1),
		}); err != nil {
			t.Fatalf("insert filler: %v", err)
		}
	}

	key := ids.NewUUIDv7()
	w := dispatchReq(t, mux, key, map[string]any{
		"mission": "my_mission",
		"lane":    "normal",
	})

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("want 503, got %d: %s", w.Code, w.Body.String())
	}
	assertErrorCode(t, w, "queue_full")
	if w.Header().Get("Retry-After") == "" {
		t.Error("missing Retry-After header")
	}
}

// TestDispatchGlobalQueueFull503 verifies 503 when global queue limit hit.
func TestDispatchGlobalQueueFull503(t *testing.T) {
	cfg := defaultCfg()
	cfg.Limits.MaxQueueTotal = 1

	applied := &apply.AppliedState{
		MissionDir: "/tmp/missions",
		Lanes: map[string]apply.LaneCfg{
			"normal": {Concurrency: 5},
			"other":  {Concurrency: 5},
		},
	}
	h, _ := makeDispatchHandler(t, cfg, func() (*apply.AppliedState, bool) { return applied, true })
	mux := http.NewServeMux()
	h.Register(mux)

	// Insert one queued mission (in "other" lane) to hit the global limit.
	mid := ids.NewUUIDv7()
	if err := storage.InsertMission(context.Background(), h.DB, &storage.Mission{
		ID:               mid,
		Kind:             storage.KindMission,
		Lane:             "other",
		MissionName:      "filler",
		Status:           storage.StatusQueued,
		Input:            []byte("null"),
		InputFingerprint: "fp-global-" + mid,
		TimeCreatedMs:    1,
	}); err != nil {
		t.Fatalf("insert filler: %v", err)
	}

	key := ids.NewUUIDv7()
	w := dispatchReq(t, mux, key, map[string]any{
		"mission": "my_mission",
		"lane":    "normal",
	})

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("want 503, got %d: %s", w.Code, w.Body.String())
	}
	assertErrorCode(t, w, "queue_full")
}

// TestDispatchHappyPath202 verifies the full happy path: 202, events file, DB row, runtime row.
func TestDispatchHappyPath202(t *testing.T) {
	applied := &apply.AppliedState{
		MissionDir: "/tmp/missions",
		Lanes:      map[string]apply.LaneCfg{"normal": {Concurrency: 5}},
		Runtime: apply.Runtime{
			CommandTemplate:     []string{"php", "{mission_path}"},
			MissionPathTemplate: "{mission_dir}/{mission}.php",
		},
	}
	h, dataDir := makeDispatchHandler(t, nil, func() (*apply.AppliedState, bool) { return applied, true })
	mux := http.NewServeMux()
	h.Register(mux)

	missionID := ids.NewUUIDv7()
	w := dispatchReq(t, mux, missionID, map[string]any{
		"mission": "my_mission",
		"lane":    "normal",
		"input":   map[string]any{"k": 1},
	})

	if w.Code != http.StatusAccepted {
		t.Fatalf("want 202, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	_ = json.NewDecoder(w.Body).Decode(&resp)
	if resp["mission_id"] != missionID {
		t.Errorf("mission_id: want %q, got %v", missionID, resp["mission_id"])
	}
	if resp["status"] != "queued" {
		t.Errorf("status: want queued, got %v", resp["status"])
	}

	// Verify events file exists.
	shard := missionID[0:2] + "/" + missionID[2:4]
	evPath := dataDir + "/output/" + shard + "/" + missionID + "-events"
	if _, err := os.Stat(evPath); err != nil {
		t.Errorf("events file missing: %v", err)
	}

	// Verify DB mission row.
	ctx := context.Background()
	m, err := storage.GetMission(ctx, h.DB, missionID)
	if err != nil {
		t.Fatalf("GetMission: %v", err)
	}
	if m.Status != storage.StatusQueued {
		t.Errorf("status: want queued, got %v", m.Status)
	}

	// Verify runtime row.
	rt, err := storage.GetRuntime(ctx, h.DB, missionID)
	if err != nil {
		t.Fatalf("GetRuntime: %v", err)
	}
	if rt.MissionDir != "/tmp/missions" {
		t.Errorf("MissionDir: want /tmp/missions, got %q", rt.MissionDir)
	}
}

// TestDispatchOrphanCleanup verifies that an orphan events file is cleaned up and
// dispatch proceeds successfully.
func TestDispatchOrphanCleanup(t *testing.T) {
	applied := &apply.AppliedState{
		MissionDir: "/tmp/missions",
		Lanes:      map[string]apply.LaneCfg{"normal": {Concurrency: 5}},
	}
	h, dataDir := makeDispatchHandler(t, nil, func() (*apply.AppliedState, bool) { return applied, true })
	mux := http.NewServeMux()
	h.Register(mux)

	missionID := ids.NewUUIDv7()
	shard := missionID[0:2] + "/" + missionID[2:4]
	outDir := dataDir + "/output/" + shard

	// Create the orphan events file before dispatching.
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	orphanPath := outDir + "/" + missionID + "-events"
	if err := os.WriteFile(orphanPath, []byte("orphan data\n"), 0o644); err != nil {
		t.Fatalf("write orphan: %v", err)
	}

	w := dispatchReq(t, mux, missionID, map[string]any{
		"mission": "my_mission",
		"lane":    "normal",
	})

	if w.Code != http.StatusAccepted {
		t.Fatalf("want 202, got %d: %s", w.Code, w.Body.String())
	}

	// Verify mission row exists (orphan was cleaned up, dispatch succeeded).
	m, err := storage.GetMission(context.Background(), h.DB, missionID)
	if err != nil {
		t.Fatalf("GetMission: %v", err)
	}
	if m.Status != storage.StatusQueued {
		t.Errorf("status: want queued, got %v", m.Status)
	}
}

// TestDispatchFileRefStagingDeleted verifies 400 when staging ref disappears between
// phase 2 and phase 3. We simulate this by inserting a staging row in 'deleting' state.
// The handler should return 400 unknown_staging_ref and clean up the events file.
func TestDispatchFileRefStagingDeleted(t *testing.T) {
	applied := &apply.AppliedState{
		MissionDir: "/tmp/missions",
		Lanes:      map[string]apply.LaneCfg{"normal": {Concurrency: 5}},
	}
	h, dataDir := makeDispatchHandler(t, nil, func() (*apply.AppliedState, bool) { return applied, true })
	mux := http.NewServeMux()
	h.Register(mux)

	// Insert a staging file in 'complete' state initially.
	stagingID := ids.NewUUIDv7()
	if err := storage.InsertStaging(context.Background(), h.DB, &storage.StagingFile{
		StagingID:     stagingID,
		State:         storage.StagingComplete,
		Sha256:        "abc123",
		Size:          100,
		Path:          "/staging/" + stagingID,
		TimeCreatedMs: 1000,
		TimeUpdatedMs: 1000,
		TimeExpiresMs: 9999999999999,
	}); err != nil {
		t.Fatalf("insert staging: %v", err)
	}

	// Change it to deleting to simulate it being removed between phase 2 and 3.
	if err := storage.MarkStagingDeleting(context.Background(), h.DB, stagingID); err != nil {
		t.Fatalf("mark deleting: %v", err)
	}

	missionID := ids.NewUUIDv7()
	w := dispatchReq(t, mux, missionID, map[string]any{
		"mission": "my_mission",
		"lane":    "normal",
		"files": []map[string]any{
			{"role": "input_data", "staging_id": stagingID},
		},
	})

	// Should fail at staging resolve in phase 1 (state != complete) → 400.
	if w.Code != http.StatusBadRequest {
		t.Errorf("want 400, got %d: %s", w.Code, w.Body.String())
	}

	// The insertErr cleanup must remove ALL files dispatch
	// could have left behind, not only the events file. The orphan-cleanup
	// branch already did this; the failure branch was asymmetric and
	// would have leaked stdout/stderr/combined sentinels and any workdir
	// scaffolding. Today none of those are created before insertErr fires
	// at THIS test's stage (1), but the failure branch is the codepath
	// for late insertErrs too (e.g. SQL writer-tx fail after the events
	// queue), and the helper is shared. Pin every output cleanup.
	shard := missionID[0:2] + "/" + missionID[2:4]
	outDir := dataDir + "/output/" + shard
	for _, suffix := range []string{"-events", "-stdout", "-stderr", "-combined"} {
		p := outDir + "/" + missionID + suffix
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("%s should not exist after insertErr cleanup; stat: %v", p, err)
		}
	}
	if _, err := os.Stat(dataDir + "/work/" + missionID); !os.IsNotExist(err) {
		t.Errorf("workdir should not exist after insertErr cleanup")
	}
}

// TestDispatchIdempotencyReplayAfterJ4 verifies 200 replay still works.
func TestDispatchIdempotencyReplayAfterJ4(t *testing.T) {
	// Covered by TestDispatchIdempotencyReplay200 which still applies.
	t.Skip("covered by TestDispatchIdempotencyReplay200")
}

// TestDispatchInvalidLaneNameFormat400 verifies 400 when lane name fails the
// lane regex (phase 2 validation).
func TestDispatchInvalidLaneNameFormat400(t *testing.T) {
	applied := &apply.AppliedState{
		MissionDir: "/tmp/missions",
		Lanes:      map[string]apply.LaneCfg{"normal": {Concurrency: 2}},
	}
	h, _ := makeDispatchHandler(t, nil, func() (*apply.AppliedState, bool) { return applied, true })
	mux := http.NewServeMux()
	h.Register(mux)

	key := ids.NewUUIDv7()
	// Lane names must match ^[a-z][a-z0-9_-]{0,31}$ — uppercase is invalid.
	w := dispatchReq(t, mux, key, map[string]any{
		"mission": "my_mission",
		"lane":    "UPPERCASE_INVALID",
	})

	if w.Code != http.StatusBadRequest {
		t.Errorf("want 400, got %d: %s", w.Code, w.Body.String())
	}
	assertErrorCode(t, w, "bad_request")
}

// TestDispatchWithFileRef202 verifies the happy path with a staging file ref:
// 202, mission row, ref row, and runtime row all exist in the DB.
func TestDispatchWithFileRef202(t *testing.T) {
	applied := &apply.AppliedState{
		MissionDir: "/tmp/missions",
		Lanes:      map[string]apply.LaneCfg{"normal": {Concurrency: 5}},
		Runtime: apply.Runtime{
			CommandTemplate:     []string{"php", "{mission_path}"},
			MissionPathTemplate: "{mission_dir}/{mission}.php",
		},
	}
	h, _ := makeDispatchHandler(t, nil, func() (*apply.AppliedState, bool) { return applied, true })
	mux := http.NewServeMux()
	h.Register(mux)

	// Insert a complete staging file.
	stagingID := ids.NewUUIDv7()
	if err := storage.InsertStaging(context.Background(), h.DB, &storage.StagingFile{
		StagingID:     stagingID,
		State:         storage.StagingComplete,
		Sha256:        "deadbeefdeadbeef",
		Size:          512,
		Path:          "/staging/" + stagingID,
		TimeCreatedMs: 1000,
		TimeUpdatedMs: 1000,
		TimeExpiresMs: 9999999999999,
	}); err != nil {
		t.Fatalf("insert staging: %v", err)
	}

	missionID := ids.NewUUIDv7()
	w := dispatchReq(t, mux, missionID, map[string]any{
		"mission": "my_mission",
		"lane":    "normal",
		"files": []map[string]any{
			{"role": "input_data", "staging_id": stagingID},
		},
	})

	if w.Code != http.StatusAccepted {
		t.Fatalf("want 202, got %d: %s", w.Code, w.Body.String())
	}

	ctx := context.Background()

	// Verify mission row.
	m, err := storage.GetMission(ctx, h.DB, missionID)
	if err != nil {
		t.Fatalf("GetMission: %v", err)
	}
	if m.Status != storage.StatusQueued {
		t.Errorf("mission status: want queued, got %v", m.Status)
	}

	// Verify staging ref row.
	refs, err := storage.RefsByMission(ctx, h.DB, missionID)
	if err != nil {
		t.Fatalf("RefsByMission: %v", err)
	}
	if len(refs) != 1 {
		t.Fatalf("refs: want 1, got %d", len(refs))
	}
	if refs[0].StagingID != stagingID {
		t.Errorf("ref staging_id: want %q, got %q", stagingID, refs[0].StagingID)
	}
	if refs[0].RefKind != storage.RefInput {
		t.Errorf("ref kind: want input, got %q", refs[0].RefKind)
	}

	// Verify runtime row.
	rt, err := storage.GetRuntime(ctx, h.DB, missionID)
	if err != nil {
		t.Fatalf("GetRuntime: %v", err)
	}
	if rt.MissionDir != "/tmp/missions" {
		t.Errorf("runtime MissionDir: want /tmp/missions, got %q", rt.MissionDir)
	}
}

// --- helpers ---

func assertErrorCode(t *testing.T, w *httptest.ResponseRecorder, code string) {
	t.Helper()
	var resp map[string]any
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if resp["error"] != code {
		t.Errorf("error code: want %q, got %v", code, resp["error"])
	}
}
