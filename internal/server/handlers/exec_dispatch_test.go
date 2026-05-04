package handlers_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
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

// TestExecDisabledStubReturns404 asserts ExecDisabledStub serves 404 with the
// canonical feature_disabled error code. The stub is registered OUTSIDE Auth
// middleware so the response shape must not depend on bearer presence.
func TestExecDisabledStubReturns404(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/v1/exec/dispatch",
		bytes.NewReader([]byte(`{"lane":"normal","command":["uptime"]}`)))
	handlers.ExecDisabledStub()(rec, req)
	if rec.Code != 404 {
		t.Errorf("code=%d, want 404", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "feature_disabled") {
		t.Errorf("body=%q, want feature_disabled", rec.Body.String())
	}
}

// --- fixtures ---

// execTestCfg is the minimum DugdaleConfig needed for the exec handler. We
// enable exec, set a small body limit so payload_too_large is easy to
// trigger from a test, and leave the rest at applyDefaults values.
func execTestCfg(t *testing.T) *config.DugdaleConfig {
	t.Helper()
	cfg, err := config.LoadDugdaleBytes([]byte(`
listen: "127.0.0.1:0"
data_dir: "/tmp/letts-exec-test"
exec:
  enabled: true
  tokens: ["exec-tok"]
  max_inputs_per_exec: 4
  max_outputs_per_exec: 4
  max_script_size: "1KiB"
limits:
  max_exec_body_size: "8KiB"
`))
	if err != nil {
		t.Fatalf("execTestCfg: %v", err)
	}
	return cfg
}

// execAppliedNormal returns an AppliedState with a single "normal" lane so
// step 3 (lane existence) and step 14 staging metadata lookups can proceed.
func execAppliedNormal() *apply.AppliedState {
	return &apply.AppliedState{
		MissionDir: "/tmp/missions",
		Lanes:      map[string]apply.LaneCfg{"normal": {Concurrency: 4}},
	}
}

// makeExecHandler wires a ExecDispatchHandler with a fresh in-memory DB, a
// noop lane Manager, and an injectable applied-state callback. Returns the
// handler and the resolved data dir.
func makeExecHandler(t *testing.T, cfg *config.DugdaleConfig, getApplied func() (*apply.AppliedState, bool)) (*handlers.ExecDispatchHandler, string) {
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
		cfg = execTestCfg(t)
	}
	if getApplied == nil {
		getApplied = execAppliedNormal2OK
	}
	return &handlers.ExecDispatchHandler{
		DB:          db,
		Cfg:         cfg,
		DataDir:     dataDir,
		LaneManager: mgr,
		KeyMu:       handlers.NewKeyMutex(),
		GetApplied:  getApplied,
		Logger:      newTestLogger(),
	}, dataDir
}

func execAppliedNormal2OK() (*apply.AppliedState, bool) {
	return execAppliedNormal(), true
}

// execMux mounts the handler on a fresh ServeMux behind BodyLimit
// middleware so payload_too_large semantics match cmd/dugdale's wiring.
func execMux(h *handlers.ExecDispatchHandler) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/exec/dispatch",
		middleware.BodyLimit(h.Cfg.Limits.MaxExecBodySize, h.Dispatch))
	return mux
}

// doExecDispatch sends a POST with the given idem key and raw body to the
// mux returned by execMux. Returns the recorder for assertions.
func doExecDispatch(t *testing.T, mux *http.ServeMux, idemKey string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest("POST", "/v1/exec/dispatch", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

// doExecDispatchJSON marshals body via encoding/json and forwards to doExecDispatch.
func doExecDispatchJSON(t *testing.T, mux *http.ServeMux, idemKey string, body map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return doExecDispatch(t, mux, idemKey, b)
}

// stagingUUID returns a UUIDv7 for tests that need a fresh staging id.
func stagingUUID(t *testing.T) string {
	t.Helper()
	return ids.NewUUIDv7()
}

// --- validation tests (steps 0-13) ---

// TestExecDispatchValidationTable runs one case per validation error code
// emitted by steps 0-13 of the pipeline. Each row mutates a
// "valid baseline" body so the targeted check is the first thing to fail.
func TestExecDispatchValidationTable(t *testing.T) {
	cfg := execTestCfg(t)
	h, _ := makeExecHandler(t, cfg, nil)
	mux := execMux(h)

	baseIdem := ids.NewUUIDv7()
	baseBody := map[string]any{
		"lane":    "normal",
		"command": []any{"uptime"},
	}

	cases := []struct {
		name     string
		idem     string // "" → use baseIdem
		body     map[string]any
		raw      []byte // when non-nil, sent verbatim (bypasses body map)
		wantCode int
		wantErr  string
	}{
		// Step 0: body size cap (handler middleware enforces 413).
		{
			name:     "step0_body_too_large",
			raw:      bytes.Repeat([]byte("x"), int(cfg.Limits.MaxExecBodySize)+128),
			wantCode: http.StatusRequestEntityTooLarge,
			wantErr:  "payload_too_large",
		},
		// Step 1: Idempotency-Key absent.
		{
			name:     "step1_missing_idem",
			idem:     " ", // sentinel that doExecDispatch interprets as absent
			body:     baseBody,
			wantCode: http.StatusBadRequest,
			wantErr:  "bad_request",
		},
		{
			name:     "step1_invalid_idem",
			idem:     "not-a-uuid",
			body:     baseBody,
			wantCode: http.StatusBadRequest,
			wantErr:  "bad_request",
		},
		// Step 2: JSON parse error.
		{
			name:     "step2_invalid_json",
			raw:      []byte("{not json}"),
			wantCode: http.StatusBadRequest,
			wantErr:  "bad_request",
		},
		// Step 3 (partial): empty lane.
		{
			name:     "step3_empty_lane",
			body:     map[string]any{"command": []any{"uptime"}},
			wantCode: http.StatusBadRequest,
			wantErr:  "bad_request",
		},
		// Step 4: command non-empty argv.
		{
			name:     "step4_missing_command",
			body:     map[string]any{"lane": "normal"},
			wantCode: http.StatusBadRequest,
			wantErr:  "bad_request",
		},
		// Step 5: invalid_key (regex / __ prefix).
		{
			name: "step5_in_key_regex",
			body: map[string]any{
				"lane": "normal", "command": []any{"uptime"},
				"in": []any{map[string]any{"key": "1invalid", "staging_id": ids.NewUUIDv7()}},
			},
			wantCode: http.StatusBadRequest,
			wantErr:  "invalid_key",
		},
		{
			name: "step5_out_key_reserved",
			body: map[string]any{
				"lane": "normal", "command": []any{"uptime"},
				"out": []any{map[string]any{"key": "__reserved"}},
			},
			wantCode: http.StatusBadRequest,
			wantErr:  "invalid_key",
		},
		// Step 6: duplicate key within in[].
		{
			name: "step6_dup_in",
			body: map[string]any{
				"lane": "normal", "command": []any{"uptime"},
				"in": []any{
					map[string]any{"key": "pdf", "staging_id": ids.NewUUIDv7()},
					map[string]any{"key": "pdf", "staging_id": ids.NewUUIDv7()},
				},
			},
			wantCode: http.StatusBadRequest,
			wantErr:  "duplicate_key",
		},
		// Step 7: duplicate key within out[].
		{
			name: "step7_dup_out",
			body: map[string]any{
				"lane": "normal", "command": []any{"uptime"},
				"out": []any{
					map[string]any{"key": "png"},
					map[string]any{"key": "png"},
				},
			},
			wantCode: http.StatusBadRequest,
			wantErr:  "duplicate_key",
		},
		// Step 8: too_many_files in in[] (cfg max=4).
		{
			name: "step8_in_too_many",
			body: map[string]any{
				"lane": "normal", "command": []any{"uptime"},
				"in": []any{
					map[string]any{"key": "a", "staging_id": ids.NewUUIDv7()},
					map[string]any{"key": "b", "staging_id": ids.NewUUIDv7()},
					map[string]any{"key": "c", "staging_id": ids.NewUUIDv7()},
					map[string]any{"key": "d", "staging_id": ids.NewUUIDv7()},
					map[string]any{"key": "e", "staging_id": ids.NewUUIDv7()},
				},
			},
			wantCode: http.StatusBadRequest,
			wantErr:  "too_many_files",
		},
		// Step 9: too_many_files in out[].
		{
			name: "step9_out_too_many",
			body: map[string]any{
				"lane": "normal", "command": []any{"uptime"},
				"out": []any{
					map[string]any{"key": "a"}, map[string]any{"key": "b"},
					map[string]any{"key": "c"}, map[string]any{"key": "d"},
					map[string]any{"key": "e"},
				},
			},
			wantCode: http.StatusBadRequest,
			wantErr:  "too_many_files",
		},
		// Step 10: shell guard triggers on `bash -c ...` when allow_shell=false.
		{
			name: "step10_shell_disabled",
			body: map[string]any{
				"lane":    "normal",
				"command": []any{"bash", "-c", "echo hi"},
			},
			wantCode: http.StatusBadRequest,
			wantErr:  "shell_form_disabled",
		},
		// Step 10 sibling: short -lc cluster also tripped.
		{
			name: "step10_shell_lc_cluster",
			body: map[string]any{
				"lane":    "normal",
				"command": []any{"/bin/zsh", "-lc", "echo hi"},
			},
			wantCode: http.StatusBadRequest,
			wantErr:  "shell_form_disabled",
		},
		// Step 11: stdin mode invalid.
		{
			name: "step11_stdin_bad_mode",
			body: map[string]any{
				"lane": "normal", "command": []any{"uptime"},
				"stdin": "weird",
			},
			wantCode: http.StatusBadRequest,
			wantErr:  "bad_request",
		},
		// Step 11: stdin=single missing staging id.
		{
			name: "step11_stdin_single_missing_id",
			body: map[string]any{
				"lane": "normal", "command": []any{"uptime"},
				"stdin": "single",
			},
			wantCode: http.StatusBadRequest,
			wantErr:  "bad_request",
		},
		// Step 11: stdin=single with bad uuid → invalid_staging_id.
		{
			name: "step11_stdin_bad_uuid",
			body: map[string]any{
				"lane": "normal", "command": []any{"uptime"},
				"stdin": "single", "stdin_staging_id": "not-a-uuid",
			},
			wantCode: http.StatusBadRequest,
			wantErr:  "invalid_staging_id",
		},
		// Step 12: script.staging_id bad uuid.
		{
			name: "step12_script_bad_uuid",
			body: map[string]any{
				"lane": "normal", "command": []any{"uptime"},
				"script": map[string]any{"staging_id": "not-a-uuid"},
			},
			wantCode: http.StatusBadRequest,
			wantErr:  "invalid_staging_id",
		},
		// Step 12: in[].staging_id bad uuid.
		{
			name: "step12_in_bad_uuid",
			body: map[string]any{
				"lane": "normal", "command": []any{"uptime"},
				"in": []any{map[string]any{"key": "pdf", "staging_id": "not-a-uuid"}},
			},
			wantCode: http.StatusBadRequest,
			wantErr:  "invalid_staging_id",
		},
		// Step 13: timeout bad format.
		{
			name: "step13_bad_timeout",
			body: map[string]any{
				"lane": "normal", "command": []any{"uptime"},
				"timeout": "not-a-duration",
			},
			wantCode: http.StatusBadRequest,
			wantErr:  "bad_request",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			idem := tc.idem
			if idem == "" {
				idem = baseIdem
			}
			if idem == " " {
				// sentinel — caller wants no header at all
				idem = ""
			}
			var rec *httptest.ResponseRecorder
			if tc.raw != nil {
				rec = doExecDispatch(t, mux, idem, tc.raw)
			} else {
				rec = doExecDispatchJSON(t, mux, idem, tc.body)
			}
			if rec.Code != tc.wantCode {
				t.Errorf("code=%d want=%d body=%s", rec.Code, tc.wantCode, rec.Body.String())
			}
			assertErrorCode(t, rec, tc.wantErr)
		})
	}
}

// TestExecDispatchAllowShellPasses verifies the shell guard is only active
// when allow_shell=false. With true, `bash -c ...` should proceed past
// step 10 — we expect 501 not_implemented from the stub tail.
// TestExecDispatchReplayBeforeReadinessChecks: a valid exec retry must
// replay 200 even if the lane has since disappeared from applied config — the
// idempotency replay must not depend on current readiness.
// Running the 412/400/503 readiness/lane checks before the replay would
// fail a retry of an already-created exec with 412/400 after a config reload.
func TestExecDispatchReplayBeforeReadinessChecks(t *testing.T) {
	hasLane := true
	getApplied := func() (*apply.AppliedState, bool) {
		if hasLane {
			return execAppliedNormal(), true
		}
		// Lane removed / config reloaded between the original exec and retry.
		return &apply.AppliedState{MissionDir: "/tmp/missions", Lanes: map[string]apply.LaneCfg{}}, true
	}
	h, _ := makeExecHandler(t, nil, getApplied)
	mux := execMux(h)

	idem := ids.NewUUIDv7()
	body := map[string]any{"lane": "normal", "command": []any{"uptime"}}

	rec1 := doExecDispatchJSON(t, mux, idem, body)
	if rec1.Code != http.StatusAccepted {
		t.Fatalf("first dispatch: status=%d body=%s, want 202", rec1.Code, rec1.Body.String())
	}

	hasLane = false // lane disappears from applied config

	rec2 := doExecDispatchJSON(t, mux, idem, body)
	if rec2.Code != http.StatusOK {
		t.Fatalf("retry after lane removed: status=%d body=%s, want 200 replay", rec2.Code, rec2.Body.String())
	}
}

func TestExecDispatchAllowShellPasses(t *testing.T) {
	cfg := execTestCfg(t)
	cfg.Exec.AllowShell = true
	h, _ := makeExecHandler(t, cfg, nil)
	mux := execMux(h)

	rec := doExecDispatchJSON(t, mux, ids.NewUUIDv7(), map[string]any{
		"lane":    "normal",
		"command": []any{"bash", "-c", "echo hi"},
	})
	if rec.Code == http.StatusBadRequest && strings.Contains(rec.Body.String(), "shell_form_disabled") {
		t.Fatalf("allow_shell=true should not trip shell guard; body=%s", rec.Body.String())
	}
}

// TestExecDispatchNonShellWithDashCAllowed verifies argv[0] not in the
// fingerprint.IsShellForm shell set passes even with -c args (e.g. `gcc -c`).
// The stub returns 501 once step 10 is bypassed.
func TestExecDispatchNonShellWithDashCAllowed(t *testing.T) {
	cfg := execTestCfg(t)
	h, _ := makeExecHandler(t, cfg, nil)
	mux := execMux(h)

	rec := doExecDispatchJSON(t, mux, ids.NewUUIDv7(), map[string]any{
		"lane":    "normal",
		"command": []any{"gcc", "-c", "foo.c"},
	})
	if rec.Code == http.StatusBadRequest && strings.Contains(rec.Body.String(), "shell_form_disabled") {
		t.Fatalf("non-shell argv[0] should not trip shell guard; body=%s", rec.Body.String())
	}
}

// --- tests: applied state and staging metadata ---

// TestExecDispatchNoLanesConfigured412 asserts a bootstrap dugdale (no
// applied state) returns 412 no_lanes_configured before any other check
// past step 2.
func TestExecDispatchNoLanesConfigured412(t *testing.T) {
	cfg := execTestCfg(t)
	h, _ := makeExecHandler(t, cfg, func() (*apply.AppliedState, bool) { return nil, false })
	mux := execMux(h)

	rec := doExecDispatchJSON(t, mux, ids.NewUUIDv7(), map[string]any{
		"lane":    "normal",
		"command": []any{"uptime"},
	})
	if rec.Code != http.StatusPreconditionFailed {
		t.Errorf("code=%d want=%d body=%s", rec.Code, http.StatusPreconditionFailed, rec.Body.String())
	}
	assertErrorCode(t, rec, "no_lanes_configured")
}

// TestExecDispatchUnknownLane400 asserts 400 unknown_lane when the requested
// lane is not in the applied state.
func TestExecDispatchUnknownLane400(t *testing.T) {
	cfg := execTestCfg(t)
	h, _ := makeExecHandler(t, cfg, nil)
	mux := execMux(h)

	rec := doExecDispatchJSON(t, mux, ids.NewUUIDv7(), map[string]any{
		"lane":    "nonexistent",
		"command": []any{"uptime"},
	})
	if rec.Code != http.StatusBadRequest {
		t.Errorf("code=%d want=%d body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	assertErrorCode(t, rec, "unknown_lane")
}

// TestExecDispatchUnknownStagingRef400 asserts 400 unknown_staging_ref when
// a referenced staging id does not exist in the DB.
func TestExecDispatchUnknownStagingRef400(t *testing.T) {
	cfg := execTestCfg(t)
	h, _ := makeExecHandler(t, cfg, nil)
	mux := execMux(h)

	rec := doExecDispatchJSON(t, mux, ids.NewUUIDv7(), map[string]any{
		"lane":    "normal",
		"command": []any{"uptime"},
		"in": []any{
			map[string]any{"key": "pdf", "staging_id": stagingUUID(t)},
		},
	})
	if rec.Code != http.StatusBadRequest {
		t.Errorf("code=%d want=%d body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	assertErrorCode(t, rec, "unknown_staging_ref")
}

// TestExecDispatchStagingNotComplete400 asserts that a staging row in
// state='uploading' (not complete) yields unknown_staging_ref — refs are
// only valid against finalized staging.
func TestExecDispatchStagingNotComplete400(t *testing.T) {
	cfg := execTestCfg(t)
	h, _ := makeExecHandler(t, cfg, nil)
	mux := execMux(h)

	sid := stagingUUID(t)
	if err := storage.InsertStaging(context.Background(), h.DB, &storage.StagingFile{
		StagingID:     sid,
		State:         storage.StagingUploading,
		Path:          "/tmp/staging/" + sid,
		TimeCreatedMs: 1, TimeUpdatedMs: 1, TimeExpiresMs: 9_999_999_999_999,
	}); err != nil {
		t.Fatalf("insert staging: %v", err)
	}

	rec := doExecDispatchJSON(t, mux, ids.NewUUIDv7(), map[string]any{
		"lane":    "normal",
		"command": []any{"uptime"},
		"in":      []any{map[string]any{"key": "pdf", "staging_id": sid}},
	})
	if rec.Code != http.StatusBadRequest {
		t.Errorf("code=%d want=%d body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	assertErrorCode(t, rec, "unknown_staging_ref")
}

// TestExecDispatchScriptTooLarge400 asserts 400 script_too_large when the
// script staging file exceeds cfg.Exec.MaxScriptSize (1 KiB in execTestCfg).
func TestExecDispatchScriptTooLarge400(t *testing.T) {
	cfg := execTestCfg(t)
	h, _ := makeExecHandler(t, cfg, nil)
	mux := execMux(h)

	sid := stagingUUID(t)
	if err := storage.InsertStaging(context.Background(), h.DB, &storage.StagingFile{
		StagingID:     sid,
		State:         storage.StagingComplete,
		Sha256:        "deadbeef",
		Size:          int64(cfg.Exec.MaxScriptSize) + 1,
		Path:          "/tmp/staging/" + sid,
		TimeCreatedMs: 1, TimeUpdatedMs: 1, TimeExpiresMs: 9_999_999_999_999,
	}); err != nil {
		t.Fatalf("insert staging: %v", err)
	}

	rec := doExecDispatchJSON(t, mux, ids.NewUUIDv7(), map[string]any{
		"lane":    "normal",
		"command": []any{"uptime"},
		"script":  map[string]any{"staging_id": sid},
	})
	if rec.Code != http.StatusBadRequest {
		t.Errorf("code=%d want=%d body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	assertErrorCode(t, rec, "script_too_large")
}

// --- idempotency tests ---

// execFingerprintFor builds the fingerprint a request body would produce so
// pre-seeded rows can match it. Mirrors handler's computeExecFingerprint
// logic in the public fingerprint package.
func execFingerprintFor(t *testing.T, lane string, command []string, timeoutMs *int64) string {
	t.Helper()
	fp, err := fingerprint.Exec(fingerprint.ExecInput{
		Lane:      lane,
		Command:   command,
		Stdin:     "none",
		TimeoutMs: timeoutMs,
	})
	if err != nil {
		t.Fatalf("Exec fingerprint: %v", err)
	}
	return fp
}

// TestExecDispatchIdempotencyReplay200 asserts that re-sending the same body
// with the same idem key against a queued row returns 200 with the current
// status (no new row created).
func TestExecDispatchIdempotencyReplay200(t *testing.T) {
	cfg := execTestCfg(t)
	h, _ := makeExecHandler(t, cfg, nil)
	mux := execMux(h)

	execID := ids.NewUUIDv7()
	fp := execFingerprintFor(t, "normal", []string{"uptime"}, nil)

	// Pre-insert a queued exec row with matching fingerprint.
	if err := storage.InsertMission(context.Background(), h.DB, &storage.Mission{
		ID:               execID,
		Kind:             storage.KindExec,
		Lane:             "normal",
		MissionName:      "exec",
		Status:           storage.StatusQueued,
		Input:            []byte("null"),
		InputFingerprint: fp,
		TimeCreatedMs:    1000,
	}); err != nil {
		t.Fatalf("seed mission: %v", err)
	}

	rec := doExecDispatchJSON(t, mux, execID, map[string]any{
		"lane":    "normal",
		"command": []any{"uptime"},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d want=200 body=%s", rec.Code, rec.Body.String())
	}
	var resp map[string]any
	_ = json.NewDecoder(rec.Body).Decode(&resp)
	if resp["exec_id"] != execID {
		t.Errorf("exec_id: want %q got %v", execID, resp["exec_id"])
	}
	if resp["status"] != "queued" {
		t.Errorf("status: want queued got %v", resp["status"])
	}
}

// TestExecDispatchIdempotencyConflict409 asserts that re-sending a different
// body under an existing idem key returns 409 idempotency_conflict.
func TestExecDispatchIdempotencyConflict409(t *testing.T) {
	cfg := execTestCfg(t)
	h, _ := makeExecHandler(t, cfg, nil)
	mux := execMux(h)

	execID := ids.NewUUIDv7()
	// Seed with a fingerprint built from command=["uptime"].
	fp := execFingerprintFor(t, "normal", []string{"uptime"}, nil)
	if err := storage.InsertMission(context.Background(), h.DB, &storage.Mission{
		ID:               execID,
		Kind:             storage.KindExec,
		Lane:             "normal",
		MissionName:      "exec",
		Status:           storage.StatusQueued,
		Input:            []byte("null"),
		InputFingerprint: fp,
		TimeCreatedMs:    1000,
	}); err != nil {
		t.Fatalf("seed mission: %v", err)
	}

	// Submit different command — fingerprint will differ → 409.
	rec := doExecDispatchJSON(t, mux, execID, map[string]any{
		"lane":    "normal",
		"command": []any{"date"},
	})
	if rec.Code != http.StatusConflict {
		t.Fatalf("code=%d want=409 body=%s", rec.Code, rec.Body.String())
	}
	assertErrorCode(t, rec, "idempotency_conflict")
}

// TestExecDispatchDeletingMatch410 asserts a matching fingerprint against a
// status='deleting' row returns 410 mission_deleting.
func TestExecDispatchDeletingMatch410(t *testing.T) {
	cfg := execTestCfg(t)
	h, _ := makeExecHandler(t, cfg, nil)
	mux := execMux(h)

	execID := ids.NewUUIDv7()
	fp := execFingerprintFor(t, "normal", []string{"uptime"}, nil)
	if err := storage.InsertMission(context.Background(), h.DB, &storage.Mission{
		ID:               execID,
		Kind:             storage.KindExec,
		Lane:             "normal",
		MissionName:      "exec",
		Status:           storage.StatusDeleting,
		Input:            []byte("null"),
		InputFingerprint: fp,
		TimeCreatedMs:    1000,
	}); err != nil {
		t.Fatalf("seed mission: %v", err)
	}

	rec := doExecDispatchJSON(t, mux, execID, map[string]any{
		"lane":    "normal",
		"command": []any{"uptime"},
	})
	if rec.Code != http.StatusGone {
		t.Fatalf("code=%d want=410 body=%s", rec.Code, rec.Body.String())
	}
	assertErrorCode(t, rec, "mission_deleting")
}

// insertCompleteStaging is a one-liner helper for tests that need a
// staging_files row in state='complete' so an exec ref resolves.
func insertCompleteStaging(t *testing.T, db interface {
	// keep helper independent of concrete *sql.DB to dodge import cycles.
}, h *handlers.ExecDispatchHandler, sid, sha string, size int64) {
	t.Helper()
	if err := storage.InsertStaging(context.Background(), h.DB, &storage.StagingFile{
		StagingID:     sid,
		State:         storage.StagingComplete,
		Sha256:        sha,
		Size:          size,
		Path:          "/tmp/staging/" + sid,
		TimeCreatedMs: 1, TimeUpdatedMs: 1, TimeExpiresMs: 9_999_999_999_999,
	}); err != nil {
		t.Fatalf("insert staging %s: %v", sid, err)
	}
}

// --- happy path ---

// TestExecDispatchHappyPath verifies argv-only dispatch produces a 202, a
// kind=exec row in the DB, and an events file with seq=1 queued event.
func TestExecDispatchHappyPath(t *testing.T) {
	cfg := execTestCfg(t)
	h, dataDir := makeExecHandler(t, cfg, nil)
	mux := execMux(h)

	execID := ids.NewUUIDv7()
	rec := doExecDispatchJSON(t, mux, execID, map[string]any{
		"lane":         "normal",
		"command":      []any{"uptime"},
		"display_name": "uptime check",
		"group_id":     "01900000-0000-7000-8000-000000bbb026",
	})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("code=%d want=202 body=%s", rec.Code, rec.Body.String())
	}
	var resp map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp["exec_id"] != execID {
		t.Errorf("exec_id: want %q got %v", execID, resp["exec_id"])
	}
	if resp["status"] != "queued" {
		t.Errorf("status: want queued got %v", resp["status"])
	}

	// Mission row exists with kind=exec.
	m, err := storage.GetMission(context.Background(), h.DB, execID)
	if err != nil {
		t.Fatalf("GetMission: %v", err)
	}
	if m.Kind != storage.KindExec {
		t.Errorf("kind: want exec got %q", m.Kind)
	}
	if m.MissionName != "exec" {
		t.Errorf("mission_name: want 'exec' got %q", m.MissionName)
	}
	if !m.DisplayName.Valid || m.DisplayName.String != "uptime check" {
		t.Errorf("display_name: want valid='uptime check' got %v", m.DisplayName)
	}
	if !m.GroupID.Valid || m.GroupID.String != "01900000-0000-7000-8000-000000bbb026" {
		t.Errorf("group_id: want UUIDv7 got %v", m.GroupID)
	}
	if m.Status != storage.StatusQueued {
		t.Errorf("status: want queued got %q", m.Status)
	}

	// Events file exists at the sharded path with seq=1 queued event.
	shard, _ := ids.ShardPath(execID)
	evPath := filepath.Join(dataDir, "output", shard, execID+"-events")
	raw, err := os.ReadFile(evPath)
	if err != nil {
		t.Fatalf("read events file %s: %v", evPath, err)
	}
	var ev map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(raw), &ev); err != nil {
		t.Fatalf("decode event line: %v (raw=%q)", err, raw)
	}
	if ev["seq"] != float64(1) {
		t.Errorf("seq: want 1 got %v", ev["seq"])
	}
	if ev["event"] != "queued" {
		t.Errorf("event: want queued got %v", ev["event"])
	}
	if ev["mission_id"] != execID {
		t.Errorf("mission_id in event: want %q got %v", execID, ev["mission_id"])
	}
}

// TestExecDispatchHappyPathWithRefs verifies script, in[], out[], and stdin
// staging refs all get persisted with the expected ref_kind / role columns.
func TestExecDispatchHappyPathWithRefs(t *testing.T) {
	cfg := execTestCfg(t)
	h, _ := makeExecHandler(t, cfg, nil)
	mux := execMux(h)

	scriptID := stagingUUID(t)
	insertCompleteStaging(t, nil, h, scriptID, "scriptsha", 128)
	inID := stagingUUID(t)
	insertCompleteStaging(t, nil, h, inID, "insha", 200)
	stdinID := stagingUUID(t)
	insertCompleteStaging(t, nil, h, stdinID, "stdinsha", 64)

	execID := ids.NewUUIDv7()
	rec := doExecDispatchJSON(t, mux, execID, map[string]any{
		"lane":             "normal",
		"command":          []any{"convert", "{in:pdf}", "{out:png}"},
		"script":           map[string]any{"staging_id": scriptID},
		"in":               []any{map[string]any{"key": "pdf", "staging_id": inID}},
		"out":              []any{map[string]any{"key": "png"}},
		"stdin":            "single",
		"stdin_staging_id": stdinID,
	})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("code=%d want=202 body=%s", rec.Code, rec.Body.String())
	}

	refs, err := storage.RefsByMission(context.Background(), h.DB, execID)
	if err != nil {
		t.Fatalf("RefsByMission: %v", err)
	}
	if len(refs) != 3 {
		t.Fatalf("refs: want 3 got %d (%+v)", len(refs), refs)
	}

	// Index for assertion convenience.
	byKey := map[string]storage.StagingRef{}
	for _, r := range refs {
		key := string(r.RefKind) + ":" + r.Role
		byKey[key] = r
	}

	scriptRef, ok := byKey["script:"]
	if !ok {
		t.Errorf("missing script ref (kind=script role='')")
	} else if scriptRef.StagingID != scriptID {
		t.Errorf("script staging_id: want %q got %q", scriptID, scriptRef.StagingID)
	}

	inRef, ok := byKey["input:pdf"]
	if !ok {
		t.Errorf("missing input ref kind=input role=pdf")
	} else if inRef.StagingID != inID {
		t.Errorf("input staging_id: want %q got %q", inID, inRef.StagingID)
	}

	stdinRef, ok := byKey["input:__stdin__"]
	if !ok {
		t.Errorf("missing stdin ref kind=input role=__stdin__")
	} else if stdinRef.StagingID != stdinID {
		t.Errorf("stdin staging_id: want %q got %q", stdinID, stdinRef.StagingID)
	}
}

// TestExecDispatchOrphanCleanup verifies that an existing events file with
// the same exec id is treated as a crash orphan, cleaned up, and dispatch
// proceeds normally.
func TestExecDispatchOrphanCleanup(t *testing.T) {
	cfg := execTestCfg(t)
	h, dataDir := makeExecHandler(t, cfg, nil)
	mux := execMux(h)

	execID := ids.NewUUIDv7()
	shard, _ := ids.ShardPath(execID)
	outDir := filepath.Join(dataDir, "output", shard)
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	orphan := filepath.Join(outDir, execID+"-events")
	if err := os.WriteFile(orphan, []byte("orphan data\n"), 0o644); err != nil {
		t.Fatalf("write orphan: %v", err)
	}

	rec := doExecDispatchJSON(t, mux, execID, map[string]any{
		"lane":    "normal",
		"command": []any{"uptime"},
	})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("code=%d want=202 body=%s", rec.Code, rec.Body.String())
	}

	// The orphan should have been replaced with a fresh single-event file.
	raw, err := os.ReadFile(orphan)
	if err != nil {
		t.Fatalf("read events file: %v", err)
	}
	if bytes.Contains(raw, []byte("orphan data")) {
		t.Errorf("orphan data leaked into new events file: %q", raw)
	}
	if !bytes.Contains(raw, []byte(`"event":"queued"`)) {
		t.Errorf("new events file missing queued line: %q", raw)
	}
}

// --- audit log ---

// TestExecDispatchAuditLogEmitted verifies a successful dispatch produces a
// slog.Info line with audit=true and the canonical action / lane / argv /
// stdin fields (audit format).
func TestExecDispatchAuditLogEmitted(t *testing.T) {
	cfg := execTestCfg(t)

	// Build a handler whose Logger writes JSON to an in-memory buffer so we
	// can decode and assert specific fields.
	var buf bytes.Buffer
	jsonLog := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	h, _ := makeExecHandler(t, cfg, nil)
	h.Logger = jsonLog
	mux := execMux(h)

	execID := ids.NewUUIDv7()
	groupID := "01900000-0000-7000-8000-000000bbb007"
	rec := doExecDispatchJSON(t, mux, execID, map[string]any{
		"lane":     "normal",
		"command":  []any{"uptime"},
		"group_id": groupID,
	})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("code=%d want=202 body=%s", rec.Code, rec.Body.String())
	}

	// Find the audit line among any other lines the handler emitted.
	var audit map[string]any
	for _, line := range bytes.Split(bytes.TrimRight(buf.Bytes(), "\n"), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal(line, &m); err != nil {
			t.Fatalf("decode log line %q: %v", line, err)
		}
		if m["action"] == "exec.dispatch" {
			audit = m
			break
		}
	}
	if audit == nil {
		t.Fatalf("no exec.dispatch audit line found; log: %s", buf.String())
	}
	if audit["audit"] != true {
		t.Errorf("audit field: want true got %v", audit["audit"])
	}
	if audit["exec_id"] != execID {
		t.Errorf("exec_id: want %q got %v", execID, audit["exec_id"])
	}
	if audit["group_id"] != groupID {
		t.Errorf("group_id: want %s got %v", groupID, audit["group_id"])
	}
	if audit["lane"] != "normal" {
		t.Errorf("lane: want normal got %v", audit["lane"])
	}
	if audit["stdin"] != "none" {
		t.Errorf("stdin: want none got %v", audit["stdin"])
	}
	if audit["actor"] != "exec-token" {
		// no Identity in ctx → defaults to exec-token
		t.Errorf("actor: want exec-token got %v", audit["actor"])
	}
	argv, ok := audit["command_argv"].([]any)
	if !ok || len(argv) != 1 || argv[0] != "uptime" {
		t.Errorf("command_argv: want [uptime] got %v", audit["command_argv"])
	}
}

// TestExecDispatchAuditActorAdmin verifies an admin-scope Identity in ctx
// flips the actor field to admin-token (admin tokens reach exec endpoints
// as a superset).
func TestExecDispatchAuditActorAdmin(t *testing.T) {
	cfg := execTestCfg(t)
	var buf bytes.Buffer
	jsonLog := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	h, _ := makeExecHandler(t, cfg, nil)
	h.Logger = jsonLog

	// Bypass the mux/middleware so we can inject an admin Identity directly.
	execID := ids.NewUUIDv7()
	body, _ := json.Marshal(map[string]any{
		"lane": "normal", "command": []any{"uptime"},
	})
	req := httptest.NewRequest("POST", "/v1/exec/dispatch", bytes.NewReader(body))
	req.Header.Set("Idempotency-Key", execID)
	ctx := context.WithValue(req.Context(), middleware.IdentityCtxKey(),
		middleware.Identity{Scope: middleware.ScopeAdmin})
	req = req.WithContext(ctx)
	rec := httptest.NewRecorder()
	h.Dispatch(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("code=%d want=202 body=%s", rec.Code, rec.Body.String())
	}

	for _, line := range bytes.Split(bytes.TrimRight(buf.Bytes(), "\n"), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal(line, &m); err != nil {
			continue
		}
		if m["action"] == "exec.dispatch" {
			if m["actor"] != "admin-token" {
				t.Errorf("actor: want admin-token got %v", m["actor"])
			}
			return
		}
	}
	t.Fatalf("no exec.dispatch audit line found; log: %s", buf.String())
}

// TestExecDispatchAuditLogIncludesSizes verifies the audit line carries the
// resolved byte sizes for every referenced staging file: script_size for the
// script ref, in_sizes parallel to in_keys, and stdin_size for the stdin ref.
// The audit format requires "list of in/out keys, sizes";
// this asserts the missing sizes piece.
func TestExecDispatchAuditLogIncludesSizes(t *testing.T) {
	cfg := execTestCfg(t)
	var buf bytes.Buffer
	jsonLog := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	h, _ := makeExecHandler(t, cfg, nil)
	h.Logger = jsonLog
	mux := execMux(h)

	// Seed staging rows for script (size 700), two in[] entries (sizes 100
	// and 200), and stdin (size 500). All in state='complete' so dispatch
	// can resolve and pin them.
	scriptID := stagingUUID(t)
	in1ID := stagingUUID(t)
	in2ID := stagingUUID(t)
	stdinID := stagingUUID(t)
	for _, s := range []struct {
		id   string
		size int64
	}{
		{scriptID, 700},
		{in1ID, 100},
		{in2ID, 200},
		{stdinID, 500},
	} {
		if err := storage.InsertStaging(context.Background(), h.DB, &storage.StagingFile{
			StagingID:     s.id,
			State:         storage.StagingComplete,
			Sha256:        "deadbeef",
			Size:          s.size,
			Path:          "/tmp/staging/" + s.id,
			TimeCreatedMs: 1, TimeUpdatedMs: 1, TimeExpiresMs: 9_999_999_999_999,
		}); err != nil {
			t.Fatalf("insert staging %s: %v", s.id, err)
		}
	}

	execID := ids.NewUUIDv7()
	rec := doExecDispatchJSON(t, mux, execID, map[string]any{
		"lane":             "normal",
		"command":          []any{"./run.sh"},
		"script":           map[string]any{"staging_id": scriptID},
		"in":               []any{map[string]any{"key": "a", "staging_id": in1ID}, map[string]any{"key": "b", "staging_id": in2ID}},
		"out":              []any{map[string]any{"key": "result"}},
		"stdin":            "single",
		"stdin_staging_id": stdinID,
	})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("code=%d want=202 body=%s", rec.Code, rec.Body.String())
	}

	var audit map[string]any
	for _, line := range bytes.Split(bytes.TrimRight(buf.Bytes(), "\n"), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal(line, &m); err != nil {
			continue
		}
		if m["action"] == "exec.dispatch" {
			audit = m
			break
		}
	}
	if audit == nil {
		t.Fatalf("no exec.dispatch audit line found; log: %s", buf.String())
	}
	if got, want := audit["script_size"], float64(700); got != want {
		t.Errorf("script_size: want %v got %v", want, got)
	}
	if got, want := audit["stdin_size"], float64(500); got != want {
		t.Errorf("stdin_size: want %v got %v", want, got)
	}
	// stdin_staging_id must be present in the audit line so
	// operators can trace which staging artifact fed the exec process
	// stdin. Without it the line only shows mode and size, which is not
	// enough to correlate a leak/incident back to its upload.
	if got, want := audit["stdin_staging_id"], stdinID; got != want {
		t.Errorf("stdin_staging_id: want %q got %v", want, got)
	}
	sizes, ok := audit["in_sizes"].([]any)
	if !ok || len(sizes) != 2 {
		t.Fatalf("in_sizes: want 2-element array got %v", audit["in_sizes"])
	}
	if sizes[0] != float64(100) || sizes[1] != float64(200) {
		t.Errorf("in_sizes: want [100 200] got %v", sizes)
	}
}

// TestExecDispatchAuditLogSizesAbsentForBareCommand verifies that an exec
// without script / in[] / stdin staging refs does not emit script_size or
// stdin_size keys, and in_sizes is empty — keeps audit log tidy when there
// are no staging references to report.
func TestExecDispatchAuditLogSizesAbsentForBareCommand(t *testing.T) {
	cfg := execTestCfg(t)
	var buf bytes.Buffer
	jsonLog := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	h, _ := makeExecHandler(t, cfg, nil)
	h.Logger = jsonLog
	mux := execMux(h)

	rec := doExecDispatchJSON(t, mux, ids.NewUUIDv7(), map[string]any{
		"lane":    "normal",
		"command": []any{"uptime"},
	})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("code=%d want=202 body=%s", rec.Code, rec.Body.String())
	}

	var audit map[string]any
	for _, line := range bytes.Split(bytes.TrimRight(buf.Bytes(), "\n"), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal(line, &m); err != nil {
			continue
		}
		if m["action"] == "exec.dispatch" {
			audit = m
			break
		}
	}
	if audit == nil {
		t.Fatalf("no exec.dispatch audit line found; log: %s", buf.String())
	}
	if _, ok := audit["script_size"]; ok {
		t.Errorf("script_size should be absent when no script ref, got %v", audit["script_size"])
	}
	if _, ok := audit["stdin_size"]; ok {
		t.Errorf("stdin_size should be absent when no stdin ref, got %v", audit["stdin_size"])
	}
	if _, ok := audit["stdin_staging_id"]; ok {
		t.Errorf("stdin_staging_id should be absent when no stdin ref, got %v", audit["stdin_staging_id"])
	}
	sizes, ok := audit["in_sizes"].([]any)
	if !ok {
		t.Errorf("in_sizes should be present (empty array), got %v", audit["in_sizes"])
	} else if len(sizes) != 0 {
		t.Errorf("in_sizes: want empty got %v", sizes)
	}
}

// TestExecDispatchDeletingMismatch409 asserts a mismatched fingerprint
// against a status='deleting' row prefers 409 idempotency_conflict over 410
// (mismatch is the cardinal error).
func TestExecDispatchDeletingMismatch409(t *testing.T) {
	cfg := execTestCfg(t)
	h, _ := makeExecHandler(t, cfg, nil)
	mux := execMux(h)

	execID := ids.NewUUIDv7()
	fp := execFingerprintFor(t, "normal", []string{"uptime"}, nil)
	if err := storage.InsertMission(context.Background(), h.DB, &storage.Mission{
		ID:               execID,
		Kind:             storage.KindExec,
		Lane:             "normal",
		MissionName:      "exec",
		Status:           storage.StatusDeleting,
		Input:            []byte("null"),
		InputFingerprint: fp,
		TimeCreatedMs:    1000,
	}); err != nil {
		t.Fatalf("seed mission: %v", err)
	}

	rec := doExecDispatchJSON(t, mux, execID, map[string]any{
		"lane":    "normal",
		"command": []any{"date"},
	})
	if rec.Code != http.StatusConflict {
		t.Fatalf("code=%d want=409 body=%s", rec.Code, rec.Body.String())
	}
	assertErrorCode(t, rec, "idempotency_conflict")
}
