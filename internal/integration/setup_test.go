// Package integration_test holds end-to-end tests for the crash-consistency
// matrix: the crash and recovery scenarios the daemon must survive without
// losing or corrupting mission state.
//
// Coverage map (scenario → test that exercises it):
//
//	dispatch crash after events fsync, before DB insert
//	      → covered by orphan-events sweep in repair.SweepOrphans
//	        (internal/repair/repair_test.go: TestSweepOrphansRemovesUnknownOutputFiles).
//	dispatch crash after DB insert, before lane notify
//	      → covered by lane runner's periodic ticker reading queued rows
//	        (internal/lane/runner_test.go: TestRunnerConcurrencyLimit).
//	pickup crash after running update, before spawn
//	      → covered by repair.SweepRunningToLost
//	        (internal/repair/repair_test.go: TestSweepRunningToLost*).
//	fd3 oversized line without newline
//	      → integration_test.go: TestFd3OversizedLineRecoversAndDrains.
//	cmd exit after success, crash before finalize intent
//	      → covered by SweepRunningToLost: row left running → swept to lost
//	        (internal/repair/repair_test.go: TestSweepRunningToLost*).
//	crash after intent prepared, before committing
//	      → internal/repair/intents_test.go:
//	        TestRepairPreparedWithOutputsAllTmpExists.
//	crash after committing, before tmp rename
//	      → internal/repair/intents_test.go: TestRepairCommittingFinishesRenames.
//	crash after rename, before public done append
//	      → internal/repair/intents_test.go: TestRepairCommittingFinalAlreadyExistsSkipsRename.
//	crash after public done, before DB done update
//	      → AppendDoneIdempotent ensures replay is no-op
//	        (internal/eventfile/writer_test.go: TestAppendDoneIdempotent),
//	        and intent commit re-runs the SQL UPDATE (CommitFromIntent).
//	pending_output row missing tmp, ± intent
//	      → integration_test.go: TestPendingOutputWithoutIntentRepairDrops
//	        and internal/repair/intents_test.go: TestRepairPreparedTmpMissingRevertsToFailed.
//	cleanup crash after status=deleting, before SQL DELETE
//	      → internal/cleanup/missions_test.go: TestMissionCleanupResumeOrphanDeleting.
//	staging upload incomplete-states matrix
//	      → internal/server/handlers/staging_put_test.go: full state matrix
//	        (initial/resume/wrong sha/wrong range/oversized/short body)
//	        and internal/stagingstore/upload_lock_test.go: idle-timeout janitor.
//	idempotency replay across statuses
//	      → integration_test.go: TestIdempotencyReplayMatrix.
//	restart with retained vs expired refs
//	      → integration_test.go: TestRestartWithRetainedAndExpiredRefs.
//	metrics route labels never include UUIDs
//	      → integration_test.go: TestMetricsRouteLabelsNeverContainUUIDs
//	        and internal/server/middleware/reqlog_test.go: TestRequestLogObservesMetricsByRouteTemplate.
package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"letts/internal/apply"
	"letts/internal/config"
	"letts/internal/runtime"
	"letts/internal/server/handlers"
	"letts/internal/server/middleware"
	"letts/internal/stagingstore"
	"letts/internal/storage"
)

// stack is the integration-test fixture: a fully-wired dugdale stack with a
// real httptest.Server you can drive with a stdlib http.Client.
type stack struct {
	t          *testing.T
	srv        *httptest.Server
	DB         *sql.DB
	Cfg        *config.DugdaleConfig
	DataDir    string
	Runtime    *runtime.Runtime
	UploadLock *stagingstore.UploadLock
	cancel     context.CancelFunc
}

// newTestStack constructs a complete dugdale stack and returns the live
// httptest.Server URL. t.Cleanup tears everything down.
func newTestStack(t *testing.T) *stack {
	t.Helper()
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	for _, sub := range []string{"output", "staging", "tombstone", "work"} {
		if err := os.MkdirAll(filepath.Join(dataDir, sub), 0o755); err != nil {
			t.Fatal(err)
		}
	}

	db, err := storage.Open(filepath.Join(dataDir, "state.db"), storage.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := storage.Migrate(context.Background(), db); err != nil {
		t.Fatal(err)
	}

	cfg := &config.DugdaleConfig{
		DataDir: dataDir,
		Listen:  "127.0.0.1:0",
		Auth:    config.AuthConfig{Tokens: []string{"disp-token"}},
		Admin:   config.AdminConfig{Tokens: []string{"admin-token"}},
		Cleanup: config.CleanupConfig{
			SuccessTTL: time.Hour, FailedTTL: 24 * time.Hour,
			StagingTTL: time.Hour, DownloadedGrace: time.Hour,
			LostCleanupGrace: 10 * time.Minute, SweepInterval: time.Minute,
		},
		Limits: config.LimitsConfig{
			MaxDispatchBodySize:  1 * 1024 * 1024,
			MaxApplyBodySize:     1 * 1024 * 1024,
			MaxStagingUploadSize: 10 * 1024 * 1024,
			MaxMissionInputSize:  64 * 1024,
			MaxOutputBuffer:      8 * 1024 * 1024,
			MaxEventsBuffer:      1024 * 1024,
			MaxEventLineSize:     1024 * 1024,
			MaxReturnValueSize:   64 * 1024,
			MaxFailMessageSize:   64 * 1024,
			MaxFailDetailsSize:   64 * 1024,
			MaxOutputFilesPerMsn: 16,
			MaxProgressRate:      100,
			ProgressBufferSize:   16 * 1024,
			DefaultKillGrace:     150 * time.Millisecond,
			ReaderPostExitGrace:  300 * time.Millisecond,
		},
		Exec: config.ExecConfig{
			ExecSuccessTTL: time.Hour, ExecFailedTTL: 24 * time.Hour,
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	rt := runtime.NewRuntime(ctx, cfg, db, slog.Default())
	uploadLock := stagingstore.NewUploadLock(time.Minute, nil)

	mux := http.NewServeMux()
	wireRoutesForTest(mux, cfg, db, rt, uploadLock)
	httpHandler := middleware.RequestLog(slog.Default(), mux)

	srv := httptest.NewServer(httpHandler)

	t.Cleanup(func() {
		srv.Close()
		cancel()
		uploadLock.Stop()
		rt.Manager.StopAll()
		_ = db.Close()
	})

	return &stack{
		t: t, srv: srv, DB: db, Cfg: cfg, DataDir: dataDir,
		Runtime: rt, UploadLock: uploadLock, cancel: cancel,
	}
}

// wireRoutesForTest mirrors cmd/dugdale/main.go wireRoutes; duplicated here
// so this package doesn't import cmd/dugdale (which would create a build
// cycle with internal/integration).
func wireRoutesForTest(mux *http.ServeMux, cfg *config.DugdaleConfig, db *sql.DB, rt *runtime.Runtime, uploadLock *stagingstore.UploadLock) {
	authCfg := middleware.AuthConfig{
		Dispatch: cfg.Auth.Tokens,
		Admin:    cfg.Admin.Tokens,
	}

	(&handlers.Health{DB: db}).Register(mux)
	(&handlers.MetricsHandler{}).Register(mux)

	dispatchHandler := &handlers.DispatchHandler{
		DB: db, Cfg: cfg, DataDir: cfg.DataDir,
		LaneManager: rt.Manager, KeyMu: handlers.NewKeyMutex(),
		GetApplied: func() (*apply.AppliedState, bool) {
			a, err := storage.GetAppliedConfig(context.Background(), db)
			if err != nil || a == nil {
				return nil, false
			}
			var s apply.AppliedState
			if json.Unmarshal(a.Data, &s) != nil {
				return nil, false
			}
			return &s, true
		},
	}
	mux.HandleFunc("POST /v1/dispatch",
		middleware.Auth(authCfg, middleware.ScopeDispatch,
			middleware.BodyLimit(cfg.Limits.MaxDispatchBodySize, dispatchHandler.Dispatch)))

	(&handlers.EventsHandler{DataDir: cfg.DataDir, DB: db}).Register(mux)
	(&handlers.OutputHandler{DataDir: cfg.DataDir, DB: db}).Register(mux)
	(&handlers.MissionsHandler{DB: db}).Register(mux)

	// Wrap lifecycle routes with admin auth so the kind-gate inside
	// restartOne/deleteOne can read the caller's scope from ctx. Without
	// this, every restart/delete would 500 "missing identity" — matches
	// the production wiring in cmd/dugdale/main.go.
	lifecycleHandler := &handlers.LifecycleHandler{
		DB: db, Cfg: cfg, DataDir: cfg.DataDir,
		LaneManager: rt.Manager, Runtime: rt,
		GetApplied: func() (*apply.AppliedState, bool) {
			a, err := storage.GetAppliedConfig(context.Background(), db)
			if err != nil || a == nil {
				return nil, false
			}
			var s apply.AppliedState
			if json.Unmarshal(a.Data, &s) != nil {
				return nil, false
			}
			return &s, true
		},
	}
	mux.HandleFunc("POST /v1/missions/{id}/restart",
		middleware.Auth(authCfg, middleware.ScopeAdmin, lifecycleHandler.Restart))
	mux.HandleFunc("POST /v1/missions/{id}/kill",
		middleware.Auth(authCfg, middleware.ScopeAdmin, lifecycleHandler.Kill))
	mux.HandleFunc("DELETE /v1/missions/{id}",
		middleware.Auth(authCfg, middleware.ScopeAdmin, lifecycleHandler.Delete))
	mux.HandleFunc("POST /v1/missions/bulk-restart",
		middleware.Auth(authCfg, middleware.ScopeAdmin, lifecycleHandler.BulkRestart))
	mux.HandleFunc("POST /v1/missions/bulk-delete",
		middleware.Auth(authCfg, middleware.ScopeAdmin, lifecycleHandler.BulkDelete))

	(&handlers.Admin{DB: db, Manager: rt.Manager, Killer: rt, DataDir: cfg.DataDir}).Register(mux)
	(&handlers.Inspect{DB: db, Manager: rt.Manager, StartedAt: time.Now()}).Register(mux)
	(&handlers.StagingHandler{DB: db, Cfg: cfg, DataDir: cfg.DataDir, UploadLock: uploadLock}).Register(mux)
}

// applyMinimalLanes posts /v1/admin/apply with the "normal" lane PAUSED so
// dispatch's readiness check passes but no lane runner picks queued missions
// out from under tests that want to manipulate mission rows directly.
// Tests that actually need a mission to run (e.g. T4) call applyAndResume
// to start pickups after their setup is in place.
func (s *stack) applyMinimalLanes() {
	s.t.Helper()
	s.applyLanes(true)
}

func (s *stack) applyAndResume() {
	s.t.Helper()
	s.applyLanes(false)
}

func (s *stack) applyLanes(paused bool) {
	s.t.Helper()
	body := map[string]any{
		"mission_dir": s.t.TempDir(),
		"lanes": map[string]any{
			"normal": map[string]any{"concurrency": 2, "paused": paused},
		},
		"runtime": map[string]any{
			"validate_mission_file": false,
		},
	}
	b, _ := json.Marshal(body)
	r, _ := http.NewRequest("POST", s.srv.URL+"/v1/admin/apply", strings.NewReader(string(b)))
	r.Header.Set("Authorization", "Bearer admin-token")
	r.Header.Set("Content-Type", "application/json")
	resp, err := s.srv.Client().Do(r)
	if err != nil {
		s.t.Fatalf("apply lanes: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(resp.Body)
		s.t.Fatalf("apply lanes: status=%d body=%s", resp.StatusCode, body)
	}
}

// dispatchOnce posts /v1/dispatch with the given Idempotency-Key and returns
// (status, body). Lane defaults to "normal", input is {} unless overridden.
func (s *stack) dispatchOnce(idemKey, lane string, inputJSON string) (int, map[string]any) {
	s.t.Helper()
	if inputJSON == "" {
		inputJSON = `{}`
	}
	body := `{"mission":"FixtureMission","lane":"` + lane + `","input":` + inputJSON + `}`
	r, _ := http.NewRequest("POST", s.srv.URL+"/v1/dispatch", strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer disp-token")
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Idempotency-Key", idemKey)
	resp, err := s.srv.Client().Do(r)
	if err != nil {
		s.t.Fatalf("dispatch: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	bodyBytes, _ := io.ReadAll(resp.Body)
	var parsed map[string]any
	_ = json.Unmarshal(bodyBytes, &parsed)
	return resp.StatusCode, parsed
}

// markMissionStatus is a test-helper that flips a row's status directly in
// SQL. Used to simulate state transitions for replay/restart tests without
// running the full lifecycle pipeline.
func (s *stack) markMissionStatus(id, status string) {
	s.t.Helper()
	if _, err := s.DB.Exec(`UPDATE missions SET status=? WHERE mission_id=?`, status, id); err != nil {
		s.t.Fatal(err)
	}
}
