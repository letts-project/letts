// Package main implements the dugdale daemon binary.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"letts/internal/apply"
	"letts/internal/cleanup"
	"letts/internal/config"
	"letts/internal/lock"
	logpkg "letts/internal/log"
	"letts/internal/metrics"
	"letts/internal/repair"
	"letts/internal/runtime"
	"letts/internal/server/handlers"
	"letts/internal/server/middleware"
	"letts/internal/shutdown"
	"letts/internal/stagingstore"
	"letts/internal/storage"
	"letts/internal/version"
)

func main() {
	os.Exit(run(os.Args, os.Stdout))
}

// Exit codes:
//
//	0 — clean exit
//	1 — runtime failure (db, listen, etc.)
//	2 — flag parse error
//	3 — config error (discovery, parse, permissions, log init)
func run(argv []string, stdout io.Writer) int {
	fs := flag.NewFlagSet(argv[0], flag.ContinueOnError)
	fs.SetOutput(stdout)

	cfgPath := fs.String("config", "", "path to dugdale.yaml")
	checkOnly := fs.Bool("check-config", false, "validate config and exit")
	migrateOnly := fs.Bool("migrate-only", false, "apply migrations and exit")
	insecure := fs.Bool("insecure-config-permissions", false, "skip config permissions check")
	logLevel := fs.String("log-level", "", "override log.level (debug|info|warn|error)")
	logFormat := fs.String("log-format", "", "override log.format (json|text)")
	showVersion := fs.Bool("version", false, "print version and exit")
	fs.Usage = func() {
		_, _ = fmt.Fprintf(stdout, "Usage: %s [flags]\n\nFlags:\n", argv[0])
		fs.PrintDefaults()
	}

	if err := fs.Parse(argv[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		_, _ = fmt.Fprintln(stdout, err)
		return 2
	}

	if *showVersion {
		_, _ = fmt.Fprintf(stdout, "dugdale %s (commit=%s, built_at=%s)\n",
			version.Version, version.Commit, version.BuiltAt)
		return 0
	}

	resolvedCfgPath, err := config.DiscoverDugdale(*cfgPath, os.Getenv("DUGDALE_CONFIG"), config.DefaultDugdaleCandidates())
	if err != nil {
		_, _ = fmt.Fprintln(stdout, "config discovery:", err)
		return 3
	}
	cfg, err := config.LoadDugdaleFile(resolvedCfgPath)
	if err != nil {
		_, _ = fmt.Fprintln(stdout, "config parse:", err)
		return 3
	}
	if err := config.CheckConfigPermissions(resolvedCfgPath, cfg, *insecure); err != nil {
		_, _ = fmt.Fprintln(stdout, err)
		return 3
	}
	// Validate referenced OS paths so typos and
	// permission mistakes surface up-front rather than at first MkdirAll
	// or socket-bind. Reads no state — pure path stat/probe.
	if err := config.ValidateConfigPaths(cfg); err != nil {
		_, _ = fmt.Fprintln(stdout, "config paths:", err)
		return 3
	}
	if *logLevel != "" {
		cfg.Log.Level = *logLevel
	}
	if *logFormat != "" {
		cfg.Log.Format = *logFormat
	}
	if *checkOnly {
		_, _ = fmt.Fprintln(stdout, "ok")
		return 0
	}

	logger, closeLog, err := logpkg.New(cfg.Log)
	if err != nil {
		_, _ = fmt.Fprintln(stdout, "log init:", err)
		return 3
	}
	defer func() { _ = closeLog.Close() }()

	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		logger.Error("mkdir data_dir", "err", err)
		return 1
	}

	host, _ := os.Hostname()
	lk, err := lock.Acquire(filepath.Join(cfg.DataDir, "dugdale.lock"), lock.Info{
		Pid: os.Getpid(), Host: host, Version: version.Version, Listen: cfg.Listen,
	})
	if err != nil {
		logger.Error("data_dir lock", "err", err)
		return 1
	}
	defer func() { _ = lk.Release() }()

	db, err := storage.Open(filepath.Join(cfg.DataDir, "state.db"), storage.Options{CacheSizeKB: cfg.Limits.CacheSize})
	if err != nil {
		logger.Error("open db", "err", err)
		return 1
	}
	defer func() { _ = db.Close() }()
	if err := storage.Migrate(context.Background(), db); err != nil {
		logger.Error("migrate", "err", err)
		return 1
	}
	if *migrateOnly {
		_, _ = fmt.Fprintln(stdout, "migrations applied")
		return 0
	}

	for _, sub := range []string{"output", "staging", "tombstone", "work"} {
		if err := os.MkdirAll(filepath.Join(cfg.DataDir, sub), 0o755); err != nil {
			logger.Error("mkdir subdir", "sub", sub, "err", err)
			return 1
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Startup repair runs synchronously before the listener opens.
	logger.Info("startup repair: finalize intents")
	if err := repair.RepairFinalizeIntents(ctx, cfg, db, logger); err != nil {
		logger.Error("repair intents", "err", err)
	}
	// Order matters: intents first — they carry the exact outcome and flip
	// rows to done; only then can the consistency pass patch done rows whose
	// stream lost its terminal event. Both run before the lost sweep, which
	// ignores done rows entirely.
	logger.Info("startup repair: terminal-event consistency")
	if err := repair.EnsureTerminalEvents(ctx, cfg, db, logger); err != nil {
		logger.Error("terminal-event consistency", "err", err)
	}
	logger.Info("startup repair: running → lost sweep")
	if err := repair.SweepRunningToLost(ctx, cfg, db, logger); err != nil {
		logger.Error("sweep lost", "err", err)
	}
	logger.Info("startup repair: orphan files")
	if err := repair.SweepOrphans(ctx, cfg, db, logger); err != nil {
		logger.Error("sweep orphans", "err", err)
	}

	rt := runtime.NewRuntime(ctx, cfg, db, logger)
	if err := apply.ReplayFromDB(ctx, db, rt.Manager); err != nil {
		logger.Warn("apply replay", "err", err)
	}

	uploadLock := stagingstore.NewUploadLock(cfg.Limits.UploadIdleTimeout, nil)
	uploadLock.Start(ctx, 5*time.Second)
	defer uploadLock.Stop()

	go (&cleanup.MissionCleaner{DB: db, Cfg: cfg, Logger: logger}).Run(ctx)
	go (&cleanup.StagingGC{DB: db, Cfg: cfg, DataDir: cfg.DataDir, UploadLock: uploadLock, Logger: logger}).Run(ctx)
	go (&cleanup.DiskScanner{DB: db, DataDir: cfg.DataDir, Logger: logger}).Run(ctx)
	go (&cleanup.Vacuumer{DB: db, Logger: logger}).Run(ctx)
	go (&metrics.Poller{DB: db, Mgr: rt.Manager, DataDir: cfg.DataDir, Logger: logger}).Run(ctx)

	// Disk-usage monitor for the max_data_dir_size enforcement.
	// dispatch, exec_dispatch, and staging PUT consult its cached Size();
	// mission output collection consults cfg.DiskUsage so the
	// soft cap also applies to mid-mission outputs, not just new work.
	diskUsage := &cleanup.DiskUsageMonitor{DataDir: cfg.DataDir, Logger: logger}
	go diskUsage.Run(ctx)
	cfg.DiskUsage = diskUsage.Size

	publishBuildInfo()

	// Coordinator built first so dispatch/exec_dispatch/health handlers can
	// gate their behavior on the drain phase (stop accepting new
	// requests and flip readyz to 503 when SIGTERM arrives).
	coord := shutdown.New(db, cfg, rt.Manager, rt, logger)

	// flock is per-inode, so an admin who manually rm'd
	// <data_dir>/dugdale.lock could let a second dugdale start re-create
	// the file at a fresh inode and acquire its own flock. The
	// detection window is the two-daemon-against-one-data_dir corruption
	// surface, so check often early (2s for the first 60s of process
	// life, when a rm-then-restart is most likely) and back off after.
	go func() {
		fast := time.NewTicker(2 * time.Second)
		defer fast.Stop()
		fastDeadline := time.Now().Add(60 * time.Second)
		slow := time.NewTicker(30 * time.Second)
		defer slow.Stop()
		check := func() bool {
			if err := lk.Verify(); err != nil {
				logger.Error("data_dir lock file integrity lost; initiating shutdown", "err", err)
				coord.Stop(ctx)
				return true
			}
			return false
		}
		for {
			if time.Now().Before(fastDeadline) {
				select {
				case <-ctx.Done():
					return
				case <-fast.C:
					if check() {
						return
					}
				}
				continue
			}
			select {
			case <-ctx.Done():
				return
			case <-slow.C:
				if check() {
					return
				}
			}
		}
	}()

	mux := http.NewServeMux()
	wireRoutes(mux, cfg, db, rt, uploadLock, logger, coord, diskUsage.Size)

	httpHandler := middleware.RequestLog(logger, mux)
	httpSrv := &http.Server{
		Handler:           httpHandler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	listener, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		logger.Error("listen", "err", err)
		return 1
	}

	sigCh := make(chan os.Signal, 4)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(sigCh)
	go func() {
		for sig := range sigCh {
			logger.Info("signal received", "sig", sig.String())
			coord.Stop(ctx)
		}
	}()

	// When the coordinator finishes its drain, gracefully stop the HTTP
	// server and cancel the long-lived context so background goroutines
	// (cleanup, poller, lane runners) exit cleanly.
	serverDone := make(chan struct{})
	go func() {
		coord.Wait()
		shutdownCtx, sc := context.WithTimeout(context.Background(), 30*time.Second)
		defer sc()
		_ = httpSrv.Shutdown(shutdownCtx)
		cancel()
		close(serverDone)
	}()

	logger.Info("dugdale started", "listen", cfg.Listen, "data_dir", cfg.DataDir)
	if err := httpSrv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("server", "err", err)
		coord.Stop(context.Background())
	}
	<-serverDone
	logger.Info("dugdale stopped cleanly")
	return 0
}

// parseTrustedProxies turns the YAML []string of CIDR notations into
// []*net.IPNet for middleware.ClientIP. Invalid entries are logged and
// dropped (start-up doesn't fail) so a single bad CIDR doesn't take down
// the daemon.
func parseTrustedProxies(raw []string, logger *slog.Logger) []*net.IPNet {
	if len(raw) == 0 {
		return nil
	}
	out := make([]*net.IPNet, 0, len(raw))
	for _, s := range raw {
		_, n, err := net.ParseCIDR(s)
		if err != nil {
			logger.Warn("trusted_proxies: ignoring invalid CIDR", "value", s, "err", err)
			continue
		}
		out = append(out, n)
	}
	return out
}

// wireRoutes constructs every handler with its deps and registers it on mux,
// applying the right per-route middleware (auth scope, body limits).
// The shutdown coordinator is passed so handlers that participate in
// graceful shutdown (dispatch/exec_dispatch refuse new requests; readyz
// flips to 503) can read its current phase.
func wireRoutes(mux *http.ServeMux, cfg *config.DugdaleConfig, db *sql.DB, rt *runtime.Runtime, uploadLock *stagingstore.UploadLock, logger *slog.Logger, coord *shutdown.Coordinator, diskUsage func() int64) {
	// Brute-force tracker for admin/exec auth: 1h sliding TTL,
	// keyed by client IP (XFF-aware when network.trusted_proxies is
	// configured). Dispatch endpoints intentionally skip — see auth.go
	// scopeIncludesProtected.
	trustedCIDRs := parseTrustedProxies(cfg.Network.TrustedProxies, logger)
	authCfg := middleware.AuthConfig{
		Dispatch:         cfg.Auth.Tokens,
		Exec:             cfg.Exec.Tokens,
		Admin:            cfg.Admin.Tokens,
		BruteForce:       middleware.NewBruteForceTracker(1 * time.Hour),
		TrustedProxies:   trustedCIDRs,
		UseXForwardedFor: cfg.Network.UseXForwardedFor,
	}

	// No-auth: health, version, metrics. Health takes a Drain func so /readyz
	// can flip to 503 awaiting_drain during graceful shutdown.
	(&handlers.Health{DB: db, IsDraining: coord.BlockNewDispatches}).Register(mux)
	(&handlers.MetricsHandler{}).Register(mux)

	// Dispatch (Bearer dispatch scope, body limit).
	dispatchHandler := &handlers.DispatchHandler{
		DB: db, Cfg: cfg, DataDir: cfg.DataDir,
		LaneManager: rt.Manager,
		KeyMu:       handlers.NewKeyMutex(),
		GetApplied:  func() (*apply.AppliedState, bool) { return readApplied(db) },
		IsDraining:  coord.BlockNewDispatches,
		DiskUsage:   diskUsage,
	}
	mux.HandleFunc("POST /v1/dispatch",
		middleware.Auth(authCfg, middleware.ScopeDispatch,
			middleware.BodyLimit(cfg.Limits.MaxDispatchBodySize, dispatchHandler.Dispatch)))

	// Exec dispatch (Bearer exec scope, body limit) — gated by cfg.Exec.Enabled.
	// Disabled mode registers a pre-auth 404 stub so the feature surface is
	// indistinguishable from a build without exec support.
	if cfg.Exec.Enabled {
		execDispatchHandler := &handlers.ExecDispatchHandler{
			DB: db, Cfg: cfg, DataDir: cfg.DataDir,
			LaneManager: rt.Manager,
			KeyMu:       handlers.NewKeyMutex(),
			GetApplied:  func() (*apply.AppliedState, bool) { return readApplied(db) },
			Logger:      logger,
			IsDraining:  coord.BlockNewDispatches,
			DiskUsage:   diskUsage,
		}
		mux.HandleFunc("POST /v1/exec/dispatch",
			middleware.Auth(authCfg, middleware.ScopeExec,
				middleware.BodyLimit(cfg.Limits.MaxExecBodySize, execDispatchHandler.Dispatch)))
	} else {
		mux.HandleFunc("POST /v1/exec/dispatch", handlers.ExecDisabledStub())
	}

	// Events stream — dispatch or exec, gated by mission kind.
	eventsHandler := &handlers.EventsHandler{DataDir: cfg.DataDir, DB: db}
	mux.HandleFunc("GET /v1/missions/{id}/events",
		middleware.AuthEither(authCfg,
			[]middleware.Scope{middleware.ScopeDispatch, middleware.ScopeExec},
			eventsHandler.Stream))

	// Output stream — dispatch or exec, gated by mission kind.
	outputHandler := &handlers.OutputHandler{DataDir: cfg.DataDir, DB: db}
	mux.HandleFunc("GET /v1/missions/{id}/output",
		middleware.AuthEither(authCfg,
			[]middleware.Scope{middleware.ScopeDispatch, middleware.ScopeExec},
			outputHandler.Stream))

	// Mission GET — dispatch or exec, gated by mission kind.
	// Listing missions is admin-only.
	missionsHandler := &handlers.MissionsHandler{DB: db}
	mux.HandleFunc("GET /v1/missions/{id}",
		middleware.AuthEither(authCfg,
			[]middleware.Scope{middleware.ScopeDispatch, middleware.ScopeExec},
			missionsHandler.GetByID))
	mux.HandleFunc("GET /v1/missions",
		middleware.Auth(authCfg, middleware.ScopeAdmin, missionsHandler.List))

	// Lifecycle: restart/kill/delete and bulk (admin scope — control endpoints).
	lifecycleHandler := &handlers.LifecycleHandler{
		DB: db, Cfg: cfg, DataDir: cfg.DataDir,
		LaneManager: rt.Manager, Runtime: rt,
		GetApplied: func() (*apply.AppliedState, bool) { return readApplied(db) },
	}
	mux.HandleFunc("POST /v1/missions/{id}/restart",
		middleware.Auth(authCfg, middleware.ScopeAdmin, lifecycleHandler.Restart))
	// kill/bulk handlers JSON-decode their bodies; without
	// BodyLimit a hostile admin-token caller could push gigabyte payloads
	// before we reach our `len(ids) > 1000` check. Apply's body-size cap
	// is a sensible ceiling — these endpoints carry orders of magnitude
	// less data than apply.
	killBodyLimit := cfg.Limits.MaxApplyBodySize
	mux.HandleFunc("POST /v1/missions/{id}/kill",
		middleware.Auth(authCfg, middleware.ScopeAdmin,
			middleware.BodyLimit(killBodyLimit, lifecycleHandler.Kill)))
	mux.HandleFunc("DELETE /v1/missions/{id}",
		middleware.Auth(authCfg, middleware.ScopeAdmin, lifecycleHandler.Delete))
	mux.HandleFunc("POST /v1/missions/bulk-restart",
		middleware.Auth(authCfg, middleware.ScopeAdmin,
			middleware.BodyLimit(killBodyLimit, lifecycleHandler.BulkRestart)))
	mux.HandleFunc("POST /v1/missions/bulk-delete",
		middleware.Auth(authCfg, middleware.ScopeAdmin,
			middleware.BodyLimit(killBodyLimit, lifecycleHandler.BulkDelete)))

	// Admin: apply / state / pause-continue. Killer wired so ForcePrune
	// can signal running missions in lanes being removed.
	adminHandler := &handlers.Admin{DB: db, Manager: rt.Manager, Killer: rt, DataDir: cfg.DataDir}
	mux.HandleFunc("POST /v1/admin/apply",
		middleware.Auth(authCfg, middleware.ScopeAdmin,
			middleware.BodyLimit(cfg.Limits.MaxApplyBodySize, adminHandler.Apply)))
	mux.HandleFunc("GET /v1/admin/state",
		middleware.Auth(authCfg, middleware.ScopeAdmin, adminHandler.State))
	mux.HandleFunc("POST /v1/admin/lanes/{name}/pause",
		middleware.Auth(authCfg, middleware.ScopeAdmin, adminHandler.PauseLane))
	mux.HandleFunc("POST /v1/admin/lanes/{name}/continue",
		middleware.Auth(authCfg, middleware.ScopeAdmin, adminHandler.ContinueLane))

	// Inspect: read-only summary, accessible to all 3 scopes.
	// Holders of exec-only tokens (no dispatch token) MUST be able to read
	// /v1/dugdale and /v1/lanes to enumerate available lanes.
	inspectHandler := &handlers.Inspect{DB: db, Manager: rt.Manager, StartedAt: time.Now()}
	mux.HandleFunc("GET /v1/dugdale",
		middleware.AuthEither(authCfg,
			[]middleware.Scope{middleware.ScopeDispatch, middleware.ScopeExec},
			inspectHandler.Dugdale))
	mux.HandleFunc("GET /v1/lanes",
		middleware.AuthEither(authCfg,
			[]middleware.Scope{middleware.ScopeDispatch, middleware.ScopeExec},
			inspectHandler.Lanes))

	// Staging: PUT/HEAD/GET per-id are shared between dispatch and exec
	// callers (both kinds of mission may stage I/O). DELETE stays admin-only.
	// List is admin-only. by-content is exec-only —
	// dispatch tokens never need content lookup; admin still passes via
	// superset.
	stagingHandler := &handlers.StagingHandler{
		DB: db, Cfg: cfg, DataDir: cfg.DataDir, UploadLock: uploadLock,
		// DELETE ?force=true must retire running referencing missions via
		// the force-delete kill before any row flips to deleting; same
		// runtime and default wait bounds as the mission lifecycle handler.
		Runtime:   rt,
		DiskUsage: diskUsage,
	}
	mux.HandleFunc("PUT /v1/staging/{id}",
		middleware.AuthEither(authCfg,
			[]middleware.Scope{middleware.ScopeDispatch, middleware.ScopeExec},
			stagingHandler.Put))
	mux.HandleFunc("HEAD /v1/staging/{id}",
		middleware.AuthEither(authCfg,
			[]middleware.Scope{middleware.ScopeDispatch, middleware.ScopeExec},
			stagingHandler.Head))
	mux.HandleFunc("GET /v1/staging/{id}",
		middleware.AuthEither(authCfg,
			[]middleware.Scope{middleware.ScopeDispatch, middleware.ScopeExec},
			stagingHandler.Get))
	mux.HandleFunc("DELETE /v1/staging/{id}",
		middleware.Auth(authCfg, middleware.ScopeAdmin, stagingHandler.Delete))
	mux.HandleFunc("GET /v1/staging",
		middleware.Auth(authCfg, middleware.ScopeAdmin, stagingHandler.List))
	mux.HandleFunc("GET /v1/staging/by-content/{sha}",
		middleware.AuthEither(authCfg,
			[]middleware.Scope{middleware.ScopeExec},
			stagingHandler.ByContent))

	_ = logger
}

// publishBuildInfo sets the Prometheus letts_dugdale_info gauge from the
// linker-overridable version package. Wrapped in its own function so tests
// can exercise the wiring without booting the whole daemon. Override values
// at build time via:
//
//	go build -ldflags "-X letts/internal/version.Version=$(git describe) \
//	                   -X letts/internal/version.Commit=$(git rev-parse HEAD)"
func publishBuildInfo() {
	metrics.SetInfo(version.Version, version.Commit)
}

// readApplied reads the current applied config from DB. Used by the dispatch
// handler's GetApplied callback (recomputed per call — cheap query, avoids
// invalidation logic when the admin endpoint mutates state).
func readApplied(db *sql.DB) (*apply.AppliedState, bool) {
	a, err := storage.GetAppliedConfig(context.Background(), db)
	if err != nil || a == nil {
		return nil, false
	}
	var state apply.AppliedState
	if err := json.Unmarshal(a.Data, &state); err != nil {
		return nil, false
	}
	return &state, true
}
