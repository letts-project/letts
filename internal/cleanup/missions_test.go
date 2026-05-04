package cleanup_test

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"letts/internal/cleanup"
	"letts/internal/config"
	"letts/internal/ids"
	"letts/internal/storage"
)

func setupCleanupDB(t *testing.T) *sql.DB {
	t.Helper()
	dir := t.TempDir()
	db, err := storage.Open(filepath.Join(dir, "state.db"), storage.Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := storage.Migrate(context.Background(), db); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return db
}

func cleanupCfg(dataDir string) *config.DugdaleConfig {
	return &config.DugdaleConfig{
		DataDir: dataDir,
		Cleanup: config.CleanupConfig{
			SuccessTTL:       1 * time.Hour,
			FailedTTL:        7 * 24 * time.Hour,
			StagingTTL:       1 * time.Hour,
			DownloadedGrace:  1 * time.Hour,
			LostCleanupGrace: 10 * time.Minute,
			SweepInterval:    time.Minute,
		},
		Exec: config.ExecConfig{
			ExecSuccessTTL: 1 * time.Hour,
			ExecFailedTTL:  24 * time.Hour,
		},
	}
}

func insertCleanupMission(t *testing.T, db *sql.DB, kind storage.Kind, status storage.Status, outcome string, finishedMs int64) string {
	t.Helper()
	id := ids.NewUUIDv7()
	m := storage.Mission{
		ID: id, Kind: kind, Lane: "normal",
		MissionName: "CleanupFixture", Status: status,
		Input: []byte(`{}`), InputFingerprint: "fp",
		TimeCreatedMs: finishedMs - 1000,
	}
	if outcome != "" {
		m.Outcome = sql.NullString{String: outcome, Valid: true}
	}
	if finishedMs > 0 {
		m.TimeFinishedMs = sql.NullInt64{Int64: finishedMs, Valid: true}
	}
	if err := storage.InsertMission(context.Background(), db, &m); err != nil {
		t.Fatalf("insert: %v", err)
	}
	return id
}

func writeOutputArtifacts(t *testing.T, dataDir, id string) {
	t.Helper()
	shard, _ := ids.ShardPath(id)
	outDir := filepath.Join(dataDir, "output", shard)
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, sfx := range []string{"-stdout", "-stderr", "-combined", "-events"} {
		if err := os.WriteFile(filepath.Join(outDir, id+sfx), []byte("data"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	workDir := filepath.Join(dataDir, "work", id)
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workDir, "input"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func countMissions(t *testing.T, db *sql.DB) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM missions`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func newCleaner(db *sql.DB, cfg *config.DugdaleConfig) *cleanup.MissionCleaner {
	return &cleanup.MissionCleaner{
		DB: db, Cfg: cfg, Logger: slog.Default(),
		BatchInterPause:    1 * time.Millisecond,
		MaxBatchesPerSweep: 100,
	}
}

func TestMissionCleanupRemovesExpiredSuccess(t *testing.T) {
	db := setupCleanupDB(t)
	dataDir := t.TempDir()
	cfg := cleanupCfg(dataDir)

	old := time.Now().Add(-2 * time.Hour).UnixMilli() // > SuccessTTL=1h
	id := insertCleanupMission(t, db, storage.KindMission, storage.StatusDone, "success", old)
	writeOutputArtifacts(t, dataDir, id)

	c := newCleaner(db, cfg)
	c.RunOnce(context.Background())

	if countMissions(t, db) != 0 {
		t.Errorf("missions still present: %d", countMissions(t, db))
	}
	shard, _ := ids.ShardPath(id)
	for _, sfx := range []string{"-stdout", "-stderr", "-combined", "-events"} {
		if _, err := os.Stat(filepath.Join(dataDir, "output", shard, id+sfx)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("file not removed: %s", sfx)
		}
	}
	if _, err := os.Stat(filepath.Join(dataDir, "work", id)); !errors.Is(err, os.ErrNotExist) {
		t.Error("work dir not removed")
	}
}

func TestMissionCleanupKeepsRecentSuccess(t *testing.T) {
	db := setupCleanupDB(t)
	dataDir := t.TempDir()
	cfg := cleanupCfg(dataDir)

	recent := time.Now().Add(-30 * time.Minute).UnixMilli() // < SuccessTTL
	id := insertCleanupMission(t, db, storage.KindMission, storage.StatusDone, "success", recent)
	writeOutputArtifacts(t, dataDir, id)

	c := newCleaner(db, cfg)
	c.RunOnce(context.Background())

	if countMissions(t, db) != 1 {
		t.Errorf("missions=%d, want 1 (kept)", countMissions(t, db))
	}
}

func TestMissionCleanupRemovesExpiredFailed(t *testing.T) {
	db := setupCleanupDB(t)
	dataDir := t.TempDir()
	cfg := cleanupCfg(dataDir)
	cfg.Cleanup.FailedTTL = 1 * time.Hour

	old := time.Now().Add(-2 * time.Hour).UnixMilli()
	insertCleanupMission(t, db, storage.KindMission, storage.StatusDone, "failed", old)

	c := newCleaner(db, cfg)
	c.RunOnce(context.Background())

	if countMissions(t, db) != 0 {
		t.Errorf("failed mission not removed: count=%d", countMissions(t, db))
	}
}

func TestMissionCleanupSkipsRunningAndQueued(t *testing.T) {
	db := setupCleanupDB(t)
	dataDir := t.TempDir()
	cfg := cleanupCfg(dataDir)

	old := time.Now().Add(-2 * time.Hour).UnixMilli()
	insertCleanupMission(t, db, storage.KindMission, storage.StatusRunning, "", old)
	insertCleanupMission(t, db, storage.KindMission, storage.StatusQueued, "", old)

	c := newCleaner(db, cfg)
	c.RunOnce(context.Background())

	if countMissions(t, db) != 2 {
		t.Errorf("missions=%d, want 2 (untouched)", countMissions(t, db))
	}
}

func TestMissionCleanupResumeOrphanDeleting(t *testing.T) {
	db := setupCleanupDB(t)
	dataDir := t.TempDir()
	cfg := cleanupCfg(dataDir)

	// Simulate prior crash: row in deleting state, files already gone.
	id := insertCleanupMission(t, db, storage.KindMission, storage.StatusDeleting, "success", time.Now().UnixMilli())

	c := newCleaner(db, cfg)
	c.RunOnce(context.Background())

	if countMissions(t, db) != 0 {
		t.Errorf("orphan deleting not resumed: count=%d", countMissions(t, db))
	}
	_ = id
}

// TestMissionCleanupDeletingWithIntentIsDeferred: a 'deleting' row that still
// carries an unapplied finalize intent (admin force-delete racing a live
// finalize, or a crash window before startup repair ran) must NOT be drained.
// Hard-deleting it would CASCADE the intent away and orphan the finalize's
// Phase-B staging work. Once the finalize/repair machinery clears the intent,
// the next sweep drains the row as usual.
func TestMissionCleanupDeletingWithIntentIsDeferred(t *testing.T) {
	db := setupCleanupDB(t)
	dataDir := t.TempDir()
	cfg := cleanupCfg(dataDir)

	id := insertCleanupMission(t, db, storage.KindMission, storage.StatusDeleting, "", 0)
	if err := storage.InsertFinalizeIntent(context.Background(), db, &storage.FinalizeIntent{
		MissionID: id, Phase: storage.PhasePrepared, Outcome: "success",
		Outputs: []byte("[]"), DoneSeq: 1, DoneEvent: `{"event":"done"}`,
		TimeCreatedMs: time.Now().UnixMilli(),
	}); err != nil {
		t.Fatalf("insert intent: %v", err)
	}

	c := newCleaner(db, cfg)
	c.RunOnce(context.Background())

	if countMissions(t, db) != 1 {
		t.Fatal("deleting row with unapplied finalize intent was drained; intent CASCADE-lost")
	}

	// The intent is applied (or discarded) by finalize/repair; the drain
	// must then pick the row up on its next sweep.
	if err := storage.DeleteFinalizeIntent(context.Background(), db, id); err != nil {
		t.Fatalf("delete intent: %v", err)
	}
	c.RunOnce(context.Background())
	if countMissions(t, db) != 0 {
		t.Errorf("deleting row not drained after intent cleared: count=%d", countMissions(t, db))
	}
}

func TestMissionCleanupLostRespectsGrace(t *testing.T) {
	db := setupCleanupDB(t)
	dataDir := t.TempDir()
	cfg := cleanupCfg(dataDir)
	cfg.Cleanup.FailedTTL = 1 * time.Millisecond
	cfg.Cleanup.LostCleanupGrace = 30 * time.Minute

	// finished 10m ago — within grace; should be kept.
	recent := time.Now().Add(-10 * time.Minute).UnixMilli()
	insertCleanupMission(t, db, storage.KindMission, storage.StatusDone, "lost", recent)

	c := newCleaner(db, cfg)
	c.RunOnce(context.Background())

	if countMissions(t, db) != 1 {
		t.Errorf("lost mission removed within grace: count=%d", countMissions(t, db))
	}

	// finished 1h ago — past grace; should go.
	old := time.Now().Add(-1 * time.Hour).UnixMilli()
	insertCleanupMission(t, db, storage.KindMission, storage.StatusDone, "lost", old)
	c.RunOnce(context.Background())

	if countMissions(t, db) != 1 { // recent one kept, old one gone
		t.Errorf("count=%d; expected only the recent lost mission", countMissions(t, db))
	}
}

// An exec mission marked outcome='lost' must use exec_failed_ttl
// + lost_cleanup_grace, not the mission-side failed_ttl + grace. The
// previous implementation shared one cutoffLost for both kinds, so an
// exec that crashed and was reclassified `lost` would linger for
// mission-side failed_ttl (default 7d) instead of exec_failed_ttl
// (default 24h). Test: short ExecFailedTTL, long FailedTTL — the exec
// lost row goes, the mission lost row stays.
func TestMissionCleanupLostRespectsExecTTL(t *testing.T) {
	db := setupCleanupDB(t)
	dataDir := t.TempDir()
	cfg := cleanupCfg(dataDir)
	cfg.Cleanup.FailedTTL = 24 * time.Hour
	cfg.Exec.ExecFailedTTL = 5 * time.Minute
	cfg.Cleanup.LostCleanupGrace = 1 * time.Minute

	// Finished 15m ago — past exec ExecFailedTTL + grace (=6m) but well
	// within mission FailedTTL + grace (=24h1m).
	old := time.Now().Add(-15 * time.Minute).UnixMilli()
	insertCleanupMission(t, db, storage.KindExec, storage.StatusDone, "lost", old)
	insertCleanupMission(t, db, storage.KindMission, storage.StatusDone, "lost", old)

	c := newCleaner(db, cfg)
	c.RunOnce(context.Background())

	if countMissions(t, db) != 1 {
		t.Errorf("count=%d, want 1 (mission lost kept, exec lost removed)", countMissions(t, db))
	}
	var kind string
	if err := db.QueryRow(`SELECT kind FROM missions LIMIT 1`).Scan(&kind); err != nil {
		t.Fatal(err)
	}
	if kind != "mission" {
		t.Errorf("surviving kind=%q, want mission (exec should have been purged)", kind)
	}
}

func TestMissionCleanupExecHasOwnTTL(t *testing.T) {
	db := setupCleanupDB(t)
	dataDir := t.TempDir()
	cfg := cleanupCfg(dataDir)
	cfg.Cleanup.SuccessTTL = 24 * time.Hour
	cfg.Exec.ExecSuccessTTL = 1 * time.Minute

	old := time.Now().Add(-10 * time.Minute).UnixMilli() // > exec ttl, < mission ttl
	insertCleanupMission(t, db, storage.KindExec, storage.StatusDone, "success", old)
	insertCleanupMission(t, db, storage.KindMission, storage.StatusDone, "success", old)

	c := newCleaner(db, cfg)
	c.RunOnce(context.Background())

	if countMissions(t, db) != 1 {
		t.Errorf("count=%d, want 1 (mission kept, exec removed)", countMissions(t, db))
	}
	// Verify the surviving row is the mission.
	var kind string
	if err := db.QueryRow(`SELECT kind FROM missions LIMIT 1`).Scan(&kind); err != nil {
		t.Fatal(err)
	}
	if kind != "mission" {
		t.Errorf("kind=%q, want mission", kind)
	}
}

func TestMissionCleanupBatchedHandlesManyVictims(t *testing.T) {
	db := setupCleanupDB(t)
	dataDir := t.TempDir()
	cfg := cleanupCfg(dataDir)

	old := time.Now().Add(-2 * time.Hour).UnixMilli()
	const N = 1100 // crosses the 1000-row batch boundary
	for i := 0; i < N; i++ {
		insertCleanupMission(t, db, storage.KindMission, storage.StatusDone, "success", old+int64(i))
	}
	if countMissions(t, db) != N {
		t.Fatalf("setup: %d", countMissions(t, db))
	}

	c := newCleaner(db, cfg)
	c.RunOnce(context.Background())

	if countMissions(t, db) != 0 {
		t.Errorf("after sweep: %d remaining", countMissions(t, db))
	}
}

func TestMissionCleanupRecalcsAffectedStaging(t *testing.T) {
	db := setupCleanupDB(t)
	dataDir := t.TempDir()
	cfg := cleanupCfg(dataDir)

	old := time.Now().Add(-2 * time.Hour).UnixMilli()
	mID := insertCleanupMission(t, db, storage.KindMission, storage.StatusDone, "success", old)

	stagingID := ids.NewUUIDv7()
	now := time.Now().UnixMilli()
	if err := storage.InsertStaging(context.Background(), db, &storage.StagingFile{
		StagingID: stagingID, State: storage.StagingComplete,
		Sha256: "abc", Size: 1, BytesReceived: 1,
		Path: "p", TimeCreatedMs: now, TimeUpdatedMs: now, TimeExpiresMs: now + 60_000,
	}); err != nil {
		t.Fatal(err)
	}
	if err := storage.InsertRef(context.Background(), db, storage.StagingRef{
		MissionID: mID, StagingID: stagingID, RefKind: storage.RefInput, Role: "in",
	}); err != nil {
		t.Fatal(err)
	}

	c := newCleaner(db, cfg)
	c.RunOnce(context.Background())

	if countMissions(t, db) != 0 {
		t.Errorf("mission not removed")
	}
	// Staging row remains but TTL was recalculated for the now-orphan staging.
	sf, err := storage.GetStaging(context.Background(), db, stagingID)
	if err != nil {
		t.Fatalf("staging: %v", err)
	}
	// orphan with no downloaded_at → time_expires = TimeCreated + StagingTTL.
	expected := sf.TimeCreatedMs + cfg.Cleanup.StagingTTL.Milliseconds()
	if sf.TimeExpiresMs != expected {
		t.Errorf("staging TimeExpires=%d, want %d", sf.TimeExpiresMs, expected)
	}
}

func TestCleanupExecMissionPurgesAndRecalcsStagingTTL(t *testing.T) {
	db := setupCleanupDB(t)
	dataDir := t.TempDir()
	cfg := cleanupCfg(dataDir)
	cfg.Exec.ExecSuccessTTL = 1 * time.Hour
	cfg.Cleanup.StagingTTL = 1 * time.Hour

	// Seed an exec mission that finished 2h ago (> ExecSuccessTTL=1h).
	old := time.Now().Add(-2 * time.Hour).UnixMilli()
	mID := insertCleanupMission(t, db, storage.KindExec, storage.StatusDone, "success", old)
	writeOutputArtifacts(t, dataDir, mID)

	// Seed a staging file referenced by the exec mission.
	stagingID := ids.NewUUIDv7()
	now := time.Now().UnixMilli()
	if err := storage.InsertStaging(context.Background(), db, &storage.StagingFile{
		StagingID: stagingID, State: storage.StagingComplete,
		Sha256: "sha-x", Size: 100, BytesReceived: 100,
		Path:          "p",
		TimeCreatedMs: now, TimeUpdatedMs: now,
		// Pin TimeExpires far in the future — recalc should pull it back to orphan window.
		TimeExpiresMs: now + 7*24*60*60*1000,
	}); err != nil {
		t.Fatalf("insert staging: %v", err)
	}
	if err := storage.InsertRef(context.Background(), db, storage.StagingRef{
		MissionID: mID, StagingID: stagingID, RefKind: storage.RefInput, Role: "data",
	}); err != nil {
		t.Fatalf("insert ref: %v", err)
	}

	c := newCleaner(db, cfg)
	c.RunOnce(context.Background())

	// Exec mission row is gone (purged on exec_success_ttl).
	if _, err := storage.GetMission(context.Background(), db, mID); !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("exec mission still present after cleanup: err=%v", err)
	}

	// Staging row remains, but TTL was recalc'd to orphan window
	// (TimeCreated + StagingTTL), no longer pinned by the deleted mission.
	sf, err := storage.GetStaging(context.Background(), db, stagingID)
	if err != nil {
		t.Fatalf("staging gone too early: %v", err)
	}
	expected := sf.TimeCreatedMs + cfg.Cleanup.StagingTTL.Milliseconds()
	if sf.TimeExpiresMs != expected {
		t.Errorf("staging TimeExpires=%d, want %d (orphan TimeCreated+StagingTTL)",
			sf.TimeExpiresMs, expected)
	}
}

func TestMissionCleanupRunHonorsCtxCancel(t *testing.T) {
	db := setupCleanupDB(t)
	dataDir := t.TempDir()
	cfg := cleanupCfg(dataDir)
	cfg.Cleanup.SweepInterval = 10 * time.Millisecond

	c := newCleaner(db, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { c.Run(ctx); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run didn't return after cancel")
	}
}
