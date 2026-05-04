package cleanup_test

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"letts/internal/cleanup"
	"letts/internal/ids"
	"letts/internal/stagingstore"
	"letts/internal/storage"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func setupStagingGC(t *testing.T) (*cleanup.StagingGC, *sql.DB, string, *fakeClock, *stagingstore.UploadLock) {
	t.Helper()
	db := setupCleanupDB(t)
	dataDir := t.TempDir()
	cfg := cleanupCfg(dataDir)
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	lock := stagingstore.NewUploadLock(time.Minute, clock.Now)
	gc := &cleanup.StagingGC{
		DB: db, Cfg: cfg, DataDir: dataDir, UploadLock: lock,
		Logger: slog.Default(), GracePeriod: 60 * time.Second,
		BatchSize: 100, Now: clock.Now,
	}
	return gc, db, dataDir, clock, lock
}

func insertStagingFile(t *testing.T, db *sql.DB, dataDir string, state storage.StagingState, contents []byte, expiresMs int64) (string, string) {
	t.Helper()
	id := ids.NewUUIDv7()
	shard, _ := ids.ShardPath(id)
	relPath := filepath.Join("staging", shard, id)
	abs := filepath.Join(dataDir, relPath)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(abs, contents, 0o644); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixMilli()
	if err := storage.InsertStaging(context.Background(), db, &storage.StagingFile{
		StagingID: id, State: state, Sha256: "sha", Size: int64(len(contents)),
		BytesReceived: int64(len(contents)), Path: relPath,
		TimeCreatedMs: now, TimeUpdatedMs: now, TimeExpiresMs: expiresMs,
	}); err != nil {
		t.Fatal(err)
	}
	return id, abs
}

func TestStagingGCDeletingMovesToTombstone(t *testing.T) {
	gc, db, dataDir, _, _ := setupStagingGC(t)
	id, abs := insertStagingFile(t, db, dataDir, storage.StagingDeleting, []byte("data"), time.Now().UnixMilli()+60_000)

	gc.RunOnce(context.Background())

	if _, err := os.Stat(abs); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("source file still present: err=%v", err)
	}
	tombPath := filepath.Join(dataDir, "tombstone", id)
	if _, err := os.Stat(tombPath); err != nil {
		t.Errorf("tombstone file missing: %v", err)
	}
	// Row still present (grace period hasn't elapsed yet).
	if _, err := storage.GetStaging(context.Background(), db, id); err != nil {
		t.Errorf("row removed prematurely: %v", err)
	}
}

func TestStagingGCAfterGraceUnlinksAndDeletes(t *testing.T) {
	gc, db, dataDir, clock, _ := setupStagingGC(t)
	id, _ := insertStagingFile(t, db, dataDir, storage.StagingDeleting, []byte("data"), time.Now().UnixMilli()+60_000)

	gc.RunOnce(context.Background())
	tombPath := filepath.Join(dataDir, "tombstone", id)
	if _, err := os.Stat(tombPath); err != nil {
		t.Fatalf("tombstone missing after first pass: %v", err)
	}

	clock.advance(2 * time.Minute)
	gc.RunOnce(context.Background())

	if _, err := os.Stat(tombPath); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("tombstone not unlinked after grace")
	}
	if _, err := storage.GetStaging(context.Background(), db, id); !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("row not deleted after grace: %v", err)
	}
}

// A file lurking in the tombstone directory whose name isn't a
// valid UUIDv7 must not reach SQL DELETE — the WHERE clause would be
// equivalent to `DELETE WHERE staging_id = '..'` which is harmless against
// the existing rows but still wastes a write tx and confuses log searches.
// The fix: validate the directory entry's name against UUIDv7 before
// unlinking and before issuing the SQL. Foreign files (left by an admin
// or another tool) are simply skipped — they're not ours to touch.
func TestStagingGCUnlinkOldTombstonesSkipsNonUUIDFiles(t *testing.T) {
	gc, _, dataDir, clock, _ := setupStagingGC(t)
	tombDir := filepath.Join(dataDir, "tombstone")
	if err := os.MkdirAll(tombDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// Drop two foreign files whose mtime is BEFORE cutoff = gc.Now() -
	// gracePeriod. mtime must use the fake-clock baseline (gc.Now()) so
	// the gc's "is this old enough?" check fires; using wall-clock here
	// would put the file in the future relative to gc.Now() and the
	// cleanup would correctly skip via the After(cutoff) branch (which
	// is NOT the path this test fixes).
	foreignNames := []string{"README", "not-a-uuid.txt"}
	oldMtime := gc.Now().Add(-5 * time.Minute)
	for _, n := range foreignNames {
		p := filepath.Join(tombDir, n)
		if err := os.WriteFile(p, []byte("admin notes"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(p, oldMtime, oldMtime); err != nil {
			t.Fatal(err)
		}
	}

	clock.advance(2 * time.Minute) // ensure cutoff passes
	gc.RunOnce(context.Background())

	for _, n := range foreignNames {
		p := filepath.Join(tombDir, n)
		if _, err := os.Stat(p); err != nil {
			t.Errorf("foreign tombstone file %q removed unexpectedly: %v", n, err)
		}
	}
}

func TestStagingGCInFlightReaderSurvivesRename(t *testing.T) {
	gc, db, dataDir, _, _ := setupStagingGC(t)
	payload := []byte("payload-content")
	_, abs := insertStagingFile(t, db, dataDir, storage.StagingDeleting, payload, time.Now().UnixMilli()+60_000)

	// Open the file BEFORE GC moves it.
	f, err := os.Open(abs)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	gc.RunOnce(context.Background())

	// Reader still sees the content via its open fd (POSIX guarantee).
	got, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("read after rename: %v", err)
	}
	if string(got) != string(payload) {
		t.Errorf("content=%q, want %q", got, payload)
	}
}

func TestStagingGCExpireTTLFlipsCompleteToDeleting(t *testing.T) {
	gc, db, dataDir, _, _ := setupStagingGC(t)
	expiredAt := gc.Now().UnixMilli() - 1000
	id, _ := insertStagingFile(t, db, dataDir, storage.StagingComplete, []byte("x"), expiredAt)

	gc.RunOnce(context.Background())

	sf, err := storage.GetStaging(context.Background(), db, id)
	// Row may already be in tombstone after a single RunOnce (expire → tombstone in same pass).
	if err == nil && sf.State != storage.StagingDeleting {
		t.Errorf("state=%q, want deleting", sf.State)
	}
	tombPath := filepath.Join(dataDir, "tombstone", id)
	if _, err := os.Stat(tombPath); err != nil {
		t.Errorf("expected tombstone file, err=%v", err)
	}
}

func TestStagingGCExpireTTLSkipsLockedUploading(t *testing.T) {
	gc, db, dataDir, _, lock := setupStagingGC(t)
	expiredAt := gc.Now().UnixMilli() - 1000
	id, abs := insertStagingFile(t, db, dataDir, storage.StagingUploading, []byte("partial"), expiredAt)

	rel, ok := lock.TryAcquire(id, nil)
	if !ok {
		t.Fatal("lock acquire failed")
	}
	defer rel()

	gc.RunOnce(context.Background())

	sf, err := storage.GetStaging(context.Background(), db, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if sf.State != storage.StagingUploading {
		t.Errorf("state=%q, want uploading (locked)", sf.State)
	}
	if _, err := os.Stat(abs); err != nil {
		t.Errorf("partial file removed despite lock: %v", err)
	}
}

func TestStagingGCExpireTTLPicksUploadingWhenUnlocked(t *testing.T) {
	gc, db, dataDir, _, _ := setupStagingGC(t)
	expiredAt := gc.Now().UnixMilli() - 1000
	id, _ := insertStagingFile(t, db, dataDir, storage.StagingUploading, []byte("partial"), expiredAt)

	gc.RunOnce(context.Background())

	tombPath := filepath.Join(dataDir, "tombstone", id)
	if _, err := os.Stat(tombPath); err != nil {
		t.Errorf("expected tombstone for unlocked uploading, err=%v", err)
	}
}

func TestStagingGCLeavesNonExpiredAlone(t *testing.T) {
	gc, db, dataDir, _, _ := setupStagingGC(t)
	farFuture := gc.Now().UnixMilli() + 24*60*60*1000
	id, abs := insertStagingFile(t, db, dataDir, storage.StagingComplete, []byte("ok"), farFuture)

	gc.RunOnce(context.Background())

	sf, err := storage.GetStaging(context.Background(), db, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if sf.State != storage.StagingComplete {
		t.Errorf("state=%q changed", sf.State)
	}
	if _, err := os.Stat(abs); err != nil {
		t.Errorf("file removed: %v", err)
	}
}

func TestStagingGCDeletingFileMissingDropsRow(t *testing.T) {
	gc, db, dataDir, _, _ := setupStagingGC(t)
	id := ids.NewUUIDv7()
	if err := storage.InsertStaging(context.Background(), db, &storage.StagingFile{
		StagingID: id, State: storage.StagingDeleting, Sha256: "x", Size: 1, BytesReceived: 1,
		Path: "staging/00/00/missing", TimeCreatedMs: 1, TimeUpdatedMs: 1, TimeExpiresMs: 9999999999,
	}); err != nil {
		t.Fatal(err)
	}

	gc.RunOnce(context.Background())

	if _, err := storage.GetStaging(context.Background(), db, id); !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("row should be deleted when file missing: err=%v", err)
	}
	_ = dataDir
}

func TestStagingGCRunHonorsCtxCancel(t *testing.T) {
	gc, _, _, _, _ := setupStagingGC(t)
	gc.Cfg.Cleanup.SweepInterval = 10 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { gc.Run(ctx); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run didn't return after cancel")
	}
}

// TestStagingGCDrainsRecalcSentinel verifies that rows
// left at time_expires=0 by the CASCADE trigger must be re-recalculated
// by the GC pass even when the live-path recalc in MissionCleaner is
// missed (e.g. crashed, errored). Without the drain the row stays at 0
// forever (skipped by expireTTLs, so disk leak).
func TestStagingGCDrainsRecalcSentinel(t *testing.T) {
	gc, db, dataDir, clock, _ := setupStagingGC(t)
	// time_expires=0 (trigger sentinel) with NO refs — would be an orphan.
	id, _ := insertStagingFile(t, db, dataDir, storage.StagingComplete, []byte("x"), 0)
	_ = dataDir

	gc.RunOnce(context.Background())

	got, err := storage.GetStaging(context.Background(), db, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.TimeExpiresMs == 0 {
		t.Errorf("time_expires still 0 after GC pass; drain should have recalc'd")
	}
	// Sanity: drain replaced 0 with a positive future value (staging_ttl).
	if got.TimeExpiresMs <= clock.Now().UnixMilli() {
		t.Errorf("time_expires=%d already in the past for staging_ttl-based orphan", got.TimeExpiresMs)
	}
}

// TestStagingGCSkipsTimeExpiresZeroSentinel verifies the CASCADE
// trigger sentinel handling. The trigger
//
//	CREATE TRIGGER staging_recalc_after_ref_delete AFTER DELETE
//	    ON mission_staging_refs ...
//	UPDATE staging_files SET time_expires=0 WHERE staging_id=OLD.staging_id;
//
// uses 0 as "needs recalc, defer" marker. GC must not reap rows whose
// time_expires==0; otherwise a staging file refed by a still-running
// mission can be tombstoned in the brief window between CASCADE commit
// and the cleanup's RecalcStagingTTL call.
func TestStagingGCSkipsTimeExpiresZeroSentinel(t *testing.T) {
	gc, db, dataDir, _, _ := setupStagingGC(t)
	// time_expires=0 (sentinel value written by CASCADE trigger).
	id, abs := insertStagingFile(t, db, dataDir, storage.StagingComplete, []byte("live-input"), 0)

	gc.RunOnce(context.Background())

	// Row must still be in 'complete' — not transitioned to 'deleting'.
	got, err := storage.GetStaging(context.Background(), db, id)
	if err != nil {
		t.Fatalf("row deleted unexpectedly: %v", err)
	}
	if got.State != storage.StagingComplete {
		t.Errorf("state=%q after GC; want complete (sentinel rows must be deferred to RecalcStagingTTL)",
			got.State)
	}
	// File must still be in place (not tombstoned).
	if _, err := os.Stat(abs); err != nil {
		t.Errorf("source file moved/removed: %v", err)
	}
}

func TestStagingGCTombstoneAlreadyExists(t *testing.T) {
	gc, db, dataDir, clock, _ := setupStagingGC(t)
	id, _ := insertStagingFile(t, db, dataDir, storage.StagingDeleting, []byte("data"), time.Now().UnixMilli()+60_000)

	// Pre-populate the tombstone (simulating a prior crash mid-rename).
	tombDir := filepath.Join(dataDir, "tombstone")
	_ = os.MkdirAll(tombDir, 0o755)
	tombPath := filepath.Join(tombDir, id)
	_ = os.WriteFile(tombPath, []byte("data"), 0o644)
	old := clock.Now().Add(-2 * time.Minute)
	_ = os.Chtimes(tombPath, old, old)

	gc.RunOnce(context.Background())

	if _, err := os.Stat(tombPath); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("aged tombstone not unlinked")
	}
	if _, err := storage.GetStaging(context.Background(), db, id); !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("row not deleted after aged tombstone unlink: %v", err)
	}
}
