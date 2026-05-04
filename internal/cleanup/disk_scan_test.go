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
	"letts/internal/ids"
	"letts/internal/storage"
)

func setupDiskScanner(t *testing.T) (*cleanup.DiskScanner, *sql.DB, string, *fakeClock) {
	t.Helper()
	db := setupCleanupDB(t)
	dataDir := t.TempDir()
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	return &cleanup.DiskScanner{
		DB: db, DataDir: dataDir, Logger: slog.Default(),
		SkipRecent: 5 * time.Minute,
		Now:        clock.Now,
	}, db, dataDir, clock
}

func writeFileOld(t *testing.T, path string, mtime time.Time) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, mtime, mtime); err != nil {
		t.Fatal(err)
	}
}

func TestDiskScanRemovesOrphanStaging(t *testing.T) {
	s, _, dataDir, clock := setupDiskScanner(t)
	id := ids.NewUUIDv7()
	shard, _ := ids.ShardPath(id)
	path := filepath.Join(dataDir, "staging", shard, id)
	writeFileOld(t, path, clock.Now().Add(-10*time.Minute))

	s.RunOnce(context.Background())

	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("orphan staging not removed: err=%v", err)
	}
}

func TestDiskScanRemovesOrphanStagingTmp(t *testing.T) {
	s, _, dataDir, clock := setupDiskScanner(t)
	id := ids.NewUUIDv7()
	shard, _ := ids.ShardPath(id)
	path := filepath.Join(dataDir, "staging", shard, id+".tmp")
	writeFileOld(t, path, clock.Now().Add(-10*time.Minute))

	s.RunOnce(context.Background())

	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("orphan .tmp not removed: err=%v", err)
	}
}

func TestDiskScanKeepsRecentFiles(t *testing.T) {
	s, _, dataDir, clock := setupDiskScanner(t)
	id := ids.NewUUIDv7()
	shard, _ := ids.ShardPath(id)
	path := filepath.Join(dataDir, "staging", shard, id)
	// Recent file (within SkipRecent window).
	writeFileOld(t, path, clock.Now().Add(-1*time.Minute))

	s.RunOnce(context.Background())

	if _, err := os.Stat(path); err != nil {
		t.Errorf("recent file removed: %v", err)
	}
}

func TestDiskScanKeepsKnownStaging(t *testing.T) {
	s, db, dataDir, clock := setupDiskScanner(t)
	id := ids.NewUUIDv7()
	shard, _ := ids.ShardPath(id)
	path := filepath.Join(dataDir, "staging", shard, id)
	writeFileOld(t, path, clock.Now().Add(-10*time.Minute))

	now := time.Now().UnixMilli()
	_ = storage.InsertStaging(context.Background(), db, &storage.StagingFile{
		StagingID: id, State: storage.StagingComplete, Sha256: "x", Size: 1, BytesReceived: 1,
		Path:          filepath.Join("staging", shard, id),
		TimeCreatedMs: now, TimeUpdatedMs: now, TimeExpiresMs: now + 999_999,
	})

	s.RunOnce(context.Background())

	if _, err := os.Stat(path); err != nil {
		t.Errorf("known staging removed: %v", err)
	}
}

func TestDiskScanRemovesOrphanOutputFiles(t *testing.T) {
	s, _, dataDir, clock := setupDiskScanner(t)
	id := ids.NewUUIDv7()
	shard, _ := ids.ShardPath(id)
	dir := filepath.Join(dataDir, "output", shard)
	for _, sfx := range []string{"-stdout", "-stderr", "-combined", "-events"} {
		writeFileOld(t, filepath.Join(dir, id+sfx), clock.Now().Add(-10*time.Minute))
	}

	s.RunOnce(context.Background())

	for _, sfx := range []string{"-stdout", "-stderr", "-combined", "-events"} {
		if _, err := os.Stat(filepath.Join(dir, id+sfx)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("orphan output%s not removed: %v", sfx, err)
		}
	}
}

func TestDiskScanKeepsKnownOutputFiles(t *testing.T) {
	s, db, dataDir, clock := setupDiskScanner(t)
	id := insertCleanupMission(t, db, storage.KindMission, storage.StatusDone, "success", time.Now().UnixMilli())
	shard, _ := ids.ShardPath(id)
	path := filepath.Join(dataDir, "output", shard, id+"-stdout")
	writeFileOld(t, path, clock.Now().Add(-10*time.Minute))

	s.RunOnce(context.Background())

	if _, err := os.Stat(path); err != nil {
		t.Errorf("known output removed: %v", err)
	}
}

func TestDiskScanRemovesOrphanWorkdir(t *testing.T) {
	s, _, dataDir, clock := setupDiskScanner(t)
	id := ids.NewUUIDv7()
	dir := filepath.Join(dataDir, "work", id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "input"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	old := clock.Now().Add(-10 * time.Minute)
	if err := os.Chtimes(dir, old, old); err != nil {
		t.Fatal(err)
	}

	s.RunOnce(context.Background())

	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("orphan workdir not removed: %v", err)
	}
}

func TestDiskScanKeepsKnownWorkdir(t *testing.T) {
	s, db, dataDir, clock := setupDiskScanner(t)
	id := insertCleanupMission(t, db, storage.KindMission, storage.StatusRunning, "", 0)
	dir := filepath.Join(dataDir, "work", id)
	_ = os.MkdirAll(dir, 0o755)
	old := clock.Now().Add(-10 * time.Minute)
	_ = os.Chtimes(dir, old, old)

	s.RunOnce(context.Background())

	if _, err := os.Stat(dir); err != nil {
		t.Errorf("known workdir removed: %v", err)
	}
}

func TestDiskScanIgnoresNonUUIDNames(t *testing.T) {
	s, _, dataDir, clock := setupDiskScanner(t)
	path := filepath.Join(dataDir, "staging", "00", "11", "not-a-uuid-file")
	writeFileOld(t, path, clock.Now().Add(-10*time.Minute))

	s.RunOnce(context.Background())

	if _, err := os.Stat(path); err != nil {
		t.Errorf("non-UUID file removed: %v", err)
	}
}

func TestDiskScanHandlesMissingDirs(t *testing.T) {
	s, _, _, _ := setupDiskScanner(t)
	// No subdirs created; should not panic.
	s.RunOnce(context.Background())
}

func TestDiskScanRunHonorsCtxCancel(t *testing.T) {
	s, _, _, _ := setupDiskScanner(t)
	s.Interval = 10 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run didn't return after cancel")
	}
}
