package cleanup_test

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"letts/internal/cleanup"
	"letts/internal/storage"
)

func TestVacuumRunOnce(t *testing.T) {
	db := setupCleanupDB(t)
	v := &cleanup.Vacuumer{DB: db, Logger: slog.Default()}
	v.RunOnce(context.Background())
}

func TestVacuumRunHonorsCtxCancel(t *testing.T) {
	db := setupCleanupDB(t)
	v := &cleanup.Vacuumer{DB: db, Logger: slog.Default(), Interval: 10 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { v.Run(ctx); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run didn't return after cancel")
	}
}

// TestVacuumRunOnceTruncatesWAL verifies that RunOnce triggers
// `PRAGMA wal_checkpoint(TRUNCATE)` and the WAL file shrinks to 0 bytes.
// Without TRUNCATE, the WAL grows until the auto-checkpoint threshold
// (1000 pages by default) and never reclaims disk.
func TestVacuumRunOnceTruncatesWAL(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "state.db")
	db, err := storage.Open(dbPath, storage.Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := storage.Migrate(context.Background(), db); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// Insert enough rows to actually grow the WAL beyond the empty header.
	// We use the missions table since it's already migrated; values are
	// arbitrary but distinct so SQLite has real work to record.
	ctx := context.Background()
	for i := 0; i < 50; i++ {
		mid := "0192aaaa-0000-7000-8000-" + leftPadHex(int64(i), 12)
		if err := storage.InsertMission(ctx, db, &storage.Mission{
			ID:               mid,
			Kind:             storage.KindMission,
			Lane:             "normal",
			MissionName:      "WALFixture",
			Status:           storage.StatusQueued,
			Input:            []byte(`{"i":` + leftPadHex(int64(i), 4) + `}`),
			InputFingerprint: "fp-" + mid,
			TimeCreatedMs:    time.Now().UnixMilli(),
		}); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	walPath := dbPath + "-wal"
	preInfo, preErr := os.Stat(walPath)
	if preErr != nil {
		t.Fatalf("WAL file missing before RunOnce: %v", preErr)
	}
	preSize := preInfo.Size()
	if preSize == 0 {
		t.Fatalf("WAL file empty before RunOnce; inserts didn't grow the log")
	}

	v := &cleanup.Vacuumer{DB: db, Logger: slog.Default()}
	v.RunOnce(ctx)

	postInfo, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("WAL stat after RunOnce: %v", err)
	}
	if postInfo.Size() != 0 {
		t.Errorf("WAL not truncated: pre=%d post=%d", preSize, postInfo.Size())
	}
}

// buildFreelistDB returns a fresh state.db with a freelist built by inserting
// then deleting padded rows. Deterministic fixtures (fixed ids/content) so two
// DBs built this way are byte-comparable for the equivalence test.
func buildFreelistDB(t *testing.T) *sql.DB {
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
	ctx := context.Background()
	for i := 0; i < 300; i++ {
		mid := "0192bbbb-0000-7000-8000-" + leftPadHex(int64(i), 12)
		if err := storage.InsertMission(ctx, db, &storage.Mission{
			ID: mid, Kind: storage.KindMission, Lane: "normal", MissionName: "VacFixture",
			Status: storage.StatusQueued, Input: []byte(`{"pad":"` + strings.Repeat("ab", 256) + `"}`),
			InputFingerprint: "fp-" + mid, TimeCreatedMs: int64(1_700_000_000_000 + i),
		}); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	if _, err := db.Exec(`DELETE FROM missions`); err != nil {
		t.Fatalf("delete: %v", err)
	}
	return db
}

func freelistCount(t *testing.T, db *sql.DB) int64 {
	t.Helper()
	var n int64
	if err := db.QueryRow(`PRAGMA freelist_count`).Scan(&n); err != nil {
		t.Fatalf("freelist_count: %v", err)
	}
	return n
}

// TestVacuumIncrementalReclaimsInBatches exercises the bounded multi-batch
// vacuum loop (a tiny BatchPages forces several iterations) and asserts it
// makes progress reclaiming the freelist without erroring. The fix bounds how
// long a single incremental_vacuum statement holds the write lock; it must not
// regress reclamation (the freelist may only shrink), and at least the
// reclaimable trailing pages must be returned.
func TestVacuumIncrementalReclaimsInBatches(t *testing.T) {
	ctx := context.Background()
	db := buildFreelistDB(t)
	before := freelistCount(t, db)
	if before == 0 {
		t.Skip("no freelist built; nothing to reclaim")
	}

	// Tiny batch -> the loop runs multiple bounded statements rather than one
	// unbounded one.
	v := &cleanup.Vacuumer{DB: db, Logger: slog.Default(), BatchPages: 4}
	v.RunOnce(ctx)

	after := freelistCount(t, db)
	if after >= before {
		t.Errorf("bounded vacuum made no progress: before=%d after=%d", before, after)
	}
}

// leftPadHex turns a small int into a fixed-width hex string for fake
// UUIDv7 construction. Local helper so the test can produce distinct ids
// without depending on ids.NewUUIDv7 (which is non-deterministic).
func leftPadHex(n int64, width int) string {
	hex := []byte("0123456789abcdef")
	out := make([]byte, width)
	for i := width - 1; i >= 0; i-- {
		out[i] = hex[n&0xF]
		n >>= 4
	}
	return string(out)
}

// silences unused-import warning if the helper above is later moved.
var _ sql.Result = nil
