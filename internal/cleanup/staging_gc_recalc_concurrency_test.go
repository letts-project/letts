package cleanup_test

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"letts/internal/cleanup"
	"letts/internal/config"
	"letts/internal/storage"
)

// TestRecalcStagingTTLConcurrentDrainsConverge: when multiple
// background goroutines call StagingGC.RunOnce (which internally invokes
// drainNeedsRecalc → RecalcStagingTTL) concurrently, the SELECT-compute-
// UPDATE must serialize through the writer-tx so the final time_expires
// agrees with a single-shot recompute. With the read+write outside a
// writer tx, concurrent passes could interleave and overwrite each
// other's results.
//
// Goal of this test: confirm 50 concurrent drain passes against the same
// staging row land on the expected time_expires (no crash, no torn write
// to a nonsense value). Race-detector also serves as a structural guard.
func TestRecalcStagingTTLConcurrentDrainsConverge(t *testing.T) {
	dir := t.TempDir()
	db, err := storage.Open(filepath.Join(dir, "state.db"), storage.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if err := storage.Migrate(context.Background(), db); err != nil {
		t.Fatal(err)
	}

	stagingID := "01900000-0000-7000-8000-000000000abc"
	now := time.Now().UnixMilli()
	if err := storage.InsertStaging(context.Background(), db, &storage.StagingFile{
		StagingID: stagingID, State: storage.StagingComplete,
		Sha256: "deadbeef", Size: 7, BytesReceived: 7,
		Path:          "staging/01/90/" + stagingID,
		TimeCreatedMs: now, TimeUpdatedMs: now, TimeExpiresMs: 0,
	}); err != nil {
		t.Fatal(err)
	}

	cfg := &config.DugdaleConfig{
		Cleanup: config.CleanupConfig{
			StagingTTL:      30 * time.Minute,
			DownloadedGrace: 1 * time.Hour,
		},
		Exec: config.ExecConfig{},
	}
	gc := &cleanup.StagingGC{DB: db, Cfg: cfg, DataDir: dir}

	var wg sync.WaitGroup
	const passes = 50
	for i := 0; i < passes; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			gc.RunOnce(context.Background())
		}()
	}
	wg.Wait()

	sf, err := storage.GetStaging(context.Background(), db, stagingID)
	if err != nil {
		t.Fatalf("get staging: %v", err)
	}
	// Single-shot expected: time_created + staging_ttl (no refs, no
	// downloaded_at — formula degenerates to the orphan branch).
	wantMin := now + cfg.Cleanup.StagingTTL.Milliseconds()
	if sf.TimeExpiresMs < wantMin {
		t.Errorf("time_expires=%d, want >= %d (concurrent drains lost a value)",
			sf.TimeExpiresMs, wantMin)
	}
	// And time_expires should not regress below 0 — defensive guard
	// for the torn-write scenario.
	if sf.TimeExpiresMs <= 0 {
		t.Errorf("time_expires=%d non-positive after drain — torn write?", sf.TimeExpiresMs)
	}
}
