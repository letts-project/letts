package cleanup

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"
)

// Vacuumer runs PRAGMA incremental_vacuum AND PRAGMA wal_checkpoint(TRUNCATE)
// on a slow timer (every hour). The pragmas together reclaim:
//   - freelist pages from prior DELETEs (incremental_vacuum)
//   - WAL frames sitting on disk after checkpointing (wal_checkpoint TRUNCATE)
//
// modernc.org/sqlite auto-checkpoints at 1000 pages so the WAL would
// eventually drain on its own, but it never *shrinks* — only TRUNCATE
// returns disk to the filesystem.
//
// The incremental_vacuum runs in BOUNDED page batches (see BatchPages): a
// bare `PRAGMA incremental_vacuum` reclaims the entire freelist in a single
// statement that holds the write lock for the whole sweep — after a large
// mission cleanup that can be many seconds, long enough to push concurrent
// writers past busy_timeout. That unbounded hold was a contributor to the
// 2026-06-27 lock storm.
type Vacuumer struct {
	DB       *sql.DB
	Logger   *slog.Logger
	Interval time.Duration
	// BatchPages caps how many freelist pages one incremental_vacuum statement
	// reclaims, bounding how long a single statement holds the write lock.
	// <=0 uses defaultVacuumBatchPages.
	BatchPages int
}

const (
	// defaultVacuumBatchPages bounds a single incremental_vacuum statement.
	defaultVacuumBatchPages = 1000
	// vacuumMaxBatchesPerRun caps total reclamation per RunOnce so a huge
	// freelist is drained across several ticks instead of one long pass; the
	// remainder waits for the next tick.
	vacuumMaxBatchesPerRun = 16
	// vacuumBatchPause yields the write lock between batches so other writers
	// interleave.
	vacuumBatchPause = 10 * time.Millisecond
)

// Run loops RunOnce on Interval (default 1h) until ctx is cancelled.
func (v *Vacuumer) Run(ctx context.Context) {
	interval := v.Interval
	if interval <= 0 {
		interval = time.Hour
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			v.RunOnce(ctx)
		}
	}
}

// RunOnce reclaims the freelist in bounded batches, then runs one
// PRAGMA wal_checkpoint(TRUNCATE). Errors are logged at warn level so a
// transient failure (busy DB, concurrent reader) doesn't silently leak disk;
// the next tick will retry.
func (v *Vacuumer) RunOnce(ctx context.Context) {
	v.incrementalVacuum(ctx)
	if _, err := v.DB.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		v.logger().Warn("wal_checkpoint_truncate", "err", err)
	}
}

// incrementalVacuum reclaims freelist pages in bounded batches, yielding the
// write lock between batches, so a large freelist never wedges concurrent
// writers behind one long-held lock. It stops when the freelist is empty or
// the per-run batch cap is reached (remainder waits for the next tick).
func (v *Vacuumer) incrementalVacuum(ctx context.Context) {
	batch := v.BatchPages
	if batch <= 0 {
		batch = defaultVacuumBatchPages
	}
	for i := 0; i < vacuumMaxBatchesPerRun; i++ {
		var before int64
		if err := v.DB.QueryRowContext(ctx, `PRAGMA freelist_count`).Scan(&before); err != nil {
			v.logger().Warn("freelist_count", "err", err)
			return
		}
		if before == 0 {
			return
		}
		// batch is an int constant/config value, never user input — safe to
		// interpolate (PRAGMA args can't be bound parameters).
		if _, err := v.DB.ExecContext(ctx, fmt.Sprintf("PRAGMA incremental_vacuum(%d)", batch)); err != nil {
			v.logger().Warn("incremental_vacuum", "err", err)
			return
		}
		var after int64
		if err := v.DB.QueryRowContext(ctx, `PRAGMA freelist_count`).Scan(&after); err != nil {
			v.logger().Warn("freelist_count", "err", err)
			return
		}
		// Stop when the freelist is drained OR this batch reclaimed nothing
		// more: only trailing free pages are reclaimable, so once a batch makes
		// no progress, further batches won't either — don't spin the cap.
		if after == 0 || after >= before {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(vacuumBatchPause):
		}
	}
}

func (v *Vacuumer) logger() *slog.Logger {
	if v.Logger == nil {
		return slog.Default()
	}
	return v.Logger
}
