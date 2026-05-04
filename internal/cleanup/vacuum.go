package cleanup

import (
	"context"
	"database/sql"
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
type Vacuumer struct {
	DB       *sql.DB
	Logger   *slog.Logger
	Interval time.Duration
}

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

// RunOnce executes one PRAGMA incremental_vacuum followed by one
// PRAGMA wal_checkpoint(TRUNCATE). Errors on either are logged at warn
// level so a transient failure (busy DB, concurrent reader) doesn't
// silently leak disk; the next tick will retry.
func (v *Vacuumer) RunOnce(ctx context.Context) {
	if _, err := v.DB.ExecContext(ctx, `PRAGMA incremental_vacuum`); err != nil {
		v.logger().Warn("incremental_vacuum", "err", err)
	}
	if _, err := v.DB.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		v.logger().Warn("wal_checkpoint_truncate", "err", err)
	}
}

func (v *Vacuumer) logger() *slog.Logger {
	if v.Logger == nil {
		return slog.Default()
	}
	return v.Logger
}
