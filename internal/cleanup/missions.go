// Package cleanup implements the background goroutines that reclaim disk and
// SQL state for retired missions and staging artifacts.
package cleanup

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"letts/internal/config"
	"letts/internal/ids"
	"letts/internal/storage"
)

const (
	// recalcYieldEvery / recalcYieldPause bound how long the post-delete
	// staging-TTL recalc loop may run back-to-back writer transactions before
	// yielding the write lock to other writers (notably dispatch).
	recalcYieldEvery = 64
	recalcYieldPause = 20 * time.Millisecond
)

// MissionCleaner walks done/deleting missions on a periodic ticker and runs
// the two-phase cleanup: pick victims, mark deleting, remove files,
// SQL DELETE, recalc affected staging TTLs.
type MissionCleaner struct {
	DB     *sql.DB
	Cfg    *config.DugdaleConfig
	Logger *slog.Logger

	// BatchInterPause is the delay between batches inside a single sweep.
	// Zero defaults to 100ms.
	BatchInterPause time.Duration
	// MaxBatchesPerSweep caps batches per RunOnce iteration. Zero defaults to
	// 1000 (so at most ~1M missions per pass).
	MaxBatchesPerSweep int
}

// Run sweeps every cleanup.sweep_interval until ctx is cancelled.
func (c *MissionCleaner) Run(ctx context.Context) {
	interval := c.Cfg.Cleanup.SweepInterval
	if interval <= 0 {
		interval = time.Minute
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			c.RunOnce(ctx)
		}
	}
}

// RunOnce executes a single sweep — repeated batches until either no victims
// remain or MaxBatchesPerSweep is hit. Used directly by tests; Run loops it
// on a ticker.
func (c *MissionCleaner) RunOnce(ctx context.Context) {
	maxBatches := c.MaxBatchesPerSweep
	if maxBatches <= 0 {
		maxBatches = 1000
	}
	pause := c.BatchInterPause
	if pause <= 0 {
		pause = 100 * time.Millisecond
	}
	for batch := 0; batch < maxBatches; batch++ {
		if ctx.Err() != nil {
			return
		}
		victims, err := c.pickVictims(ctx)
		if err != nil {
			c.logger().Error("cleanup pickVictims", "err", err)
			return
		}
		if len(victims) == 0 {
			return
		}
		c.processBatch(ctx, victims)
		select {
		case <-ctx.Done():
			return
		case <-time.After(pause):
		}
	}
}

func (c *MissionCleaner) logger() *slog.Logger {
	if c.Logger == nil {
		return slog.Default()
	}
	return c.Logger
}

// pickVictims returns the next batch of mission_ids to delete. We
// drain orphan deleting first; only when none remain do we promote new
// done rows whose TTL has expired.
func (c *MissionCleaner) pickVictims(ctx context.Context) ([]string, error) {
	// Same NOT-EXISTS finalize-intent gate as the new-victims query below.
	// A 'deleting' row can briefly carry an unapplied intent (an admin
	// force-delete racing a live finalize, or a crash window before startup
	// repair has run); draining it would hard-delete the row, CASCADE the
	// intent away, and orphan Phase B's outstanding staging work (committing
	// rows, renamed files) until the periodic disk scan reaps. Such rows are
	// left for the finalize/repair machinery to clear first: the live commit
	// path deletes the intent in the same transaction even when it finds the
	// row already deleting (outcome discarded, outputs handed to the GC), and
	// startup repair drains surviving intents before the cleaner's first
	// sweep — so the gate self-clears and the next sweep picks the row up.
	// One exception: a terminal-event-conflict intent deliberately survives
	// both (it trips the sticky readyz flag for operator repair) and holds
	// its own row un-drained until resolved; the gate is per-row, so every
	// other deleting row keeps draining.
	deletingIDs, err := c.queryIDs(ctx,
		`SELECT mission_id FROM missions WHERE status='deleting'
		   AND NOT EXISTS (SELECT 1 FROM mission_finalize_intents fi WHERE fi.mission_id = missions.mission_id)
		 LIMIT 1000`)
	if err != nil {
		return nil, fmt.Errorf("query deleting: %w", err)
	}
	if len(deletingIDs) > 0 {
		return deletingIDs, nil
	}

	nowMs := time.Now().UnixMilli()
	cl := c.Cfg.Cleanup
	ex := c.Cfg.Exec
	cutoffSuccess := nowMs - cl.SuccessTTL.Milliseconds()
	cutoffFailed := nowMs - cl.FailedTTL.Milliseconds()
	cutoffExecS := nowMs - ex.ExecSuccessTTL.Milliseconds()
	cutoffExecF := nowMs - ex.ExecFailedTTL.Milliseconds()
	// Lost cleanup uses failed_ttl + lost_cleanup_grace —
	// lost_cleanup_grace is ADDITIONAL retention on top of the normal
	// failed TTL, not a replacement. The lost cleanup must
	// pick its base TTL by kind, because exec_failed_ttl (default 24h)
	// is typically shorter than mission failed_ttl (default 7d). Sharing
	// one cutoff held lost exec rows for the longer of the two —
	// observable as stale staging refs preserved past their exec budget.
	cutoffLostMission := nowMs - (cl.FailedTTL + cl.LostCleanupGrace).Milliseconds()
	cutoffLostExec := nowMs - (ex.ExecFailedTTL + cl.LostCleanupGrace).Milliseconds()

	// Gate on NOT EXISTS finalize intent. Without this gate
	// cleanup could pick a row whose intent hasn't been applied yet
	// (rare under default TTLs, observable under aggressive test
	// configs or when cleanup races repair on startup), CASCADE would
	// remove the intent along with the row, and Phase B's outstanding
	// rename/state-flip work would never happen — orphan committing
	// staging rows and tmp files until the periodic disk scan reaps.
	q := `SELECT mission_id FROM missions
	      WHERE status='done' AND time_finished IS NOT NULL AND (
	        (kind='mission' AND outcome='success' AND time_finished < ?) OR
	        (kind='mission' AND outcome IS NOT NULL AND outcome NOT IN ('success','lost') AND time_finished < ?) OR
	        (kind='exec'    AND outcome='success' AND time_finished < ?) OR
	        (kind='exec'    AND outcome IS NOT NULL AND outcome NOT IN ('success','lost') AND time_finished < ?) OR
	        (kind='mission' AND outcome='lost' AND time_finished < ?) OR
	        (kind='exec'    AND outcome='lost' AND time_finished < ?))
	        AND NOT EXISTS (SELECT 1 FROM mission_finalize_intents fi WHERE fi.mission_id = missions.mission_id)
	      ORDER BY time_finished LIMIT 1000`
	newIDs, err := c.queryIDs(ctx, q,
		cutoffSuccess, cutoffFailed, cutoffExecS, cutoffExecF,
		cutoffLostMission, cutoffLostExec)
	if err != nil {
		return nil, fmt.Errorf("query new: %w", err)
	}
	if len(newIDs) == 0 {
		return nil, nil
	}

	if err := storage.WithWriter(ctx, c.DB, func(conn *sql.Conn) error {
		for _, id := range newIDs {
			if _, err := conn.ExecContext(ctx,
				`UPDATE missions SET status='deleting' WHERE mission_id=? AND status='done'`, id); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("mark deleting: %w", err)
	}
	return newIDs, nil
}

func (c *MissionCleaner) queryIDs(ctx context.Context, q string, args ...any) ([]string, error) {
	rows, err := c.DB.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// processBatch performs the file-and-SQL removal on a victim list whose rows are all
// already in deleting state. File errors are logged but do not block SQL
// DELETE — the periodic disk scan handles file leaks.
func (c *MissionCleaner) processBatch(ctx context.Context, victims []string) {
	for _, id := range victims {
		shard, err := ids.ShardPath(id)
		if err != nil {
			c.logger().Warn("invalid mission id in cleanup", "id", id, "err", err)
			continue
		}
		outDir := filepath.Join(c.Cfg.DataDir, "output", shard)
		for _, sfx := range []string{"-stdout", "-stderr", "-combined", "-events"} {
			_ = os.Remove(filepath.Join(outDir, id+sfx))
		}
		_ = os.RemoveAll(filepath.Join(c.Cfg.DataDir, "work", id))
	}

	// Collect affected staging_ids INSIDE the writer tx so the
	// snapshot can't drift between read and DELETE. Without the tx, a
	// concurrent INSERT into mission_staging_refs (e.g. a restart that
	// happened to land in this window) would be CASCADE-deleted with the
	// mission row but its staging_id would never make it into `affected`,
	// so the TTL recalc step below would skip it — leaving the staging
	// row's time_expires at 0 (set by the trigger) until the next GC
	// cycle catches up. Low impact (the trigger still correctness-flags
	// the row), but moving the read into the tx makes the pass exact.
	//
	// Both the collect and the delete are SINGLE IN-list statements: one
	// RefsByMission + one DELETE per victim turned this transaction into ~2
	// statements per row, holding the write lock long enough (seconds, on a
	// large batch of real rows) to starve dispatch writers past busy_timeout.
	var affected []string
	if err := storage.WithWriter(ctx, c.DB, func(conn *sql.Conn) error {
		sids, err := storage.StagingIDsForMissions(ctx, conn, victims)
		if err != nil {
			return err
		}
		affected = sids
		return storage.DeleteMissions(ctx, conn, victims)
	}); err != nil {
		c.logger().Error("cleanup DELETE batch", "err", err)
		return
	}

	ttl := storage.TTLPolicy{
		MissionSuccess: c.Cfg.Cleanup.SuccessTTL,
		MissionFailed:  c.Cfg.Cleanup.FailedTTL,
		ExecSuccess:    c.Cfg.Exec.ExecSuccessTTL,
		ExecFailed:     c.Cfg.Exec.ExecFailedTTL,
		StagingTTL:     c.Cfg.Cleanup.StagingTTL,
		DownloadGrace:  c.Cfg.Cleanup.DownloadedGrace,
	}
	nowMs := time.Now().UnixMilli()
	for i, sid := range affected {
		// SELECT-compute-UPDATE must be inside a writer tx so
		// a concurrent dispatch's recalc on the same staging_id can't
		// overwrite ours mid-flight. Holding the lock per-id (not for
		// the whole loop) keeps writer fairness intact.
		err := storage.WithWriter(ctx, c.DB, func(conn *sql.Conn) error {
			_, e := storage.RecalcStagingTTL(ctx, conn, sid, ttl, nowMs)
			return e
		})
		if err != nil {
			c.logger().Warn("RecalcStagingTTL", "staging_id", sid, "err", err)
		}
		// Yield the write lock periodically: a batch that touches many
		// staging rows would otherwise fire these back-to-back writer
		// transactions with no gap, re-acquiring the lock before a waiting
		// dispatch can, and starve it for the whole loop.
		if (i+1)%recalcYieldEvery == 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(recalcYieldPause):
			}
		}
	}
}
