package cleanup

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"letts/internal/config"
	"letts/internal/fsutil"
	"letts/internal/ids"
	"letts/internal/metrics"
	"letts/internal/stagingstore"
	"letts/internal/storage"
)

// StagingGC implements the staging garbage collector. Three sub-passes
// per cycle:
//  1. expireTTLs: flip uploading/complete rows whose time_expires elapsed to
//     deleting. uploading rows currently held by UploadLock are skipped.
//  2. tombstoneDeleting: rename the on-disk file from staging/<sh>/<sh>/<id>
//     to tombstone/<id> (preserving open fds for in-flight downloaders) and
//     touch its mtime to "now" so grace is measured from tombstone time.
//  3. unlinkOldTombstones: walk the tombstone dir, unlink and DELETE row for
//     entries older than GracePeriod (default 60s).
type StagingGC struct {
	DB         *sql.DB
	Cfg        *config.DugdaleConfig
	DataDir    string
	UploadLock *stagingstore.UploadLock
	Logger     *slog.Logger

	GracePeriod     time.Duration
	BatchSize       int
	BatchInterPause time.Duration
	Now             func() time.Time
}

// Run loops RunOnce on the cleanup.sweep_interval ticker.
func (g *StagingGC) Run(ctx context.Context) {
	interval := g.Cfg.Cleanup.SweepInterval
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
			g.RunOnce(ctx)
		}
	}
}

// RunOnce runs one full GC cycle.
//
// Order matters: drainNeedsRecalc runs first so any rows that the
// staging_recalc_after_ref_delete CASCADE trigger marked with the
// time_expires=0 sentinel get a fresh expires before expireTTLs scans.
// This is the safety net for the live-path Recalc in MissionCleaner.
// Without it a row whose live-path recalc was
// skipped or errored stays at 0 forever (effectively a disk leak).
func (g *StagingGC) RunOnce(ctx context.Context) {
	g.drainNeedsRecalc(ctx)
	g.expireTTLs(ctx)
	g.tombstoneDeleting(ctx)
	g.unlinkOldTombstones(ctx)
}

// drainNeedsRecalc finds staging rows whose time_expires was zeroed by the
// CASCADE trigger and re-applies the TTL formula. Limited per pass to
// the batch size so a flood doesn't monopolise the writer.
func (g *StagingGC) drainNeedsRecalc(ctx context.Context) {
	ids, err := storage.FindStagingNeedingRecalc(ctx, g.DB, g.batchSize())
	if err != nil {
		g.logger().Warn("find staging needing recalc", "err", err)
		return
	}
	if len(ids) == 0 {
		return
	}
	ttl := storage.TTLPolicy{
		MissionSuccess: g.Cfg.Cleanup.SuccessTTL,
		MissionFailed:  g.Cfg.Cleanup.FailedTTL,
		ExecSuccess:    g.Cfg.Exec.ExecSuccessTTL,
		ExecFailed:     g.Cfg.Exec.ExecFailedTTL,
		StagingTTL:     g.Cfg.Cleanup.StagingTTL,
		DownloadGrace:  g.Cfg.Cleanup.DownloadedGrace,
	}
	nowMs := g.now().UnixMilli()
	for _, id := range ids {
		// Hold the writer-tx lock for each SELECT+UPDATE so a
		// concurrent dispatch's recalc on the same staging_id doesn't
		// race ours.
		err := storage.WithWriter(ctx, g.DB, func(conn *sql.Conn) error {
			_, e := storage.RecalcStagingTTL(ctx, conn, id, ttl, nowMs)
			return e
		})
		if err != nil {
			g.logger().Warn("drain RecalcStagingTTL", "staging_id", id, "err", err)
		}
	}
}

func (g *StagingGC) logger() *slog.Logger {
	if g.Logger == nil {
		return slog.Default()
	}
	return g.Logger
}

func (g *StagingGC) gracePeriod() time.Duration {
	if g.GracePeriod > 0 {
		return g.GracePeriod
	}
	return 60 * time.Second
}

func (g *StagingGC) batchSize() int {
	if g.BatchSize > 0 {
		return g.BatchSize
	}
	return 1000
}

func (g *StagingGC) now() time.Time {
	if g.Now != nil {
		return g.Now()
	}
	return time.Now()
}

// expireTTLs flips uploading/complete rows past time_expires to deleting,
// skipping uploads currently locked by an active PUT handler.
//
// Sentinel `time_expires=0` rows are excluded — that value is written by
// the staging_recalc_after_ref_delete CASCADE trigger to mark a row
// as "needs recalc". They're drained by RecalcStagingTTL in cleanup's
// mission-finalize path; reaping them in the window between CASCADE
// commit and the recalc call would destroy artifacts still referenced
// by live missions. The periodic recalc drain in cleanup
// closes the case where the live-path recalc was skipped or failed.
func (g *StagingGC) expireTTLs(ctx context.Context) {
	nowMs := g.now().UnixMilli()
	rows, err := g.DB.QueryContext(ctx,
		`SELECT staging_id, state FROM staging_files
		 WHERE time_expires > 0 AND time_expires <= ? AND state IN ('uploading','complete') LIMIT ?`,
		nowMs, g.batchSize())
	if err != nil {
		g.logger().Warn("query expired staging", "err", err)
		return
	}
	type pendingRow struct{ id, state string }
	var pending []pendingRow
	for rows.Next() {
		var s pendingRow
		if err := rows.Scan(&s.id, &s.state); err != nil {
			_ = rows.Close()
			g.logger().Warn("scan expired staging", "err", err)
			return
		}
		pending = append(pending, s)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		g.logger().Warn("expired staging rows", "err", err)
		return
	}
	for _, s := range pending {
		if s.state == string(storage.StagingUploading) && g.UploadLock != nil && g.UploadLock.IsLocked(s.id) {
			continue
		}
		// Re-check time_expires inside the UPDATE so a
		// concurrent dispatch that promoted the row to live (recalc to
		// MaxInt64) wins the race instead of being silently reaped.
		if err := storage.MarkStagingDeletingIfExpired(ctx, g.DB, s.id, nowMs); err != nil && !errors.Is(err, storage.ErrNotFound) {
			g.logger().Warn("mark staging deleting", "id", s.id, "err", err)
		}
	}
}

// tombstoneDeleting renames each state='deleting' file to tombstone/<id>. If
// the source file is missing, the row is DELETEd immediately (nothing to
// keep around for grace).
func (g *StagingGC) tombstoneDeleting(ctx context.Context) {
	rows, err := g.DB.QueryContext(ctx,
		`SELECT staging_id, path FROM staging_files WHERE state='deleting' LIMIT ?`,
		g.batchSize())
	if err != nil {
		g.logger().Warn("query deleting staging", "err", err)
		return
	}
	type pendingRow struct{ id, path string }
	var pending []pendingRow
	for rows.Next() {
		var s pendingRow
		if err := rows.Scan(&s.id, &s.path); err != nil {
			_ = rows.Close()
			return
		}
		pending = append(pending, s)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return
	}
	if len(pending) == 0 {
		return
	}

	tombDir := filepath.Join(g.DataDir, "tombstone")
	if err := os.MkdirAll(tombDir, 0o755); err != nil {
		g.logger().Warn("mkdir tombstone", "err", err)
		return
	}

	now := g.now()
	any := false
	for _, s := range pending {
		tombAbs := filepath.Join(tombDir, s.id)
		if _, err := os.Stat(tombAbs); err == nil {
			// Tombstoned by a previous pass; the unlink phase will reap it.
			continue
		}
		srcAbs := filepath.Join(g.DataDir, s.path)
		if _, err := os.Stat(srcAbs); errors.Is(err, os.ErrNotExist) {
			// File missing — drop the row directly. WithWriter wraps the
			// DELETE in BEGIN IMMEDIATE so it can't race other writers.
			derr := storage.WithWriter(ctx, g.DB, func(c *sql.Conn) error {
				_, err := c.ExecContext(ctx,
					`DELETE FROM staging_files WHERE staging_id=?`, s.id)
				return err
			})
			if derr != nil {
				g.logger().Warn("delete missing-file staging row", "id", s.id, "err", derr)
			}
			continue
		}
		if err := os.Rename(srcAbs, tombAbs); err != nil {
			g.logger().Warn("rename to tombstone", "id", s.id, "err", err)
			continue
		}
		// Touch tombstone mtime to "now" so grace is measured from tombstone time
		// rather than from the file's original creation.
		_ = os.Chtimes(tombAbs, now, now)
		any = true
	}
	if any {
		metrics.ObserveSyncDir(
			fsutil.SyncDir(tombDir),
			g.logger(), "staging_gc_tomb")
	}
}

// unlinkOldTombstones walks the tombstone directory and unlinks files whose
// mtime is older than GracePeriod, then DELETEs the corresponding rows. The
// in-flight POSIX guarantee (open fd survives unlink) covers downloads that
// started before the rename phase.
func (g *StagingGC) unlinkOldTombstones(ctx context.Context) {
	tombDir := filepath.Join(g.DataDir, "tombstone")
	entries, err := os.ReadDir(tombDir)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			g.logger().Warn("read tombstone dir", "err", err)
		}
		return
	}
	cutoff := g.now().Add(-g.gracePeriod())
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		// Skip any directory entry whose name isn't a valid
		// UUIDv7. The staging GC owns this directory but an admin may
		// have dropped a README, leftover tooling output, or a temp
		// file alongside the real tombstones; foreign entries are not
		// ours to touch and must NOT reach the SQL DELETE (the WHERE
		// clause would harmlessly target a non-existent staging_id but
		// still wastes a write tx and pollutes audit grep).
		if !ids.ValidateUUIDv7(e.Name()) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		if info.ModTime().After(cutoff) {
			continue
		}
		path := filepath.Join(tombDir, e.Name())
		if err := os.Remove(path); err != nil {
			g.logger().Warn("unlink tombstone", "name", e.Name(), "err", err)
			continue
		}
		// Guard on state='deleting' so a row that somehow got
		// flipped back to 'complete' (manual SQL, future code path) is
		// not silently DELETE'd from under a live mission referencing
		// it. The file is already unlinked above; if the row is no
		// longer deleting we leave the SQL in place and let the next
		// cycle reconcile.
		derr := storage.WithWriter(ctx, g.DB, func(c *sql.Conn) error {
			_, err := c.ExecContext(ctx,
				`DELETE FROM staging_files WHERE staging_id=? AND state='deleting'`, e.Name())
			return err
		})
		if derr != nil {
			g.logger().Warn("delete staging row", "id", e.Name(), "err", derr)
		}
	}
}
