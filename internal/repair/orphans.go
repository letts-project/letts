package repair

import (
	"context"
	"database/sql"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"letts/internal/config"
	"letts/internal/ids"
	"letts/internal/storage"
)

// SweepOrphans removes on-disk artifacts whose mission/staging row is gone
// and recalculates staging TTLs that the schema trigger zeroed out.
// Runs unconditionally at startup; same logic as the
// daily DiskScanner but without the SkipRecent filter — startup is the right
// time to be aggressive because nothing is racing dispatch yet.
func SweepOrphans(ctx context.Context, cfg *config.DugdaleConfig, db *sql.DB, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}
	sweepOrphanOutput(ctx, cfg.DataDir, db, logger)
	sweepOrphanWork(ctx, cfg.DataDir, db, logger)
	sweepOrphanStaging(ctx, cfg.DataDir, db, logger)
	recalcZeroedTTLs(ctx, cfg, db, logger)
	return nil
}

var orphanOutputSuffixes = []string{"-stdout", "-stderr", "-combined", "-events"}

func sweepOrphanOutput(ctx context.Context, dataDir string, db *sql.DB, logger *slog.Logger) {
	root := filepath.Join(dataDir, "output")
	_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil || d.IsDir() {
			return nil
		}
		name := d.Name()
		var id string
		for _, sfx := range orphanOutputSuffixes {
			if strings.HasSuffix(name, sfx) {
				id = strings.TrimSuffix(name, sfx)
				break
			}
		}
		if id == "" || !ids.ValidateUUIDv7(id) {
			return nil
		}
		if !missionExists(ctx, db, id) {
			if err := os.Remove(path); err != nil {
				logger.Warn("repair: remove orphan output", "path", path, "err", err)
			} else {
				logger.Info("repair: removed orphan output", "path", path)
			}
		}
		return nil
	})
}

func sweepOrphanWork(ctx context.Context, dataDir string, db *sql.DB, logger *slog.Logger) {
	root := filepath.Join(dataDir, "work")
	entries, err := os.ReadDir(root)
	if err != nil {
		return
	}
	for _, e := range entries {
		if ctx.Err() != nil {
			return
		}
		if !e.IsDir() {
			continue
		}
		id := e.Name()
		if !ids.ValidateUUIDv7(id) {
			continue
		}
		if !missionExists(ctx, db, id) {
			path := filepath.Join(root, id)
			if err := os.RemoveAll(path); err != nil {
				logger.Warn("repair: remove orphan workdir", "path", path, "err", err)
			} else {
				logger.Info("repair: removed orphan workdir", "path", path)
			}
		}
	}
}

func sweepOrphanStaging(ctx context.Context, dataDir string, db *sql.DB, logger *slog.Logger) {
	root := filepath.Join(dataDir, "staging")
	_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil || d.IsDir() {
			return nil
		}
		name := d.Name()
		id := strings.TrimSuffix(name, ".tmp")
		if !ids.ValidateUUIDv7(id) {
			return nil
		}
		if !stagingExists(ctx, db, id) {
			if err := os.Remove(path); err != nil {
				logger.Warn("repair: remove orphan staging", "path", path, "err", err)
			} else {
				logger.Info("repair: removed orphan staging", "path", path)
			}
		}
		return nil
	})
}

func recalcZeroedTTLs(ctx context.Context, cfg *config.DugdaleConfig, db *sql.DB, logger *slog.Logger) {
	stagingIDs, err := storage.FindStagingNeedingRecalc(ctx, db, 10000)
	if err != nil {
		logger.Warn("repair: FindStagingNeedingRecalc", "err", err)
		return
	}
	if len(stagingIDs) == 0 {
		return
	}
	ttl := storage.TTLPolicy{
		MissionSuccess: cfg.Cleanup.SuccessTTL,
		MissionFailed:  cfg.Cleanup.FailedTTL,
		ExecSuccess:    cfg.Exec.ExecSuccessTTL,
		ExecFailed:     cfg.Exec.ExecFailedTTL,
		StagingTTL:     cfg.Cleanup.StagingTTL,
		DownloadGrace:  cfg.Cleanup.DownloadedGrace,
	}
	nowMs := time.Now().UnixMilli()
	for _, sid := range stagingIDs {
		// Even at startup the writer-tx serialization matters —
		// the HTTP listener isn't open yet but background goroutines (e.g.
		// drainNeedsRecalc) may also be sweeping.
		err := storage.WithWriter(ctx, db, func(conn *sql.Conn) error {
			_, e := storage.RecalcStagingTTL(ctx, conn, sid, ttl, nowMs)
			return e
		})
		if err != nil {
			logger.Warn("repair: RecalcStagingTTL", "staging_id", sid, "err", err)
		}
	}
	logger.Info("repair: recalculated staging TTLs", "count", len(stagingIDs))
}

func missionExists(ctx context.Context, db *sql.DB, id string) bool {
	var n int
	err := db.QueryRowContext(ctx, `SELECT 1 FROM missions WHERE mission_id=?`, id).Scan(&n)
	if errors.Is(err, sql.ErrNoRows) {
		return false
	}
	if err != nil {
		return true // err on the side of keeping the file
	}
	return true
}

func stagingExists(ctx context.Context, db *sql.DB, id string) bool {
	var n int
	err := db.QueryRowContext(ctx, `SELECT 1 FROM staging_files WHERE staging_id=?`, id).Scan(&n)
	if errors.Is(err, sql.ErrNoRows) {
		return false
	}
	if err != nil {
		return true
	}
	return true
}
