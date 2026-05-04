package cleanup

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

	"letts/internal/ids"
)

// DiskScanner walks output/, work/, and staging/ once per cycle and unlinks
// files whose corresponding DB row no longer exists. This is the resilience
// layer against the (rare) failure modes where the mission cleanup or
// staging GC removed a row but failed to remove the on-disk file.
type DiskScanner struct {
	DB      *sql.DB
	DataDir string
	Logger  *slog.Logger

	// SkipRecent is the minimum age a file must reach before it's a
	// candidate for orphan removal. Defaults to 5 minutes — protects
	// freshly created dispatch artifacts whose DB insert may race the scan.
	SkipRecent time.Duration
	// Interval between RunOnce calls in Run. Defaults to 24h.
	Interval time.Duration

	Now func() time.Time
}

// Run ticks RunOnce every Interval until ctx is cancelled.
func (s *DiskScanner) Run(ctx context.Context) {
	interval := s.Interval
	if interval <= 0 {
		interval = 24 * time.Hour
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			s.RunOnce(ctx)
		}
	}
}

// RunOnce sweeps output, work, and staging once.
func (s *DiskScanner) RunOnce(ctx context.Context) {
	cutoff := s.now().Add(-s.skipRecent())
	s.scanOutput(ctx, cutoff)
	s.scanWork(ctx, cutoff)
	s.scanStaging(ctx, cutoff)
}

func (s *DiskScanner) logger() *slog.Logger {
	if s.Logger == nil {
		return slog.Default()
	}
	return s.Logger
}

func (s *DiskScanner) skipRecent() time.Duration {
	if s.SkipRecent > 0 {
		return s.SkipRecent
	}
	return 5 * time.Minute
}

func (s *DiskScanner) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

var outputSuffixes = []string{"-stdout", "-stderr", "-combined", "-events"}

// scanOutput walks data_dir/output and unlinks <id>-* files whose mission_id
// is unknown.
func (s *DiskScanner) scanOutput(ctx context.Context, cutoff time.Time) {
	root := filepath.Join(s.DataDir, "output")
	_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			return nil // skip unreadable entries
		}
		if d.IsDir() {
			return nil
		}
		name := d.Name()
		var id string
		for _, sfx := range outputSuffixes {
			if strings.HasSuffix(name, sfx) {
				id = strings.TrimSuffix(name, sfx)
				break
			}
		}
		if id == "" || !ids.ValidateUUIDv7(id) {
			return nil
		}
		info, err := d.Info()
		if err != nil || info.ModTime().After(cutoff) {
			return nil
		}
		if !s.missionExists(ctx, id) {
			s.removeOrphan(path, "output")
		}
		return nil
	})
}

// scanWork walks data_dir/work and rm -rf any <id>/ whose mission row no
// longer exists.
func (s *DiskScanner) scanWork(ctx context.Context, cutoff time.Time) {
	root := filepath.Join(s.DataDir, "work")
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
		info, err := e.Info()
		if err != nil || info.ModTime().After(cutoff) {
			continue
		}
		if !s.missionExists(ctx, id) {
			path := filepath.Join(root, id)
			if rerr := os.RemoveAll(path); rerr != nil {
				s.logger().Warn("remove orphan workdir", "path", path, "err", rerr)
			} else {
				s.logger().Info("removed orphan workdir", "path", path)
			}
		}
	}
}

// scanStaging walks data_dir/staging and unlinks <id> or <id>.tmp files whose
// staging row is unknown.
func (s *DiskScanner) scanStaging(ctx context.Context, cutoff time.Time) {
	root := filepath.Join(s.DataDir, "staging")
	_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			return nil
		}
		if d.IsDir() {
			return nil
		}
		name := d.Name()
		id := strings.TrimSuffix(name, ".tmp")
		if !ids.ValidateUUIDv7(id) {
			return nil
		}
		info, err := d.Info()
		if err != nil || info.ModTime().After(cutoff) {
			return nil
		}
		if !s.stagingExists(ctx, id) {
			s.removeOrphan(path, "staging")
		}
		return nil
	})
}

func (s *DiskScanner) missionExists(ctx context.Context, id string) bool {
	var n int
	err := s.DB.QueryRowContext(ctx, `SELECT 1 FROM missions WHERE mission_id=?`, id).Scan(&n)
	if errors.Is(err, sql.ErrNoRows) {
		return false
	}
	if err != nil {
		s.logger().Warn("missionExists query", "id", id, "err", err)
		return true // err on the side of keeping the file
	}
	return true
}

func (s *DiskScanner) stagingExists(ctx context.Context, id string) bool {
	var n int
	err := s.DB.QueryRowContext(ctx, `SELECT 1 FROM staging_files WHERE staging_id=?`, id).Scan(&n)
	if errors.Is(err, sql.ErrNoRows) {
		return false
	}
	if err != nil {
		s.logger().Warn("stagingExists query", "id", id, "err", err)
		return true
	}
	return true
}

func (s *DiskScanner) removeOrphan(path, kind string) {
	if err := os.Remove(path); err != nil {
		s.logger().Warn("remove orphan", "kind", kind, "path", path, "err", err)
		return
	}
	s.logger().Info("removed orphan file", "kind", kind, "path", path)
}
