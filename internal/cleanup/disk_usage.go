package cleanup

import (
	"context"
	"io/fs"
	"log/slog"
	"path/filepath"
	"sync/atomic"
	"time"
)

// DiskUsageMonitor periodically computes the total size of data_dir and
// exposes the cached value via Size(). Wired into the dispatch and staging
// PUT handlers so they can refuse new work with 503 disk_quota_exceeded
// once cfg.Limits.MaxDataDirSize is exceeded.
//
// Walking the whole data tree is O(N) in file count — acceptable on the
// 30s default tick since dispatch traffic is rate-limited elsewhere and
// the cleanup package already does similar walks.
type DiskUsageMonitor struct {
	DataDir  string
	Interval time.Duration // default 30s
	Logger   *slog.Logger

	size atomic.Int64
}

// Run blocks until ctx is cancelled, recomputing size every Interval.
// Performs one walk synchronously up-front so handlers don't have to
// race the first tick.
func (m *DiskUsageMonitor) Run(ctx context.Context) {
	interval := m.Interval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	m.refresh(ctx)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.refresh(ctx)
		}
	}
}

// Size returns the most recently computed total. Zero until the first
// successful walk completes.
func (m *DiskUsageMonitor) Size() int64 { return m.size.Load() }

func (m *DiskUsageMonitor) refresh(ctx context.Context) {
	var total int64
	err := filepath.WalkDir(m.DataDir, func(path string, d fs.DirEntry, err error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			return nil // skip unreadable
		}
		if d.IsDir() {
			return nil
		}
		info, ierr := d.Info()
		if ierr != nil {
			return nil
		}
		total += info.Size()
		return nil
	})
	if err != nil && ctx.Err() == nil && m.Logger != nil {
		m.Logger.Warn("data_dir size scan", "err", err)
	}
	m.size.Store(total)
}
