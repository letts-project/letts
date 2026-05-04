package metrics

import (
	"context"
	"database/sql"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"letts/internal/lane"
)

// Poller refreshes the gauge metrics that aren't observed inline (lane
// counters, storage bytes). Called from main on a slow timer (default 15s).
type Poller struct {
	DB       *sql.DB
	Mgr      *lane.Manager
	DataDir  string
	Interval time.Duration
	Logger   *slog.Logger

	// seenLanes tracks lane names we've published gauges for so a later
	// refresh can DeleteLabelValues on lanes that vanished — without
	// this letts_lane_* gauges retain stale values forever after
	// `letts apply` removes a lane.
	seenLanes map[string]struct{}
}

// Run loops RefreshOnce on Interval until ctx is cancelled.
func (p *Poller) Run(ctx context.Context) {
	interval := p.Interval
	if interval <= 0 {
		interval = 15 * time.Second
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()
	p.RefreshOnce(ctx) // initial sample so /metrics isn't empty on first scrape
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			p.RefreshOnce(ctx)
		}
	}
}

// RefreshOnce queries DB and filesystem, then updates the gauge collectors.
func (p *Poller) RefreshOnce(ctx context.Context) {
	p.refreshLaneCounts(ctx)
	p.refreshStorageBytes()
}

func (p *Poller) logger() *slog.Logger {
	if p.Logger == nil {
		return slog.Default()
	}
	return p.Logger
}

func (p *Poller) refreshLaneCounts(ctx context.Context) {
	if p.Mgr == nil {
		return
	}
	specs := p.Mgr.CurrentLanes()
	queued := map[string]int{}
	running := map[string]int{}
	if p.DB != nil {
		rows, err := p.DB.QueryContext(ctx,
			`SELECT lane, status, COUNT(*) FROM missions
			 WHERE status IN ('queued','running') GROUP BY lane, status`)
		if err != nil {
			p.logger().Warn("metrics: lane counts query", "err", err)
		} else {
			for rows.Next() {
				var lane, status string
				var n int
				if err := rows.Scan(&lane, &status, &n); err == nil {
					switch status {
					case "queued":
						queued[lane] = n
					case "running":
						running[lane] = n
					}
				}
			}
			_ = rows.Close()
		}
	}
	if p.seenLanes == nil {
		p.seenLanes = map[string]struct{}{}
	}
	current := make(map[string]struct{}, len(specs))
	for _, s := range specs {
		SetLaneCounts(s.Name, queued[s.Name], running[s.Name], s.Concurrency, s.Paused)
		current[s.Name] = struct{}{}
	}
	// Clear gauges for lanes that disappeared since the
	// last refresh, otherwise stale series accumulate in Prometheus.
	for name := range p.seenLanes {
		if _, still := current[name]; !still {
			DeleteLaneGauges(name)
		}
	}
	p.seenLanes = current
}

func (p *Poller) refreshStorageBytes() {
	if p.DataDir == "" {
		return
	}
	for _, kind := range []string{"output", "staging"} {
		root := filepath.Join(p.DataDir, kind)
		size, err := dirSize(root)
		if err != nil {
			p.logger().Warn("metrics: dir size", "kind", kind, "err", err)
			continue
		}
		SetStorageBytes(kind, size)
	}
	if dbPath := filepath.Join(p.DataDir, "state.db"); dbPath != "" {
		if size, err := fileSize(dbPath); err == nil {
			SetStorageBytes("db", size)
		}
	}
}

func dirSize(root string) (int64, error) {
	var total int64
	err := filepath.WalkDir(root, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil // skip unreadable entries
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		total += info.Size()
		return nil
	})
	return total, err
}

func fileSize(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}
