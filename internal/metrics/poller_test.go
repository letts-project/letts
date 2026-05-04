package metrics_test

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"letts/internal/ids"
	"letts/internal/lane"
	"letts/internal/metrics"
	"letts/internal/storage"
)

func setupPollerDB(t *testing.T) *sql.DB {
	t.Helper()
	dir := t.TempDir()
	db, err := storage.Open(filepath.Join(dir, "state.db"), storage.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := storage.Migrate(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	return db
}

func matchesLabels(got []*dto.LabelPair, want map[string]string) bool {
	have := map[string]string{}
	for _, lp := range got {
		have[lp.GetName()] = lp.GetValue()
	}
	for k, v := range want {
		if have[k] != v {
			return false
		}
	}
	return true
}

func TestPollerRefreshesLaneCounts(t *testing.T) {
	db := setupPollerDB(t)
	mgr := &lane.Manager{
		DB: db, Logger: slog.Default(), Ctx: context.Background(),
		Spawner: func(_ context.Context, _ *storage.Mission, release func()) error {
			release()
			return nil
		},
	}
	mgr.Apply([]lane.LaneSpec{{Name: "polled-lane", Concurrency: 5}})
	defer mgr.StopAll()

	insertMissionPoller(t, db, "polled-lane", storage.StatusQueued)
	insertMissionPoller(t, db, "polled-lane", storage.StatusQueued)
	insertMissionPoller(t, db, "polled-lane", storage.StatusRunning)

	p := &metrics.Poller{DB: db, Mgr: mgr, DataDir: t.TempDir()}
	p.RefreshOnce(context.Background())

	queued := readGaugeValue(t, "letts_lane_queued", "polled-lane")
	running := readGaugeValue(t, "letts_lane_running", "polled-lane")
	concur := readGaugeValue(t, "letts_lane_concurrency", "polled-lane")
	if queued != 2 {
		t.Errorf("queued=%v, want 2", queued)
	}
	if running != 1 {
		t.Errorf("running=%v, want 1", running)
	}
	if concur != 5 {
		t.Errorf("concurrency=%v, want 5", concur)
	}
}

func TestPollerRefreshesStorageBytes(t *testing.T) {
	db := setupPollerDB(t)
	dataDir := t.TempDir()

	// Write some files so output and staging dirs have measurable size.
	for _, sub := range []string{"output", "staging"} {
		dir := filepath.Join(dataDir, sub)
		_ = os.MkdirAll(dir, 0o755)
		_ = os.WriteFile(filepath.Join(dir, "sample"), []byte("hello world"), 0o644)
	}

	p := &metrics.Poller{DB: db, DataDir: dataDir}
	p.RefreshOnce(context.Background())

	if v := readGaugeValue(t, "letts_storage_bytes", "output"); v < 11 {
		t.Errorf("output=%v, want >=11", v)
	}
	if v := readGaugeValue(t, "letts_storage_bytes", "staging"); v < 11 {
		t.Errorf("staging=%v, want >=11", v)
	}
}

func TestPollerRunHonorsCtxCancel(t *testing.T) {
	db := setupPollerDB(t)
	p := &metrics.Poller{DB: db, DataDir: t.TempDir(), Interval: 5 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { p.Run(ctx); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run didn't return after cancel")
	}
}

func insertMissionPoller(t *testing.T, db *sql.DB, lane string, status storage.Status) string {
	t.Helper()
	id := ids.NewUUIDv7()
	m := storage.Mission{
		ID: id, Kind: storage.KindMission, Lane: lane,
		MissionName: "PollerFixture", Status: status,
		Input: []byte(`{}`), InputFingerprint: "fp",
		TimeCreatedMs: time.Now().UnixMilli(),
	}
	if err := storage.InsertMission(context.Background(), db, &m); err != nil {
		t.Fatal(err)
	}
	return id
}

// readGaugeValue reads a single-label gauge value from the default registry.
// label is matched against either "lane" or "kind" depending on the metric.
func readGaugeValue(t *testing.T, metric, label string) float64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != metric {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetValue() == label {
					if m.Gauge != nil {
						return m.Gauge.GetValue()
					}
				}
			}
		}
	}
	return -1
}

var _ = matchesLabels
