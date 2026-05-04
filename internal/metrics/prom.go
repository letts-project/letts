// Package metrics defines the Prometheus collectors exposed at /v1/metrics.
// Cardinality guards: lane labels are bounded at MaxLanes
// (overflow remapped to "overflow"); HTTP route labels must be the routing
// pattern (never raw URLs), enforced by the request-log middleware.
package metrics

import (
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// MaxLanes caps the cardinality of lane labels. Lanes beyond this are
// reported as "overflow" so a misconfigured fleet can't blow up the metric
// store.
const MaxLanes = 100

var (
	processStart = time.Now()

	infoGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "letts_dugdale_info",
		Help: "Build info (always 1); labels carry version and commit.",
	}, []string{"version", "commit"})

	uptimeGauge = promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "letts_dugdale_uptime_seconds",
		Help: "Seconds since the dugdale process started.",
	}, func() float64 {
		return time.Since(processStart).Seconds()
	})

	missionsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "letts_missions_total",
		Help: "Total missions terminated, by kind/lane/outcome.",
	}, []string{"kind", "lane", "outcome"})

	missionDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "letts_mission_duration_seconds",
		Help:    "Mission wallclock duration from running → done.",
		Buckets: prometheus.ExponentialBuckets(0.05, 2, 20), // 50ms .. ~7.5h
	}, []string{"kind", "lane", "outcome"})

	laneQueued = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "letts_lane_queued",
		Help: "Queued missions per lane.",
	}, []string{"lane"})

	laneRunning = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "letts_lane_running",
		Help: "Running missions per lane.",
	}, []string{"lane"})

	laneConcurrency = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "letts_lane_concurrency",
		Help: "Configured concurrency per lane.",
	}, []string{"lane"})

	lanePaused = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "letts_lane_paused",
		Help: "1 if lane is paused, 0 otherwise.",
	}, []string{"lane"})

	storageBytes = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "letts_storage_bytes",
		Help: "Disk usage by kind (db|output|staging).",
	}, []string{"kind"})

	httpRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "letts_http_requests_total",
		Help: "HTTP requests by route template, method, and status.",
	}, []string{"route", "method", "status"})

	httpDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "letts_http_request_duration_seconds",
		Help:    "HTTP request duration by route template and method.",
		Buckets: prometheus.DefBuckets,
	}, []string{"route", "method"})

	adminAuthFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "letts_admin_auth_failures_total",
		Help: "Admin/auth failures (Bearer mismatch, bad scope, etc.).",
	})

	fsyncFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "letts_fsync_failures_total",
		Help: "fsync(dir) failures on durability transitions, by transition tag.",
	}, []string{"transition"})

	laneAllowlist     sync.Map // string → struct{}
	laneAllowlistSize int64
	laneMu            sync.Mutex
)

// SetInfo sets the build-info gauge with the running version/commit.
func SetInfo(version, commit string) {
	infoGauge.WithLabelValues(version, commit).Set(1)
}

// ObserveMissionDone records one terminal mission outcome.
func ObserveMissionDone(kind, lane, outcome string, duration time.Duration) {
	l := mapLane(lane)
	missionsTotal.WithLabelValues(kind, l, outcome).Inc()
	missionDuration.WithLabelValues(kind, l, outcome).Observe(duration.Seconds())
}

// ObserveHTTP records one HTTP request. route MUST be the routing template
// (e.g., "/v1/missions/{id}/events"), never the raw URL — labels with raw
// UUIDs would explode cardinality.
func ObserveHTTP(route, method string, status int, duration time.Duration) {
	httpRequests.WithLabelValues(route, method, strconv.Itoa(status)).Inc()
	httpDuration.WithLabelValues(route, method).Observe(duration.Seconds())
}

// IncAdminAuthFailure bumps the admin auth-failure counter.
func IncAdminAuthFailure() { adminAuthFailures.Inc() }

// IncFsyncFailure bumps the fsync-failure counter for transition (e.g.
// "dispatch_outdir", "output_commit", "staging_upload"). Use through
// ObserveSyncDir so log+metric stay paired.
func IncFsyncFailure(transition string) {
	fsyncFailures.WithLabelValues(transition).Inc()
}

// ObserveSyncDir emits a warn log and bumps fsync_failures_total when err
// is non-nil. Lets fsutil.SyncDir call sites surface durability-flush
// failures without aborting the request — a failed
// directory fsync isn't fatal (the next checkpoint or subsequent fsync may
// flush it), but losing visibility into it is what matters.
//
// logger == nil falls back to slog.Default().
func ObserveSyncDir(err error, logger *slog.Logger, transition string) {
	if err == nil {
		return
	}
	if logger == nil {
		logger = slog.Default()
	}
	logger.Warn("syncdir_failed",
		"transition", transition,
		"error", err.Error(),
	)
	IncFsyncFailure(transition)
}

// SetLaneCounts updates per-lane gauges. Lanes beyond MaxLanes are
// rolled into the "overflow" series.
func SetLaneCounts(lane string, queued, running, concurrency int, paused bool) {
	l := mapLane(lane)
	laneQueued.WithLabelValues(l).Set(float64(queued))
	laneRunning.WithLabelValues(l).Set(float64(running))
	laneConcurrency.WithLabelValues(l).Set(float64(concurrency))
	if paused {
		lanePaused.WithLabelValues(l).Set(1)
	} else {
		lanePaused.WithLabelValues(l).Set(0)
	}
}

// DeleteLaneGauges removes every per-lane series for the named lane.
// Called by the poller when a lane disappears between refreshes
// (apply --prune, restart with new config) so stale values don't
// linger in Prometheus.
func DeleteLaneGauges(lane string) {
	l := mapLane(lane)
	// A lane that overflowed the cardinality cap maps to the shared
	// "overflow" label — deleting it here would wipe the series that every
	// other overflowed lane still reports into. Leave the shared series alone
	// (it's only reclaimed on daemon restart).
	if l == "overflow" {
		return
	}
	laneQueued.DeleteLabelValues(l)
	laneRunning.DeleteLabelValues(l)
	laneConcurrency.DeleteLabelValues(l)
	lanePaused.DeleteLabelValues(l)
}

// SetStorageBytes updates a storage-usage gauge for kind ∈ {db,output,staging}.
func SetStorageBytes(kind string, bytes int64) {
	storageBytes.WithLabelValues(kind).Set(float64(bytes))
}

// mapLane caps lane-label cardinality. The first MaxLanes distinct names are
// passed through; any further name is reported as "overflow".
func mapLane(lane string) string {
	if lane == "" {
		return "_unset"
	}
	if _, ok := laneAllowlist.Load(lane); ok {
		return lane
	}
	laneMu.Lock()
	defer laneMu.Unlock()
	if _, ok := laneAllowlist.Load(lane); ok {
		return lane
	}
	if laneAllowlistSize >= MaxLanes {
		return "overflow"
	}
	laneAllowlist.Store(lane, struct{}{})
	laneAllowlistSize++
	return lane
}

// resetLaneAllowlist is for tests — allows re-running cardinality scenarios
// without the package-level allowlist polluting subsequent runs.
func resetLaneAllowlist() {
	laneMu.Lock()
	defer laneMu.Unlock()
	laneAllowlist.Range(func(k, _ any) bool { laneAllowlist.Delete(k); return true })
	laneAllowlistSize = 0
}
