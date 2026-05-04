package metrics

import (
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestObserveMissionDoneIncrementsCounter(t *testing.T) {
	resetLaneAllowlist()
	before := testutil.ToFloat64(missionsTotal.WithLabelValues("mission", "normal", "success"))
	ObserveMissionDone("mission", "normal", "success", 250*time.Millisecond)
	after := testutil.ToFloat64(missionsTotal.WithLabelValues("mission", "normal", "success"))
	if after-before != 1 {
		t.Errorf("delta=%v, want 1", after-before)
	}
}

func TestObserveMissionDoneRecordsHistogram(t *testing.T) {
	resetLaneAllowlist()
	before := testutil.CollectAndCount(missionDuration)
	ObserveMissionDone("exec", "normal", "failed", 100*time.Millisecond)
	after := testutil.CollectAndCount(missionDuration)
	if after < before {
		t.Errorf("histogram count regressed: before=%d after=%d", before, after)
	}
}

func TestObserveHTTPIncrementsByStatus(t *testing.T) {
	resetLaneAllowlist()
	for _, status := range []int{200, 404, 500} {
		ObserveHTTP("/v1/dispatch", "POST", status, 5*time.Millisecond)
	}
	for _, status := range []int{200, 404, 500} {
		got := testutil.ToFloat64(httpRequests.WithLabelValues("/v1/dispatch", "POST", strconv.Itoa(status)))
		if got < 1 {
			t.Errorf("status=%d got=%v, want ≥1", status, got)
		}
	}
}

func TestIncAdminAuthFailureBumpsCounter(t *testing.T) {
	before := testutil.ToFloat64(adminAuthFailures)
	IncAdminAuthFailure()
	IncAdminAuthFailure()
	after := testutil.ToFloat64(adminAuthFailures)
	if after-before != 2 {
		t.Errorf("delta=%v, want 2", after-before)
	}
}

func TestIncFsyncFailureBumpsCounterByTransition(t *testing.T) {
	before := testutil.ToFloat64(fsyncFailures.WithLabelValues("dispatch_outdir"))
	IncFsyncFailure("dispatch_outdir")
	IncFsyncFailure("dispatch_outdir")
	after := testutil.ToFloat64(fsyncFailures.WithLabelValues("dispatch_outdir"))
	if after-before != 2 {
		t.Errorf("dispatch_outdir delta=%v, want 2", after-before)
	}
	// Distinct transition labels are independent series.
	other := testutil.ToFloat64(fsyncFailures.WithLabelValues("output_commit"))
	IncFsyncFailure("output_commit")
	after2 := testutil.ToFloat64(fsyncFailures.WithLabelValues("output_commit"))
	if after2-other != 1 {
		t.Errorf("output_commit delta=%v, want 1", after2-other)
	}
}

// TestObserveSyncDirSkipsOnNilErr verifies that the no-op fast path on a
// nil err doesn't bump the counter (we don't want a metric per successful
// fsync — too many series, and the counter only tracks failures).
func TestObserveSyncDirSkipsOnNilErr(t *testing.T) {
	before := testutil.ToFloat64(fsyncFailures.WithLabelValues("noop_test"))
	ObserveSyncDir(nil, nil, "noop_test")
	after := testutil.ToFloat64(fsyncFailures.WithLabelValues("noop_test"))
	if after != before {
		t.Errorf("delta=%v, want 0 (nil err is no-op)", after-before)
	}
}

// TestObserveSyncDirBumpsCounterAndLogs verifies the helper bumps the
// failure counter and emits a warn-level slog line so operators can
// see and alert on durability sync failures: silent SyncDir errors at
// critical state transitions.
func TestObserveSyncDirBumpsCounterAndLogs(t *testing.T) {
	before := testutil.ToFloat64(fsyncFailures.WithLabelValues("test_transition"))
	var buf strings.Builder
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	ObserveSyncDir(errors.New("synthetic disk failure"), logger, "test_transition")
	after := testutil.ToFloat64(fsyncFailures.WithLabelValues("test_transition"))
	if after-before != 1 {
		t.Errorf("counter delta=%v, want 1", after-before)
	}
	out := buf.String()
	if !strings.Contains(out, "syncdir_failed") {
		t.Errorf("missing syncdir_failed line; got: %s", out)
	}
	if !strings.Contains(out, "test_transition") {
		t.Errorf("missing transition label in log line; got: %s", out)
	}
}

func TestSetInfoSetsGauge(t *testing.T) {
	SetInfo("1.2.3", "deadbeef")
	got := testutil.ToFloat64(infoGauge.WithLabelValues("1.2.3", "deadbeef"))
	if got != 1 {
		t.Errorf("info gauge=%v, want 1", got)
	}
}

func TestSetLaneCountsUpdatesAllSeries(t *testing.T) {
	resetLaneAllowlist()
	SetLaneCounts("normal", 7, 3, 10, false)
	if got := testutil.ToFloat64(laneQueued.WithLabelValues("normal")); got != 7 {
		t.Errorf("queued=%v", got)
	}
	if got := testutil.ToFloat64(laneRunning.WithLabelValues("normal")); got != 3 {
		t.Errorf("running=%v", got)
	}
	if got := testutil.ToFloat64(laneConcurrency.WithLabelValues("normal")); got != 10 {
		t.Errorf("concurrency=%v", got)
	}
	if got := testutil.ToFloat64(lanePaused.WithLabelValues("normal")); got != 0 {
		t.Errorf("paused=%v", got)
	}
	SetLaneCounts("normal", 0, 0, 5, true)
	if got := testutil.ToFloat64(lanePaused.WithLabelValues("normal")); got != 1 {
		t.Errorf("paused=%v after pausing", got)
	}
}

func TestSetStorageBytesUpdatesGauge(t *testing.T) {
	SetStorageBytes("db", 12345)
	if got := testutil.ToFloat64(storageBytes.WithLabelValues("db")); got != 12345 {
		t.Errorf("db=%v", got)
	}
	SetStorageBytes("output", 999)
	if got := testutil.ToFloat64(storageBytes.WithLabelValues("output")); got != 999 {
		t.Errorf("output=%v", got)
	}
}

func TestMapLanePassesThroughBelowCap(t *testing.T) {
	resetLaneAllowlist()
	got := mapLane("normal")
	if got != "normal" {
		t.Errorf("mapLane=%q, want normal", got)
	}
}

func TestMapLaneCapsAtMaxLanes(t *testing.T) {
	resetLaneAllowlist()
	for i := 0; i < MaxLanes; i++ {
		name := "lane-" + strconv.Itoa(i)
		if got := mapLane(name); got != name {
			t.Errorf("mapLane(%q)=%q", name, got)
		}
	}
	overflow := mapLane("excess")
	if overflow != "overflow" {
		t.Errorf("mapLane(excess)=%q, want overflow", overflow)
	}
	overflow2 := mapLane("another")
	if overflow2 != "overflow" {
		t.Errorf("mapLane(another)=%q, want overflow", overflow2)
	}
	// Existing lane still passes through.
	if got := mapLane("lane-0"); got != "lane-0" {
		t.Errorf("known lane changed to %q", got)
	}
}

func TestMapLaneEmptyMapsToUnset(t *testing.T) {
	resetLaneAllowlist()
	if got := mapLane(""); got != "_unset" {
		t.Errorf("mapLane(\"\")=%q", got)
	}
}

func TestUptimeGaugeNonNegative(t *testing.T) {
	got := testutil.ToFloat64(uptimeGauge)
	if got < 0 {
		t.Errorf("uptime=%v, want ≥0", got)
	}
}

// Smoke test: gather all collectors and verify expected metric names appear.
func TestCollectorsRegistered(t *testing.T) {
	resetLaneAllowlist()
	SetInfo("test", "abc")
	SetLaneCounts("normal", 1, 0, 1, false)
	SetStorageBytes("db", 1)
	ObserveMissionDone("mission", "normal", "success", time.Millisecond)
	ObserveHTTP("/v1/dispatch", "POST", 200, time.Millisecond)
	IncAdminAuthFailure()

	mfs, err := defaultGatherer().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	want := []string{
		"letts_dugdale_info",
		"letts_dugdale_uptime_seconds",
		"letts_missions_total",
		"letts_mission_duration_seconds",
		"letts_lane_queued",
		"letts_lane_running",
		"letts_lane_concurrency",
		"letts_lane_paused",
		"letts_storage_bytes",
		"letts_http_requests_total",
		"letts_http_request_duration_seconds",
		"letts_admin_auth_failures_total",
	}
	got := map[string]bool{}
	for _, mf := range mfs {
		got[mf.GetName()] = true
	}
	missing := []string{}
	for _, name := range want {
		if !got[name] {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		t.Errorf("missing collectors: %s", strings.Join(missing, ", "))
	}
}
