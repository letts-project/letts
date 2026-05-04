package middleware_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"letts/internal/server/middleware"
)

func TestRequestLogFields(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	wrapped := middleware.RequestLog(logger, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/v1/healthz", nil)
	w := httptest.NewRecorder()
	wrapped.ServeHTTP(w, req)

	var entry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("parse log JSON: %v (raw: %s)", err, buf.String())
	}

	for _, field := range []string{"method", "path", "status", "duration_ms"} {
		if _, ok := entry[field]; !ok {
			t.Errorf("missing log field %q", field)
		}
	}
	if entry["method"] != "GET" {
		t.Errorf("method: got %v, want GET", entry["method"])
	}
	if entry["path"] != "/v1/healthz" {
		t.Errorf("path: got %v, want /v1/healthz", entry["path"])
	}
	if entry["status"].(float64) != http.StatusOK {
		t.Errorf("status: got %v, want 200", entry["status"])
	}
}

func TestRequestLogMissionID(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	wrapped := middleware.RequestLog(logger, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	missionID := "01234567-89ab-cdef-0123-456789abcdef"
	req := httptest.NewRequest("GET", "/v1/missions/"+missionID+"/status", nil)
	w := httptest.NewRecorder()
	wrapped.ServeHTTP(w, req)

	var entry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("parse log JSON: %v", err)
	}
	if entry["mission_id"] != missionID {
		t.Errorf("mission_id: got %v, want %s", entry["mission_id"], missionID)
	}
}

func TestRequestLogNoMissionID(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	wrapped := middleware.RequestLog(logger, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/v1/healthz", nil)
	w := httptest.NewRecorder()
	wrapped.ServeHTTP(w, req)

	var entry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("parse log JSON: %v", err)
	}
	// mission_id should not appear for non-mission paths
	if _, ok := entry["mission_id"]; ok {
		t.Error("mission_id should be absent for non-mission path")
	}
}

func TestRequestLogNon200Status(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	wrapped := middleware.RequestLog(logger, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))

	req := httptest.NewRequest("GET", "/v1/missing", nil)
	w := httptest.NewRecorder()
	wrapped.ServeHTTP(w, req)

	var entry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("parse log JSON: %v", err)
	}
	if entry["status"].(float64) != http.StatusNotFound {
		t.Errorf("status: got %v, want 404", entry["status"])
	}
}

// TestRequestLogObservesMetricsByRouteTemplate verifies that the metric
// labels use the routing template (e.g. /v1/foo/{id}), not the raw URL with
// the substituted UUID — otherwise label cardinality would explode.
func TestRequestLogObservesMetricsByRouteTemplate(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil))

	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/foo/{id}", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	beforeOK := metricCounterValue(t, "letts_http_requests_total",
		map[string]string{"route": "/v1/foo/{id}", "method": "GET", "status": "200"})

	wrapped := middleware.RequestLog(logger, mux)
	req := httptest.NewRequest("GET", "/v1/foo/abcdef", nil)
	w := httptest.NewRecorder()
	wrapped.ServeHTTP(w, req)

	afterOK := metricCounterValue(t, "letts_http_requests_total",
		map[string]string{"route": "/v1/foo/{id}", "method": "GET", "status": "200"})
	if afterOK-beforeOK < 1 {
		t.Errorf("expected counter for route template to increment by 1, got delta %v", afterOK-beforeOK)
	}

	// And there should be NO counter using the raw URL.
	rawURL := metricCounterValue(t, "letts_http_requests_total",
		map[string]string{"route": "/v1/foo/abcdef", "method": "GET", "status": "200"})
	if rawURL > 0 {
		t.Errorf("counter exists for raw URL, value=%v", rawURL)
	}
}

// TestRequestLogUnmatchedRouteUsesSentinel verifies that a request that
// doesn't match any pattern is bucketed under the "_unmatched" route label.
func TestRequestLogUnmatchedRouteUsesSentinel(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil))

	before := metricCounterValue(t, "letts_http_requests_total",
		map[string]string{"route": "_unmatched", "method": "GET", "status": "404"})

	// Plain handler with no Pattern set.
	wrapped := middleware.RequestLog(logger, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	req := httptest.NewRequest("GET", "/nonexistent", nil)
	wrapped.ServeHTTP(httptest.NewRecorder(), req)

	after := metricCounterValue(t, "letts_http_requests_total",
		map[string]string{"route": "_unmatched", "method": "GET", "status": "404"})
	if after-before < 1 {
		t.Errorf("expected _unmatched counter to increment, delta=%v", after-before)
	}
}

// TestRequestLogBoundsUnknownHTTPMethodLabel verifies r.Method goes
// into the Prometheus `method` label, so an unauth client scanning with
// random verbs (PROPFIND, XYZZY, custom strings) could grow the metric
// label space without bound. The middleware now maps anything outside
// the standard verbs to "_other".
func TestRequestLogBoundsUnknownHTTPMethodLabel(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil))

	wrapped := middleware.RequestLog(logger, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))

	before := metricCounterValue(t, "letts_http_requests_total",
		map[string]string{"route": "_unmatched", "method": "_other", "status": "404"})

	for _, m := range []string{"PROPFIND", "XYZZY", "MOVE", "REPORT"} {
		req := httptest.NewRequest(m, "/nonexistent", nil)
		wrapped.ServeHTTP(httptest.NewRecorder(), req)
	}

	after := metricCounterValue(t, "letts_http_requests_total",
		map[string]string{"route": "_unmatched", "method": "_other", "status": "404"})
	if after-before < 4 {
		t.Errorf("_other counter delta=%v, want >=4 (one per non-standard method)", after-before)
	}

	// None of the raw verbs should have their own counter.
	for _, m := range []string{"PROPFIND", "XYZZY", "MOVE", "REPORT"} {
		v := metricCounterValue(t, "letts_http_requests_total",
			map[string]string{"route": "_unmatched", "method": m, "status": "404"})
		if v > 0 {
			t.Errorf("counter exists for raw method %q, value=%v — cardinality leak", m, v)
		}
	}
}

// flushRecorder is a fake ResponseWriter that records whether Flush() was
// called. Used to verify the middleware wrapper forwards Flush() through.
type flushRecorder struct {
	http.ResponseWriter
	flushed bool
}

func (f *flushRecorder) Flush() { f.flushed = true }

// nonFlushRecorder is a ResponseWriter that does NOT implement http.Flusher.
// Used to verify the wrapper's Flush() is a safe no-op when the underlying
// writer doesn't support flushing.
type nonFlushRecorder struct {
	header http.Header
	body   bytes.Buffer
	status int
}

func (n *nonFlushRecorder) Header() http.Header {
	if n.header == nil {
		n.header = http.Header{}
	}
	return n.header
}
func (n *nonFlushRecorder) Write(b []byte) (int, error) { return n.body.Write(b) }
func (n *nonFlushRecorder) WriteHeader(code int)        { n.status = code }

// TestResponseWriterForwardsFlush verifies the middleware's responseWriter
// wrapper implements http.Flusher and forwards Flush() to the underlying
// ResponseWriter. Without this, handlers like events.go that rely on chunked
// streaming see their `w.(http.Flusher).Flush()` calls become dead code,
// because the wrapper's embedded http.ResponseWriter interface does NOT
// include Flush().
func TestResponseWriterForwardsFlush(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil))

	fake := &flushRecorder{ResponseWriter: httptest.NewRecorder()}

	var (
		sawFlusher bool
		flushErr   error
	)
	wrapped := middleware.RequestLog(logger, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		sawFlusher = ok
		if !ok {
			flushErr = errors.New("middleware wrapper does not implement http.Flusher")
			return
		}
		flusher.Flush()
	}))

	req := httptest.NewRequest("GET", "/v1/healthz", nil)
	wrapped.ServeHTTP(fake, req)

	if !sawFlusher {
		t.Fatalf("middleware wrapper does not implement http.Flusher: %v", flushErr)
	}
	if !fake.flushed {
		t.Error("expected Flush() to be forwarded to the underlying ResponseWriter, but it was not")
	}
}

// TestResponseWriterFlushNoopWhenUnderlyingNotFlusher verifies the wrapper's
// Flush() is a safe no-op (no panic) when the underlying ResponseWriter does
// not implement http.Flusher.
func TestResponseWriterFlushNoopWhenUnderlyingNotFlusher(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil))

	nonFlusher := &nonFlushRecorder{}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Flush() panicked when underlying writer is not a Flusher: %v", r)
		}
	}()

	wrapped := middleware.RequestLog(logger, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Error("middleware wrapper should still expose http.Flusher even if underlying does not")
			return
		}
		flusher.Flush() // must not panic
	}))

	req := httptest.NewRequest("GET", "/v1/healthz", nil)
	wrapped.ServeHTTP(nonFlusher, req)
}

// metricCounterValue reads a counter sample matching all provided labels.
func metricCounterValue(t *testing.T, name string, labels map[string]string) float64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			have := map[string]string{}
			for _, lp := range m.GetLabel() {
				have[lp.GetName()] = lp.GetValue()
			}
			ok := true
			for k, v := range labels {
				if have[k] != v {
					ok = false
					break
				}
			}
			if ok && m.Counter != nil {
				return m.Counter.GetValue()
			}
		}
	}
	return 0
}
