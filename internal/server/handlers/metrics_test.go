package handlers_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"letts/internal/metrics"
	"letts/internal/server/handlers"
)

func TestMetricsHandlerRoundtrips(t *testing.T) {
	h := &handlers.MetricsHandler{}
	mux := http.NewServeMux()
	h.Register(mux)

	// Generate at least one observed metric so the response isn't trivially empty.
	metrics.SetInfo("0.0.0-test", "abc")
	metrics.ObserveHTTP("/v1/metrics", "GET", 200, time.Millisecond)

	r := httptest.NewRequest("GET", "/v1/metrics", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)

	if w.Code != 200 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	body := w.Body.String()
	if !strings.Contains(body, "letts_dugdale_info") {
		t.Errorf("missing letts_dugdale_info in response")
	}
	if !strings.Contains(body, "letts_http_requests_total") {
		t.Errorf("missing letts_http_requests_total in response")
	}
	// Prometheus exposition format: lines start with #-comments and metric samples.
	if !strings.Contains(body, "# HELP") || !strings.Contains(body, "# TYPE") {
		t.Errorf("response missing # HELP/# TYPE headers; not Prometheus exposition format")
	}
}
