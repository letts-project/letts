package handlers

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// MetricsHandler serves GET /v1/metrics in Prometheus exposition format.
// No auth — endpoint is informational and expected to be
// firewalled / proxy-restricted at deploy time.
type MetricsHandler struct{}

// Register mounts the metrics route.
func (h *MetricsHandler) Register(mux *http.ServeMux) {
	mux.Handle("GET /v1/metrics", promhttp.Handler())
}
