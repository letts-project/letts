package middleware

import (
	"log/slog"
	"net/http"
	"strings"
	"time"

	"letts/internal/metrics"
)

// responseWriter wraps http.ResponseWriter to capture the status code.
type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}

// Flush forwards to the underlying ResponseWriter if it implements
// http.Flusher. Required so handlers that stream (e.g. events.go) can
// actually push chunks through the middleware wrapper.
func (rw *responseWriter) Flush() {
	if f, ok := rw.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap exposes the wrapped writer to http.ResponseController. Embedding the
// http.ResponseWriter interface only promotes its three methods, so without
// this hop the controller cannot find the connection's deadline controls and
// every per-request SetReadDeadline (the staging-PUT idle abort, the JSON
// body read deadline) silently reports ErrNotSupported behind this wrapper.
func (rw *responseWriter) Unwrap() http.ResponseWriter {
	return rw.ResponseWriter
}

// RequestLog wraps h with structured request logging via logger and observes
// each request in the metrics package using the routing template (never the
// raw URL — labels with UUIDs would explode cardinality).
//
// Each completed request logs: method, path, status, duration_ms; if the
// path contains /missions/<uuid>, mission_id is also included.
func RequestLog(logger *slog.Logger, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &responseWriter{ResponseWriter: w, status: http.StatusOK}
		h.ServeHTTP(rw, r)
		dur := time.Since(start)

		args := []any{
			"method", r.Method,
			"path", r.URL.Path,
			"status", rw.status,
			"duration_ms", dur.Milliseconds(),
		}
		if mid := extractMissionID(r.URL.Path); mid != "" {
			args = append(args, "mission_id", mid)
		}
		logger.Info("request", args...)

		metrics.ObserveHTTP(routeTemplate(r), httpMethodLabel(r.Method), rw.status, dur)
	})
}

// httpMethodLabel bounds the cardinality of the {method} label on
// letts_http_requests_total / letts_http_request_duration_seconds.
// r.Method is whatever bytes the client sent; an unauth scanner cycling
// random verbs (PROPFIND, XYZZY, Mxxxxxx) would otherwise grow the
// label space without bound.
func httpMethodLabel(m string) string {
	switch m {
	case http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete,
		http.MethodHead, http.MethodOptions, http.MethodPatch:
		return m
	}
	return "_other"
}

// routeTemplate returns the routing pattern (Go 1.22+ ServeMux) without the
// method prefix, or "_unmatched" if no pattern matched. The pattern is the
// stable label for the metrics route — e.g. "/v1/missions/{id}/events" rather
// than the raw URL.
func routeTemplate(r *http.Request) string {
	pat := r.Pattern
	if pat == "" {
		return "_unmatched"
	}
	if i := strings.IndexByte(pat, ' '); i >= 0 {
		pat = pat[i+1:]
	}
	if !strings.HasPrefix(pat, "/") {
		if i := strings.IndexByte(pat, '/'); i >= 0 {
			pat = pat[i:]
		}
	}
	return pat
}

// extractMissionID looks for /missions/<id> in the URL path and returns <id>.
// Returns empty string if not found.
func extractMissionID(path string) string {
	const segment = "/missions/"
	idx := strings.Index(path, segment)
	if idx < 0 {
		return ""
	}
	rest := path[idx+len(segment):]
	if end := strings.IndexByte(rest, '/'); end >= 0 {
		rest = rest[:end]
	}
	if rest == "" {
		return ""
	}
	return rest
}
