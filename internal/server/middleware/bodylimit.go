package middleware

import (
	"net/http"

	"letts/internal/server/httputil"
)

// BodyLimit rejects requests that declare Content-Length > limit immediately
// (before reading the body), and wraps r.Body in http.MaxBytesReader for the
// remaining cases so runtime overflows are also bounded.
func BodyLimit(limit int64, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.ContentLength > limit {
			httputil.WriteError(w, http.StatusRequestEntityTooLarge,
				"payload_too_large", "request body exceeds limit",
				map[string]any{"limit_bytes": limit})
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, limit)
		h.ServeHTTP(w, r)
	}
}
