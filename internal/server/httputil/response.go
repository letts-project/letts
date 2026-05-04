// Package httputil contains shared HTTP response helpers used by both the
// server package and its sub-packages (handlers, middleware) without creating
// an import cycle.
package httputil

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"time"
)

// JSONBodyReadTimeout bounds the time to read the (size-capped) body of the
// JSON POST endpoints. Generous — 2 MiB in 30s is ~68 KB/s — so it never
// trips a legitimate client but stops a slow-trickle goroutine-exhaustion
// slowloris that MaxBytesReader (size only) and ReadHeaderTimeout (headers
// only) leave open.
const JSONBodyReadTimeout = 30 * time.Second

// SetRequestReadDeadline bounds the time to read the rest of the request on a
// JSON POST endpoint. MUST NOT be used on streaming GETs (events/output
// follow=true) or staging PUT (large/slow uploads have their own idle
// timeout). A ResponseWriter that doesn't support deadlines (e.g. a test
// ResponseRecorder) is silently ignored.
func SetRequestReadDeadline(w http.ResponseWriter, d time.Duration) {
	_ = http.NewResponseController(w).SetReadDeadline(time.Now().Add(d))
}

// ErrorResponse is the canonical error JSON shape.
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message,omitempty"`
	Details any    `json:"details,omitempty"`
}

// WriteError writes an ErrorResponse with the given status code.
func WriteError(w http.ResponseWriter, status int, code, message string, details any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(ErrorResponse{Error: code, Message: message, Details: details})
}

// WriteDBError responds with a generic "db_error" / "database error" body
// and logs the underlying err via slog.Default at error level so operators
// can still debug. SQL error strings include table and column
// names (and sometimes query fragments), which is minor info disclosure
// — defense in depth says don't echo them. op identifies the caller site
// for log grep.
func WriteDBError(w http.ResponseWriter, status int, op string, err error) {
	slog.Default().Error("db_error", "op", op, "err", err)
	WriteError(w, status, "db_error", "database error", nil)
}

// WriteJSON writes a successful JSON response.
func WriteJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// WriteIOError responds with a generic body and logs the underlying err.
// Symmetric with WriteDBError but for filesystem errors (open, mkdir,
// fsync, read, write) — those echo full absolute paths via err.Error,
// leaking data_dir layout to anyone with a known-id capability. For
// os.IsNotExist err the response is 410 Gone (consistent with
// the staging state='deleting' branch); other failures map to whatever
// status the caller chose with a "io_error" code.
func WriteIOError(w http.ResponseWriter, defaultStatus int, op string, err error) {
	slog.Default().Error("io_error", "op", op, "err", err)
	if errors.Is(err, os.ErrNotExist) {
		WriteError(w, http.StatusGone, "gone", "resource no longer available", nil)
		return
	}
	WriteError(w, defaultStatus, "io_error", "filesystem operation failed", nil)
}
