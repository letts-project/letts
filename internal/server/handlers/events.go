package handlers

import (
	"database/sql"
	"errors"
	"log/slog"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"letts/internal/eventfile"
	"letts/internal/ids"
	"letts/internal/server/httputil"
	"letts/internal/storage"
)

// EventsHandler serves GET /v1/missions/{id}/events.
//
// Logger is optional — slog.Default() is used when nil. Used to warn
// when the wrapped ResponseWriter doesn't implement http.Flusher:
// without flushing, the stream buffers indefinitely and
// looks like a hang.
type EventsHandler struct {
	DataDir string
	DB      *sql.DB
	Logger  *slog.Logger
}

func (h *EventsHandler) logger() *slog.Logger {
	if h.Logger == nil {
		return slog.Default()
	}
	return h.Logger
}

// Register mounts the events streaming route.
func (h *EventsHandler) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/missions/{id}/events", h.Stream)
}

// Stream handles GET /v1/missions/{id}/events.
func (h *EventsHandler) Stream(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	// Validate UUIDv7.
	if !ids.ValidateUUIDv7(id) {
		httputil.WriteError(w, http.StatusBadRequest, "invalid_id", "mission id must be a valid UUIDv7", nil)
		return
	}

	// Look up mission.
	m, err := storage.GetMission(r.Context(), h.DB, id)
	if errors.Is(err, storage.ErrNotFound) {
		httputil.WriteError(w, http.StatusNotFound, "not_found", "mission not found", nil)
		return
	}
	if err != nil {
		httputil.WriteDBError(w, http.StatusInternalServerError, "events.Stream/get_mission", err)
		return
	}
	if m.Status == storage.StatusDeleting {
		httputil.WriteError(w, http.StatusGone, "mission_deleting", "mission is being deleted", nil)
		return
	}
	if !RequireKindForScope(w, r, m) {
		return
	}

	// Parse query params.
	q := r.URL.Query()
	follow := strings.EqualFold(q.Get("follow"), "true")
	// A mission that is already done has an immutable, complete events file:
	// serve it as an archived read and close, regardless of the
	// follow flag. This also bounds the failure mode where the terminal done
	// line is somehow missing from the file — without forcing archived mode a
	// follow client would poll forever for a done line that never comes
	// (queued/running missions still stream live until done as normal).
	if m.Status == storage.StatusDone {
		follow = false
	}
	var from int64
	if s := q.Get("from"); s != "" {
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			httputil.WriteError(w, http.StatusBadRequest, "invalid_from", "from must be an integer", nil)
			return
		}
		from = v
	}

	// Resolve parentDir.
	shard, err := ids.ShardPath(id)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "shard_error", err.Error(), nil)
		return
	}
	parentDir := filepath.Join(h.DataDir, "output", shard)

	// Set up chunked streaming response.
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)

	flusher, canFlush := w.(http.Flusher)
	if !canFlush {
		// A wrapping middleware that breaks the Flusher
		// pass-through silently turns this stream into a buffered
		// blob — chunked transfer can stall for minutes before the
		// kernel TCP buffer flushes on connection close. Surface the
		// misconfiguration so operators see it in logs immediately.
		h.logger().Warn("flusher_unavailable",
			"mission_id", id,
			"hint", "wrapping middleware must propagate http.Flusher for events streaming",
		)
	}

	opts := eventfile.ReadOptions{
		From:      from,
		Follow:    follow,
		PollEvery: 100 * time.Millisecond,
	}

	// Stream events to the response.
	_ = eventfile.Stream(r.Context(), parentDir, id, opts, func(line []byte) error {
		// Write line and newline.
		if _, err := w.Write(line); err != nil {
			return err
		}
		if _, err := w.Write([]byte("\n")); err != nil {
			return err
		}
		if canFlush {
			flusher.Flush()
		}
		return nil
	})
}
