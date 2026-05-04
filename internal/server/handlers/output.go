package handlers

import (
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"letts/internal/ids"
	"letts/internal/server/httputil"
	"letts/internal/storage"
)

// OutputHandler serves GET /v1/missions/{id}/output.
type OutputHandler struct {
	DataDir   string
	DB        *sql.DB
	PollEvery time.Duration // 0 → defaults to 100ms (test override hook)
}

// Register mounts the output streaming route.
func (h *OutputHandler) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/missions/{id}/output", h.Stream)
}

// Stream handles GET /v1/missions/{id}/output?stream=stdout|stderr|combined&follow=true.
func (h *OutputHandler) Stream(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !ids.ValidateUUIDv7(id) {
		httputil.WriteError(w, http.StatusBadRequest, "invalid_id", "mission id must be a valid UUIDv7", nil)
		return
	}

	stream := r.URL.Query().Get("stream")
	suffix, contentType, ok := streamSpec(stream)
	if !ok {
		httputil.WriteError(w, http.StatusBadRequest, "bad_request",
			"stream must be one of stdout, stderr, combined", nil)
		return
	}

	m, err := storage.GetMission(r.Context(), h.DB, id)
	if errors.Is(err, storage.ErrNotFound) {
		httputil.WriteError(w, http.StatusNotFound, "not_found", "mission not found", nil)
		return
	}
	if err != nil {
		httputil.WriteDBError(w, http.StatusInternalServerError, "output.Stream/get_mission", err)
		return
	}
	if m.Status == storage.StatusDeleting {
		httputil.WriteError(w, http.StatusGone, "mission_deleting", "mission is being deleted", nil)
		return
	}
	if !RequireKindForScope(w, r, m) {
		return
	}

	follow := strings.EqualFold(r.URL.Query().Get("follow"), "true")

	shard, err := ids.ShardPath(id)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "shard_error", err.Error(), nil)
		return
	}
	path := filepath.Join(h.DataDir, "output", shard, id+suffix)

	pollEvery := h.PollEvery
	if pollEvery == 0 {
		pollEvery = 100 * time.Millisecond
	}

	f, err := os.Open(path)
	if err != nil && !os.IsNotExist(err) {
		httputil.WriteIOError(w, http.StatusInternalServerError, "output.Get/open", err)
		return
	}

	w.Header().Set("Content-Type", contentType)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	flusher, canFlush := w.(http.Flusher)
	if !canFlush && follow {
		// Mirror of events.go: a wrapping middleware
		// that drops Flusher silently buffers the stream until done.
		// Operators need to see this so they can fix their proxy setup.
		slog.Default().Warn("output: ResponseWriter does not implement http.Flusher; streaming disabled",
			"mission_id", id, "stream", stream,
			"hint", "wrapping middleware must propagate http.Flusher for output streaming",
		)
	}

	if err != nil {
		// File doesn't exist yet. Without follow=true we return an empty
		// stream — historical behavior (mission may have produced no output).
		// With follow=true we poll until the mission creates the file, or
		// until the mission terminates without producing anything. This
		// matters for clients (e.g. `letts run` tail goroutines) that open
		// the stream immediately after dispatch — before the lane runner
		// has picked up the mission and started its stdout.
		if !follow {
			return
		}
		if flusher != nil {
			flusher.Flush() // flush the headers so the client sees the 200
		}
		for {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(pollEvery):
			}
			cur, gerr := storage.GetMission(r.Context(), h.DB, id)
			missionTerminal := gerr == nil &&
				(cur.Status == storage.StatusDone || cur.Status == storage.StatusDeleting)
			// Always try the file BEFORE giving up on a terminal mission:
			// a fast exec (e.g. `uptime`) can finish and flip status=done in
			// the window between the client GET arriving and the next poll
			// tick. The runtime open()s the output files up front, so once
			// the lane runner picks the mission up, the file exists even if
			// the mission then finishes within the 100 ms poll budget.
			// Without this open-after-status-done dance, the client would
			// race-lose all output for sub-100 ms missions on the first GET.
			f, err = os.Open(path)
			if err == nil {
				break
			}
			if !os.IsNotExist(err) {
				return
			}
			if missionTerminal || gerr != nil {
				return // mission really ended without ever writing to this stream
			}
		}
	}
	defer func() { _ = f.Close() }()

	buf := make([]byte, 32*1024)
	for {
		n, rerr := f.Read(buf)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				return // client disconnected
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
		if rerr == io.EOF || (rerr == nil && n == 0) {
			if !follow {
				return
			}
			// Check ctx; if mission has terminated, drain once more then return.
			if r.Context().Err() != nil {
				return
			}
			cur, gerr := storage.GetMission(r.Context(), h.DB, id)
			if gerr != nil || cur.Status == storage.StatusDone || cur.Status == storage.StatusDeleting {
				drainAndFlush(f, w, flusher, buf)
				return
			}
			select {
			case <-r.Context().Done():
				return
			case <-time.After(pollEvery):
			}
			continue
		}
		if rerr != nil {
			return
		}
	}
}

func streamSpec(stream string) (suffix, contentType string, ok bool) {
	switch stream {
	case "stdout":
		return "-stdout", "application/octet-stream", true
	case "stderr":
		return "-stderr", "application/octet-stream", true
	case "combined":
		return "-combined", "application/x-ndjson", true
	default:
		return "", "", false
	}
}

func drainAndFlush(f *os.File, w http.ResponseWriter, flusher http.Flusher, buf []byte) {
	for {
		n, err := f.Read(buf)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
		if err != nil {
			return
		}
	}
}
