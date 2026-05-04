package handlers

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"strings"
	"time"

	"letts/internal/ids"
	"letts/internal/mission"
	"letts/internal/server/httputil"
	"letts/internal/storage"
)

// Delete implements DELETE /v1/missions/{id}.
func (h *LifecycleHandler) Delete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	force := strings.EqualFold(r.URL.Query().Get("force"), "true")
	res := h.deleteOne(r.Context(), id, force)
	if res.Status >= 400 {
		httputil.WriteError(w, res.Status, res.ErrorCode, res.ErrorMsg, res.Details)
		return
	}
	auditLog(nil, r, "mission.delete", "mission_id", id, "force", force)
	httputil.WriteJSON(w, res.Status, map[string]string{"status": res.StatusName})
}

// deleteOne performs the single-mission delete pipeline. force=true triggers
// SignalKill(force_delete) and waits for the mission to finalize before
// flipping the status to deleting.
func (h *LifecycleHandler) deleteOne(ctx context.Context, id string, force bool) opOutcome {
	if !ids.ValidateUUIDv7(id) {
		return opOutcome{Status: http.StatusBadRequest, ErrorCode: "bad_request", ErrorMsg: "invalid mission_id"}
	}
	m, err := storage.GetMission(ctx, h.DB, id)
	if errors.Is(err, storage.ErrNotFound) {
		return opOutcome{Status: http.StatusNotFound, ErrorCode: "not_found", ErrorMsg: "mission not found"}
	}
	if err != nil {
		return dbErrorOutcome("missions.delete", err)
	}
	// Kind-vs-scope gate. Runs after 404 and the deleting
	// short-circuit (below) so existence/pseudo-non-existence stays
	// uncorrelated with caller scope. Safe today (admin-only routes)
	// but defends future per-scope wiring of bulk-delete.
	if m.Status == storage.StatusDeleting {
		return opOutcome{Status: http.StatusAccepted, StatusName: "deletion_pending"}
	}
	if gate := gateKindForMission(ctx, m); gate.Status != 0 {
		return gate
	}
	switch m.Status {
	case storage.StatusQueued, storage.StatusDone:
		if err := markMissionDeleting(ctx, h.DB, id); err != nil {
			return dbErrorOutcome("missions.delete/mark", err)
		}
		return opOutcome{Status: http.StatusAccepted, StatusName: "deletion_pending"}
	case storage.StatusRunning:
		if !force {
			return opOutcome{Status: http.StatusConflict, ErrorCode: "mission_running",
				ErrorMsg: "running mission cannot be deleted",
				Details:  map[string]any{"hint": "POST /kill first, or use ?force=true"}}
		}
		if out := forceKillAndAwaitFinalize(ctx, h.DB, h.Runtime, id,
			h.ForceDeleteTimeout, h.ForceDeletePoll); out.Status != 0 {
			return out
		}
		if err := markMissionDeleting(ctx, h.DB, id); err != nil {
			return dbErrorOutcome("missions.delete/mark", err)
		}
		return opOutcome{Status: http.StatusAccepted, StatusName: "deletion_pending"}
	default:
		return opOutcome{Status: http.StatusInternalServerError, ErrorCode: "internal",
			ErrorMsg: "unknown mission status: " + string(m.Status)}
	}
}

var errForceDeleteTimeout = errors.New("force-delete timeout")

// forceKillAndAwaitFinalize delivers a force-delete kill to a running mission
// and blocks until the runtime commits its terminal state (status flips to
// done) or the bounded wait expires. Shared by mission DELETE ?force=true and
// staging DELETE ?force=true — both must retire a live process before
// flipping its row to 'deleting', otherwise the cleanup goroutine would
// remove the mission row and its files from under a still-running process.
// The zero opOutcome means the mission finalized and may now be marked
// deleting; any non-zero outcome is the error response to surface (504 when
// the process didn't finalize in time, 499 when the caller went away, 500
// when the kill couldn't be delivered).
func forceKillAndAwaitFinalize(ctx context.Context, db *sql.DB, rt LifecycleRuntime, id string, timeout, poll time.Duration) opOutcome {
	if rt == nil {
		return opOutcome{Status: http.StatusInternalServerError, ErrorCode: "internal", ErrorMsg: "runtime not wired"}
	}
	if !rt.SignalKill(id, mission.KillForceDelete) {
		return opOutcome{Status: http.StatusInternalServerError, ErrorCode: "internal",
			ErrorMsg: "kill not delivered (mission not registered or another kill is pending)"}
	}
	if err := waitForFinalize(ctx, db, id, timeout, poll); err != nil {
		if errors.Is(err, errForceDeleteTimeout) {
			return opOutcome{Status: http.StatusGatewayTimeout, ErrorCode: "force_delete_timeout",
				ErrorMsg: "mission did not finalize within timeout"}
		}
		if errors.Is(err, context.Canceled) {
			return opOutcome{Status: 499, ErrorCode: "client_closed"}
		}
		return dbErrorOutcome("force_delete/wait_finalize", err)
	}
	return opOutcome{}
}

func waitForFinalize(ctx context.Context, db *sql.DB, id string, timeout, poll time.Duration) error {
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	pollEvery := poll
	if pollEvery == 0 {
		pollEvery = 50 * time.Millisecond
	}
	deadline := time.Now().Add(timeout)
	for {
		cur, err := storage.GetMission(ctx, db, id)
		if err != nil {
			return err
		}
		if cur.Status == storage.StatusDone {
			return nil
		}
		if !time.Now().Before(deadline) {
			return errForceDeleteTimeout
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollEvery):
		}
	}
}

func markMissionDeleting(ctx context.Context, db *sql.DB, id string) error {
	return storage.WithWriter(ctx, db, func(c *sql.Conn) error {
		_, err := c.ExecContext(ctx,
			`UPDATE missions SET status='deleting' WHERE mission_id=? AND status IN ('queued','done')`, id)
		return err
	})
}
