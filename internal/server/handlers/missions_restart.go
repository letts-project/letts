package handlers

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"letts/internal/eventfile"
	"letts/internal/fsutil"
	"letts/internal/ids"
	"letts/internal/metrics"
	"letts/internal/server/httputil"
	"letts/internal/storage"
)

// opOutcome describes the result of a single mission control operation in
// terms suitable for both single-mission HTTP responses and per-id bulk
// result entries.
type opOutcome struct {
	Status     int            // HTTP status code
	ErrorCode  string         // empty on success
	ErrorMsg   string         // optional human-readable message
	Details    map[string]any // optional details for error response
	NewID      string         // for restart: id of the newly queued mission
	StatusName string         // for delete: e.g., "deletion_pending"
}

// dbErrorOutcome builds a 500 db_error opOutcome whose ErrorMsg is a
// generic "database error" string — the raw err is logged via slog.
// SQL error text (table/column names, query fragments) is
// minor info disclosure; the opOutcome's ErrorMsg is rendered verbatim
// into the HTTP response so we must NOT echo it.
func dbErrorOutcome(op string, err error) opOutcome {
	slog.Default().Error("db_error", "op", op, "err", err)
	return opOutcome{Status: http.StatusInternalServerError, ErrorCode: "db_error", ErrorMsg: "database error"}
}

// Restart implements POST /v1/missions/{id}/restart.
func (h *LifecycleHandler) Restart(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	res := h.restartOne(r.Context(), id)
	if res.Status >= 400 {
		httputil.WriteError(w, res.Status, res.ErrorCode, res.ErrorMsg, res.Details)
		return
	}
	auditLog(nil, r, "mission.restart",
		"mission_id", id, "new_mission_id", res.NewID)
	httputil.WriteJSON(w, res.Status, map[string]any{
		"mission_id":     res.NewID,
		"restarted_from": id,
		"status":         "queued",
	})
}

// restartOne performs the single-mission restart pipeline and returns an
// opOutcome. Used directly by Restart and (per id) by BulkRestart.
func (h *LifecycleHandler) restartOne(ctx context.Context, id string) opOutcome {
	if !ids.ValidateUUIDv7(id) {
		return opOutcome{Status: http.StatusBadRequest, ErrorCode: "bad_request", ErrorMsg: "invalid mission_id"}
	}
	old, err := storage.GetMission(ctx, h.DB, id)
	if errors.Is(err, storage.ErrNotFound) {
		return opOutcome{Status: http.StatusNotFound, ErrorCode: "not_found", ErrorMsg: "mission not found"}
	}
	if err != nil {
		return dbErrorOutcome("missions.restart", err)
	}
	if old.Status == storage.StatusDeleting {
		return opOutcome{Status: http.StatusConflict, ErrorCode: "mission_deleting"}
	}
	// Kind-vs-scope gate. Runs after 404/deleting checks so
	// existence/pseudo-non-existence stays uncorrelated with caller scope
	// (matches RequireKindForScope ordering). Safe today because all
	// restart routes are admin-only, but blocks future leak vectors if
	// dispatch/exec scope is ever added to bulk-restart.
	if gate := gateKindForMission(ctx, old); gate.Status != 0 {
		return gate
	}
	if old.Status != storage.StatusDone {
		return opOutcome{Status: http.StatusConflict, ErrorCode: "mission_not_done",
			ErrorMsg: "restart requires status=done",
			Details:  map[string]any{"status": string(old.Status)}}
	}

	// Refuse restart when the source mission's lane was
	// removed via `letts apply --prune` since the original dispatch.
	// Without this the new mission queues into a vanished lane with no
	// runner — sticky orphan forever.
	if h.GetApplied != nil {
		if applied, ok := h.GetApplied(); ok && applied != nil {
			if _, laneExists := applied.Lanes[old.Lane]; !laneExists {
				return opOutcome{Status: http.StatusBadRequest, ErrorCode: "unknown_lane",
					ErrorMsg: "lane no longer exists in applied config",
					Details:  map[string]any{"lane": old.Lane}}
			}
		}
	}

	refs, err := storage.RefsByMission(ctx, h.DB, id)
	if err != nil {
		return dbErrorOutcome("missions.restart", err)
	}
	for _, ref := range refs {
		if ref.RefKind == storage.RefOutput {
			continue
		}
		st, err := storage.GetStaging(ctx, h.DB, ref.StagingID)
		if err != nil || st.State != storage.StagingComplete {
			return opOutcome{Status: http.StatusConflict, ErrorCode: "input_artifacts_expired",
				ErrorMsg: "staging file no longer available",
				Details:  map[string]any{"staging_id": ref.StagingID, "role": ref.Role}}
		}
	}

	newID := ids.NewUUIDv7()
	nowMs := time.Now().UnixMilli()
	shard, err := ids.ShardPath(newID)
	if err != nil {
		return opOutcome{Status: http.StatusInternalServerError, ErrorCode: "shard_error", ErrorMsg: err.Error()}
	}
	outDir := filepath.Join(h.DataDir, "output", shard)
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		// Don't echo the absolute data_dir path to the client.
		slog.Default().Error("io_error", "op", "restart.mkdir_output", "err", err)
		return opOutcome{Status: http.StatusInternalServerError, ErrorCode: "io_error", ErrorMsg: "filesystem operation failed"}
	}
	// fsync the output dir chain so every newly-created entry along
	// base/<sh1>/<sh2> is durable. Plain SyncDir on "output" alone
	// leaves the <sh2> entry inside <sh1> on a dirty dir page when
	// MkdirAll created both at once. (Failures logged and counted
	// instead of swallowed.)
	metrics.ObserveSyncDir(
		fsutil.SyncDirChain(filepath.Join(h.DataDir, "output"), shard),
		nil, "restart_outdir")
	ew, err := eventfile.Create(outDir, newID)
	if err != nil {
		slog.Default().Error("io_error", "op", "restart.events_create", "err", err)
		return opOutcome{Status: http.StatusInternalServerError, ErrorCode: "io_error", ErrorMsg: "filesystem operation failed"}
	}
	if h.Cfg != nil {
		ew.SetLimits(eventfile.Limits{
			MaxEventsBuffer:  h.Cfg.Limits.MaxEventsBuffer,
			MaxEventLineSize: h.Cfg.Limits.MaxEventLineSize,
		})
	}
	if _, err := ew.Append(eventfile.KindQueued, map[string]any{
		"time_created":   nowMs,
		"mission_id":     newID,
		"restarted_from": id,
	}, true); err != nil {
		_ = ew.Close()
		_ = os.Remove(filepath.Join(outDir, newID+"-events"))
		return opOutcome{Status: http.StatusInternalServerError, ErrorCode: "internal", ErrorMsg: "queued event: " + err.Error()}
	}
	// fsync the shard directory so the events file's directory entry
	// survives a power loss. Without this, a crash after Append but
	// before any later metadata flush would leave the file invisible to
	// readdir/open while the DB row claimed it existed.
	_ = ew.SyncParentDir()
	_ = ew.Close()

	// Sentinel error: a staging file ref'd by the old mission was
	// finalised/deleted between the pre-tx check above and the writer
	// transaction. Surfaces a 409 (not a 500) like dispatch does for
	// the same condition.
	var errRefExpiredInTx = errors.New("staging ref expired during restart transaction")
	var expiredRef storage.StagingRef
	if err := storage.WithWriter(ctx, h.DB, func(c *sql.Conn) error {
		// Re-check staging refs INSIDE the writer transaction so a
		// concurrent admin force-delete or GC reap that lands after the
		// pre-check (above) cannot leave us with a queued mission whose
		// input refs point to expired/deleted staging rows. This is
		// mandated for dispatch and we mirror it here for
		// restart.
		for _, ref := range refs {
			if ref.RefKind == storage.RefOutput {
				continue
			}
			st, gerr := storage.GetStaging(ctx, c, ref.StagingID)
			if gerr != nil || st.State != storage.StagingComplete {
				expiredRef = ref
				return errRefExpiredInTx
			}
		}
		nm := &storage.Mission{
			ID:               newID,
			Kind:             old.Kind,
			Lane:             old.Lane,
			MissionName:      old.MissionName,
			DisplayName:      old.DisplayName,
			GroupID:          old.GroupID,
			Status:           storage.StatusQueued,
			Input:            old.Input,
			InputFingerprint: old.InputFingerprint,
			TimeoutMs:        old.TimeoutMs,
			TimeCreatedMs:    nowMs,
			RestartedFrom:    sql.NullString{String: id, Valid: true},
		}
		if err := storage.InsertMission(ctx, c, nm); err != nil {
			return err
		}
		for _, ref := range refs {
			if ref.RefKind == storage.RefOutput {
				continue
			}
			if err := storage.InsertRef(ctx, c, storage.StagingRef{
				MissionID: newID, StagingID: ref.StagingID, RefKind: ref.RefKind, Role: ref.Role,
			}); err != nil {
				return err
			}
		}
		// Dispatch and exec_dispatch both recalc TTL after
		// InsertRef so the staging row lifts to MaxInt64 (queued/running
		// ref → "infinity"). Restart did not, leaving the row
		// pinned to the source mission's failed_ttl — under that TTL the
		// staging GC tombstoned valid inputs out from under the
		// restarted queued mission.
		ttl := storage.TTLPolicy{
			MissionSuccess: h.Cfg.Cleanup.SuccessTTL,
			MissionFailed:  h.Cfg.Cleanup.FailedTTL,
			ExecSuccess:    h.Cfg.Exec.ExecSuccessTTL,
			ExecFailed:     h.Cfg.Exec.ExecFailedTTL,
			StagingTTL:     h.Cfg.Cleanup.StagingTTL,
			DownloadGrace:  h.Cfg.Cleanup.DownloadedGrace,
		}
		for _, ref := range refs {
			if ref.RefKind == storage.RefOutput {
				continue
			}
			if _, err := storage.RecalcStagingTTL(ctx, c, ref.StagingID, ttl, nowMs); err != nil {
				return err
			}
		}
		if old.Kind == storage.KindMission {
			oldRT, err := storage.GetRuntime(ctx, c, id)
			if err != nil {
				return err
			}
			oldRT.MissionID = newID
			if err := storage.InsertRuntime(ctx, c, oldRT); err != nil {
				return err
			}
		}
		// For kind=exec, no mission_runtime row exists (exec_dispatch.go
		// does not insert one). Runner re-parses payload from missions.input
		// on pickup via internal/mission/exec_runtime.go execPayload.
		return nil
	}); err != nil {
		_ = os.Remove(filepath.Join(outDir, newID+"-events"))
		if errors.Is(err, errRefExpiredInTx) {
			return opOutcome{Status: http.StatusConflict, ErrorCode: "input_artifacts_expired",
				ErrorMsg: "staging file no longer available",
				Details:  map[string]any{"staging_id": expiredRef.StagingID, "role": expiredRef.Role}}
		}
		return dbErrorOutcome("missions.restart/insert_tx", err)
	}

	if h.LaneManager != nil {
		h.LaneManager.Notify(old.Lane)
	}
	return opOutcome{Status: http.StatusCreated, NewID: newID}
}
