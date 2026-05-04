package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"letts/internal/apply"
	"letts/internal/config"
	"letts/internal/eventfile"
	"letts/internal/fingerprint"
	"letts/internal/fsutil"
	"letts/internal/ids"
	"letts/internal/lane"
	"letts/internal/metrics"
	"letts/internal/server/httputil"
	"letts/internal/storage"
)

// errStagingRefMissedInTx is the sentinel for the case where a staging
// ref was complete at the validation phase but flipped state between phases (admin
// force-delete, GC reap). Outer mapper returns 400 unknown_staging_ref.
var errStagingRefMissedInTx = errors.New("staging ref no longer complete")

// errStagingRefUnavailable means a referenced staging file is missing or not
// in 'complete' state (GC'd / still uploading) — as opposed to a malformed
// role/UUID. Only this case is eligible for the idempotency-replay fallback.
var errStagingRefUnavailable = errors.New("unknown_staging_ref")

// errLaneRemovingInTx is the sentinel for the force-prune race:
// the lane passed the pre-tx IsLaneRemoving gate but was being
// removed (or vanished from applied config) by the time the writer tx ran.
// Outer mapper returns 503 lane_removing so the row is never orphaned.
var errLaneRemovingInTx = errors.New("lane removing in tx")

// fileRoles returns the role of each file ref, for duplicate detection.
func fileRoles(files []FileRef) []string {
	out := make([]string, len(files))
	for i, f := range files {
		out[i] = f.Role
	}
	return out
}

// cleanupOrphanMissionFiles removes every file dispatch (steps 3-13)
// may have created under <data_dir>/output/<shard>/ for missionID plus
// the staged workdir. Used by both the orphan-from-crash branch
// (eventfile.Create returns os.IsExist) and the insertErr cleanup branch
// so the two error paths leave the filesystem in an
// identical state. Best-effort — anything missing is ignored.
func cleanupOrphanMissionFiles(dataDir, outDir, missionID string) {
	_ = os.Remove(filepath.Join(outDir, missionID+"-events"))
	_ = os.Remove(filepath.Join(outDir, missionID+"-stdout"))
	_ = os.Remove(filepath.Join(outDir, missionID+"-stderr"))
	_ = os.Remove(filepath.Join(outDir, missionID+"-combined"))
	_ = os.RemoveAll(filepath.Join(dataDir, "work", missionID))
}

// DispatchRequest matches POST /v1/dispatch body.
type DispatchRequest struct {
	Mission string          `json:"mission"`
	Lane    string          `json:"lane"`
	Input   json.RawMessage `json:"input"`
	Files   []FileRef       `json:"files"`
	Timeout string          `json:"timeout,omitempty"`
}

// FileRef is one entry in the dispatch files array.
type FileRef struct {
	Role      string `json:"role"`
	StagingID string `json:"staging_id"`
}

// DispatchHandler holds dependencies for POST /v1/dispatch.
//
// IsDraining, when non-nil, lets the handler short-circuit with 503 and
// Retry-After during graceful shutdown so new dispatches stop queuing
// while in-flight missions drain.
type DispatchHandler struct {
	DB          *sql.DB
	Cfg         *config.DugdaleConfig
	DataDir     string
	LaneManager *lane.Manager
	KeyMu       *KeyMutex
	GetApplied  func() (*apply.AppliedState, bool) // cached applied state accessor
	IsDraining  func() bool
	// DiskUsage, when non-nil, returns the most-recently-measured size of
	// data_dir in bytes. The handler refuses with 503 disk_quota_exceeded
	// once this exceeds cfg.Limits.MaxDataDirSize.
	DiskUsage func() int64
}

// Register mounts the dispatch route.
func (h *DispatchHandler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/dispatch", h.Dispatch)
}

// Dispatch handles POST /v1/dispatch.
func (h *DispatchHandler) Dispatch(w http.ResponseWriter, r *http.Request) {
	// Bound body read time on this JSON POST so a slow-trickle body
	// (under the MaxBytesReader size cap) can't tie up a goroutine indefinitely.
	httputil.SetRequestReadDeadline(w, httputil.JSONBodyReadTimeout)
	// Graceful-shutdown gate: once SIGTERM has been
	// received, refuse new dispatches with 503 and Retry-After so the daemon
	// can drain in-flight missions without unbounded backlog.
	if h.IsDraining != nil && h.IsDraining() {
		w.Header().Set("Retry-After", "30")
		httputil.WriteError(w, http.StatusServiceUnavailable, "draining",
			"dugdale is shutting down; retry on another instance", nil)
		return
	}
	// Quota gate: when max_data_dir_size is configured and the
	// cached usage exceeds it, refuse new dispatches with 503 so we don't
	// queue more work onto an already-full disk.
	if h.Cfg.Limits.MaxDataDirSize > 0 && h.DiskUsage != nil &&
		h.DiskUsage() > h.Cfg.Limits.MaxDataDirSize {
		w.Header().Set("Retry-After", "30")
		httputil.WriteError(w, http.StatusServiceUnavailable, "disk_quota_exceeded",
			"data_dir size exceeds max_data_dir_size", nil)
		return
	}
	// --- Step 1: parse Idempotency-Key first, then body ---
	idemKey := r.Header.Get("Idempotency-Key")
	if !ids.ValidateUUIDv7(idemKey) {
		httputil.WriteError(w, 400, "bad_request", "Idempotency-Key must be a valid UUIDv7", nil)
		return
	}
	missionID := idemKey // mission_id == Idempotency-Key by design

	var req DispatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		// A MaxBytesReader-wrapped body (installed by middleware.BodyLimit)
		// surfaces "http: request body too large" here; requires
		// 413 payload_too_large, not 400 bad_request.
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			httputil.WriteError(w, 413, "payload_too_large", "request body exceeds limit",
				map[string]any{"limit_bytes": maxErr.Limit})
			return
		}
		httputil.WriteError(w, 400, "bad_request", "invalid JSON body: "+err.Error(), nil)
		return
	}

	// Validate mission/lane name regex before any expensive
	// work (lock acquire, JCS canonicalization, fingerprint compute, DB
	// lookups for staging refs). Both are pure regex matches; rejecting
	// bad names here costs ~1µs vs hundreds of µs+DB later. The
	// idempotency-replay path still works for valid names with no other
	// changes because validation is deterministic on (req.Mission,
	// req.Lane) — a retry with the same valid body re-passes; a retry
	// whose name became invalid post-tightening would 400 here instead
	// of producing a stale 200 idempotent, which is acceptable.
	if err := config.ValidateMissionName(req.Mission); err != nil {
		httputil.WriteError(w, 400, "bad_request", err.Error(), nil)
		return
	}
	if err := config.ValidateLaneName(req.Lane); err != nil {
		httputil.WriteError(w, 400, "bad_request", err.Error(), nil)
		return
	}

	// Acquire per-mission-id serialization lock before idempotency check.
	unlock := h.KeyMu.Lock(missionID)
	defer unlock()

	// Compute canonical input.
	canonInput := []byte("null")
	if len(req.Input) > 0 {
		c, err := fingerprint.CanonicalizeInput(req.Input)
		if err != nil {
			httputil.WriteError(w, 400, "bad_request", "input not canonicalizable: "+err.Error(), nil)
			return
		}
		canonInput = c
	}

	timeoutMs, err := parseOptionalDuration(req.Timeout)
	if err != nil {
		httputil.WriteError(w, 400, "bad_request", "invalid timeout: "+err.Error(), nil)
		return
	}

	// Resolve staging refs. On failure, fall back to an idempotency replay:
	// a retry whose referenced staging has since been GC'd can no
	// longer be dispatched, but if the mission row still exists the client
	// should get its current state rather than a sticky 400. The fingerprint
	// can't be recomputed without the staging sha/size, so for an existing key
	// we trust the stored row — a different request reusing the key is anyway
	// undispatchable once its staging is gone.
	files, err := h.resolveStagingMetadata(r.Context(), req.Files)
	if err != nil {
		// Only "staging unavailable" (GC'd / incomplete) is eligible for the
		// replay fallback; malformed role/UUID is always a 400.
		if errors.Is(err, errStagingRefUnavailable) {
			if existing, gerr := storage.GetMission(r.Context(), h.DB, missionID); gerr == nil {
				if existing.Status == storage.StatusDeleting {
					httputil.WriteError(w, 410, "mission_deleting",
						"idempotency key belonged to a mission being deleted",
						map[string]any{"mission_id": missionID})
					return
				}
				httputil.WriteJSON(w, 200, map[string]any{"mission_id": missionID, "status": existing.Status})
				return
			} else if !errors.Is(gerr, storage.ErrNotFound) {
				httputil.WriteDBError(w, 500, "dispatch.replay_lookup", gerr)
				return
			}
			httputil.WriteError(w, 400, "unknown_staging_ref", "staging file no longer available", nil)
			return
		}
		httputil.WriteError(w, 400, "bad_request", err.Error(), nil)
		return
	}

	// Reject duplicate file roles (mirrors the exec in[]/out[] dedup).
	// A deterministic client error must be 400, not a 500 from the
	// msr_unique_role UNIQUE constraint inside the writer tx — the
	// PHP client sticky-retries 5xx, storming the daemon with a malformed request.
	if dup := firstDuplicate(fileRoles(req.Files)); dup != "" {
		httputil.WriteError(w, 400, "duplicate_role", "duplicate file role",
			map[string]any{"role": dup})
		return
	}

	// Compute mission fingerprint.
	fp, err := fingerprint.Mission(fingerprint.MissionInput{
		Lane:           req.Lane,
		Mission:        req.Mission,
		InputCanonical: canonInput,
		TimeoutMs:      timeoutMs,
		Files:          files,
	})
	if err != nil {
		httputil.WriteError(w, 500, "internal", err.Error(), nil)
		return
	}

	// Idempotency replay check.
	existing, err := storage.GetMission(r.Context(), h.DB, missionID)
	if err == nil {
		switch {
		case existing.InputFingerprint == fp && existing.Status != storage.StatusDeleting:
			httputil.WriteJSON(w, 200, map[string]any{"mission_id": missionID, "status": existing.Status})
			return
		case existing.InputFingerprint == fp && existing.Status == storage.StatusDeleting:
			httputil.WriteError(w, 410, "mission_deleting", "idempotency key belonged to a mission being deleted",
				map[string]any{"mission_id": missionID})
			return
		default:
			httputil.WriteError(w, 409, "idempotency_conflict", "fingerprint mismatch on existing mission",
				map[string]any{"existing": map[string]any{
					"mission_id": missionID,
					"kind":       existing.Kind,
					"mission":    existing.MissionName,
					"lane":       existing.Lane,
					"status":     existing.Status,
				}})
			return
		}
	} else if !errors.Is(err, storage.ErrNotFound) {
		// Don't echo the raw SQL error (schema leak on DB-busy).
		httputil.WriteDBError(w, 500, "dispatch.replay_lookup", err)
		return
	}

	// Canonical input size cap: checked here —
	// after the idempotency replay — so a retry of an already-created mission
	// isn't rejected with 413 just because the operator lowered
	// max_mission_input_size in the meantime.
	if int64(len(canonInput)) > h.Cfg.Limits.MaxMissionInputSize {
		httputil.WriteError(w, 413, "payload_too_large", "input exceeds max_mission_input_size", nil)
		return
	}

	// --- Step 2: readiness and backpressure (name regex already checked above) ---
	applied, ok := h.GetApplied()
	if !ok {
		httputil.WriteError(w, 412, "no_lanes_configured", "run letts apply first", nil)
		return
	}
	// An applied config with empty
	// `lanes:{}` is the same bootstrap state as no config at all —
	// emit 412 so clients can apply the standard retry-after-apply
	// path instead of treating it like an unknown lane 400.
	if len(applied.Lanes) == 0 {
		httputil.WriteError(w, 412, "no_lanes_configured",
			"applied config has no lanes; run letts apply", nil)
		return
	}
	laneCfg, ok := applied.Lanes[req.Lane]
	if !ok {
		httputil.WriteError(w, 400, "unknown_lane", fmt.Sprintf("lane %q not in applied config", req.Lane), nil)
		return
	}
	// Refuse if the runner is mid force-prune. Applied config
	// still contains the lane until SetAppliedConfig persists, but the
	// runner has already ack'd removing and won't pick this row up — so
	// without the gate the new mission lands as an orphan forever.
	if h.LaneManager != nil && h.LaneManager.IsLaneRemoving(req.Lane) {
		w.Header().Set("Retry-After", "5")
		httputil.WriteError(w, http.StatusServiceUnavailable, "lane_removing",
			"lane is being removed; retry after apply settles",
			map[string]any{"lane": req.Lane})
		return
	}
	_ = laneCfg

	// Backpressure (cached counts, soft limit).
	if h.Cfg.Limits.MaxQueuePerLane > 0 {
		var n int
		// Log the swallowed error so a DB hiccup doesn't
		// silently bypass backpressure. This is a soft limit, but
		// silent ≠ soft — operators need to see the breach.
		if err := h.DB.QueryRowContext(r.Context(), `SELECT COUNT(*) FROM missions
			WHERE lane=? AND status IN ('queued','running')`, req.Lane).Scan(&n); err != nil {
			slog.Default().Warn("dispatch backpressure per-lane count failed",
				"lane", req.Lane, "err", err)
		}
		if n >= h.Cfg.Limits.MaxQueuePerLane {
			w.Header().Set("Retry-After", "5")
			httputil.WriteError(w, 503, "queue_full", "lane queue limit",
				map[string]any{"lane": req.Lane, "queued": n})
			return
		}
	}
	if h.Cfg.Limits.MaxQueueTotal > 0 {
		var n int
		if err := h.DB.QueryRowContext(r.Context(), `SELECT COUNT(*) FROM missions
			WHERE status IN ('queued','running')`).Scan(&n); err != nil {
			slog.Default().Warn("dispatch backpressure total count failed", "err", err)
		}
		if n >= h.Cfg.Limits.MaxQueueTotal {
			w.Header().Set("Retry-After", "5")
			httputil.WriteError(w, 503, "queue_full", "global queue limit", nil)
			return
		}
	}

	// --- Step 3: file-first events file, then DB insert ---
	shard, _ := ids.ShardPath(missionID)
	outDir := filepath.Join(h.DataDir, "output", shard)
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		httputil.WriteIOError(w, 500, "dispatch.mkdir_output", err)
		return
	}
	// Best-effort fsync of the output dir chain. SyncDir on "output"
	// alone makes <sh1> durable but leaves <sh1>/<sh2>'s entry inside
	// <sh1> dirty when MkdirAll created both levels. Errors
	// are logged and counted rather than swallowed — losing this
	// fsync isn't fatal (next checkpoint flushes) but operators must
	// see persistent failure to alert on disk-level issues.
	metrics.ObserveSyncDir(
		fsutil.SyncDirChain(filepath.Join(h.DataDir, "output"), shard),
		nil, "dispatch_outdir")

	ew, err := eventfile.Create(outDir, missionID)
	if err != nil {
		if os.IsExist(err) {
			// Orphan from a previous crash — clean up and retry once.
			cleanupOrphanMissionFiles(h.DataDir, outDir, missionID)
			ew, err = eventfile.Create(outDir, missionID)
			if err != nil {
				httputil.WriteIOError(w, 500, "dispatch.events_create_after_orphan", err)
				return
			}
		} else {
			httputil.WriteIOError(w, 500, "dispatch.events_create", err)
			return
		}
	}
	ew.SetLimits(eventfile.Limits{
		MaxEventsBuffer:  h.Cfg.Limits.MaxEventsBuffer,
		MaxEventLineSize: h.Cfg.Limits.MaxEventLineSize,
	})

	nowMs := time.Now().UnixMilli()
	if _, err := ew.Append(eventfile.KindQueued, map[string]any{
		"mission_id":   missionID,
		"time_created": nowMs,
		"lane":         req.Lane,
		"mission":      req.Mission,
	}, true); err != nil {
		_ = ew.Close()
		_ = os.Remove(ew.Path())
		httputil.WriteIOError(w, 500, "dispatch.append_queued", err)
		return
	}
	// Best-effort fsync parent dir.
	_ = ew.SyncParentDir()
	_ = ew.Close()

	// Sentinel for the in-tx staging re-check.
	// Outer mapper turns this into a 400 unknown_staging_ref rather than
	// 500 — without the distinction, PHP-client sticky retry storms the
	// daemon when an admin force-delete races a dispatch.
	var stagingMissed string
	insertErr := storage.WithWriter(r.Context(), h.DB, func(c *sql.Conn) error {
		var existsCount int
		_ = c.QueryRowContext(r.Context(), `SELECT COUNT(*) FROM missions WHERE mission_id=?`, missionID).Scan(&existsCount)
		if existsCount > 0 {
			return errors.New("mission_id collision after lock")
		}

		// Re-check (in-memory, no DB) that the lane isn't mid
		// force-prune inside the writer tx. SetAppliedConfig during a
		// force-prune isn't serialized against this tx, so a dispatch that
		// passed the pre-tx IsLaneRemoving gate could still INSERT a queued row
		// into a lane whose runner has just ack'd removal — orphaning it. This
		// covers the removing window (the bulk of force-prune, incl. the slow
		// terminate step); the tiny post-mgr.Apply window where the runner is
		// already gone is left to the pre-tx applied-config check. We must NOT
		// re-read applied config here — GetApplied is a pool DB read and would
		// deadlock against this pinned BEGIN IMMEDIATE connection.
		if h.LaneManager != nil && h.LaneManager.IsLaneRemoving(req.Lane) {
			return errLaneRemovingInTx
		}

		for _, f := range files {
			st, err := storage.GetStaging(r.Context(), c, f.StagingID)
			if err != nil || st.State != storage.StagingComplete {
				stagingMissed = f.StagingID
				return errStagingRefMissedInTx
			}
		}

		// Insert mission row.
		m := &storage.Mission{
			ID:               missionID,
			Kind:             storage.KindMission,
			Lane:             req.Lane,
			MissionName:      req.Mission,
			Status:           storage.StatusQueued,
			Input:            canonInput,
			InputFingerprint: fp,
			TimeCreatedMs:    nowMs,
		}
		if timeoutMs != nil {
			m.TimeoutMs = sql.NullInt64{Int64: *timeoutMs, Valid: true}
		}
		if err := storage.InsertMission(r.Context(), c, m); err != nil {
			return err
		}

		// Insert staging refs.
		for _, f := range files {
			if err := storage.InsertRef(r.Context(), c, storage.StagingRef{
				MissionID: missionID,
				StagingID: f.StagingID,
				RefKind:   storage.RefInput,
				Role:      f.Role,
			}); err != nil {
				return err
			}
		}

		// Snapshot applied runtime config.
		ct, _ := json.Marshal(applied.Runtime.CommandTemplate)
		if err := storage.InsertRuntime(r.Context(), c, &storage.MissionRuntime{
			MissionID:           missionID,
			MissionDir:          applied.MissionDir,
			CommandTemplate:     string(ct),
			MissionPathTemplate: applied.Runtime.MissionPathTemplate,
			ValidateMissionFile: applied.Runtime.ValidateMissionFile,
		}); err != nil {
			return err
		}

		// Update staging TTL (keep alive while ref'd).
		ttl := storage.TTLPolicy{
			MissionSuccess: h.Cfg.Cleanup.SuccessTTL,
			MissionFailed:  h.Cfg.Cleanup.FailedTTL,
			ExecSuccess:    h.Cfg.Exec.ExecSuccessTTL,
			ExecFailed:     h.Cfg.Exec.ExecFailedTTL,
			StagingTTL:     h.Cfg.Cleanup.StagingTTL,
			DownloadGrace:  h.Cfg.Cleanup.DownloadedGrace,
		}
		for _, f := range files {
			if _, err := storage.RecalcStagingTTL(r.Context(), c, f.StagingID, ttl, nowMs); err != nil {
				return err
			}
		}
		return nil
	})
	if insertErr != nil {
		// The queued-event append and dispatch-time fsync may
		// have created stdout/stderr/combined sentinels and a workdir
		// alongside the events file. Use the shared helper so the
		// failure-branch leaves the same filesystem state the
		// orphan-from-crash branch does.
		cleanupOrphanMissionFiles(h.DataDir, outDir, missionID)
		if errors.Is(insertErr, errStagingRefMissedInTx) {
			httputil.WriteError(w, http.StatusBadRequest, "unknown_staging_ref",
				"staging file no longer available",
				map[string]any{"staging_id": stagingMissed})
			return
		}
		if errors.Is(insertErr, errLaneRemovingInTx) {
			w.Header().Set("Retry-After", "5")
			httputil.WriteError(w, http.StatusServiceUnavailable, "lane_removing",
				"lane is being removed; retry after apply settles",
				map[string]any{"lane": req.Lane})
			return
		}
		httputil.WriteDBError(w, http.StatusInternalServerError, "dispatch.insert", insertErr)
		return
	}

	h.LaneManager.Notify(req.Lane)
	// Emit a structured INFO line for log-only
	// observers so they don't have to subscribe to /v1/events to see new
	// dispatches.
	slog.Default().Info("mission", "phase", "dispatched",
		"mission_id", missionID, "lane", req.Lane, "mission_name", req.Mission)
	httputil.WriteJSON(w, 202, map[string]any{"mission_id": missionID, "status": "queued"})
}

// resolveStagingMetadata looks up each FileRef in the staging table and returns
// the fingerprint-ready FileRef slice with sha256/size. Returns an error if any
// ref is invalid or not in complete state.
func (h *DispatchHandler) resolveStagingMetadata(ctx context.Context, files []FileRef) ([]fingerprint.FileRef, error) {
	out := make([]fingerprint.FileRef, 0, len(files))
	for _, f := range files {
		if err := config.ValidateRoleKey(f.Role); err != nil {
			return nil, fmt.Errorf("invalid role: %w", err)
		}
		if !ids.ValidateUUIDv7(f.StagingID) {
			return nil, fmt.Errorf("invalid staging_id %q", f.StagingID)
		}
		st, err := storage.GetStaging(ctx, h.DB, f.StagingID)
		if err != nil {
			// errStagingRefUnavailable (not the malformed-input errors above)
			// so the dispatch handler can tell "GC'd/incomplete" (eligible for
			// the replay fallback) from "bad role/UUID" (always 400).
			return nil, fmt.Errorf("%w %s", errStagingRefUnavailable, f.StagingID)
		}
		if st.State != storage.StagingComplete {
			return nil, fmt.Errorf("%w %s (state=%s)", errStagingRefUnavailable, f.StagingID, st.State)
		}
		out = append(out, fingerprint.FileRef{
			Role:      f.Role,
			StagingID: f.StagingID,
			Sha256:    st.Sha256,
			Size:      st.Size,
		})
	}
	return out, nil
}

// parseOptionalDuration parses a Go duration string and returns milliseconds.
// Returns nil, nil when s is empty. Rejects negative durations:
// time.ParseDuration accepts "-5s", which would propagate a negative
// timeout_ms into the missions row and produce undefined runtime behaviour.
func parseOptionalDuration(s string) (*int64, error) {
	if s == "" {
		return nil, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return nil, err
	}
	if d < 0 {
		return nil, fmt.Errorf("duration must be non-negative, got %s", s)
	}
	ms := d.Milliseconds()
	return &ms, nil
}
