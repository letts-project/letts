// Package handlers — POST /v1/exec/dispatch.
//
// Exec dispatch endpoint with full 30-step pipeline
// (validation → staging metadata → fingerprint → idempotency → file-first
// init → SQL insert → wake → audit). Mounted only when exec.enabled=true;
// otherwise a pre-auth ExecDisabledStub returns 404 feature_disabled.
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
	"letts/internal/server/middleware"
	"letts/internal/storage"
)

// ExecDispatchHandler holds dependencies for POST /v1/exec/dispatch.
// Dependencies are added as later subtasks need them, but the
// struct is shaped for the full pipeline up front so wiring in main.go is
// stable across tasks.
type ExecDispatchHandler struct {
	DB          *sql.DB
	Cfg         *config.DugdaleConfig
	DataDir     string
	LaneManager *lane.Manager
	KeyMu       *KeyMutex
	GetApplied  func() (*apply.AppliedState, bool)
	Logger      *slog.Logger
	// IsDraining, when non-nil, gates the handler on graceful shutdown:
	// once SIGTERM has been received, new exec dispatches
	// are refused with 503 and Retry-After to let in-flight execs drain.
	IsDraining func() bool
	// DiskUsage gates dispatch on data_dir size — see DispatchHandler.
	DiskUsage func() int64
}

// ExecRequest mirrors POST /v1/exec/dispatch body schema.
type ExecRequest struct {
	Lane           string         `json:"lane"`
	Command        []string       `json:"command"`
	Script         *ExecScriptRef `json:"script,omitempty"`
	In             []ExecFileRef  `json:"in,omitempty"`
	Out            []ExecOutKey   `json:"out,omitempty"`
	Stdin          string         `json:"stdin,omitempty"`
	StdinStagingID string         `json:"stdin_staging_id,omitempty"`
	Timeout        string         `json:"timeout,omitempty"`
	GroupID        string         `json:"group_id,omitempty"`
	DisplayName    string         `json:"display_name,omitempty"`
}

// ExecScriptRef references the script staging file.
type ExecScriptRef struct {
	StagingID string `json:"staging_id"`
}

// ExecFileRef is one entry in the in[] array.
type ExecFileRef struct {
	Key       string `json:"key"`
	StagingID string `json:"staging_id"`
}

// ExecOutKey is one entry in the out[] array.
type ExecOutKey struct {
	Key string `json:"key"`
}

// validStdinModes ranges over the canonical stdin modes (empty == "none").
var validStdinModes = map[string]struct{}{
	"":          {},
	"none":      {},
	"single":    {},
	"broadcast": {},
}

// Dispatch handles POST /v1/exec/dispatch.
//
// 30-step pipeline:
//
//	0    body size cap (BodyLimit middleware)
//	1    Idempotency-Key UUIDv7
//	2    JSON decode
//	3    lane non-empty and applied-state lookup (412 / 400)
//	4    command non-empty argv
//	5    in/out key regex and __ prefix reservation
//	6-7  duplicate-key checks
//	8-9  in[]/out[] count caps
//	10   shell guard (allow_shell=false)
//	11   stdin mode and staging-id coupling
//	12   staging id format checks
//	13   timeout parse
//	14   staging metadata fetch (state='complete')
//	15   script size bound
//	16   fingerprint.Exec
//	17   idempotency replay (200 / 409 / 410)
//	18-24 file-first init (mkdir → events file → queued event → fsync)
//	25   INSERT missions
//	26   INSERT mission_staging_refs (script, per-input, __stdin__)
//	27   RecalcStagingTTL
//	28   wake lane runner
//	29   audit log
//	30   202 {exec_id, status: "queued"}
func (h *ExecDispatchHandler) Dispatch(w http.ResponseWriter, r *http.Request) {
	// Bound body read time on this JSON POST (see dispatch handler).
	httputil.SetRequestReadDeadline(w, httputil.JSONBodyReadTimeout)
	// Graceful-shutdown gate: refuse new exec dispatches
	// with 503 and Retry-After once draining starts. Symmetric with the
	// dispatch handler's check so both queue paths stop in lockstep.
	if h.IsDraining != nil && h.IsDraining() {
		w.Header().Set("Retry-After", "30")
		httputil.WriteError(w, http.StatusServiceUnavailable, "draining",
			"dugdale is shutting down; retry on another instance", nil)
		return
	}
	// Quota gate — see DispatchHandler.
	if h.Cfg.Limits.MaxDataDirSize > 0 && h.DiskUsage != nil &&
		h.DiskUsage() > h.Cfg.Limits.MaxDataDirSize {
		w.Header().Set("Retry-After", "30")
		httputil.WriteError(w, http.StatusServiceUnavailable, "disk_quota_exceeded",
			"data_dir size exceeds max_data_dir_size", nil)
		return
	}
	// Step 0: body cap is enforced by middleware.BodyLimit wrapper, which
	// rejects oversized Content-Length up front and wraps r.Body in
	// http.MaxBytesReader so streaming overflows also produce 413.

	// Step 1: Idempotency-Key header is a valid UUIDv7. By design
	// it also becomes the mission_id / exec_id of the row we'll create.
	idemKey := r.Header.Get("Idempotency-Key")
	if !ids.ValidateUUIDv7(idemKey) {
		httputil.WriteError(w, http.StatusBadRequest, "bad_request",
			"Idempotency-Key must be a valid UUIDv7", nil)
		return
	}
	execID := idemKey

	// Step 2: JSON decode. A MaxBytesReader-wrapped body will surface
	// "http: request body too large" here; convert to 413 for callers.
	var req ExecRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			httputil.WriteError(w, http.StatusRequestEntityTooLarge,
				"payload_too_large", "request body exceeds limit",
				map[string]any{"limit_bytes": maxErr.Limit})
			return
		}
		httputil.WriteError(w, http.StatusBadRequest, "bad_request",
			"invalid JSON body: "+err.Error(), nil)
		return
	}

	// Step 3 (validation only): lane must be non-empty. Readiness/lane-existence
	// checks (412/400/503) are deferred until AFTER the idempotency replay (step
	// 17) so a valid retry of an already-created exec replays 200 regardless of
	// current applied-config/lane state — mirror of dispatch.go.
	if req.Lane == "" {
		httputil.WriteError(w, http.StatusBadRequest, "bad_request",
			"lane is required", nil)
		return
	}

	// Step 4: Command non-empty argv.
	if len(req.Command) == 0 {
		httputil.WriteError(w, http.StatusBadRequest, "bad_request",
			"command is required", nil)
		return
	}

	// Step 5: Validate each In/Out key against ValidateRoleKey (regex and
	// __ prefix reservation).
	for _, e := range req.In {
		if err := config.ValidateRoleKey(e.Key); err != nil {
			httputil.WriteError(w, http.StatusBadRequest, "invalid_key", err.Error(),
				map[string]any{"slot": "in", "key": e.Key})
			return
		}
	}
	for _, e := range req.Out {
		if err := config.ValidateRoleKey(e.Key); err != nil {
			httputil.WriteError(w, http.StatusBadRequest, "invalid_key", err.Error(),
				map[string]any{"slot": "out", "key": e.Key})
			return
		}
	}

	// Step 6: No duplicate keys within In[].
	if dup := firstDuplicate(execInKeys(req.In)); dup != "" {
		httputil.WriteError(w, http.StatusBadRequest, "duplicate_key",
			"duplicate key in in[]", map[string]any{"key": dup})
		return
	}
	// Step 7: No duplicate keys within Out[].
	if dup := firstDuplicate(execOutKeys(req.Out)); dup != "" {
		httputil.WriteError(w, http.StatusBadRequest, "duplicate_key",
			"duplicate key in out[]", map[string]any{"key": dup})
		return
	}

	// Step 8: in[] count cap.
	if h.Cfg.Exec.MaxInputsPerExec > 0 && len(req.In) > h.Cfg.Exec.MaxInputsPerExec {
		httputil.WriteError(w, http.StatusBadRequest, "too_many_files",
			"in[] exceeds max_inputs_per_exec",
			map[string]any{"count": len(req.In), "max": h.Cfg.Exec.MaxInputsPerExec})
		return
	}
	// Step 9: out[] count cap.
	if h.Cfg.Exec.MaxOutputsPerExec > 0 && len(req.Out) > h.Cfg.Exec.MaxOutputsPerExec {
		httputil.WriteError(w, http.StatusBadRequest, "too_many_files",
			"out[] exceeds max_outputs_per_exec",
			map[string]any{"count": len(req.Out), "max": h.Cfg.Exec.MaxOutputsPerExec})
		return
	}

	// Step 10: Shell guard. Disallowed when allow_shell=false AND argv[0] is
	// a shell AND any later arg looks like a -c form (long --command or short
	// opt cluster containing 'c').
	if !h.Cfg.Exec.AllowShell && fingerprint.IsShellForm(req.Command) {
		httputil.WriteError(w, http.StatusBadRequest, "shell_form_disabled",
			"exec.allow_shell=false; shell -c form is not permitted", nil)
		return
	}

	// Step 11: Stdin mode and StdinStagingID coupling.
	if _, ok := validStdinModes[req.Stdin]; !ok {
		httputil.WriteError(w, http.StatusBadRequest, "bad_request",
			"invalid stdin mode (expected none|single|broadcast)",
			map[string]any{"stdin": req.Stdin})
		return
	}
	stdinMode := req.Stdin
	if stdinMode == "" {
		stdinMode = "none"
	}
	if stdinMode == "none" && req.StdinStagingID != "" {
		httputil.WriteError(w, http.StatusBadRequest, "bad_request",
			"stdin_staging_id provided but stdin mode is none", nil)
		return
	}
	if stdinMode != "none" {
		if req.StdinStagingID == "" {
			httputil.WriteError(w, http.StatusBadRequest, "bad_request",
				"stdin_staging_id required when stdin is single|broadcast", nil)
			return
		}
		if !ids.ValidateUUIDv7(req.StdinStagingID) {
			httputil.WriteError(w, http.StatusBadRequest, "invalid_staging_id",
				"stdin_staging_id is not a valid UUIDv7",
				map[string]any{"staging_id": req.StdinStagingID})
			return
		}
	}

	// Step 12: Each remaining staging_id validates as UUIDv7. The stdin id
	// (if present) was already checked above to keep its bad_request /
	// invalid_staging_id distinction local.
	if req.Script != nil && !ids.ValidateUUIDv7(req.Script.StagingID) {
		httputil.WriteError(w, http.StatusBadRequest, "invalid_staging_id",
			"script.staging_id is not a valid UUIDv7",
			map[string]any{"staging_id": req.Script.StagingID})
		return
	}
	for _, e := range req.In {
		if !ids.ValidateUUIDv7(e.StagingID) {
			httputil.WriteError(w, http.StatusBadRequest, "invalid_staging_id",
				"in[].staging_id is not a valid UUIDv7",
				map[string]any{"key": e.Key, "staging_id": e.StagingID})
			return
		}
	}

	// Step 13: Timeout (if present) parses as duration → ms. Consumed by
	// computeExecFingerprint (step 16) and the missions.timeout_ms column
	// (step 25).
	timeoutMs, err := parseOptionalDuration(req.Timeout)
	if err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "bad_request",
			"invalid timeout: "+err.Error(), nil)
		return
	}

	// Bound the size and charset of group_id / display_name
	// before they hit the DB column / audit log / LETTS_GROUP_ID env.
	// group_id ought to be UUIDv7; enforce
	// strictly. display_name is operator-facing free text but capped
	// and stripped of control bytes.
	if req.GroupID != "" && !ids.ValidateUUIDv7(req.GroupID) {
		httputil.WriteError(w, http.StatusBadRequest, "bad_request",
			"invalid group_id (must be UUIDv7)", nil)
		return
	}
	if l := len(req.DisplayName); l > 256 {
		httputil.WriteError(w, http.StatusBadRequest, "bad_request",
			"display_name exceeds 256 bytes",
			map[string]any{"length": l, "max": 256})
		return
	}
	for _, b := range []byte(req.DisplayName) {
		// Reject control characters (incl. newline) so they can't break
		// audit log lines or env-var formatting downstream.
		if b < 0x20 || b == 0x7f {
			httputil.WriteError(w, http.StatusBadRequest, "bad_request",
				"display_name contains control character", nil)
			return
		}
	}

	// Step 14: Staging metadata fetch for all referenced ids (script, in[],
	// stdin). Each id must exist with state='complete'. The lookup is a
	// loop of GetStaging calls (one per ref): N is bounded by
	// MaxInputsPerExec+2, so the constant factor stays trivial and we get
	// per-id error detail for free.
	stagingMeta, err := h.fetchStagingMeta(r.Context(), &req)
	if err != nil {
		var stErr *unknownStagingError
		if errors.As(err, &stErr) {
			httputil.WriteError(w, http.StatusBadRequest, "unknown_staging_ref",
				stErr.Error(), map[string]any{"staging_id": stErr.ID})
			return
		}
		httputil.WriteError(w, http.StatusInternalServerError, "internal",
			err.Error(), nil)
		return
	}

	// Step 15: Script size bound.
	if req.Script != nil {
		sm := stagingMeta[req.Script.StagingID]
		if h.Cfg.Exec.MaxScriptSize > 0 && sm.Size > h.Cfg.Exec.MaxScriptSize {
			httputil.WriteError(w, http.StatusBadRequest, "script_too_large",
				"script size exceeds max_script_size",
				map[string]any{"size": sm.Size, "max": h.Cfg.Exec.MaxScriptSize})
			return
		}
	}

	// Step 16: Compute fingerprint over the resolved payload.
	fp, err := computeExecFingerprint(&req, stagingMeta, timeoutMs)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "internal",
			"fingerprint: "+err.Error(), nil)
		return
	}

	// Acquire per-key serialization lock before idempotency check so two
	// concurrent dispatches with the same idem key can't both proceed past
	// the existence check and race the insert.
	unlock := h.KeyMu.Lock(execID)
	defer unlock()

	// Step 17: Idempotency replay check. Same semantics as /v1/dispatch
	// (200 / 409 / 410) but bounded to kind='exec' rows.
	existing, err := storage.GetMission(r.Context(), h.DB, execID)
	if err == nil {
		switch {
		case existing.InputFingerprint == fp && existing.Status != storage.StatusDeleting:
			httputil.WriteJSON(w, http.StatusOK, map[string]any{
				"exec_id": execID,
				"status":  string(existing.Status),
			})
			return
		case existing.InputFingerprint == fp && existing.Status == storage.StatusDeleting:
			httputil.WriteError(w, http.StatusGone, "mission_deleting",
				"idempotency key belonged to an exec being deleted",
				map[string]any{"exec_id": execID})
			return
		default:
			httputil.WriteError(w, http.StatusConflict, "idempotency_conflict",
				"fingerprint mismatch on existing exec",
				map[string]any{"existing": map[string]any{
					"exec_id": execID,
					"kind":    string(existing.Kind),
					"lane":    existing.Lane,
					"status":  string(existing.Status),
				}})
			return
		}
	} else if !errors.Is(err, storage.ErrNotFound) {
		httputil.WriteError(w, http.StatusInternalServerError, "internal",
			err.Error(), nil)
		return
	}

	// Step 17b: readiness/lane checks for a genuinely NEW exec. Deferred to
	// here (after the replay) so retries of an already-created exec are not
	// rejected for current readiness reasons. Bootstrap (no apply yet) is
	// 412; unknown lane is 400; mid force-prune is 503.
	applied, ok := h.GetApplied()
	if !ok {
		httputil.WriteError(w, http.StatusPreconditionFailed, "no_lanes_configured",
			"run letts apply first", nil)
		return
	}
	if len(applied.Lanes) == 0 {
		httputil.WriteError(w, http.StatusPreconditionFailed, "no_lanes_configured",
			"applied config has no lanes; run letts apply", nil)
		return
	}
	if _, ok := applied.Lanes[req.Lane]; !ok {
		httputil.WriteError(w, http.StatusBadRequest, "unknown_lane",
			fmt.Sprintf("lane %q not in applied config", req.Lane), nil)
		return
	}
	if h.LaneManager != nil && h.LaneManager.IsLaneRemoving(req.Lane) {
		w.Header().Set("Retry-After", "5")
		httputil.WriteError(w, http.StatusServiceUnavailable, "lane_removing",
			"lane is being removed; retry after apply settles",
			map[string]any{"lane": req.Lane})
		return
	}

	// --- File-first init (steps 18-24) ---
	//
	// Mirrors the /v1/dispatch pattern (handlers/dispatch.go:187-235): we
	// create the events file BEFORE the SQL row so a crash mid-insert
	// leaves the on-disk artifact visible to startup repair, never the
	// reverse. The events file is the durable source of truth for missions
	// that exist but never made it into the DB.
	shard, _ := ids.ShardPath(execID)
	outDir := filepath.Join(h.DataDir, "output", shard)
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		httputil.WriteIOError(w, http.StatusInternalServerError, "exec_dispatch.mkdir_output", err)
		return
	}
	// Best-effort fsync of the output dir chain so EVERY newly-created
	// directory entry along base/<sh1>/<sh2> is durable. Plain SyncDir
	// on "output" only makes <sh1> durable; if MkdirAll created both
	// <sh1> and <sh1>/<sh2> in one call, the <sh2> entry inside <sh1>
	// is still on a dirty dir page.
	metrics.ObserveSyncDir(
		fsutil.SyncDirChain(filepath.Join(h.DataDir, "output"), shard),
		h.Logger, "exec_dispatch_outdir")

	ew, err := eventfile.Create(outDir, execID)
	if err != nil {
		if os.IsExist(err) {
			// Orphan from a previous crash — clean up and retry once. Same
			// recovery the dispatch handler does; matches the operator
			// expectation that a fresh idempotency key always succeeds.
			_ = os.Remove(filepath.Join(outDir, execID+"-events"))
			_ = os.Remove(filepath.Join(outDir, execID+"-stdout"))
			_ = os.Remove(filepath.Join(outDir, execID+"-stderr"))
			_ = os.Remove(filepath.Join(outDir, execID+"-combined"))
			_ = os.RemoveAll(filepath.Join(h.DataDir, "work", execID))
			ew, err = eventfile.Create(outDir, execID)
			if err != nil {
				httputil.WriteIOError(w, http.StatusInternalServerError, "exec_dispatch.events_create_after_orphan", err)
				return
			}
		} else {
			httputil.WriteIOError(w, http.StatusInternalServerError, "exec_dispatch.events_create", err)
			return
		}
	}
	ew.SetLimits(eventfile.Limits{
		MaxEventsBuffer:  h.Cfg.Limits.MaxEventsBuffer,
		MaxEventLineSize: h.Cfg.Limits.MaxEventLineSize,
	})

	nowMs := time.Now().UnixMilli()
	if _, err := ew.Append(eventfile.KindQueued, map[string]any{
		"mission_id":   execID,
		"time_created": nowMs,
		"lane":         req.Lane,
	}, true); err != nil {
		_ = ew.Close()
		_ = os.Remove(ew.Path())
		httputil.WriteIOError(w, http.StatusInternalServerError, "exec_dispatch.append_queued", err)
		return
	}
	_ = ew.SyncParentDir() // best-effort fsync parent dir
	_ = ew.Close()

	// Serialize the full ExecRequest as the stored input. Finalize and the
	// audit log both need access to argv / in[] / out[] / stdin mode, and
	// the wire JSON shape is the cleanest snapshot of operator intent.
	storedInput, err := json.Marshal(&req)
	if err != nil {
		_ = os.Remove(filepath.Join(outDir, execID+"-events"))
		httputil.WriteError(w, http.StatusInternalServerError, "internal",
			"marshal input: "+err.Error(), nil)
		return
	}

	// Sentinel for staging re-check (mirror of
	// dispatch.go's errStagingRefMissedInTx).
	var stagingMissed string
	insertErr := storage.WithWriter(r.Context(), h.DB, func(c *sql.Conn) error {
		var existsCount int
		_ = c.QueryRowContext(r.Context(),
			`SELECT COUNT(*) FROM missions WHERE mission_id=?`, execID).Scan(&existsCount)
		if existsCount > 0 {
			return errors.New("mission_id collision after lock")
		}

		for sid := range stagingMeta {
			st, err := storage.GetStaging(r.Context(), c, sid)
			if err != nil || st.State != storage.StagingComplete {
				stagingMissed = sid
				return errStagingRefMissedInTx
			}
		}

		// Insert mission row. mission_name='exec' is a sentinel
		// — exec rows don't have a user-supplied mission name; finalize uses
		// the kind field to branch.
		m := &storage.Mission{
			ID:               execID,
			Kind:             storage.KindExec,
			Lane:             req.Lane,
			MissionName:      "exec",
			Status:           storage.StatusQueued,
			Input:            storedInput,
			InputFingerprint: fp,
			TimeCreatedMs:    nowMs,
		}
		if req.DisplayName != "" {
			m.DisplayName = sql.NullString{String: req.DisplayName, Valid: true}
		}
		if req.GroupID != "" {
			m.GroupID = sql.NullString{String: req.GroupID, Valid: true}
		}
		if timeoutMs != nil {
			m.TimeoutMs = sql.NullInt64{Int64: *timeoutMs, Valid: true}
		}
		if err := storage.InsertMission(r.Context(), c, m); err != nil {
			return err
		}

		// Step 26: staging refs. Script ref (ref_kind='script', role=''),
		// per-input refs (ref_kind='input', role=<key>), stdin ref
		// (ref_kind='input', role='__stdin__'). No output refs at dispatch
		// — output staging rows materialize at finalize.
		if req.Script != nil {
			if err := storage.InsertRef(r.Context(), c, storage.StagingRef{
				MissionID: execID, StagingID: req.Script.StagingID,
				RefKind: storage.RefScript, Role: "",
			}); err != nil {
				return err
			}
		}
		for _, e := range req.In {
			if err := storage.InsertRef(r.Context(), c, storage.StagingRef{
				MissionID: execID, StagingID: e.StagingID,
				RefKind: storage.RefInput, Role: e.Key,
			}); err != nil {
				return err
			}
		}
		if req.StdinStagingID != "" {
			if err := storage.InsertRef(r.Context(), c, storage.StagingRef{
				MissionID: execID, StagingID: req.StdinStagingID,
				RefKind: storage.RefInput, Role: "__stdin__",
			}); err != nil {
				return err
			}
		}

		// Step 27: recalc TTL for every referenced staging file so they stay
		// pinned for the lifetime of the queued/running exec.
		ttl := storage.TTLPolicy{
			MissionSuccess: h.Cfg.Cleanup.SuccessTTL,
			MissionFailed:  h.Cfg.Cleanup.FailedTTL,
			ExecSuccess:    h.Cfg.Exec.ExecSuccessTTL,
			ExecFailed:     h.Cfg.Exec.ExecFailedTTL,
			StagingTTL:     h.Cfg.Cleanup.StagingTTL,
			DownloadGrace:  h.Cfg.Cleanup.DownloadedGrace,
		}
		for sid := range stagingMeta {
			if _, err := storage.RecalcStagingTTL(r.Context(), c, sid, ttl, nowMs); err != nil {
				return err
			}
		}
		return nil
	})
	if insertErr != nil {
		_ = os.Remove(filepath.Join(outDir, execID+"-events"))
		if errors.Is(insertErr, errStagingRefMissedInTx) {
			httputil.WriteError(w, http.StatusBadRequest, "unknown_staging_ref",
				"staging file no longer available",
				map[string]any{"staging_id": stagingMissed})
			return
		}
		httputil.WriteDBError(w, http.StatusInternalServerError, "exec.dispatch.insert", insertErr)
		return
	}

	// Step 28: wake the lane runner so the queued exec gets picked up
	// promptly (runner also polls but Notify avoids the worst-case latency).
	h.LaneManager.Notify(req.Lane)

	// Step 29: audit log.
	h.auditDispatch(r, &req, stagingMeta, execID, fp)

	// Step 30: 202 with the canonical wire fields.
	httputil.WriteJSON(w, http.StatusAccepted, map[string]any{
		"exec_id": execID,
		"status":  "queued",
	})
}

// auditDispatch emits the audit log line for a successful
// dispatch. Fields: action="exec.dispatch", actor (exec-token vs
// admin-token via Identity.Scope), full argv, script sha256, in/out keys,
// stdin mode, remote_addr. Logger is allowed to be nil (no-op) so tests
// that don't care about audit can skip wiring it.
func (h *ExecDispatchHandler) auditDispatch(r *http.Request, req *ExecRequest, stagingMeta map[string]*storage.StagingFile, execID, fp string) {
	if h.Logger == nil {
		return
	}
	actor := "exec-token"
	if id, ok := middleware.FromCtx(r.Context()); ok && id.Scope == middleware.ScopeAdmin {
		actor = "admin-token"
	}
	inKeys := make([]string, len(req.In))
	inSizes := make([]int64, len(req.In))
	for i, e := range req.In {
		inKeys[i] = e.Key
		if sm := stagingMeta[e.StagingID]; sm != nil {
			inSizes[i] = sm.Size
		}
	}
	outKeys := make([]string, len(req.Out))
	for i, e := range req.Out {
		outKeys[i] = e.Key
	}
	scriptSha := ""
	var scriptSize int64
	hasScript := false
	if req.Script != nil {
		hasScript = true
		if sm := stagingMeta[req.Script.StagingID]; sm != nil {
			scriptSha = sm.Sha256
			scriptSize = sm.Size
		}
	}
	stdin := req.Stdin
	if stdin == "" {
		stdin = "none"
	}
	var stdinSize int64
	hasStdin := false
	if req.StdinStagingID != "" {
		hasStdin = true
		if sm := stagingMeta[req.StdinStagingID]; sm != nil {
			stdinSize = sm.Size
		}
	}
	// Build args slice so size fields can be conditionally omitted when no
	// staging ref exists for that slot. The audit format calls for
	// "list of in/out keys, sizes"; in_sizes is always emitted parallel to
	// in_keys, script_size / stdin_size only when present.
	args := []any{
		"audit", true,
		"action", "exec.dispatch",
		"exec_id", execID,
		"group_id", req.GroupID,
		"display_name", req.DisplayName,
		"lane", req.Lane,
		"command_argv", req.Command,
		"script_sha256", scriptSha,
		"in_keys", inKeys,
		"in_sizes", inSizes,
		"out_keys", outKeys,
		"stdin", stdin,
		"actor", actor,
		"remote_addr", r.RemoteAddr,
		"input_fingerprint", fp,
	}
	if hasScript {
		args = append(args, "script_size", scriptSize)
	}
	if hasStdin {
		// stdin_staging_id is required alongside stdin_size so
		// operators can trace audit lines back to specific uploads. Mode
		// and size alone don't pin the artifact.
		args = append(args, "stdin_size", stdinSize, "stdin_staging_id", req.StdinStagingID)
	}
	h.Logger.Info("exec.dispatch", args...)
}

// computeExecFingerprint builds the fingerprint.ExecInput from the validated
// request and resolved staging metadata and delegates to fingerprint.Exec.
// Pulled out so the handler tail stays readable and so the test pre-seed
// for idempotency replays can call it.
func computeExecFingerprint(req *ExecRequest, meta map[string]*storage.StagingFile, timeoutMs *int64) (string, error) {
	in := fingerprint.ExecInput{
		Lane:           req.Lane,
		Command:        req.Command,
		Stdin:          req.Stdin,
		StdinStagingID: req.StdinStagingID,
		TimeoutMs:      timeoutMs,
		GroupID:        req.GroupID,
		DisplayName:    req.DisplayName,
	}
	if req.Script != nil {
		sm := meta[req.Script.StagingID]
		sha := ""
		if sm != nil {
			sha = sm.Sha256
		}
		in.Script = &fingerprint.ExecScriptRef{
			StagingID: req.Script.StagingID,
			Sha256:    sha,
		}
	}
	in.In = make([]fingerprint.ExecFileRef, 0, len(req.In))
	for _, e := range req.In {
		sm := meta[e.StagingID]
		ref := fingerprint.ExecFileRef{Key: e.Key, StagingID: e.StagingID}
		if sm != nil {
			ref.Sha256 = sm.Sha256
			ref.Size = sm.Size
		}
		in.In = append(in.In, ref)
	}
	in.Out = make([]fingerprint.ExecOutKey, 0, len(req.Out))
	for _, e := range req.Out {
		in.Out = append(in.Out, fingerprint.ExecOutKey{Key: e.Key})
	}
	return fingerprint.Exec(in)
}

// unknownStagingError signals that a referenced staging id does not exist
// or is not in state='complete'. The handler maps it to the
// 400 unknown_staging_ref response.
type unknownStagingError struct {
	ID     string
	Reason string
}

func (e *unknownStagingError) Error() string {
	return fmt.Sprintf("staging %s: %s", e.ID, e.Reason)
}

// fetchStagingMeta loads metadata for every staging id referenced by req
// (script, in[], stdin). Returns a map keyed by staging_id. Each row must
// exist with state='complete' — otherwise returns *unknownStagingError so
// the caller can produce the 400 with the offending id.
func (h *ExecDispatchHandler) fetchStagingMeta(ctx context.Context, req *ExecRequest) (map[string]*storage.StagingFile, error) {
	ids := make([]string, 0, len(req.In)+2)
	if req.Script != nil {
		ids = append(ids, req.Script.StagingID)
	}
	for _, e := range req.In {
		ids = append(ids, e.StagingID)
	}
	if req.StdinStagingID != "" {
		ids = append(ids, req.StdinStagingID)
	}
	out := make(map[string]*storage.StagingFile, len(ids))
	for _, id := range ids {
		if _, seen := out[id]; seen {
			continue
		}
		st, err := storage.GetStaging(ctx, h.DB, id)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return nil, &unknownStagingError{ID: id, Reason: "not found"}
			}
			return nil, err
		}
		if st.State != storage.StagingComplete {
			return nil, &unknownStagingError{ID: id, Reason: fmt.Sprintf("state=%s", st.State)}
		}
		out[id] = st
	}
	return out, nil
}

// ExecDisabledStub returns 404 feature_disabled. Mounted in place of the
// real handler when cfg.Exec.Enabled=false. Registered OUTSIDE the Auth
// middleware (pre-auth gate) so the response is identical regardless of
// whether the caller presented a bearer token — the feature simply does
// not exist on this dugdale.
func ExecDisabledStub() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		httputil.WriteError(w, http.StatusNotFound, "feature_disabled",
			"exec is disabled in dugdale.yaml", nil)
	}
}

// firstDuplicate returns the first repeated value in keys, or "" if all
// values are unique. Empty slice → "". Used by duplicate-key validation
// for in[] and out[]. Linear scan over a map: O(n) time, O(n) space.
func firstDuplicate(keys []string) string {
	seen := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		if _, ok := seen[k]; ok {
			return k
		}
		seen[k] = struct{}{}
	}
	return ""
}

// execInKeys returns the .Key slice of in[].
func execInKeys(in []ExecFileRef) []string {
	out := make([]string, len(in))
	for i, e := range in {
		out[i] = e.Key
	}
	return out
}

// execOutKeys returns the .Key slice of out[].
func execOutKeys(out []ExecOutKey) []string {
	keys := make([]string, len(out))
	for i, e := range out {
		keys[i] = e.Key
	}
	return keys
}

// parseOptionalDuration is shared with dispatch.go (same package). Defined
// there to keep the duration→ms helper next to its first caller.
