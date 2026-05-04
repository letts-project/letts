package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync"

	"letts/internal/apply"
	"letts/internal/lane"
	"letts/internal/server/httputil"
	"letts/internal/storage"
	"letts/pkg/lettsconfig"
)

// Admin holds dependencies for admin endpoints.
//
// Killer, when non-nil, is forwarded to apply.Apply so ForcePrune can
// signal running missions in lanes being removed. Production wiring
// uses *runtime.Runtime; tests can stub.
//
// applyMu serializes concurrent POST /v1/admin/apply requests so the
// read-current → diff → write-desired → reconcile-manager sequence is
// atomic — without it two parallel applies could interleave and
// produce an inconsistent lane manager state.
type Admin struct {
	DB      *sql.DB
	Manager *lane.Manager
	Killer  apply.Killer
	// DataDir is the daemon's data_dir, forwarded to apply.Options.DataDir
	// so ForcePrune can append durable terminal `done(killed,lane_removed)`
	// events through the finalize-intent journal.
	DataDir string

	applyMu sync.Mutex
}

// Register mounts admin routes.
func (h *Admin) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/admin/apply", h.Apply)
	mux.HandleFunc("GET /v1/admin/state", h.State)
	mux.HandleFunc("POST /v1/admin/lanes/{name}/pause", h.PauseLane)
	mux.HandleFunc("POST /v1/admin/lanes/{name}/continue", h.ContinueLane)
}

// Apply handles POST /v1/admin/apply.
func (h *Admin) Apply(w http.ResponseWriter, r *http.Request) {
	// Bound body read time on this JSON POST (see dispatch handler).
	httputil.SetRequestReadDeadline(w, httputil.JSONBodyReadTimeout)
	// Serialize concurrent applies so the read-diff-write-reconcile path
	// is atomic. The mutex is held across the whole apply.Apply call —
	// fine because applies are rare and apply.Apply itself uses the
	// writer-locked DB transaction for the persistence step.
	h.applyMu.Lock()
	defer h.applyMu.Unlock()
	var desired apply.AppliedState
	if err := json.NewDecoder(r.Body).Decode(&desired); err != nil {
		// MaxBytesReader-wrapped body (installed by middleware.BodyLimit)
		// surfaces as a decode error; requires 413, not 400.
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			httputil.WriteError(w, http.StatusRequestEntityTooLarge,
				"payload_too_large", "request body exceeds limit",
				map[string]any{"limit_bytes": maxErr.Limit})
			return
		}
		httputil.WriteError(w, http.StatusBadRequest, "invalid_json", err.Error(), nil)
		return
	}
	// Validate every lane name in the desired state. The
	// HTTP-side decode bypasses lettsconfig.Validate which the CLI
	// runs, so direct admin posts could persist arbitrary strings as
	// lane names — flowing into metric labels, SQL WHERE clauses, and
	// JSON responses. Defense in depth even though admin scope is high
	// trust.
	for name := range desired.Lanes {
		if err := lettsconfig.ValidateLaneName(name); err != nil {
			httputil.WriteError(w, http.StatusBadRequest, "invalid_lane_name",
				"lane name validation failed",
				map[string]any{"lane": name, "reason": err.Error()})
			return
		}
	}

	q := r.URL.Query()
	prune := strings.EqualFold(q.Get("prune"), "true")
	forcePrune := strings.EqualFold(q.Get("force_prune"), "true")
	// force_prune is meaningless without prune (no lanes to reap). Treat it
	// as implying prune so older clients sending only force_prune=true
	// retain previous semantics.
	if forcePrune {
		prune = true
	}
	opts := apply.Options{
		Force:      strings.EqualFold(q.Get("force"), "true"),
		Prune:      prune,
		ForcePrune: forcePrune,
		DataDir:    h.DataDir,
		Killer:     h.Killer,
	}

	result, err := apply.Apply(r.Context(), h.DB, h.Manager, desired, opts)
	if err != nil {
		var ce *apply.ConflictError
		if errors.As(err, &ce) {
			httputil.WriteError(w, http.StatusConflict, "conflict", ce.Error(), ce.Detail)
			return
		}
		slog.Default().Error("admin.apply", "err", err)
		httputil.WriteError(w, http.StatusInternalServerError, "internal", "apply failed", nil)
		return
	}
	auditLog(nil, r, "admin.apply",
		"force", opts.Force, "prune", opts.Prune, "force_prune", opts.ForcePrune,
		"started", result.Started, "stopped", result.Stopped, "resized", result.Resized)
	httputil.WriteJSON(w, http.StatusOK, result)
}

// State handles GET /v1/admin/state.
func (h *Admin) State(w http.ResponseWriter, r *http.Request) {
	cfg, err := storage.GetAppliedConfig(r.Context(), h.DB)
	if errors.Is(err, storage.ErrNotFound) {
		httputil.WriteJSON(w, http.StatusOK, map[string]any{
			"applied_at": nil,
			"lanes":      []any{},
		})
		return
	}
	if err != nil {
		httputil.WriteDBError(w, http.StatusInternalServerError, "admin.GetApplied/get_applied_config", err)
		return
	}

	var state apply.AppliedState
	if err := json.Unmarshal(cfg.Data, &state); err != nil {
		slog.Default().Error("admin.State/parse_applied", "err", err)
		httputil.WriteError(w, http.StatusInternalServerError, "internal", "applied config parse failed", nil)
		return
	}

	httputil.WriteJSON(w, http.StatusOK, map[string]any{
		"applied_at": cfg.AppliedAt,
		"source":     cfg.Source,
		"state":      state,
	})
}

// PauseLane handles POST /v1/admin/lanes/{name}/pause. Updates both the
// runtime Manager state (so pickup stops immediately) AND the persisted
// applied config so a subsequent `letts apply` (or a daemon restart that
// replays config) doesn't silently un-pause.
func (h *Admin) PauseLane(w http.ResponseWriter, r *http.Request) {
	// Serialize with concurrent Apply so a snapshot →
	// SetAppliedConfig sequence can't overwrite this Pause's paused
	// flag. This closes the race-conditioned variant of the
	// daemon-restart case.
	h.applyMu.Lock()
	defer h.applyMu.Unlock()
	name := r.PathValue("name")
	if err := h.Manager.PauseLane(name); err != nil {
		httputil.WriteError(w, http.StatusNotFound, "lane_not_found", err.Error(), nil)
		return
	}
	if err := setLanePausedInConfig(r.Context(), h.DB, name, true); err != nil {
		// Persist failed but runtime is now paused. Roll the
		// runtime back so the response 500 actually reflects state. A
		// subsequent retry from the operator hits a coherent starting
		// point instead of a runtime-vs-persisted split.
		if rbErr := h.Manager.ContinueLane(name); rbErr != nil {
			slog.Default().Error("PauseLane rollback failed; runtime+persisted out of sync",
				"lane", name, "persist_err", err, "rollback_err", rbErr, "audit", true)
		}
		httputil.WriteError(w, http.StatusInternalServerError, "internal",
			"persist paused flag", nil)
		return
	}
	auditLog(nil, r, "lane.pause", "lane", name)
	httputil.WriteJSON(w, http.StatusOK, map[string]string{"status": "paused"})
}

// ContinueLane handles POST /v1/admin/lanes/{name}/continue. Symmetric
// to PauseLane: also clears the persisted paused flag.
func (h *Admin) ContinueLane(w http.ResponseWriter, r *http.Request) {
	// See PauseLane comment.
	h.applyMu.Lock()
	defer h.applyMu.Unlock()
	name := r.PathValue("name")
	if err := h.Manager.ContinueLane(name); err != nil {
		httputil.WriteError(w, http.StatusNotFound, "lane_not_found", err.Error(), nil)
		return
	}
	if err := setLanePausedInConfig(r.Context(), h.DB, name, false); err != nil {
		// Symmetric rollback.
		if rbErr := h.Manager.PauseLane(name); rbErr != nil {
			slog.Default().Error("ContinueLane rollback failed; runtime+persisted out of sync",
				"lane", name, "persist_err", err, "rollback_err", rbErr, "audit", true)
		}
		httputil.WriteError(w, http.StatusInternalServerError, "internal",
			"persist resume flag", nil)
		return
	}
	auditLog(nil, r, "lane.continue", "lane", name)
	httputil.WriteJSON(w, http.StatusOK, map[string]string{"status": "running"})
}

// setLanePausedInConfig reads the applied config, sets/clears the lane's
// paused flag, stamps PausedBy="ctl" (or clears it on resume) and
// re-persists. No-op if the lane is not in applied state (the lane may
// exist via a recent in-memory apply that hasn't gone through
// storage.SetAppliedConfig yet — extremely unlikely given admin.Apply
// persists synchronously, but defensive). Run inside a writer-locked txn
// so concurrent apply doesn't race the read+write.
//
// PausedBy="ctl" makes a subsequent `letts apply` preserve the pause.
// Without it Apply would treat the row as yaml-origin and
// quietly unpause on the next re-apply.
func setLanePausedInConfig(ctx context.Context, db *sql.DB, name string, paused bool) error {
	return storage.WithWriter(ctx, db, func(c *sql.Conn) error {
		cfg, err := storage.GetAppliedConfig(ctx, c)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return nil
			}
			return err
		}
		var state apply.AppliedState
		if err := json.Unmarshal(cfg.Data, &state); err != nil {
			return err
		}
		lane, ok := state.Lanes[name]
		if !ok {
			return nil // not in applied; nothing to persist
		}
		nextBy := ""
		if paused {
			nextBy = apply.PausedByCtl
		}
		if lane.Paused == paused && lane.PausedBy == nextBy {
			return nil // already matches
		}
		lane.Paused = paused
		lane.PausedBy = nextBy
		state.Lanes[name] = lane
		data, err := json.Marshal(state)
		if err != nil {
			return err
		}
		return storage.SetAppliedConfig(ctx, c, storage.AppliedConfig{
			Data:      data,
			AppliedAt: cfg.AppliedAt, // keep original apply timestamp
			Source:    cfg.Source,
		})
	})
}
