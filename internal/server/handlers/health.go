// Package handlers contains HTTP handler types for the dugdale HTTP server.
package handlers

import (
	"database/sql"
	"errors"
	"net/http"

	"letts/internal/criticalerr"
	"letts/internal/server/httputil"
	"letts/internal/storage"
	"letts/internal/version"
)

// Health holds the minimal deps for /healthz, /readyz, /version.
//
// IsDraining, when non-nil, lets /readyz flip to 503 awaiting_drain
// during a graceful shutdown — load balancers checking readiness then
// route traffic away from this instance before the dispatch path also
// starts 503'ing.
type Health struct {
	DB         *sql.DB
	IsDraining func() bool
}

// Register attaches health routes to mux.
func (h *Health) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/healthz", h.Healthz)
	mux.HandleFunc("GET /v1/readyz", h.Readyz)
	mux.HandleFunc("GET /v1/version", h.Version)
}

// Healthz always returns 200 {"status":"ok"}. Does not touch the DB.
func (h *Health) Healthz(w http.ResponseWriter, r *http.Request) {
	httputil.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// Readyz returns 200 if an applied config row exists, 503 otherwise.
// During graceful shutdown the response flips to 503 awaiting_drain so
// LBs stop routing new traffic to this instance.
//
// A sticky terminal-event-conflict trip forces 503
// awaiting_manual_repair until operator resolves. The
// flag is process-scoped — restarting the daemon does NOT clear it
// because the offending intent row is still in the DB.
func (h *Health) Readyz(w http.ResponseWriter, r *http.Request) {
	if d, tripped := criticalerr.Get(); tripped {
		httputil.WriteError(w, http.StatusServiceUnavailable, "awaiting_manual_repair",
			"critical consistency error; operator must resolve",
			map[string]any{"kind": d.Kind, "mission_id": d.MissionID, "op": d.Op})
		return
	}
	if h.IsDraining != nil && h.IsDraining() {
		w.Header().Set("Retry-After", "30")
		httputil.WriteError(w, http.StatusServiceUnavailable, "awaiting_drain",
			"dugdale is shutting down", nil)
		return
	}
	cfg, err := storage.GetAppliedConfig(r.Context(), h.DB)
	if errors.Is(err, storage.ErrNotFound) {
		httputil.WriteError(w, http.StatusServiceUnavailable, "awaiting_apply",
			"no applied config; run letts apply", map[string]any{"applied_at": nil})
		return
	}
	if err != nil {
		httputil.WriteDBError(w, http.StatusServiceUnavailable, "health.Readyz/get_applied_config", err)
		return
	}
	httputil.WriteJSON(w, http.StatusOK, map[string]any{
		"status":     "ok",
		"applied_at": cfg.AppliedAt,
	})
}

// Version returns build-time version metadata.
func (h *Health) Version(w http.ResponseWriter, r *http.Request) {
	httputil.WriteJSON(w, http.StatusOK, map[string]string{
		"version":  version.Version,
		"commit":   version.Commit,
		"built_at": version.BuiltAt,
	})
}
