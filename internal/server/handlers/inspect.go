package handlers

import (
	"database/sql"
	"errors"
	"net/http"
	"time"

	"letts/internal/lane"
	"letts/internal/server/httputil"
	"letts/internal/server/middleware"
	"letts/internal/storage"
	"letts/internal/version"
)

// kindFilterForCtx returns the kind value to filter mission counts by for
// the caller's auth scope, for kind isolation. An empty string
// means "no filter" (admin sees all kinds).
func kindFilterForCtx(r *http.Request) string {
	id, ok := middleware.FromCtx(r.Context())
	if !ok {
		return ""
	}
	switch id.Scope {
	case middleware.ScopeDispatch:
		return string(storage.KindMission)
	case middleware.ScopeExec:
		return string(storage.KindExec)
	}
	return ""
}

// Inspect holds dependencies for read-only inspection endpoints.
type Inspect struct {
	DB        *sql.DB
	Manager   *lane.Manager
	StartedAt time.Time
}

// Register mounts inspection routes.
func (h *Inspect) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/dugdale", h.Dugdale)
	mux.HandleFunc("GET /v1/lanes", h.Lanes)
}

// queueSummary holds aggregated queue counts.
type queueSummary struct {
	Queued  int `json:"queued"`
	Running int `json:"running"`
}

// laneInfo holds per-lane inspection data.
type laneInfo struct {
	Name        string `json:"name"`
	Concurrency int    `json:"concurrency"`
	Paused      bool   `json:"paused"`
	Queued      int    `json:"queued"`
	Running     int    `json:"running"`
}

// Dugdale handles GET /v1/dugdale — returns version, uptime, applied_at, queue summary.
func (h *Inspect) Dugdale(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// applied_at
	var appliedAt any
	cfg, err := storage.GetAppliedConfig(ctx, h.DB)
	if err == nil {
		appliedAt = cfg.AppliedAt
	} else if !errors.Is(err, storage.ErrNotFound) {
		httputil.WriteDBError(w, http.StatusInternalServerError, "inspect.Status/get_applied_config", err)
		return
	}

	// queue summary — filter by kind for non-admin scopes.
	summary := queueSummary{}
	kindFilter := kindFilterForCtx(r)
	var rows *sql.Rows
	if kindFilter != "" {
		rows, err = h.DB.QueryContext(ctx,
			`SELECT status, COUNT(*) FROM missions WHERE status IN ('queued','running') AND kind=? GROUP BY status`,
			kindFilter)
	} else {
		rows, err = h.DB.QueryContext(ctx,
			`SELECT status, COUNT(*) FROM missions WHERE status IN ('queued','running') GROUP BY status`)
	}
	if err != nil {
		httputil.WriteDBError(w, http.StatusInternalServerError, "inspect.Status/queue_summary", err)
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			continue
		}
		switch status {
		case "queued":
			summary.Queued = count
		case "running":
			summary.Running = count
		}
	}

	uptime := time.Since(h.StartedAt).Seconds()

	httputil.WriteJSON(w, http.StatusOK, map[string]any{
		"version":        version.Version,
		"uptime_seconds": uptime,
		"applied_at":     appliedAt,
		"queue_summary":  summary,
	})
}

// Lanes handles GET /v1/lanes — returns per-lane status with queued/running counts.
func (h *Inspect) Lanes(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	current := h.Manager.CurrentLanes()

	// Build lookup map.
	specs := make(map[string]lane.LaneSpec, len(current))
	for _, s := range current {
		specs[s.Name] = s
	}

	// Aggregate queued/running per lane.
	counts := make(map[string]*laneInfo, len(current))
	for _, s := range current {
		counts[s.Name] = &laneInfo{
			Name:        s.Name,
			Concurrency: s.Concurrency,
			Paused:      s.Paused,
		}
	}

	// Filter by kind per caller scope.
	kindFilter := kindFilterForCtx(r)
	var rows *sql.Rows
	var err error
	if kindFilter != "" {
		rows, err = h.DB.QueryContext(ctx,
			`SELECT lane, status, COUNT(*) FROM missions
			 WHERE status IN ('queued','running') AND kind=? GROUP BY lane, status`,
			kindFilter)
	} else {
		rows, err = h.DB.QueryContext(ctx,
			`SELECT lane, status, COUNT(*) FROM missions
			 WHERE status IN ('queued','running') GROUP BY lane, status`)
	}
	if err != nil {
		httputil.WriteDBError(w, http.StatusInternalServerError, "inspect.Lanes/per_lane_counts", err)
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var laneName, status string
		var count int
		if err := rows.Scan(&laneName, &status, &count); err != nil {
			continue
		}
		info, ok := counts[laneName]
		if !ok {
			continue
		}
		switch status {
		case "queued":
			info.Queued = count
		case "running":
			info.Running = count
		}
	}

	out := make([]laneInfo, 0, len(counts))
	for _, info := range counts {
		out = append(out, *info)
	}

	httputil.WriteJSON(w, http.StatusOK, out)
}
