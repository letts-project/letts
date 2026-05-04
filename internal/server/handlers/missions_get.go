package handlers

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"letts/internal/ids"
	"letts/internal/server/httputil"
	"letts/internal/storage"
)

const defaultListLimit = 100
const maxListLimit = 1000

// MissionsHandler serves GET /v1/missions and GET /v1/missions/{id}.
type MissionsHandler struct {
	DB *sql.DB
}

// Register mounts the get/list routes.
func (h *MissionsHandler) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/missions/{id}", h.GetByID)
	mux.HandleFunc("GET /v1/missions", h.List)
}

// GetByID returns the full mission resource (including input and output staging
// refs joined with their staging metadata).
func (h *MissionsHandler) GetByID(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !ids.ValidateUUIDv7(id) {
		httputil.WriteError(w, http.StatusBadRequest, "invalid_id", "mission id must be a valid UUIDv7", nil)
		return
	}
	m, err := storage.GetMission(r.Context(), h.DB, id)
	if errors.Is(err, storage.ErrNotFound) {
		httputil.WriteError(w, http.StatusNotFound, "not_found", "mission not found", nil)
		return
	}
	if err != nil {
		httputil.WriteDBError(w, http.StatusInternalServerError, "missions.GetByID/get_mission", err)
		return
	}
	if m.Status == storage.StatusDeleting {
		httputil.WriteError(w, http.StatusNotFound, "not_found", "mission not found", nil)
		return
	}
	if !RequireKindForScope(w, r, m) {
		return
	}
	resp, err := buildMissionResponse(r.Context(), h.DB, m)
	if err != nil {
		httputil.WriteDBError(w, http.StatusInternalServerError, "missions.GetByID/build_response", err)
		return
	}
	httputil.WriteJSON(w, http.StatusOK, resp)
}

// List returns missions matching the query filter, paginated by cursor. The
// response is `{"missions": [...], "next_cursor": "..."}`. next_cursor is
// omitted when there are no more pages.
func (h *MissionsHandler) List(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	filter := storage.ListFilter{
		Status:        q.Get("status"),
		Outcome:       q.Get("outcome"),
		Lane:          q.Get("lane"),
		Mission:       q.Get("mission"),
		MissionPrefix: q.Get("mission_prefix"),
		Kind:          q.Get("kind"),
		GroupID:       q.Get("group_id"), // exec group filter
	}
	switch order := q.Get("order"); order {
	case "", "created":
		filter.Order = "created"
	case "finished":
		filter.Order = "finished"
	default:
		httputil.WriteError(w, http.StatusBadRequest, "bad_request", "order must be 'created' or 'finished'", nil)
		return
	}
	if s := q.Get("since"); s != "" {
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			httputil.WriteError(w, http.StatusBadRequest, "bad_request", "since must be an integer", nil)
			return
		}
		filter.SinceMs = v
	}
	if s := q.Get("until"); s != "" {
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			httputil.WriteError(w, http.StatusBadRequest, "bad_request", "until must be an integer", nil)
			return
		}
		filter.UntilMs = v
	}
	limit := defaultListLimit
	if s := q.Get("limit"); s != "" {
		v, err := strconv.Atoi(s)
		if err != nil || v <= 0 {
			httputil.WriteError(w, http.StatusBadRequest, "bad_request", "limit must be a positive integer", nil)
			return
		}
		if v > maxListLimit {
			v = maxListLimit
		}
		limit = v
	}
	cursor, err := decodeCursor(q.Get("cursor"))
	if err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "bad_request", "invalid cursor: "+err.Error(), nil)
		return
	}

	// Ask for limit+1 so we know whether a next cursor is needed.
	rows, err := storage.ListMissions(r.Context(), h.DB, filter, cursor, limit+1)
	if err != nil {
		httputil.WriteDBError(w, http.StatusInternalServerError, "missions.List/query", err)
		return
	}
	var nextCursor string
	if len(rows) > limit {
		last := rows[limit-1]
		if filter.Order == "finished" {
			nextCursor = encodeCursor(&storage.Cursor{TimeFinishedMs: last.TimeFinishedMs.Int64, MissionID: last.ID})
		} else {
			nextCursor = encodeCursor(&storage.Cursor{TimeCreatedMs: last.TimeCreatedMs, MissionID: last.ID})
		}
		rows = rows[:limit]
	}
	missions := make([]map[string]any, 0, len(rows))
	for i := range rows {
		mr, berr := buildMissionResponse(r.Context(), h.DB, &rows[i])
		if berr != nil {
			httputil.WriteDBError(w, http.StatusInternalServerError, "missions.List/build_response", berr)
			return
		}
		missions = append(missions, mr)
	}
	resp := map[string]any{"missions": missions}
	if nextCursor != "" {
		resp["next_cursor"] = nextCursor
	}
	httputil.WriteJSON(w, http.StatusOK, resp)
}

// buildMissionResponse converts a storage.Mission and its refs into the public
// JSON shape used by both GET and listing.
func buildMissionResponse(ctx context.Context, db *sql.DB, m *storage.Mission) (map[string]any, error) {
	resp := map[string]any{
		"mission_id":        m.ID,
		"kind":              string(m.Kind),
		"lane":              m.Lane,
		"mission_name":      m.MissionName,
		"status":            string(m.Status),
		"input_fingerprint": m.InputFingerprint,
		"truncated_stdout":  m.TruncatedStdout,
		"truncated_stderr":  m.TruncatedStderr,
		"time_created":      m.TimeCreatedMs,
	}
	if len(m.Input) > 0 {
		resp["input"] = json.RawMessage(m.Input)
	}
	if m.DisplayName.Valid {
		resp["display_name"] = m.DisplayName.String
	}
	if m.GroupID.Valid {
		resp["group_id"] = m.GroupID.String
	}
	if m.Outcome.Valid {
		resp["outcome"] = m.Outcome.String
	}
	if m.FailReason.Valid {
		resp["fail_reason"] = m.FailReason.String
	}
	if m.FailMessage.Valid {
		resp["fail_message"] = m.FailMessage.String
	}
	if m.FailDetails.Valid {
		resp["fail_details"] = json.RawMessage(m.FailDetails.String)
	}
	if m.ExitCode.Valid {
		resp["exit_code"] = m.ExitCode.Int64
	}
	if m.Signal.Valid {
		resp["signal"] = m.Signal.String
	}
	if m.PID.Valid {
		resp["pid"] = m.PID.Int64
	}
	if len(m.ReturnValue) > 0 {
		resp["return"] = json.RawMessage(m.ReturnValue)
	}
	if m.TimeStartedMs.Valid {
		resp["time_started"] = m.TimeStartedMs.Int64
	}
	if m.TimeFinishedMs.Valid {
		resp["time_finished"] = m.TimeFinishedMs.Int64
		if m.TimeStartedMs.Valid {
			resp["duration_ms"] = m.TimeFinishedMs.Int64 - m.TimeStartedMs.Int64
		}
	}
	if m.TimeoutMs.Valid {
		resp["timeout_ms"] = m.TimeoutMs.Int64
	}
	if m.RestartedFrom.Valid {
		resp["restarted_from"] = m.RestartedFrom.String
	}

	refs, err := storage.RefsByMission(ctx, db, m.ID)
	if err != nil {
		return nil, err
	}
	inputs := []map[string]any{}
	outputs := map[string]map[string]any{}
	for _, ref := range refs {
		st, gerr := storage.GetStaging(ctx, db, ref.StagingID)
		if gerr != nil {
			continue // staging GC may have raced; skip
		}
		entry := map[string]any{
			"role":       ref.Role,
			"staging_id": ref.StagingID,
			"sha256":     st.Sha256,
			"size":       st.Size,
		}
		switch ref.RefKind {
		case storage.RefInput:
			inputs = append(inputs, entry)
		case storage.RefOutput:
			outputs[ref.Role] = map[string]any{
				"staging_id": ref.StagingID,
				"sha256":     st.Sha256,
				"size":       st.Size,
			}
		}
	}
	resp["inputs"] = inputs
	resp["outputs"] = outputs
	return resp, nil
}

func encodeCursor(c *storage.Cursor) string {
	if c == nil {
		return ""
	}
	b, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeCursor(s string) (*storage.Cursor, error) {
	if s == "" {
		return nil, nil
	}
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}
	var c storage.Cursor
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, err
	}
	return &c, nil
}
