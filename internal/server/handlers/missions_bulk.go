package handlers

import (
	"encoding/json"
	"errors"
	"net/http"

	"letts/internal/server/httputil"
)

// bulkRequest is the body shape for both bulk-restart and bulk-delete.
type bulkRequest struct {
	IDs   []string `json:"ids"`
	Force bool     `json:"force,omitempty"`
}

// bulkResultItem mirrors the per-id result shape.
type bulkResultItem struct {
	ID        string         `json:"id"`
	OK        bool           `json:"ok"`
	Error     string         `json:"error,omitempty"`
	MissionID string         `json:"mission_id,omitempty"` // restart success → new id
	Status    string         `json:"status,omitempty"`     // delete success → "deletion_pending"
	Details   map[string]any `json:"details,omitempty"`
}

const maxBulkIDs = 1000

// auditIDCap bounds how many ids land in the audit log line per bulk
// op. The full count is already in the "count" field; ids past this
// cap are summarized as "truncated".
const auditIDCap = 100

// truncateIDsForAudit returns at most auditIDCap ids; if the input is
// longer, the second return signals truncation so the audit emitter
// can add a marker key.
func truncateIDsForAudit(ids []string) (kept []string, truncated bool) {
	if len(ids) <= auditIDCap {
		return ids, false
	}
	return ids[:auditIDCap], true
}

// BulkRestart implements POST /v1/missions/bulk-restart.
func (h *LifecycleHandler) BulkRestart(w http.ResponseWriter, r *http.Request) {
	body, ok := decodeBulk(w, r)
	if !ok {
		return
	}
	results := make([]bulkResultItem, 0, len(body.IDs))
	for _, id := range body.IDs {
		res := h.restartOne(r.Context(), id)
		results = append(results, restartToBulk(id, res))
	}
	ids, truncated := truncateIDsForAudit(body.IDs)
	args := []any{"count", len(body.IDs), "ids", ids}
	if truncated {
		args = append(args, "ids_truncated_at", auditIDCap)
	}
	auditLog(nil, r, "mission.bulk_restart", args...)
	httputil.WriteJSON(w, http.StatusOK, map[string]any{"results": results})
}

// BulkDelete implements POST /v1/missions/bulk-delete. Without
// force=true, running missions return per-id "mission_running" rather than
// blocking the whole batch.
func (h *LifecycleHandler) BulkDelete(w http.ResponseWriter, r *http.Request) {
	body, ok := decodeBulk(w, r)
	if !ok {
		return
	}
	results := make([]bulkResultItem, 0, len(body.IDs))
	for _, id := range body.IDs {
		res := h.deleteOne(r.Context(), id, body.Force)
		results = append(results, deleteToBulk(id, res))
	}
	ids, truncated := truncateIDsForAudit(body.IDs)
	args := []any{"count", len(body.IDs), "force", body.Force, "ids", ids}
	if truncated {
		args = append(args, "ids_truncated_at", auditIDCap)
	}
	auditLog(nil, r, "mission.bulk_delete", args...)
	httputil.WriteJSON(w, http.StatusOK, map[string]any{"results": results})
}

func decodeBulk(w http.ResponseWriter, r *http.Request) (bulkRequest, bool) {
	var body bulkRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		// A MaxBytesReader-wrapped body surfaces *http.MaxBytesError
		// here; requires 413, consistent with dispatch/exec/apply.
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			httputil.WriteError(w, http.StatusRequestEntityTooLarge, "payload_too_large",
				"request body exceeds limit", map[string]any{"limit_bytes": maxErr.Limit})
			return body, false
		}
		httputil.WriteError(w, http.StatusBadRequest, "bad_request", "invalid JSON: "+err.Error(), nil)
		return body, false
	}
	if len(body.IDs) == 0 {
		httputil.WriteError(w, http.StatusBadRequest, "bad_request", "ids must be a non-empty array", nil)
		return body, false
	}
	if len(body.IDs) > maxBulkIDs {
		httputil.WriteError(w, http.StatusBadRequest, "bad_request",
			"too many ids (max 1000)", map[string]any{"count": len(body.IDs)})
		return body, false
	}
	return body, true
}

func restartToBulk(id string, res opOutcome) bulkResultItem {
	if res.Status >= 400 {
		return bulkResultItem{ID: id, OK: false, Error: res.ErrorCode, Details: res.Details}
	}
	return bulkResultItem{ID: id, OK: true, MissionID: res.NewID}
}

func deleteToBulk(id string, res opOutcome) bulkResultItem {
	if res.Status >= 400 {
		return bulkResultItem{ID: id, OK: false, Error: res.ErrorCode, Details: res.Details}
	}
	return bulkResultItem{ID: id, OK: true, Status: res.StatusName}
}
