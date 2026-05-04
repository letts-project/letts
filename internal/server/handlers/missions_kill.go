package handlers

import (
	"encoding/json"
	"errors"
	"net/http"

	"letts/internal/ids"
	"letts/internal/mission"
	"letts/internal/server/httputil"
	"letts/internal/storage"
)

// Kill implements POST /v1/missions/{id}/kill.
//
//	queued   → finalize through the outputs=[] fast path under writer lock so
//	           pickup cannot race; outcome=killed, fail_reason=killed_by_api.
//	running  → push KillByAPI to Runtime.SignalKill; mission.Run handles the
//	           SIGTERM → grace → SIGKILL cycle and finalizes.
//	done     → 409 mission_done.
//	deleting → 409 mission_deleting.
func (h *LifecycleHandler) Kill(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !ids.ValidateUUIDv7(id) {
		httputil.WriteError(w, http.StatusBadRequest, "bad_request", "invalid mission_id", nil)
		return
	}
	// Body is optional; signal field is currently advisory (mission.Run picks
	// the actual signal sequence). Accept and ignore to allow client forward-
	// compat.
	var body struct {
		Signal string `json:"signal"`
	}
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&body)
	}

	m, err := storage.GetMission(r.Context(), h.DB, id)
	if errors.Is(err, storage.ErrNotFound) {
		httputil.WriteError(w, http.StatusNotFound, "not_found", "mission not found", nil)
		return
	}
	if err != nil {
		httputil.WriteDBError(w, http.StatusInternalServerError, "missions.Kill/get_mission", err)
		return
	}
	// Defensive kind-vs-scope gate. Currently all kill
	// routes are admin-only, but the gate matches the pattern in
	// deleteOne/restartOne so future per-scope wiring of bulk-kill
	// doesn't leak across kinds.
	if gate := gateKindForMission(r.Context(), m); gate.Status != 0 {
		httputil.WriteError(w, gate.Status, gate.ErrorCode, gate.ErrorMsg, gate.Details)
		return
	}

	switch m.Status {
	case storage.StatusDone:
		httputil.WriteError(w, http.StatusConflict, "mission_done", "already done", nil)
		return
	case storage.StatusDeleting:
		httputil.WriteError(w, http.StatusConflict, "mission_deleting", "", nil)
		return
	case storage.StatusQueued:
		if h.Cfg == nil {
			httputil.WriteError(w, http.StatusInternalServerError, "internal", "config not wired", nil)
			return
		}
		if err := mission.KillQueued(r.Context(), h.Cfg.DataDir, h.DB, m, "killed_by_api"); err != nil {
			if errors.Is(err, mission.ErrMissionNotQueued) {
				httputil.WriteError(w, http.StatusConflict, "mission_state_changed",
					"mission left queued state during kill", nil)
				return
			}
			// KillQueued errors can wrap an *os.PathError
			// (open events) whose message embeds the absolute data_dir path —
			// route through the generic logger instead of echoing it.
			httputil.WriteDBError(w, http.StatusInternalServerError, "missions.kill_queued", err)
			return
		}
		auditLog(nil, r, "mission.kill", "mission_id", id, "kind", string(m.Kind), "from_status", "queued")
		httputil.WriteJSON(w, http.StatusOK, map[string]string{"status": "killed"})
		return
	case storage.StatusRunning:
		if h.Runtime == nil {
			httputil.WriteError(w, http.StatusInternalServerError, "internal", "runtime not wired", nil)
			return
		}
		if !h.Runtime.SignalKill(id, mission.KillByAPI) {
			// SignalKill false typically means the mission
			// finished between GetMission and SignalKill — that's a
			// benign 409 mission_done race, not a 500. Re-read status
			// to distinguish from the "runtime not registered" bug.
			if cur, gerr := storage.GetMission(r.Context(), h.DB, id); gerr == nil {
				if cur.Status == storage.StatusDone {
					httputil.WriteError(w, http.StatusConflict, "mission_done",
						"already done", nil)
					return
				}
				if cur.Status == storage.StatusDeleting {
					httputil.WriteError(w, http.StatusConflict, "mission_deleting", "", nil)
					return
				}
			}
			httputil.WriteError(w, http.StatusInternalServerError, "internal",
				"kill not delivered (mission not registered or another kill is pending)", nil)
			return
		}
		// Kill on a running mission must emit audit:true the
		// same way the queued path does — otherwise a grep for
		// audit=true action=mission.kill would silently miss every
		// running-mission kill.
		auditLog(nil, r, "mission.kill", "mission_id", id, "kind", string(m.Kind), "from_status", "running")
		httputil.WriteJSON(w, http.StatusOK, map[string]string{"status": "kill_sent"})
		return
	default:
		httputil.WriteError(w, http.StatusInternalServerError, "internal",
			"unknown mission status: "+string(m.Status), nil)
		return
	}
}
