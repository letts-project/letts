package handlers

import (
	"database/sql"
	"net/http"
	"time"

	"letts/internal/apply"
	"letts/internal/config"
	"letts/internal/lane"
	"letts/internal/mission"
)

// LifecycleHandler serves the mission control endpoints (restart, kill,
// delete, bulk-restart, bulk-delete).
type LifecycleHandler struct {
	DB          *sql.DB
	Cfg         *config.DugdaleConfig
	DataDir     string
	LaneManager *lane.Manager
	Runtime     LifecycleRuntime

	// GetApplied returns the current applied state so Restart can reject
	// a restart targeting a lane that no longer exists. Mirror
	// of dispatch.go's GetApplied wiring; nil means restart skips the
	// lane-presence check (test fixtures).
	GetApplied func() (*apply.AppliedState, bool)

	// ForceDeleteTimeout caps how long DELETE ?force=true blocks waiting for
	// the running mission to finalize (default 30s).
	ForceDeleteTimeout time.Duration
	// ForceDeletePoll is the interval between status re-reads while waiting
	// for force-delete (default 50ms).
	ForceDeletePoll time.Duration
}

// LifecycleRuntime is the subset of *runtime.Runtime used by the lifecycle
// handlers. Modeled as an interface so tests can stub kill semantics
// without spawning real missions.
type LifecycleRuntime interface {
	SignalKill(missionID string, reason mission.ExternalKillReason) bool
	IsRunning(missionID string) bool
}

// Register mounts all control routes. New methods are wired here as their
// tasks land (see missions_{restart,kill,delete,bulk}.go).
func (h *LifecycleHandler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/missions/{id}/restart", h.Restart)
	mux.HandleFunc("POST /v1/missions/{id}/kill", h.Kill)
	mux.HandleFunc("DELETE /v1/missions/{id}", h.Delete)
	mux.HandleFunc("POST /v1/missions/bulk-restart", h.BulkRestart)
	mux.HandleFunc("POST /v1/missions/bulk-delete", h.BulkDelete)
}
