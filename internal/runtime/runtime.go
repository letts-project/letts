// Package runtime wires the lane manager to the mission spawner and tracks a
// kill channel per running mission so HTTP handlers (kill-by-API,
// shutdown, lane-removed) can signal a SIGTERM/SIGKILL cycle.
package runtime

import (
	"context"
	"database/sql"
	"log/slog"
	"sync"

	"letts/internal/config"
	"letts/internal/lane"
	"letts/internal/mission"
	"letts/internal/storage"
)

// Runtime ties components together; main.go constructs a *Runtime, applies
// lane specs, and registers HTTP handlers that call SignalKill / Notify.
type Runtime struct {
	Cfg     *config.DugdaleConfig
	DB      *sql.DB
	Manager *lane.Manager
	Logger  *slog.Logger

	mu      sync.Mutex
	killChs map[string]chan mission.ExternalKillReason
}

// NewRuntime constructs a Runtime with an initialized lane manager whose
// Spawner is wired to mission.Run via this Runtime's spawn closure.
//
// ctx is the long-lived runtime context; cancellation propagates to lane
// runners and to in-flight missions (each treats it as dugdale_shutdown).
func NewRuntime(ctx context.Context, cfg *config.DugdaleConfig, db *sql.DB, logger *slog.Logger) *Runtime {
	if logger == nil {
		logger = slog.Default()
	}
	r := &Runtime{
		Cfg:     cfg,
		DB:      db,
		Logger:  logger,
		killChs: make(map[string]chan mission.ExternalKillReason),
	}
	r.Manager = &lane.Manager{
		DB:       db,
		Spawner:  r.spawn,
		PreSpawn: r.preSpawnRegister,
		Logger:   logger,
		Ctx:      ctx,
	}
	return r
}

// preSpawnRegister is the lane.PreSpawnHook. Called
// synchronously inside the lane loop after MarkRunning commits but
// before the Spawner goroutine starts. Idempotent on the same id —
// the spawn closure uses-or-creates so a missing pre-registration
// still works (test fixtures that don't wire PreSpawn).
func (r *Runtime) preSpawnRegister(missionID string) {
	r.mu.Lock()
	if _, ok := r.killChs[missionID]; !ok {
		r.killChs[missionID] = make(chan mission.ExternalKillReason, 1)
	}
	r.mu.Unlock()
}

// spawn is the Spawner closure handed to lane.Manager. It pulls (or
// lazily creates) the per-mission kill channel, calls mission.Run, and
// unregisters before returning.
func (r *Runtime) spawn(ctx context.Context, m *storage.Mission, release func()) error {
	r.mu.Lock()
	killCh, ok := r.killChs[m.ID]
	if !ok {
		killCh = make(chan mission.ExternalKillReason, 1)
		r.killChs[m.ID] = killCh
	}
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		delete(r.killChs, m.ID)
		r.mu.Unlock()
	}()
	return mission.Run(ctx, r.Cfg, r.DB, m, killCh, release)
}

// SignalKill posts a kill reason to a running mission's kill channel.
// Returns true if the signal was queued (the mission is currently running and
// the buffer was empty), false otherwise (mission not running, or another
// kill signal is already pending).
func (r *Runtime) SignalKill(missionID string, reason mission.ExternalKillReason) bool {
	r.mu.Lock()
	ch := r.killChs[missionID]
	r.mu.Unlock()
	if ch == nil {
		return false
	}
	select {
	case ch <- reason:
		return true
	default:
		return false
	}
}

// IsRunning reports whether a kill channel is currently registered for the
// mission. Callers can use this to decide between online kill (SignalKill)
// and a queued/done state lookup in storage.
func (r *Runtime) IsRunning(missionID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.killChs[missionID]
	return ok
}
