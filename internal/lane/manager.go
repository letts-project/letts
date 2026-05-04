package lane

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
)

// LaneSpec describes desired state for one lane.
type LaneSpec struct {
	Name        string
	Concurrency int
	Paused      bool
}

// Manager tracks active runners and applies declarative diffs.
type Manager struct {
	DB       *sql.DB
	Spawner  Spawner
	PreSpawn PreSpawnHook // optional; threaded into every new Runner
	Logger   *slog.Logger
	Ctx      context.Context

	mu      sync.Mutex
	runners map[string]*Runner
}

// Apply reconciles current runners with desired specs. Returns names of
// lanes started, stopped, and resized for caller logging.
func (m *Manager) Apply(specs []LaneSpec) (started, stopped, resized []string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.runners == nil {
		m.runners = make(map[string]*Runner)
	}

	wanted := make(map[string]LaneSpec, len(specs))
	for _, s := range specs {
		wanted[s.Name] = s
	}

	// Stop removed lanes.
	for name, r := range m.runners {
		if _, ok := wanted[name]; !ok {
			r.Stop()
			delete(m.runners, name)
			stopped = append(stopped, name)
		}
	}

	// Start new lanes / reconcile existing.
	for name, s := range wanted {
		if r, ok := m.runners[name]; ok {
			// Existing lane.
			if r.Concurrency != s.Concurrency {
				r.SetConcurrency(s.Concurrency)
				resized = append(resized, name)
			}
			if s.Paused {
				r.Pause()
			} else {
				r.Resume()
			}
			continue
		}
		// New lane.
		r := &Runner{
			Lane:        name,
			DB:          m.DB,
			Spawner:     m.Spawner,
			PreSpawn:    m.PreSpawn,
			Concurrency: s.Concurrency,
			Logger:      m.Logger,
		}
		r.Start(m.Ctx)
		if s.Paused {
			r.Pause()
		}
		m.runners[name] = r
		started = append(started, name)
	}
	return
}

// StopLanes stops and removes the named runners (if managed). A
// force-prune that errors after MarkLaneRemoving but before the normal
// reconcile would otherwise leave those runners stuck in their removing branch
// (blocked on ctx.Done, never re-checking the flag) — IsLaneRemoving stays
// true and dispatch 503s the lane forever until a daemon restart. Stopping the
// removed runners un-strands them; unmanaged names are ignored.
func (m *Manager) StopLanes(names []string) {
	m.mu.Lock()
	var stop []*Runner
	for _, name := range names {
		if r, ok := m.runners[name]; ok {
			stop = append(stop, r)
			delete(m.runners, name)
		}
	}
	m.mu.Unlock()
	for _, r := range stop {
		r.Stop()
	}
}

// Notify wakes a specific lane's runner (called by dispatch handler).
func (m *Manager) Notify(lane string) {
	m.mu.Lock()
	r := m.runners[lane]
	m.mu.Unlock()
	if r != nil {
		r.Notify()
	}
}

// StopAll stops all runners (for shutdown).
func (m *Manager) StopAll() {
	m.mu.Lock()
	rs := make([]*Runner, 0, len(m.runners))
	for _, r := range m.runners {
		rs = append(rs, r)
	}
	m.runners = nil
	m.mu.Unlock()
	for _, r := range rs {
		r.Stop()
	}
}

// MarkLaneRemoving signals the named lane's runner to refuse all
// further pickups and waits for the runner to acknowledge. This ack
// is required BEFORE apply terminates queued missions
// in a force-prune flow, so a concurrent dispatch landing on the
// doomed lane cannot race the terminate loop. Returns an
// error if the lane is not currently managed.
func (m *Manager) MarkLaneRemoving(name string) error {
	m.mu.Lock()
	r, ok := m.runners[name]
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("lane %q not found", name)
	}
	r.MarkRemoving()
	return nil
}

// IsLaneRemoving reports whether the named lane is in the force-prune
// transition window (MarkLaneRemoving called but applied config not yet
// persisted/runner not yet stopped). Dispatch consults this before
// INSERTing a new queued row so the row doesn't land on a doomed lane
// during the gap.
func (m *Manager) IsLaneRemoving(name string) bool {
	m.mu.Lock()
	r, ok := m.runners[name]
	m.mu.Unlock()
	if !ok {
		return false
	}
	return r.IsRemoving()
}

// PauseLane pauses a running lane by name. Returns an error if the lane is not
// currently managed by this Manager.
func (m *Manager) PauseLane(name string) error {
	m.mu.Lock()
	r, ok := m.runners[name]
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("lane %q not found", name)
	}
	r.Pause()
	return nil
}

// ContinueLane resumes a paused lane by name. Returns an error if the lane is not
// currently managed by this Manager.
func (m *Manager) ContinueLane(name string) error {
	m.mu.Lock()
	r, ok := m.runners[name]
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("lane %q not found", name)
	}
	r.Resume()
	return nil
}

// CurrentLanes lists runners with their current concurrency and paused state.
func (m *Manager) CurrentLanes() []LaneSpec {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]LaneSpec, 0, len(m.runners))
	for name, r := range m.runners {
		r.mu.Lock()
		paused := r.paused
		conc := r.Concurrency
		r.mu.Unlock()
		out = append(out, LaneSpec{Name: name, Concurrency: conc, Paused: paused})
	}
	return out
}
