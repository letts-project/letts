// Package shutdown coordinates the two-stage graceful shutdown.
//
// Phase Running   — normal operation; dispatch accepts requests.
// Phase Draining  — first SIGTERM observed: dispatch returns 503 with Retry-After,
//
//	lane pickups are paused, in-flight missions continue, the
//	coordinator polls until none remain.
//
// Phase Aggressive — second SIGTERM/SIGINT observed: every running mission is
//
//	signalled (via runtime.Killer.SignalKill with
//	KillDugdaleShutdown) so mission.Run's kill watcher
//	SIGTERM/grace/SIGKILLs the process group; drain loop
//	converges within 100s of ms.
//
// Phase Done      — all missions finalized; lane runners stopped; Wait returns.
package shutdown

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"letts/internal/config"
	"letts/internal/lane"
	"letts/internal/mission"
)

// Phase is the public name for the shutdown state. See package doc.
type Phase int32

const (
	PhaseRunning Phase = iota
	PhaseDraining
	PhaseAggressive
	PhaseDone
)

// String renders the phase for log fields.
func (p Phase) String() string {
	switch p {
	case PhaseRunning:
		return "running"
	case PhaseDraining:
		return "draining"
	case PhaseAggressive:
		return "aggressive"
	case PhaseDone:
		return "done"
	}
	return "unknown"
}

// Killer is the subset of runtime.Runtime that the coordinator uses to
// trigger an aggressive kill and to learn which missions are still genuinely
// in-flight. Defined as an interface so tests can stub without spawning real
// missions.
type Killer interface {
	SignalKill(missionID string, reason mission.ExternalKillReason) bool
	// LiveMissionIDs returns the IDs of missions with a live run goroutine —
	// the authoritative in-flight set the drain waits on. It is independent of
	// the DB, so neither a lock storm nor a 'running' row stranded by a failed
	// finalize (no live goroutine) can stall shutdown.
	LiveMissionIDs() []string
}

// Coordinator orchestrates graceful shutdown. Construct via New; call Stop
// from the signal handler; Wait blocks until drain completes.
type Coordinator struct {
	DB     *sql.DB
	Cfg    *config.DugdaleConfig
	Mgr    *lane.Manager
	Killer Killer // optional; nil disables aggressive kill
	Logger *slog.Logger

	// StatusInterval is the cadence at which Draining-phase status is printed
	// and pending counts are re-read. Defaults to 10s.
	StatusInterval time.Duration
	// AggressiveInterval is the polling interval used after the second SIGTERM.
	// Defaults to 200ms.
	AggressiveInterval time.Duration
	// StatusOut is where the "[graceful-shutdown] waiting for N missions"
	// table is printed. Defaults to os.Stderr.
	StatusOut io.Writer

	phase      atomic.Int32
	drainOnce  sync.Once
	finishOnce sync.Once
	done       chan struct{}
}

// New constructs an idle coordinator.
func New(db *sql.DB, cfg *config.DugdaleConfig, mgr *lane.Manager, killer Killer, logger *slog.Logger) *Coordinator {
	if logger == nil {
		logger = slog.Default()
	}
	return &Coordinator{
		DB: db, Cfg: cfg, Mgr: mgr, Killer: killer, Logger: logger,
		done: make(chan struct{}),
	}
}

// Phase reports the current shutdown phase.
func (c *Coordinator) Phase() Phase { return Phase(c.phase.Load()) }

// BlockNewDispatches reports whether the dispatch handler should refuse new
// requests with 503 and Retry-After. True for any non-Running phase —
// including PhaseDone: Done means the drain loop finished
// but the daemon hasn't completed http.Server.Shutdown yet; a dispatch
// landing in that brief window deserves the same 503 draining as
// Draining/Aggressive. Phase progression is monotonic, so once tripped
// the function never returns false again — that's intentional.
func (c *Coordinator) BlockNewDispatches() bool { return c.Phase() != PhaseRunning }

// Stop is the signal-handler entry. Idempotent w.r.t. transitions:
// 1st call: Running → Draining (pauses lanes, starts drainLoop).
// 2nd call: Draining → Aggressive (signals KillDugdaleShutdown to running).
// Further calls and calls in PhaseDone are no-ops.
func (c *Coordinator) Stop(ctx context.Context) {
	for {
		old := Phase(c.phase.Load())
		switch old {
		case PhaseRunning:
			if c.phase.CompareAndSwap(int32(PhaseRunning), int32(PhaseDraining)) {
				c.Logger.Info("graceful shutdown: drain phase begins")
				c.pauseAllLanes()
				c.drainOnce.Do(func() { go c.drainLoop(ctx) })
				return
			}
			// CAS lost — retry to observe the new phase.
		case PhaseDraining:
			if c.phase.CompareAndSwap(int32(PhaseDraining), int32(PhaseAggressive)) {
				c.Logger.Info("graceful shutdown: aggressive phase — signaling kills")
				go c.aggressiveKillAll(ctx)
				return
			}
		default:
			return
		}
	}
}

// Wait blocks until the coordinator finishes draining. Safe to call from
// multiple goroutines; the channel is closed exactly once.
func (c *Coordinator) Wait() { <-c.done }

func (c *Coordinator) pauseAllLanes() {
	if c.Mgr == nil {
		return
	}
	for _, lane := range c.Mgr.CurrentLanes() {
		if err := c.Mgr.PauseLane(lane.Name); err != nil {
			c.Logger.Warn("pause lane", "name", lane.Name, "err", err)
		}
	}
}

func (c *Coordinator) drainLoop(ctx context.Context) {
	statusInterval := c.StatusInterval
	if statusInterval <= 0 {
		statusInterval = 10 * time.Second
	}
	aggressiveInterval := c.AggressiveInterval
	if aggressiveInterval <= 0 {
		aggressiveInterval = 200 * time.Millisecond
	}

	for {
		rows, err := c.listRunning(ctx)
		if err != nil {
			// A transient DB error must NOT stall or falsely complete the
			// drain. Completion is decided by drainComplete from the in-memory
			// live-mission set, which never touches the DB, so a busy/closed DB
			// here costs only this tick's status print and re-signal.
			c.Logger.Warn("graceful shutdown: listRunning error; will retry", "err", err)
		}

		if c.drainComplete(rows, err) {
			if c.Mgr != nil {
				c.Mgr.StopAll()
			}
			c.phase.Store(int32(PhaseDone))
			c.Logger.Info("graceful shutdown: complete")
			c.finish()
			return
		}

		if err == nil {
			// In aggressive phase, re-signal every running mission on every
			// tick. SignalKill is idempotent at the runtime layer (returns
			// false when the kill channel already has a pending signal, or when
			// the row is stranded with no live goroutine), so the cost is
			// bounded; the benefit is that missions which transitioned
			// queued→running just after the initial aggressiveKillAll snapshot
			// still get a kill. Stranded rows never block completion —
			// drainComplete excludes them.
			if c.Phase() == PhaseAggressive && c.Killer != nil {
				for _, r := range rows {
					_ = c.Killer.SignalKill(r.ID, mission.KillDugdaleShutdown)
				}
			}
			if c.Phase() == PhaseDraining {
				c.printStatus(rows)
			}
		}
		sleep := statusInterval
		if c.Phase() == PhaseAggressive {
			sleep = aggressiveInterval
		}
		select {
		case <-ctx.Done():
			c.Logger.Warn("graceful shutdown: ctx cancelled mid-drain")
			c.finish()
			return
		case <-time.After(sleep):
		}
	}
}

// drainComplete reports whether the drain has finished. The authoritative
// signal is the in-memory live-mission set from the Killer (the runtime
// kill-channel registry): a mission with a live run goroutine is genuinely
// in-flight, whereas a status='running' DB row WITHOUT one is stranded — its
// process is already gone and its finalize never landed (e.g. a transient DB
// lock during finalize left it stuck) — so waiting for it would hang shutdown
// forever (the 2026-06-27 incident, where a graceful restart never converged
// and had to be kill -9'd). Startup repair reclaims stranded rows as 'lost' on
// the next boot.
//
// With no Killer wired (tests), it falls back to the DB snapshot and, as
// before, never treats a transient listRunning error as completion.
func (c *Coordinator) drainComplete(rows []runningRow, listErr error) bool {
	if c.Killer != nil {
		return len(c.Killer.LiveMissionIDs()) == 0
	}
	return listErr == nil && len(rows) == 0
}

func (c *Coordinator) aggressiveKillAll(ctx context.Context) {
	if c.Killer == nil {
		c.Logger.Warn("aggressive shutdown: no Killer wired — running missions will not be signalled")
		return
	}
	// One-shot signal of the current snapshot. The drainLoop re-signals
	// every aggressive tick to catch missions that transitioned
	// queued→running just after this initial pass.
	rows, err := c.listRunning(ctx)
	if err != nil {
		c.Logger.Warn("aggressive shutdown: listRunning error", "err", err)
		return
	}
	for _, r := range rows {
		if !c.Killer.SignalKill(r.ID, mission.KillDugdaleShutdown) {
			c.Logger.Warn("kill signal not delivered", "mission_id", r.ID)
		}
	}
}

type runningRow struct {
	ID            string
	Lane          string
	MissionName   string
	TimeStartedMs int64
}

// listRunning returns the in-DB snapshot of status='running' rows. A
// non-nil error means the query (or row decode) failed and the caller
// MUST NOT treat the returned slice as a complete picture — see
// drainLoop / aggressiveKillAll for handling.
func (c *Coordinator) listRunning(ctx context.Context) ([]runningRow, error) {
	rows, err := c.DB.QueryContext(ctx,
		`SELECT mission_id, lane, mission_name, time_started FROM missions WHERE status='running'`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []runningRow
	for rows.Next() {
		var r runningRow
		var startedMs sql.NullInt64
		if err := rows.Scan(&r.ID, &r.Lane, &r.MissionName, &startedMs); err != nil {
			return nil, err
		}
		if startedMs.Valid {
			r.TimeStartedMs = startedMs.Int64
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (c *Coordinator) printStatus(rows []runningRow) {
	out := c.StatusOut
	if out == nil {
		out = os.Stderr
	}
	_, _ = fmt.Fprintf(out, "[graceful-shutdown] waiting for %d missions to finish:\n", len(rows))
	nowMs := time.Now().UnixMilli()
	for _, r := range rows {
		var dur time.Duration
		if r.TimeStartedMs > 0 {
			dur = time.Duration(nowMs-r.TimeStartedMs) * time.Millisecond
		}
		short := r.ID
		if len(short) > 8 {
			short = short[:8]
		}
		_, _ = fmt.Fprintf(out, "  %s  %-12s %-30s running %s\n",
			short, r.Lane, r.MissionName, formatDuration(dur))
	}
}

func formatDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	s := int64(d.Seconds())
	h := s / 3600
	m := (s % 3600) / 60
	sec := s % 60
	if h > 0 {
		return fmt.Sprintf("%dh%02dm", h, m)
	}
	if m > 0 {
		return fmt.Sprintf("%dm%02ds", m, sec)
	}
	return fmt.Sprintf("%ds", sec)
}

func (c *Coordinator) finish() {
	c.finishOnce.Do(func() { close(c.done) })
}
