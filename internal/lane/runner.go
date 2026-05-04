package lane

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"letts/internal/storage"
)

// PreSpawnHook runs synchronously inside the lane loop after MarkRunning
// commits but BEFORE the Spawner goroutine starts. Used by the runtime
// to pre-register a per-mission kill channel so a kill API hitting the
// daemon between "status=running visible" and "spawner goroutine
// scheduled" can still deliver.
type PreSpawnHook func(missionID string)

// Spawner is the callback invoked once a mission is picked. Implementation
// lives in package mission; injected here to keep lane runner pure.
// The releaseSlot callback is invoked exactly once when the spawned mission
// (or its goroutine) is done — it decrements the runner's in-flight count
// so the pickup gate re-opens.
type Spawner func(ctx context.Context, m *storage.Mission, releaseSlot func()) error

// Runner is one lane's pickup goroutine.
//
// Concurrency is gated via an atomic in-flight counter compared against the
// current Concurrency limit on every pickup. SetConcurrency mutates the
// limit directly under r.mu; running missions are NOT preempted
// (hot-resize), but new pickups are blocked until in-flight drops below
// the new limit. A naive channel-semaphore swap would let new
// pickups draw from a fresh channel while running missions still held
// tickets in the old one, exceeding the new limit until drain.
type Runner struct {
	Lane        string
	DB          *sql.DB
	Spawner     Spawner
	PreSpawn    PreSpawnHook
	Concurrency int
	Logger      *slog.Logger

	mu      sync.Mutex
	paused  bool
	cancel  context.CancelFunc
	notify  chan struct{}
	avail   chan struct{} // signaled (buffered=1) on every slot release
	stopped chan struct{}

	inflight atomic.Int64 // current spawned-but-not-released count

	// removing is the "lane being removed" signal:
	// once set, the loop refuses any further pickup and the apply
	// flow waits for removingAck before transitioning queued missions
	// to done(killed,lane_removed). This closes the race
	// where a new dispatch landed between terminateLaneMissions and
	// the lane runner stop, letting the runner pick the doomed row.
	removing        atomic.Bool
	removingAck     chan struct{}
	removingAckOnce sync.Once
}

// Start launches the goroutine. Idempotent (no-op if already started).
func (r *Runner) Start(ctx context.Context) {
	r.mu.Lock()
	if r.cancel != nil {
		r.mu.Unlock()
		return
	}
	ctx, r.cancel = context.WithCancel(ctx)
	r.notify = make(chan struct{}, 1)
	r.avail = make(chan struct{}, 1)
	r.stopped = make(chan struct{})
	r.removingAck = make(chan struct{})
	r.mu.Unlock()
	go r.loop(ctx)
}

// Notify wakes the runner from a tick wait (called by dispatch handler).
func (r *Runner) Notify() {
	r.mu.Lock()
	c := r.notify
	r.mu.Unlock()
	if c == nil {
		return
	}
	select {
	case c <- struct{}{}:
	default:
	}
}

// Pause stops new pickups. Running missions continue.
func (r *Runner) Pause() {
	r.mu.Lock()
	r.paused = true
	r.mu.Unlock()
}

// Resume re-enables pickup.
func (r *Runner) Resume() {
	r.mu.Lock()
	r.paused = false
	r.mu.Unlock()
	r.Notify()
}

// SetConcurrency adjusts the lane's pickup limit. Safe to call concurrently
// with the loop. New limit takes effect IMMEDIATELY for new pickups —
// running missions continue to natural completion (hot-resize).
// If shrinking below current in-flight, the runner blocks all pickups
// until releases bring in-flight below the new limit.
func (r *Runner) SetConcurrency(n int) {
	r.mu.Lock()
	r.Concurrency = n
	avail := r.avail
	r.mu.Unlock()
	// Wake loop so it observes the new (potentially larger) limit.
	if avail != nil {
		select {
		case avail <- struct{}{}:
		default:
		}
	}
}

// Stop cancels the loop and waits for it to exit.
func (r *Runner) Stop() {
	r.mu.Lock()
	if r.cancel == nil {
		r.mu.Unlock()
		return
	}
	cancel := r.cancel
	r.cancel = nil
	stopped := r.stopped
	r.mu.Unlock()
	cancel()
	<-stopped
}

func (r *Runner) loop(ctx context.Context) {
	defer close(r.stopped)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Lane being removed. Ack
		// exactly once so apply's terminateLaneMissions can proceed,
		// then idle until ctx is cancelled by Stop.
		if r.removing.Load() {
			r.removingAckOnce.Do(func() { close(r.removingAck) })
			<-ctx.Done()
			return
		}

		r.mu.Lock()
		paused := r.paused
		limit := r.Concurrency
		r.mu.Unlock()

		if paused {
			r.waitForWake(ctx)
			continue
		}

		// Gate on in-flight count vs. current limit. Acts as the
		// concurrency cap — replaces the previous channel-semaphore
		// whose ticket pool couldn't be resized without leaking old
		// tickets to running spawners.
		if r.inflight.Load() >= int64(limit) {
			r.waitForAvail(ctx)
			continue
		}

		// Pick a mission in a single writer transaction.
		var picked *storage.Mission
		err := storage.WithWriter(ctx, r.DB, func(c *sql.Conn) error {
			m, err := storage.PickQueuedForLane(ctx, c, r.Lane)
			if errors.Is(err, storage.ErrNotFound) {
				return nil
			}
			if err != nil {
				return err
			}
			// pid/pgid/procStart filled later by spawn; pass placeholders.
			if err := storage.MarkRunning(ctx, c, m.ID, time.Now().UnixMilli(), 0, 0, 0); err != nil {
				return err
			}
			picked = m
			return nil
		})
		if err != nil {
			r.Logger.Error("lane pickup failed", "lane", r.Lane, "err", err)
			r.waitForWake(ctx)
			continue
		}
		if picked == nil {
			r.waitForWake(ctx)
			continue
		}

		// Mission acquired — count it in-flight until releaseSlot.
		r.inflight.Add(1)
		// Pre-spawn registration runs synchronously here: a
		// kill API arriving between MarkRunning commit and Spawner
		// goroutine scheduling would otherwise observe an empty
		// killChs map and 500. PreSpawn registers the channel; Spawner
		// then runs mission.Run with it.
		if r.PreSpawn != nil {
			r.PreSpawn(picked.ID)
		}
		release := func() {
			r.inflight.Add(-1)
			r.mu.Lock()
			avail := r.avail
			r.mu.Unlock()
			if avail != nil {
				select {
				case avail <- struct{}{}:
				default:
				}
			}
		}
		go func(m *storage.Mission) {
			if err := r.Spawner(ctx, m, release); err != nil {
				r.Logger.Error("spawn failed", "mission_id", m.ID, "err", err)
			}
		}(picked)
	}
}

// IsRemoving reports whether the runner is in the force-prune window.
// Used by dispatch handlers to refuse new queued rows for a lane that
// is being torn down.
func (r *Runner) IsRemoving() bool { return r.removing.Load() }

// MarkRemoving signals the loop that this lane is being removed by an
// apply force-prune. Returns after the loop has observed the signal and
// is guaranteed not to start a new pickup transaction. Subsequent calls
// are idempotent — the loop's ack channel is closed-once.
func (r *Runner) MarkRemoving() {
	r.removing.Store(true)
	// Wake the loop if it's blocked in waitForAvail or waitForWake.
	r.mu.Lock()
	avail := r.avail
	notify := r.notify
	r.mu.Unlock()
	if avail != nil {
		select {
		case avail <- struct{}{}:
		default:
		}
	}
	if notify != nil {
		select {
		case notify <- struct{}{}:
		default:
		}
	}
	// Block until loop acks.
	<-r.removingAck
}

// waitForAvail blocks until a slot release notifies us, the runner is
// cancelled, or 30s elapse (defensive fallback in case the avail signal
// gets dropped by a race we missed).
func (r *Runner) waitForAvail(ctx context.Context) {
	r.mu.Lock()
	avail := r.avail
	r.mu.Unlock()
	if avail == nil {
		return
	}
	select {
	case <-ctx.Done():
	case <-avail:
	case <-time.After(30 * time.Second):
	}
}

func (r *Runner) waitForWake(ctx context.Context) {
	r.mu.Lock()
	notify := r.notify
	r.mu.Unlock()
	select {
	case <-ctx.Done():
	case <-notify:
	case <-time.After(30 * time.Second):
	}
}
