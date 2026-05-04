// Package stagingstore holds the per-process synchronization primitives that
// coordinate concurrent staging-file uploads. Persistent state lives in the
// storage package; this package only carries in-memory locks and the janitor
// that aborts idle uploads.
package stagingstore

import (
	"context"
	"sync"
	"time"
)

// UploadLock is a per-staging_id mutex with an idle-timeout janitor that
// invokes a per-entry abort callback when no progress is reported for
// IdleTimeout. The janitor is optional (call Start to enable it).
type UploadLock struct {
	mu          sync.Mutex
	entries     map[string]*uploadEntry
	idleTimeout time.Duration
	now         func() time.Time

	janitorStop chan struct{}
	janitorWg   sync.WaitGroup
	janitorOnce sync.Once
}

type uploadEntry struct {
	lastProgress time.Time
	onIdle       func()
	fired        bool
}

// NewUploadLock returns a new lock with the given idle timeout. nowFn defaults
// to time.Now if nil — tests can inject a fake clock.
func NewUploadLock(idleTimeout time.Duration, nowFn func() time.Time) *UploadLock {
	if nowFn == nil {
		nowFn = time.Now
	}
	return &UploadLock{
		entries:     make(map[string]*uploadEntry),
		idleTimeout: idleTimeout,
		now:         nowFn,
	}
}

// TryAcquire registers an upload for id and returns a release callback. If
// another upload for the same id is already active, ok is false and release
// is nil.
//
// onIdle is invoked at most once if the janitor observes the entry idle past
// the configured timeout. It is invoked outside the lock and never after
// release returns. Pass nil to opt out.
func (u *UploadLock) TryAcquire(id string, onIdle func()) (release func(), ok bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if _, exists := u.entries[id]; exists {
		return nil, false
	}
	e := &uploadEntry{lastProgress: u.now(), onIdle: onIdle}
	u.entries[id] = e
	return func() {
		u.mu.Lock()
		// Mark fired so an in-flight janitor pass that has already
		// captured the entry pointer doesn't invoke onIdle after release.
		e.fired = true
		delete(u.entries, id)
		u.mu.Unlock()
	}, true
}

// Touch updates the entry's lastProgress to "now". No-op if id isn't held.
// Called by the upload handler after each accepted body chunk so that long
// but live uploads aren't reaped as idle.
func (u *UploadLock) Touch(id string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if e, ok := u.entries[id]; ok {
		e.lastProgress = u.now()
	}
}

// IsLocked reports whether id is currently held.
func (u *UploadLock) IsLocked(id string) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	_, ok := u.entries[id]
	return ok
}

// Sweep examines every entry and fires onIdle for those whose lastProgress is
// older than idleTimeout. Exposed for tests; the janitor calls it on a tick.
func (u *UploadLock) Sweep() {
	u.mu.Lock()
	now := u.now()
	var toFire []*uploadEntry
	for id, e := range u.entries {
		if e.fired {
			continue
		}
		if now.Sub(e.lastProgress) >= u.idleTimeout {
			e.fired = true
			delete(u.entries, id)
			toFire = append(toFire, e)
		}
	}
	u.mu.Unlock()
	for _, e := range toFire {
		if e.onIdle != nil {
			e.onIdle()
		}
	}
}

// Start launches the janitor goroutine, which calls Sweep every interval until
// ctx is cancelled or Stop is invoked. Safe to call once.
func (u *UploadLock) Start(ctx context.Context, interval time.Duration) {
	u.janitorOnce.Do(func() {
		u.janitorStop = make(chan struct{})
		u.janitorWg.Add(1)
		go func() {
			defer u.janitorWg.Done()
			t := time.NewTicker(interval)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-u.janitorStop:
					return
				case <-t.C:
					u.Sweep()
				}
			}
		}()
	})
}

// Stop signals the janitor to exit and waits for it. Idempotent.
func (u *UploadLock) Stop() {
	if u.janitorStop != nil {
		select {
		case <-u.janitorStop:
		default:
			close(u.janitorStop)
		}
	}
	u.janitorWg.Wait()
}
