package stagingstore

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestUploadLockTryAcquireExclusive(t *testing.T) {
	u := NewUploadLock(time.Second, nil)
	rel1, ok1 := u.TryAcquire("id1", nil)
	if !ok1 {
		t.Fatal("first acquire failed")
	}
	rel2, ok2 := u.TryAcquire("id1", nil)
	if ok2 {
		t.Fatal("second acquire succeeded; should be false")
	}
	if rel2 != nil {
		t.Fatal("second release should be nil on failure")
	}
	rel1()
	rel3, ok3 := u.TryAcquire("id1", nil)
	if !ok3 {
		t.Fatal("re-acquire after release failed")
	}
	rel3()
}

func TestUploadLockDifferentIDsConcurrent(t *testing.T) {
	u := NewUploadLock(time.Second, nil)
	r1, ok1 := u.TryAcquire("a", nil)
	r2, ok2 := u.TryAcquire("b", nil)
	if !ok1 || !ok2 {
		t.Fatalf("ok=%v,%v", ok1, ok2)
	}
	r1()
	r2()
}

func TestUploadLockIsLocked(t *testing.T) {
	u := NewUploadLock(time.Second, nil)
	if u.IsLocked("x") {
		t.Fatal("locked before acquire")
	}
	rel, _ := u.TryAcquire("x", nil)
	if !u.IsLocked("x") {
		t.Fatal("should be locked")
	}
	rel()
	if u.IsLocked("x") {
		t.Fatal("locked after release")
	}
}

func TestUploadLockSweepFiresOnIdle(t *testing.T) {
	now := time.Unix(0, 0)
	clock := &fakeClock{now: now}
	u := NewUploadLock(100*time.Millisecond, clock.Now)

	var fired atomic.Int32
	rel, ok := u.TryAcquire("x", func() { fired.Add(1) })
	if !ok {
		t.Fatal("acquire failed")
	}

	clock.advance(50 * time.Millisecond)
	u.Sweep()
	if fired.Load() != 0 {
		t.Errorf("fired prematurely: %d", fired.Load())
	}

	clock.advance(60 * time.Millisecond) // total 110ms
	u.Sweep()
	if fired.Load() != 1 {
		t.Errorf("fired=%d, want 1", fired.Load())
	}
	// Entry should be removed; release is now a no-op.
	rel()
	if u.IsLocked("x") {
		t.Error("entry still held after sweep")
	}
}

func TestUploadLockTouchExtendsDeadline(t *testing.T) {
	clock := &fakeClock{now: time.Unix(0, 0)}
	u := NewUploadLock(100*time.Millisecond, clock.Now)
	var fired atomic.Int32
	_, _ = u.TryAcquire("x", func() { fired.Add(1) })

	clock.advance(80 * time.Millisecond)
	u.Touch("x")
	clock.advance(80 * time.Millisecond) // 160ms total but only 80 since touch
	u.Sweep()
	if fired.Load() != 0 {
		t.Errorf("fired despite touch: %d", fired.Load())
	}
	clock.advance(50 * time.Millisecond) // 130ms since touch
	u.Sweep()
	if fired.Load() != 1 {
		t.Errorf("fired=%d, want 1 after exhausting touch", fired.Load())
	}
}

func TestUploadLockReleaseSuppressesOnIdle(t *testing.T) {
	clock := &fakeClock{now: time.Unix(0, 0)}
	u := NewUploadLock(50*time.Millisecond, clock.Now)
	var fired atomic.Int32
	rel, _ := u.TryAcquire("x", func() { fired.Add(1) })
	rel() // released before sweep
	clock.advance(100 * time.Millisecond)
	u.Sweep()
	if fired.Load() != 0 {
		t.Errorf("onIdle fired after release: %d", fired.Load())
	}
}

func TestUploadLockJanitorRuns(t *testing.T) {
	clock := &fakeClock{now: time.Unix(0, 0)}
	u := NewUploadLock(20*time.Millisecond, clock.Now)
	var fired atomic.Int32
	_, _ = u.TryAcquire("x", func() { fired.Add(1) })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	u.Start(ctx, 5*time.Millisecond)
	defer u.Stop()

	// Advance the clock; janitor must observe idle and fire.
	clock.advance(50 * time.Millisecond)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if fired.Load() > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if fired.Load() != 1 {
		t.Errorf("fired=%d, want 1", fired.Load())
	}
}

func TestUploadLockStopIdempotent(t *testing.T) {
	u := NewUploadLock(time.Second, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	u.Start(ctx, 10*time.Millisecond)
	u.Stop()
	u.Stop() // should not panic
}

func TestUploadLockTouchOnUnknownIDIsNoop(t *testing.T) {
	u := NewUploadLock(time.Second, nil)
	u.Touch("nope")
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}
