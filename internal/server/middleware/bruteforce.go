package middleware

import (
	"sync"
	"time"
)

// BruteForceTracker keeps per-key failure counters with exponential backoff
// and a TTL after which a quiet key is automatically cleared.
type BruteForceTracker struct {
	mu      sync.Mutex
	entries map[string]*bfEntry
	ttl     time.Duration
	now     func() time.Time
}

type bfEntry struct {
	count       int
	nextAllowed time.Time
	updatedAt   time.Time
}

// NewBruteForceTracker creates a tracker with the given TTL and real clock.
func NewBruteForceTracker(ttl time.Duration) *BruteForceTracker {
	return NewBruteForceTrackerWithClock(ttl, time.Now)
}

// NewBruteForceTrackerWithClock creates a tracker with injected clock (for tests).
func NewBruteForceTrackerWithClock(ttl time.Duration, now func() time.Time) *BruteForceTracker {
	return &BruteForceTracker{
		entries: make(map[string]*bfEntry),
		ttl:     ttl,
		now:     now,
	}
}

// CheckBlocked returns how long until the key is unblocked (0 means not blocked).
// Entries past TTL are cleared on this call.
func (b *BruteForceTracker) CheckBlocked(key string) time.Duration {
	if key == "" {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	e, ok := b.entries[key]
	if !ok {
		return 0
	}
	now := b.now()
	if now.Sub(e.updatedAt) > b.ttl {
		delete(b.entries, key)
		return 0
	}
	if d := e.nextAllowed.Sub(now); d > 0 {
		return d
	}
	return 0
}

// RecordFailure increments the failure counter and sets backoff if >= 5 failures.
// Backoff schedule: 100ms × 2^(count-5), capped at 30s.
func (b *BruteForceTracker) RecordFailure(key string) {
	if key == "" {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	e, ok := b.entries[key]
	if !ok {
		e = &bfEntry{}
		b.entries[key] = e
	}
	now := b.now()
	e.count++
	e.updatedAt = now

	if e.count >= 5 {
		exp := e.count - 5
		delay := 100 * time.Millisecond
		for i := 0; i < exp; i++ {
			delay *= 2
			if delay >= 30*time.Second {
				delay = 30 * time.Second
				break
			}
		}
		e.nextAllowed = now.Add(delay)
	}
}

// RecordSuccess clears the failure counter for key, unblocking it.
func (b *BruteForceTracker) RecordSuccess(key string) {
	if key == "" {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.entries, key)
}
