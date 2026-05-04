package middleware_test

import (
	"testing"
	"time"

	"letts/internal/server/middleware"
)

func TestBruteForceNoBlockBelowThreshold(t *testing.T) {
	tr := middleware.NewBruteForceTracker(time.Hour)
	for i := 0; i < 4; i++ {
		tr.RecordFailure("key1")
	}
	if d := tr.CheckBlocked("key1"); d != 0 {
		t.Errorf("want 0 before threshold, got %v", d)
	}
}

func TestBruteForceBlocksAtFive(t *testing.T) {
	tr := middleware.NewBruteForceTracker(time.Hour)
	for i := 0; i < 5; i++ {
		tr.RecordFailure("key1")
	}
	if d := tr.CheckBlocked("key1"); d == 0 {
		t.Error("want block after 5 failures, got 0")
	}
}

func TestBruteForceBackoffDoubles(t *testing.T) {
	tr := middleware.NewBruteForceTracker(time.Hour)
	// 5 failures → 100ms backoff
	for i := 0; i < 5; i++ {
		tr.RecordFailure("key1")
	}
	d5 := tr.CheckBlocked("key1")
	if d5 <= 0 {
		t.Fatal("expected block after 5 failures")
	}

	// 6th failure → 200ms backoff
	tr.RecordFailure("key1")
	d6 := tr.CheckBlocked("key1")
	if d6 <= d5 {
		t.Errorf("backoff should grow: d5=%v d6=%v", d5, d6)
	}
}

func TestBruteForceBackoffCappedAt30s(t *testing.T) {
	tr := middleware.NewBruteForceTracker(time.Hour)
	for i := 0; i < 30; i++ {
		tr.RecordFailure("key1")
	}
	d := tr.CheckBlocked("key1")
	if d > 30*time.Second+time.Millisecond {
		t.Errorf("backoff should be capped at 30s, got %v", d)
	}
}

func TestBruteForceRecordSuccessClears(t *testing.T) {
	tr := middleware.NewBruteForceTracker(time.Hour)
	for i := 0; i < 5; i++ {
		tr.RecordFailure("key1")
	}
	if tr.CheckBlocked("key1") == 0 {
		t.Fatal("should be blocked before RecordSuccess")
	}
	tr.RecordSuccess("key1")
	if d := tr.CheckBlocked("key1"); d != 0 {
		t.Errorf("want 0 after RecordSuccess, got %v", d)
	}
}

func TestBruteForceEmptyKeyNoOp(t *testing.T) {
	tr := middleware.NewBruteForceTracker(time.Hour)
	tr.RecordFailure("")
	tr.RecordSuccess("")
	if d := tr.CheckBlocked(""); d != 0 {
		t.Errorf("empty key should always return 0, got %v", d)
	}
}

func TestBruteForceTTLExpiry(t *testing.T) {
	// Use a very short TTL with injectable clock.
	tr := middleware.NewBruteForceTrackerWithClock(10*time.Millisecond, func() time.Time {
		return time.Now()
	})
	for i := 0; i < 5; i++ {
		tr.RecordFailure("key1")
	}
	if tr.CheckBlocked("key1") == 0 {
		t.Fatal("should be blocked initially")
	}

	// Advance clock past TTL by using a tracker with a future clock.
	past := time.Now().Add(-time.Hour)
	tr2 := middleware.NewBruteForceTrackerWithClock(10*time.Millisecond, func() time.Time {
		return past
	})
	for i := 0; i < 5; i++ {
		tr2.RecordFailure("key1")
	}
	// Now check with a clock well after TTL.
	future := time.Now().Add(time.Hour)
	tr3 := middleware.NewBruteForceTrackerWithClock(10*time.Millisecond, func() time.Time {
		return future
	})
	// Manually copy entries from tr2 is not possible — instead verify via the
	// exported constructor that injecting a "now" far in the future correctly
	// clears a past-TTL entry on CheckBlocked.
	// Since tr2 used a very early clock, its entries' updatedAt == past.
	// We can't directly share state, so we test the logic indirectly:
	// create tracker with short TTL, sleep past TTL, then CheckBlocked returns 0.
	_ = tr3
	_ = future

	// Simpler: test with real sleep for the very short TTL.
	trShort := middleware.NewBruteForceTracker(50 * time.Millisecond)
	for i := 0; i < 5; i++ {
		trShort.RecordFailure("ip1")
	}
	if trShort.CheckBlocked("ip1") == 0 {
		t.Fatal("should be blocked before TTL")
	}
	time.Sleep(60 * time.Millisecond)
	if d := trShort.CheckBlocked("ip1"); d != 0 {
		t.Errorf("want 0 after TTL expiry, got %v", d)
	}
}

func TestBruteForceIsolatedKeys(t *testing.T) {
	tr := middleware.NewBruteForceTracker(time.Hour)
	for i := 0; i < 5; i++ {
		tr.RecordFailure("key1")
	}
	if tr.CheckBlocked("key2") != 0 {
		t.Error("key2 should be unaffected by key1's failures")
	}
}
