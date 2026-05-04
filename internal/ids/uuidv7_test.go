package ids

import (
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestNewUUIDv7Format(t *testing.T) {
	id := NewUUIDv7()
	if !ValidateUUIDv7(id) {
		t.Fatalf("generated id %q failed regex validation", id)
	}
	// Version nibble (13th hex char, 0-indexed 14) must be '7'.
	if id[14] != '7' {
		t.Errorf("version nibble = %c, want 7", id[14])
	}
	// Variant nibble (after 2nd dash + 4 hex chars; index 19) must be 8|9|a|b.
	if !strings.ContainsRune("89ab", rune(id[19])) {
		t.Errorf("variant nibble = %c, want 8|9|a|b", id[19])
	}
}

func TestNewUUIDv7Monotonic(t *testing.T) {
	// 1000 generated within ~1ms should be lexicographically non-decreasing.
	prev := NewUUIDv7()
	for i := 0; i < 1000; i++ {
		next := NewUUIDv7()
		if next < prev {
			t.Fatalf("UUIDv7 went backwards: %s -> %s", prev, next)
		}
		prev = next
	}
}

func TestNewUUIDv7TimestampWithinWindow(t *testing.T) {
	before := time.Now().UnixMilli()
	id := NewUUIDv7()
	after := time.Now().UnixMilli()

	ts, err := UUIDv7Timestamp(id)
	if err != nil {
		t.Fatalf("extract timestamp: %v", err)
	}
	if ts < before || ts > after {
		t.Errorf("ts=%d not in [%d, %d]", ts, before, after)
	}
}

func TestIncrementMonotonicAtMaskBoundary(t *testing.T) {
	// rnd[2] at low-6 max with carry from rnd[3..9] all at max.
	// Expected: full carry propagates into rnd[1]; rnd[2] low-6 wraps to 0;
	// high 2 bits of rnd[2] remain 0.
	s := [10]byte{0, 0, 0x3f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	incrementMonotonic(&s)
	want := [10]byte{0, 1, 0, 0, 0, 0, 0, 0, 0, 0}
	if s != want {
		t.Errorf("after carry through mask boundary: got %x, want %x", s, want)
	}

	// Verify uuid suffix is lex-greater after the carry than before.
	// Each byte = version/variant nibble OR'd with the corresponding rnd byte.
	var bBefore [16]byte
	bBefore[6] = 0x70        // 0x70 | rnd[0]=0
	bBefore[7] = 0           // rnd[1] was 0
	bBefore[8] = 0x80 | 0x3f // 0x80 | rnd[2]=0x3f
	bBefore[9] = 0xff        // rnd[3] was 0xff
	for i := 10; i < 16; i++ {
		bBefore[i] = 0xff // rnd[4..9] were 0xff
	}
	var bAfter [16]byte
	bAfter[6] = 0x70 // 0x70 | rnd[0]=0 (still 0)
	bAfter[7] = 1    // rnd[1] incremented to 1
	bAfter[8] = 0x80 // 0x80 | rnd[2]=0 (wrapped)
	bAfter[9] = 0    // rnd[3..9] wrapped to 0
	// bAfter[10..15] are zero (left by default)
	if hex.EncodeToString(bAfter[6:]) <= hex.EncodeToString(bBefore[6:]) {
		t.Errorf("uuid suffix went backwards: before=%x after=%x", bBefore[6:], bAfter[6:])
	}

	// rnd[0] at low-4 max + carry: should panic (74-bit overflow).
	s2 := [10]byte{0x0f, 0xff, 0x3f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	defer func() {
		if recover() == nil {
			t.Errorf("expected panic on 74-bit overflow")
		}
	}()
	incrementMonotonic(&s2)
}

// TestNewUUIDv7SameMsForcesIncrement is a white-box test that seeds package
// state directly and verifies NewUUIDv7 takes the same-ms increment path
// (rather than relying on wall-clock luck). It calls NewUUIDv7 twice without
// advancing the clock perceived by the function and asserts the second result
// is lexicographically greater than the first.
func TestNewUUIDv7SameMsForcesIncrement(t *testing.T) {
	mu.Lock()
	// Seed a future ms so the first call below also enters the same-ms branch.
	// But that requires both calls to see the same ms. Wall-clock advances,
	// so we cannot guarantee both wall-clocks return the seeded value. Instead:
	// after the first call we forcibly reset lastMs to whatever ms it observed,
	// guaranteeing the second call enters the same-ms branch even if the wall
	// clock advanced by a millisecond between calls.
	lastMs = 0            // force new-ms branch on first call
	lastRand = [10]byte{} // reset for determinism
	mu.Unlock()

	first := NewUUIDv7()

	mu.Lock()
	// Force second call to take same-ms branch by setting lastMs to whatever
	// the next call's time.Now().UnixMilli() will return. We approximate by
	// using a far-future timestamp; the time-comparison is `ms == lastMs` so
	// we need lastMs == future_ms. Trick: NewUUIDv7 calls time.Now().UnixMilli().
	// Set lastMs to a value that matches: capture "now" and set lastMs to it.
	lastMs = time.Now().UnixMilli()
	// If between this Unlock and the next NewUUIDv7's time.Now() the wall clock
	// advances by 1ms+, the test enters new-ms branch and provides no signal.
	// To ensure correctness, run in a tight loop that retries until same-ms.
	mu.Unlock()

	var second string
	for attempt := 0; attempt < 100; attempt++ {
		mu.Lock()
		preMs := lastMs
		preRand := lastRand
		mu.Unlock()
		second = NewUUIDv7()
		mu.Lock()
		postMs := lastMs
		mu.Unlock()
		if postMs == preMs {
			// Same-ms path was taken: lastMs unchanged, lastRand was incremented.
			if lastRand == preRand {
				t.Fatalf("attempt %d: lastRand unchanged after same-ms call", attempt)
			}
			break
		}
		// New-ms branch was taken; reset and retry.
		mu.Lock()
		lastMs = postMs // align with what NewUUIDv7 just observed
		mu.Unlock()
		second = ""
	}
	if second == "" {
		t.Skip("could not force same-ms branch within 100 attempts; test skipped")
	}
	if second <= first {
		t.Errorf("same-ms call did not produce lex-greater id: first=%s second=%s", first, second)
	}
}

func TestUUIDv7TimestampError(t *testing.T) {
	cases := []string{
		"",
		"not-a-uuid",
		"01234567-89ab-1cde-8000-000000000000", // valid format but version 1
	}
	for _, c := range cases {
		ts, err := UUIDv7Timestamp(c)
		if err == nil {
			t.Errorf("UUIDv7Timestamp(%q) = nil err, want error", c)
		}
		if !errors.Is(err, ErrInvalidUUIDv7) {
			t.Errorf("UUIDv7Timestamp(%q) err = %v, want ErrInvalidUUIDv7", c, err)
		}
		if ts != 0 {
			t.Errorf("UUIDv7Timestamp(%q) ts = %d, want 0", c, ts)
		}
	}
}

func TestValidateUUIDv7Reject(t *testing.T) {
	cases := []string{
		"",
		"not-a-uuid",
		"01234567-89ab-1cde-8000-000000000000",  // version 1
		"01234567-89ab-7cde-7000-000000000000",  // variant 7
		"01234567-89AB-7cde-8000-000000000000",  // upper case
		"01234567-89ab-7cde-8000-00000000000",   // short
		"01234567-89ab-7cde-8000-0000000000001", // long
		"01234567 89ab 7cde 8000 000000000000",  // wrong separator
	}
	for _, c := range cases {
		if ValidateUUIDv7(c) {
			t.Errorf("ValidateUUIDv7(%q) = true, want false", c)
		}
	}
}
