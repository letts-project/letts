// Package ids generates and validates RFC 9562 UUIDv7 identifiers used as
// mission_id, staging_id, and Idempotency-Key.
package ids

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"sync"
	"time"
)

var uuidv7Regex = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

// state for monotonic UUIDv7 generation within the same millisecond.
var (
	mu       sync.Mutex
	lastMs   int64
	lastRand [10]byte // 74 bits of randomness used; we add 1 to the counter on collision.
)

// NewUUIDv7 returns a new RFC 9562 UUIDv7 in canonical lowercase 36-char form.
// Same-ms calls are monotonic by adding 1 to the 74-bit random counter.
func NewUUIDv7() string {
	ms := time.Now().UnixMilli()
	mu.Lock()
	defer mu.Unlock()

	if ms == lastMs {
		incrementMonotonic(&lastRand)
	} else {
		if _, err := rand.Read(lastRand[:]); err != nil {
			panic(fmt.Sprintf("crypto/rand failed: %v", err))
		}
		// Mask the bits that will be replaced by version/variant at output time,
		// so that lastRand is a pure 74-bit counter (4 + 8 + 6 + 56 bits).
		lastRand[0] &= 0x0f
		lastRand[2] &= 0x3f
		lastMs = ms
	}

	var b [16]byte
	// 48-bit ms timestamp big-endian
	binary.BigEndian.PutUint64(b[:8], uint64(ms)<<16)
	// version nibble = 7; high nibble of lastRand[0] is always 0 by invariant.
	b[6] = 0x70 | lastRand[0]
	// rand_a low byte
	b[7] = lastRand[1]
	// variant = 10xx; high 2 bits of lastRand[2] are always 0 by invariant.
	b[8] = 0x80 | lastRand[2]
	b[9] = lastRand[3]
	copy(b[10:], lastRand[4:])

	hexBuf := make([]byte, 32)
	hex.Encode(hexBuf, b[:])
	return string(hexBuf[0:8]) + "-" + string(hexBuf[8:12]) + "-" + string(hexBuf[12:16]) + "-" + string(hexBuf[16:20]) + "-" + string(hexBuf[20:32])
}

// incrementMonotonic adds 1 to the 74-bit counter stored in r, preserving the
// invariant that r[0] high nibble = 0 and r[2] high 2 bits = 0.
// Panics if the 74-bit counter saturates (requires ~1.9e22 calls within 1 ms).
func incrementMonotonic(r *[10]byte) {
	carry := uint16(1)
	for i := len(r) - 1; i >= 0 && carry != 0; i-- {
		var mask byte = 0xff
		switch i {
		case 0:
			mask = 0x0f
		case 2:
			mask = 0x3f
		}
		sum := uint16(r[i]&mask) + carry
		newLow := byte(sum) & mask
		// Preserve the bits outside the mask (always 0 by invariant for i==0 or i==2).
		r[i] = (r[i] &^ mask) | newLow
		if uint16(newLow) == sum {
			carry = 0
		} else {
			carry = 1
		}
	}
	if carry != 0 {
		panic("ids: UUIDv7 same-ms counter saturated")
	}
}

// ValidateUUIDv7 returns true if s is a canonical lowercase UUIDv7 string.
func ValidateUUIDv7(s string) bool {
	return uuidv7Regex.MatchString(s)
}

// ErrInvalidUUIDv7 is returned by UUIDv7Timestamp when input is not a valid v7.
var ErrInvalidUUIDv7 = errors.New("invalid UUIDv7")

// UUIDv7Timestamp extracts the embedded unix-ms timestamp from a UUIDv7.
func UUIDv7Timestamp(id string) (int64, error) {
	if !ValidateUUIDv7(id) {
		return 0, ErrInvalidUUIDv7
	}
	// Concatenate the first 12 hex chars (skipping the '-' at index 8).
	hexStr := id[0:8] + id[9:13]
	raw, err := hex.DecodeString(hexStr)
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrInvalidUUIDv7, err)
	}
	var ts int64
	for _, b := range raw {
		ts = (ts << 8) | int64(b)
	}
	return ts, nil
}
