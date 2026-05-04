// Package criticalerr tracks a sticky process-wide "manual repair
// required" signal. When a finalize
// attempt observes a terminal `done` event in the events file that
// disagrees with the intent's outcome, the daemon must surface a
// critical error via /v1/readyz until an operator manually reconciles
// — overwriting the DB outcome would lie about what was already shown
// to public stream consumers.
//
// State is process-scoped. A flag set here lives until the operator
// resolves the underlying intent (typically by deleting the mission
// row and intent, then restarting the daemon).
package criticalerr

import (
	"sync"
	"sync/atomic"
)

// Detail describes the most recent critical event for /v1/readyz body
// and audit log. Read via Get(); reset only on process restart.
type Detail struct {
	Kind      string // "terminal_event_conflict"
	MissionID string
	Op        string // human-readable context tag
}

var (
	tripped atomic.Bool
	mu      sync.Mutex
	current Detail
)

// Trip records a critical error. Subsequent calls are no-ops at the
// flag level — first trip wins for the human-facing detail. Idempotent
// across goroutines.
func Trip(d Detail) {
	if tripped.CompareAndSwap(false, true) {
		mu.Lock()
		current = d
		mu.Unlock()
	}
}

// Get reports the current state. ok=true means a Trip happened earlier
// in the process lifetime; d carries the first-trip detail.
func Get() (d Detail, ok bool) {
	if !tripped.Load() {
		return Detail{}, false
	}
	mu.Lock()
	defer mu.Unlock()
	return current, true
}

// Reset is for tests only; production never clears the sticky flag.
func Reset() {
	tripped.Store(false)
	mu.Lock()
	current = Detail{}
	mu.Unlock()
}
