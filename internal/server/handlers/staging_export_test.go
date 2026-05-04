package handlers

import "context"

// MakeIdleAbortFnForTest exposes the StagingHandler.idleAbortFn helper to
// _test files outside the package. Used by tests verifying
// the idle-abort callback flips time_expires and invokes cancel, without
// having to drive the full PUT pipeline.
func MakeIdleAbortFnForTest(h *StagingHandler, id string, cancel context.CancelFunc) func() {
	return h.idleAbortFn(id, cancel)
}
