//go:build !linux

package mission

// readProcStarttime is a no-op on non-Linux platforms; the caller persists 0
// to mission_runtime.proc_starttime, which startup-repair treats as "no
// strong identity available" and falls back to pid-only reuse detection.
func readProcStarttime(_ int) int64 { return 0 }
