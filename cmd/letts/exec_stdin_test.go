package main

import (
	"testing"
)

// init flips isTerminalFD to "always TTY" for the test binary so the
// existing test corpus (which never piped stdin) keeps falling into the
// auto-none branch instead of being silently coerced into auto-single
// (which would inject empty stdin bytes, an upload, and a Stdin field on
// the dispatched ExecRequest, breaking every pre-E6 dispatch assertion).
// Tests that need to exercise the piped branches override the package
// var locally and restore it via defer (see TestExecStdinSingleHostUploads).
func init() {
	isTerminalFD = func(uintptr) bool { return true }
}

// TestResolveStdinModeAutoNoneTTY: no --stdin with TTY stdin → "none" (auto).
// Mirrors the natural interactive case: `letts exec --host s1 -- bash`
// from a terminal must NOT block reading from the operator's keyboard.
func TestResolveStdinModeAutoNoneTTY(t *testing.T) {
	got, err := resolveStdinMode("", 1, true)
	if err != nil || got != "none" {
		t.Errorf("got %q err %v, want none/nil", got, err)
	}
}

// TestResolveStdinModeAutoSinglePiped: no --stdin, non-TTY, 1 host →
// "single" (auto). The `cat data | letts exec --host s1 -- cmd` shape
// short-circuits the explicit --stdin=single requirement.
func TestResolveStdinModeAutoSinglePiped(t *testing.T) {
	got, err := resolveStdinMode("", 1, false)
	if err != nil || got != "single" {
		t.Errorf("got %q err %v, want single/nil", got, err)
	}
}

// TestResolveStdinModeAutoMultiHostRejected: no --stdin, non-TTY, N>1
// hosts must reject with BadUsage so the operator opts into the per-host
// fan-out cost explicitly via --stdin=broadcast.
func TestResolveStdinModeAutoMultiHostRejected(t *testing.T) {
	_, err := resolveStdinMode("", 3, false)
	if err == nil {
		t.Error("expected BadUsage for multi-host non-TTY no --stdin")
	}
}

// TestResolveStdinModeExplicitSingleMultiHostRejected: --stdin=single
// is exclusively a single-host mode; multi-host must reject.
func TestResolveStdinModeExplicitSingleMultiHostRejected(t *testing.T) {
	_, err := resolveStdinMode("single", 3, false)
	if err == nil {
		t.Error("expected BadUsage for --stdin=single with N>1 hosts")
	}
}

// TestResolveStdinModeBroadcastTTYRejected: --stdin=broadcast requires
// piped stdin; reject when a TTY is on fd0 to avoid blocking on keyboard.
func TestResolveStdinModeBroadcastTTYRejected(t *testing.T) {
	_, err := resolveStdinMode("broadcast", 2, true)
	if err == nil {
		t.Error("expected BadUsage for --stdin=broadcast with TTY stdin")
	}
}

// TestResolveStdinModeBroadcastPipedMulti: --stdin=broadcast with piped
// stdin and N>1 hosts is the canonical multi-host stdin path.
func TestResolveStdinModeBroadcastPipedMulti(t *testing.T) {
	got, err := resolveStdinMode("broadcast", 2, false)
	if err != nil || got != "broadcast" {
		t.Errorf("got %q err %v, want broadcast/nil", got, err)
	}
}
