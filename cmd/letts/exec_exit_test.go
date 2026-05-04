package main

import (
	"errors"
	"testing"
)

func TestExecOutcomeMappingSuccess(t *testing.T) {
	cases := []struct {
		exit int
		want int
	}{
		{0, 0}, {1, 1}, {42, 42}, {123, 123}, {126, 126}, {254, 254},
		{124, 125}, {125, 125}, {255, 125}, // collision avoidance
	}
	for _, c := range cases {
		got := mapErrorToExit(&ExecOutcomeError{Outcome: "success", ExitCode: c.exit})
		if got != c.want {
			t.Errorf("success exit=%d → %d, want %d", c.exit, got, c.want)
		}
	}
}

func TestExecOutcomeMappingFailedExitZero(t *testing.T) {
	// outcome=failed && exit_code=0 → CLI 1 (treat as failure, not success).
	got := mapErrorToExit(&ExecOutcomeError{Outcome: "failed", ExitCode: 0})
	if got != 1 {
		t.Errorf("failed exit=0 → %d, want 1", got)
	}
}

func TestExecOutcomeMappingFailedExitPassthrough(t *testing.T) {
	cases := []struct{ exit, want int }{
		{1, 1}, {42, 42}, {123, 123}, {126, 126}, {254, 254},
		{124, 125}, {125, 125}, {255, 125}, // collision
	}
	for _, c := range cases {
		got := mapErrorToExit(&ExecOutcomeError{Outcome: "failed", ExitCode: c.exit})
		if got != c.want {
			t.Errorf("failed exit=%d → %d, want %d", c.exit, got, c.want)
		}
	}
}

func TestExecOutcomeMappingAbnormal(t *testing.T) {
	for _, oc := range []string{"killed", "timeout", "oom", "crashed", "lost"} {
		got := mapErrorToExit(&ExecOutcomeError{Outcome: oc, ExitCode: 0})
		if got != 125 {
			t.Errorf("outcome=%s → %d, want 125", oc, got)
		}
	}
}

func TestExecTransportMaps255(t *testing.T) {
	got := mapErrorToExit(&ExecTransportError{Inner: errors.New("dial: connection refused")})
	if got != 255 {
		t.Errorf("transport → %d, want 255", got)
	}
}

func TestExecWaitTimeoutStillMaps124(t *testing.T) {
	got := mapErrorToExit(NewWaitTimeoutError())
	if got != 124 {
		t.Errorf("wait-timeout → %d, want 124 (preserved)", got)
	}
}

func TestExecBadUsageStillMaps2(t *testing.T) {
	got := mapErrorToExit(NewBadUsageError("--lane required"))
	if got != 2 {
		t.Errorf("badusage → %d, want 2 (preserved)", got)
	}
}

// TestShouldReportErrSuppressesExecOutcome — `letts exec` on a remote
// command exiting non-zero (or even 0) returns an *ExecOutcomeError so
// mapErrorToExit can produce the correct CLI exit code, but the wrapper
// must NOT print `letts: exec outcome=...` to stderr — the command's own
// stderr already explained anything worth reporting.
func TestShouldReportErrSuppressesExecOutcome(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"success+0", &ExecOutcomeError{Outcome: "success", ExitCode: 0}, false},
		{"success+42", &ExecOutcomeError{Outcome: "success", ExitCode: 42}, false},
		{"failed+1", &ExecOutcomeError{Outcome: "failed", ExitCode: 1}, false},
		{"killed", &ExecOutcomeError{Outcome: "killed", ExitCode: 137}, false},
		{"transport", &ExecTransportError{Inner: errors.New("dial: refused")}, true},
		{"wait-timeout", NewWaitTimeoutError(), true},
		{"bad-usage", NewBadUsageError("--lane required"), true},
		{"plain", errors.New("boom"), true},
	}
	for _, c := range cases {
		got := shouldReportErr(c.err)
		if got != c.want {
			t.Errorf("%s: shouldReportErr=%v, want %v", c.name, got, c.want)
		}
	}
}

func TestWrapExecTransportIdempotent(t *testing.T) {
	bu := NewBadUsageError("--host required")
	wrapped := wrapExecTransport(bu)
	if wrapped != bu {
		t.Error("wrapExecTransport changed a BadUsageError; should pass through")
	}
	wt := NewWaitTimeoutError()
	if wrapExecTransport(wt) != wt {
		t.Error("wrapExecTransport changed a WaitTimeoutError; should pass through")
	}
	eo := &ExecOutcomeError{Outcome: "success", ExitCode: 0}
	if wrapExecTransport(eo) != eo {
		t.Error("wrapExecTransport changed an ExecOutcomeError; should pass through")
	}
	et := &ExecTransportError{Inner: errors.New("x")}
	if wrapExecTransport(et) != et {
		t.Error("wrapExecTransport double-wrapped (not idempotent)")
	}
	plain := errors.New("dial error")
	out := wrapExecTransport(plain)
	if _, ok := out.(*ExecTransportError); !ok {
		t.Errorf("wrapExecTransport(plain) = %T, want *ExecTransportError", out)
	}
}
