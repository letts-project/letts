package main

import (
	"errors"
	"fmt"
)

// ExecOutcomeError carries the done-event outcome and exit_code so
// mapErrorToExit can apply the outcome → exit-code rules.
type ExecOutcomeError struct {
	Outcome  string
	ExitCode int
}

func (e *ExecOutcomeError) Error() string {
	return fmt.Sprintf("exec outcome=%s exit_code=%d", e.Outcome, e.ExitCode)
}

// ExecTransportError wraps any pre-terminal transport/auth/config/staging
// error so mapErrorToExit produces 255.
type ExecTransportError struct{ Inner error }

func (e *ExecTransportError) Error() string { return e.Inner.Error() }
func (e *ExecTransportError) Unwrap() error { return e.Inner }

// wrapExecTransport wraps any error that is NOT BadUsage / WaitTimeout /
// ExecOutcome / already-wrapped into ExecTransportError. Idempotent.
func wrapExecTransport(err error) error {
	if err == nil {
		return nil
	}
	var bu *BadUsageError
	if errors.As(err, &bu) {
		return err
	}
	var wt *WaitTimeoutError
	if errors.As(err, &wt) {
		return err
	}
	var eo *ExecOutcomeError
	if errors.As(err, &eo) {
		return err
	}
	var et *ExecTransportError
	if errors.As(err, &et) {
		return err
	}
	return &ExecTransportError{Inner: err}
}

// mapExecOutcomeExitCode maps an ExecOutcomeError to the CLI exit code.
// Extracted so the fan-out aggregator can call it with an integer return
// without going through the cobra typed-error path.
func mapExecOutcomeExitCode(outcome string, code int) int {
	switch outcome {
	case "success":
		if code == 124 || code == 125 || code == 255 {
			return 125
		}
		return code
	case "failed":
		if code == 0 {
			// outcome=failed must surface as non-zero; remap to 1.
			return 1
		}
		if code == 124 || code == 125 || code == 255 {
			return 125
		}
		return code
	default: // killed | timeout | oom | crashed | lost
		return 125
	}
}
