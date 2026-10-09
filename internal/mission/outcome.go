package mission

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// sigName maps a termination signal to its stable symbolic short name
// ("KILL", "SEGV", …) for the missions.signal field and the done event.
// Storing syscall.Signal.String() persisted OS-localised long forms
// ("segmentation fault", and different text on darwin), so clients
// filtering on `signal` saw inconsistent values and signalToFailReason's
// short-form cases were dead on Linux / misclassified on darwin. Normalising
// at the source makes both the stored field and the fail_reason mapping
// OS-independent.
func sigName(sig syscall.Signal) string {
	switch sig {
	case syscall.SIGKILL:
		return "KILL"
	case syscall.SIGTERM:
		return "TERM"
	case syscall.SIGINT:
		return "INT"
	case syscall.SIGQUIT:
		return "QUIT"
	case syscall.SIGHUP:
		return "HUP"
	case syscall.SIGSEGV:
		return "SEGV"
	case syscall.SIGBUS:
		return "BUS"
	case syscall.SIGILL:
		return "ILL"
	case syscall.SIGFPE:
		return "FPE"
	case syscall.SIGTRAP:
		return "TRAP"
	case syscall.SIGABRT:
		return "ABRT"
	case syscall.SIGPIPE:
		return "PIPE"
	}
	return strconv.Itoa(int(sig))
}

// ExternalKillReason names why dugdale itself initiated SIGTERM/SIGKILL of
// the mission. KillNone means dugdale didn't kill it.
type ExternalKillReason string

const (
	KillNone            ExternalKillReason = ""
	KillTimeout         ExternalKillReason = "timeout"
	KillForceDelete     ExternalKillReason = "force_delete"
	KillLaneRemoved     ExternalKillReason = "lane_removed"
	KillDugdaleShutdown ExternalKillReason = "dugdale_shutdown"
	KillByAPI           ExternalKillReason = "killed_by_api"
)

// OutcomeInputs aggregates everything needed to compute a mission outcome.
type OutcomeInputs struct {
	ExternalKill  ExternalKillReason
	TimeoutMs     int64  // mission timeout, 0 if none; named in the timeout message
	Lane          string // named in the lane_removed message
	OOMDetected   bool   // PHP marker observed in stderr-copy goroutine
	OOMLine       string // stderr line holding the PHP marker, if captured
	ExitCode      int    // ProcessState.ExitCode (-1 if signaled)
	Signal        string // "TERM"|"KILL"|"SEGV"|... empty if no signal
	Fd3Final      *Fd3Final
	Fd3Violations []Fd3Violation
}

// OutcomeResult is the tuple persisted to the missions row by Finalize.
type OutcomeResult struct {
	Outcome     string          // success | failed | killed | timeout | crashed | oom
	FailReason  string          // empty for success/timeout
	FailMessage string          // truncated downstream by capOutcome
	FailDetails json.RawMessage // truncated downstream by capOutcome
	ExitCode    int
	Signal      string
	Return      json.RawMessage // populated only on success path
	DropReturn  bool            // hint to finalize: don't persist Return even if non-nil
}

// Compute applies the priority table to map inputs to a terminal
// outcome. The first matching row wins.
func Compute(in OutcomeInputs) OutcomeResult {
	// 1. External kill wins over everything — even a fd3 success that arrived
	// the millisecond before SIGTERM.
	killMsg, killDetails := killFailure(in.ExternalKill, in.TimeoutMs, in.Lane)
	switch in.ExternalKill {
	case KillTimeout:
		return OutcomeResult{Outcome: "timeout", FailMessage: killMsg, FailDetails: killDetails, ExitCode: in.ExitCode, Signal: in.Signal}
	case KillForceDelete:
		return OutcomeResult{Outcome: "killed", FailReason: "force_delete", FailMessage: killMsg, ExitCode: in.ExitCode, Signal: in.Signal}
	case KillLaneRemoved:
		return OutcomeResult{Outcome: "killed", FailReason: "lane_removed", FailMessage: killMsg, ExitCode: in.ExitCode, Signal: in.Signal}
	case KillDugdaleShutdown:
		return OutcomeResult{Outcome: "killed", FailReason: "dugdale_shutdown", FailMessage: killMsg, ExitCode: in.ExitCode, Signal: in.Signal}
	case KillByAPI:
		return OutcomeResult{Outcome: "killed", FailReason: "killed_by_api", FailMessage: killMsg, ExitCode: in.ExitCode, Signal: in.Signal}
	}

	// 2. OOM proof, then SIGKILL without proof.
	if in.OOMDetected {
		msg := in.OOMLine
		if msg == "" {
			msg = "PHP memory limit exhausted (detected in stderr)"
		}
		return OutcomeResult{Outcome: "oom", FailReason: "php_memory_limit", FailMessage: msg, ExitCode: in.ExitCode, Signal: in.Signal}
	}
	if in.Signal == "KILL" || in.Signal == "killed" {
		return OutcomeResult{Outcome: "killed", FailReason: "unknown_sigkill",
			FailMessage: "process received SIGKILL not sent by dugdale (kernel OOM killer or an external kill)",
			ExitCode:    in.ExitCode, Signal: in.Signal}
	}

	// 3. Signal != KILL without fd3 final. "segfault or corresponding" —
	// map the specific signal so operator filtering on fail_reason can
	// distinguish SEGV from BUS/ILL/FPE.
	if in.Signal != "" && in.Fd3Final == nil {
		return OutcomeResult{Outcome: "failed", FailReason: signalToFailReason(in.Signal),
			FailMessage: fmt.Sprintf("process terminated by signal %s without a final event", in.Signal),
			ExitCode:    in.ExitCode, Signal: in.Signal}
	}

	// 4. Fd3 protocol violations (only when no kill/OOM/signal pre-empted).
	for _, v := range in.Fd3Violations {
		switch v.Reason {
		case "event_line_too_large", "event_protocol_error", "duplicate_final_event", "too_many_output_files":
			return OutcomeResult{Outcome: "failed", FailReason: v.Reason, FailMessage: v.Message, FailDetails: v.Details,
				ExitCode: in.ExitCode, Signal: in.Signal, DropReturn: true}
		}
	}

	// 5. Fd3 final and exit code.
	if in.Fd3Final != nil {
		switch in.Fd3Final.Kind {
		case "success":
			if in.ExitCode == 0 {
				return OutcomeResult{Outcome: "success", ExitCode: 0, Return: in.Fd3Final.Return}
			}
			return OutcomeResult{Outcome: "failed", FailReason: "success_then_failed_exit",
				FailMessage: fmt.Sprintf("mission emitted success but %s; return discarded", describeExit(in.ExitCode, in.Signal)),
				ExitCode:    in.ExitCode, Signal: in.Signal, DropReturn: true}
		case "fail":
			reason := in.Fd3Final.Reason
			if reason == "" {
				reason = "explicit"
			}
			if in.ExitCode == 0 {
				msg := in.Fd3Final.Message
				if msg == "" {
					msg = "mission emitted fail but exited with code 0"
				}
				return OutcomeResult{Outcome: "failed", FailReason: "fail_then_zero_exit", FailMessage: msg, FailDetails: in.Fd3Final.Details, ExitCode: 0}
			}
			return OutcomeResult{Outcome: "failed", FailReason: reason, FailMessage: in.Fd3Final.Message, FailDetails: in.Fd3Final.Details, ExitCode: in.ExitCode, Signal: in.Signal}
		}
	}

	// 6. Implicit final.
	if in.ExitCode == 0 {
		return OutcomeResult{Outcome: "success", ExitCode: 0}
	}
	return OutcomeResult{Outcome: "failed", FailReason: "no_event_nonzero_exit",
		FailMessage: fmt.Sprintf("process exited with code %d without a success/fail event; see stderr", in.ExitCode),
		ExitCode:    in.ExitCode, Signal: in.Signal}
}

// killFailure returns the fail_message and fail_details of a process dugdale
// killed for reason r. timeoutMs and lane are named in the timeout and
// lane_removed messages when known. Returns "" and nil for KillNone.
func killFailure(r ExternalKillReason, timeoutMs int64, lane string) (string, json.RawMessage) {
	switch r {
	case KillTimeout:
		if timeoutMs <= 0 {
			return "mission exceeded its timeout and was killed", nil
		}
		return fmt.Sprintf("mission exceeded its timeout of %s and was killed", formatTimeoutMs(timeoutMs)),
			json.RawMessage(fmt.Sprintf(`{"timeout_ms":%d}`, timeoutMs))
	case KillForceDelete:
		return "killed because the mission was force-deleted", nil
	case KillLaneRemoved:
		if lane == "" {
			return "killed because its lane was removed from the config", nil
		}
		return fmt.Sprintf("killed because lane %q was removed from the config", lane), nil
	case KillDugdaleShutdown:
		return "killed because dugdale was shutting down", nil
	case KillByAPI:
		return "killed via the kill API", nil
	}
	return "", nil
}

// formatTimeoutMs renders a millisecond timeout as a Go duration without
// trailing zero units: 30s, 1m30s, 1h.
func formatTimeoutMs(ms int64) string {
	s := (time.Duration(ms) * time.Millisecond).String()
	if strings.HasSuffix(s, "m0s") {
		s = strings.TrimSuffix(s, "0s")
	}
	if strings.HasSuffix(s, "h0m") {
		s = strings.TrimSuffix(s, "0m")
	}
	return s
}

// describeExit names how a process ended: "exited with code N" or
// "was terminated by signal X".
func describeExit(exitCode int, signal string) string {
	if signal != "" {
		return "was terminated by signal " + signal
	}
	return fmt.Sprintf("exited with code %d", exitCode)
}

// signalToFailReason maps a process-termination signal name to the
// fail_reason. SEGV is the canonical "segfault"; other crash-class
// signals get their own taxonomy entry so operators can distinguish them.
func signalToFailReason(sig string) string {
	switch sig {
	case "SEGV", "segmentation fault":
		return "segfault"
	case "BUS", "bus error":
		return "sigbus"
	case "ILL", "illegal instruction":
		return "sigill"
	case "FPE", "floating point exception":
		return "sigfpe"
	case "TRAP", "trace/breakpoint trap":
		return "sigtrap"
	case "ABRT", "aborted":
		return "sigabrt"
	}
	return "signal_" + sig
}
