package mission

import (
	"encoding/json"
	"strconv"
	"syscall"
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
	OOMDetected   bool   // PHP marker observed in stderr-copy goroutine
	ExitCode      int    // ProcessState.ExitCode (-1 if signaled)
	Signal        string // "TERM"|"KILL"|"SEGV"|... empty if no signal
	Fd3Final      *Fd3Final
	Fd3Violations []string
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
	switch in.ExternalKill {
	case KillTimeout:
		return OutcomeResult{Outcome: "timeout", ExitCode: in.ExitCode, Signal: in.Signal}
	case KillForceDelete:
		return OutcomeResult{Outcome: "killed", FailReason: "force_delete", ExitCode: in.ExitCode, Signal: in.Signal}
	case KillLaneRemoved:
		return OutcomeResult{Outcome: "killed", FailReason: "lane_removed", ExitCode: in.ExitCode, Signal: in.Signal}
	case KillDugdaleShutdown:
		return OutcomeResult{Outcome: "killed", FailReason: "dugdale_shutdown", ExitCode: in.ExitCode, Signal: in.Signal}
	case KillByAPI:
		return OutcomeResult{Outcome: "killed", FailReason: "killed_by_api", ExitCode: in.ExitCode, Signal: in.Signal}
	}

	// 2. OOM proof, then SIGKILL without proof.
	if in.OOMDetected {
		return OutcomeResult{Outcome: "oom", FailReason: "php_memory_limit", ExitCode: in.ExitCode, Signal: in.Signal}
	}
	if in.Signal == "KILL" || in.Signal == "killed" {
		return OutcomeResult{Outcome: "killed", FailReason: "unknown_sigkill", ExitCode: in.ExitCode, Signal: in.Signal}
	}

	// 3. Signal != KILL without fd3 final. "segfault or corresponding" —
	// map the specific signal so operator filtering on fail_reason can
	// distinguish SEGV from BUS/ILL/FPE.
	if in.Signal != "" && in.Fd3Final == nil {
		return OutcomeResult{Outcome: "failed", FailReason: signalToFailReason(in.Signal),
			ExitCode: in.ExitCode, Signal: in.Signal}
	}

	// 4. Fd3 protocol violations (only when no kill/OOM/signal pre-empted).
	for _, v := range in.Fd3Violations {
		switch v {
		case "event_line_too_large", "event_protocol_error", "duplicate_final_event", "too_many_output_files":
			return OutcomeResult{Outcome: "failed", FailReason: v, ExitCode: in.ExitCode, Signal: in.Signal, DropReturn: true}
		}
	}

	// 5. Fd3 final and exit code.
	if in.Fd3Final != nil {
		switch in.Fd3Final.Kind {
		case "success":
			if in.ExitCode == 0 {
				return OutcomeResult{Outcome: "success", ExitCode: 0, Return: in.Fd3Final.Return}
			}
			return OutcomeResult{Outcome: "failed", FailReason: "success_then_failed_exit", ExitCode: in.ExitCode, Signal: in.Signal, DropReturn: true}
		case "fail":
			reason := in.Fd3Final.Reason
			if reason == "" {
				reason = "explicit"
			}
			if in.ExitCode == 0 {
				return OutcomeResult{Outcome: "failed", FailReason: "fail_then_zero_exit", FailMessage: in.Fd3Final.Message, FailDetails: in.Fd3Final.Details, ExitCode: 0}
			}
			return OutcomeResult{Outcome: "failed", FailReason: reason, FailMessage: in.Fd3Final.Message, FailDetails: in.Fd3Final.Details, ExitCode: in.ExitCode, Signal: in.Signal}
		}
	}

	// 6. Implicit final.
	if in.ExitCode == 0 {
		return OutcomeResult{Outcome: "success", ExitCode: 0}
	}
	return OutcomeResult{Outcome: "failed", FailReason: "no_event_nonzero_exit", ExitCode: in.ExitCode, Signal: in.Signal}
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
