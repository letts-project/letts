package mission

import (
	"encoding/json"
	"testing"
)

func TestComputeOutcomeTable(t *testing.T) {
	cases := []struct {
		name        string
		in          OutcomeInputs
		wantOutcome string
		wantReason  string
		wantExit    int
		wantSignal  string
		wantReturn  string // raw JSON, "" means no return
		wantDrop    bool
		wantMsg     string
		wantDetails string // raw JSON, "" means no details
	}{
		{
			name:        "external timeout overrides fd3 success",
			in:          OutcomeInputs{ExternalKill: KillTimeout, TimeoutMs: 30000, Signal: "TERM", ExitCode: -1, Fd3Final: &Fd3Final{Kind: "success", Return: json.RawMessage(`{"x":1}`)}},
			wantOutcome: "timeout",
			wantSignal:  "TERM",
			wantExit:    -1,
			wantMsg:     "mission exceeded its timeout of 30s and was killed",
			wantDetails: `{"timeout_ms":30000}`,
		},
		{
			name:        "external timeout without a known timeout value",
			in:          OutcomeInputs{ExternalKill: KillTimeout, Signal: "TERM", ExitCode: -1},
			wantOutcome: "timeout",
			wantSignal:  "TERM",
			wantExit:    -1,
			wantMsg:     "mission exceeded its timeout and was killed",
		},
		{
			name:        "external force_delete + fd3 success",
			in:          OutcomeInputs{ExternalKill: KillForceDelete, Signal: "KILL", ExitCode: -1, Fd3Final: &Fd3Final{Kind: "success"}},
			wantOutcome: "killed",
			wantReason:  "force_delete",
			wantSignal:  "KILL",
			wantExit:    -1,
			wantMsg:     "killed because the mission was force-deleted",
		},
		{
			name:        "external lane_removed",
			in:          OutcomeInputs{ExternalKill: KillLaneRemoved, Lane: "normal", Signal: "TERM", ExitCode: -1},
			wantOutcome: "killed",
			wantReason:  "lane_removed",
			wantSignal:  "TERM",
			wantExit:    -1,
			wantMsg:     `killed because lane "normal" was removed from the config`,
		},
		{
			name:        "external dugdale_shutdown",
			in:          OutcomeInputs{ExternalKill: KillDugdaleShutdown, Signal: "TERM", ExitCode: -1},
			wantOutcome: "killed",
			wantReason:  "dugdale_shutdown",
			wantSignal:  "TERM",
			wantExit:    -1,
			wantMsg:     "killed because dugdale was shutting down",
		},
		{
			name:        "external killed_by_api",
			in:          OutcomeInputs{ExternalKill: KillByAPI, Signal: "TERM", ExitCode: -1},
			wantOutcome: "killed",
			wantReason:  "killed_by_api",
			wantSignal:  "TERM",
			wantExit:    -1,
			wantMsg:     "killed via the kill API",
		},
		{
			name:        "OOM detected with captured marker line",
			in:          OutcomeInputs{OOMDetected: true, OOMLine: "PHP Fatal error:  Allowed memory size of 134217728 bytes exhausted (tried to allocate 20480 bytes)", ExitCode: 255},
			wantOutcome: "oom",
			wantReason:  "php_memory_limit",
			wantExit:    255,
			wantMsg:     "PHP Fatal error:  Allowed memory size of 134217728 bytes exhausted (tried to allocate 20480 bytes)",
		},
		{
			name:        "OOM detected without a captured line",
			in:          OutcomeInputs{OOMDetected: true, ExitCode: -1, Signal: "KILL"},
			wantOutcome: "oom",
			wantReason:  "php_memory_limit",
			wantSignal:  "KILL",
			wantExit:    -1,
			wantMsg:     "PHP memory limit exhausted (detected in stderr)",
		},
		{
			name:        "SIGKILL without OOM proof",
			in:          OutcomeInputs{Signal: "KILL", ExitCode: -1},
			wantOutcome: "killed",
			wantReason:  "unknown_sigkill",
			wantSignal:  "KILL",
			wantExit:    -1,
			wantMsg:     "process received SIGKILL not sent by dugdale (kernel OOM killer or an external kill)",
		},
		{
			name:        "SIGKILL with fd3 success but no OOM",
			in:          OutcomeInputs{Signal: "KILL", ExitCode: -1, Fd3Final: &Fd3Final{Kind: "success"}},
			wantOutcome: "killed",
			wantReason:  "unknown_sigkill",
			wantSignal:  "KILL",
			wantExit:    -1,
			wantMsg:     "process received SIGKILL not sent by dugdale (kernel OOM killer or an external kill)",
		},
		{
			name:        "SEGV without fd3 final",
			in:          OutcomeInputs{Signal: "SEGV", ExitCode: -1},
			wantOutcome: "failed",
			wantReason:  "segfault",
			wantSignal:  "SEGV",
			wantExit:    -1,
			wantMsg:     "process terminated by signal SEGV without a final event",
		},
		{
			name:        "TERM without fd3 final and without a dugdale kill",
			in:          OutcomeInputs{Signal: "TERM", ExitCode: -1},
			wantOutcome: "failed",
			wantReason:  "signal_TERM",
			wantSignal:  "TERM",
			wantExit:    -1,
			wantMsg:     "process terminated by signal TERM without a final event",
		},
		{
			name:        "violation event_line_too_large overrides fd3 success",
			in:          OutcomeInputs{Fd3Violations: []Fd3Violation{{Reason: "event_line_too_large", Message: "line too large"}}, Fd3Final: &Fd3Final{Kind: "success"}, ExitCode: 0},
			wantOutcome: "failed",
			wantReason:  "event_line_too_large",
			wantDrop:    true,
			wantMsg:     "line too large",
		},
		{
			name:        "violation event_protocol_error",
			in:          OutcomeInputs{Fd3Violations: []Fd3Violation{{Reason: "event_protocol_error", Message: "bad json"}}, ExitCode: 1},
			wantOutcome: "failed",
			wantReason:  "event_protocol_error",
			wantExit:    1,
			wantDrop:    true,
			wantMsg:     "bad json",
		},
		{
			name:        "violation duplicate_final_event",
			in:          OutcomeInputs{Fd3Violations: []Fd3Violation{{Reason: "duplicate_final_event", Message: "second final"}}, Fd3Final: &Fd3Final{Kind: "success"}, ExitCode: 0},
			wantOutcome: "failed",
			wantReason:  "duplicate_final_event",
			wantDrop:    true,
			wantMsg:     "second final",
		},
		{
			name:        "violation too_many_output_files",
			in:          OutcomeInputs{Fd3Violations: []Fd3Violation{{Reason: "too_many_output_files", Message: "too many"}}, Fd3Final: &Fd3Final{Kind: "success"}, ExitCode: 0},
			wantOutcome: "failed",
			wantReason:  "too_many_output_files",
			wantDrop:    true,
			wantMsg:     "too many",
		},
		{
			name:        "fd3 success + exit 0",
			in:          OutcomeInputs{Fd3Final: &Fd3Final{Kind: "success", Return: json.RawMessage(`{"a":1}`)}, ExitCode: 0},
			wantOutcome: "success",
			wantReturn:  `{"a":1}`,
		},
		{
			name:        "fd3 success + exit nonzero",
			in:          OutcomeInputs{Fd3Final: &Fd3Final{Kind: "success", Return: json.RawMessage(`{"a":1}`)}, ExitCode: 7},
			wantOutcome: "failed",
			wantReason:  "success_then_failed_exit",
			wantExit:    7,
			wantDrop:    true,
			wantMsg:     "mission emitted success but exited with code 7; return discarded",
		},
		{
			name:        "fd3 success + crash signal",
			in:          OutcomeInputs{Fd3Final: &Fd3Final{Kind: "success", Return: json.RawMessage(`{"a":1}`)}, Signal: "SEGV", ExitCode: -1},
			wantOutcome: "failed",
			wantReason:  "success_then_failed_exit",
			wantSignal:  "SEGV",
			wantExit:    -1,
			wantDrop:    true,
			wantMsg:     "mission emitted success but was terminated by signal SEGV; return discarded",
		},
		{
			name:        "fd3 fail + exit nonzero with reason",
			in:          OutcomeInputs{Fd3Final: &Fd3Final{Kind: "fail", Reason: "bad_arg", Message: "oops"}, ExitCode: 1},
			wantOutcome: "failed",
			wantReason:  "bad_arg",
			wantExit:    1,
			wantMsg:     "oops",
		},
		{
			name:        "fd3 fail + exit nonzero default reason",
			in:          OutcomeInputs{Fd3Final: &Fd3Final{Kind: "fail", Message: "oops"}, ExitCode: 2},
			wantOutcome: "failed",
			wantReason:  "explicit",
			wantExit:    2,
			wantMsg:     "oops",
		},
		{
			name:        "fd3 fail without message + exit nonzero stays as sent",
			in:          OutcomeInputs{Fd3Final: &Fd3Final{Kind: "fail"}, ExitCode: 1},
			wantOutcome: "failed",
			wantReason:  "explicit",
			wantExit:    1,
		},
		{
			name:        "fd3 fail + exit 0",
			in:          OutcomeInputs{Fd3Final: &Fd3Final{Kind: "fail", Message: "oops", Reason: "bad", Details: json.RawMessage(`{"k":1}`)}, ExitCode: 0},
			wantOutcome: "failed",
			wantReason:  "fail_then_zero_exit",
			wantExit:    0,
			wantMsg:     "oops",
			wantDetails: `{"k":1}`,
		},
		{
			name:        "fd3 fail without message + exit 0",
			in:          OutcomeInputs{Fd3Final: &Fd3Final{Kind: "fail", Details: json.RawMessage(`{"k":1}`)}, ExitCode: 0},
			wantOutcome: "failed",
			wantReason:  "fail_then_zero_exit",
			wantExit:    0,
			wantMsg:     "mission emitted fail but exited with code 0",
			wantDetails: `{"k":1}`,
		},
		{
			name:        "implicit success exit 0",
			in:          OutcomeInputs{ExitCode: 0},
			wantOutcome: "success",
		},
		{
			name:        "implicit failed nonzero exit",
			in:          OutcomeInputs{ExitCode: 5},
			wantOutcome: "failed",
			wantReason:  "no_event_nonzero_exit",
			wantExit:    5,
			wantMsg:     "process exited with code 5 without a success/fail event; see stderr",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Compute(tc.in)
			if got.Outcome != tc.wantOutcome {
				t.Errorf("Outcome=%q, want %q", got.Outcome, tc.wantOutcome)
			}
			if got.FailReason != tc.wantReason {
				t.Errorf("FailReason=%q, want %q", got.FailReason, tc.wantReason)
			}
			if got.ExitCode != tc.wantExit {
				t.Errorf("ExitCode=%d, want %d", got.ExitCode, tc.wantExit)
			}
			if got.Signal != tc.wantSignal {
				t.Errorf("Signal=%q, want %q", got.Signal, tc.wantSignal)
			}
			if string(got.Return) != tc.wantReturn {
				t.Errorf("Return=%q, want %q", string(got.Return), tc.wantReturn)
			}
			if got.DropReturn != tc.wantDrop {
				t.Errorf("DropReturn=%v, want %v", got.DropReturn, tc.wantDrop)
			}
			if got.FailMessage != tc.wantMsg {
				t.Errorf("FailMessage=%q, want %q", got.FailMessage, tc.wantMsg)
			}
			if string(got.FailDetails) != tc.wantDetails {
				t.Errorf("FailDetails=%s, want %s", got.FailDetails, tc.wantDetails)
			}
		})
	}
}

func TestComputePreservesFailMessageAndDetails(t *testing.T) {
	o := Compute(OutcomeInputs{
		Fd3Final: &Fd3Final{Kind: "fail", Reason: "bad", Message: "boom", Details: json.RawMessage(`{"k":"v"}`)},
		ExitCode: 1,
	})
	if o.FailMessage != "boom" {
		t.Errorf("FailMessage=%q, want boom", o.FailMessage)
	}
	if string(o.FailDetails) != `{"k":"v"}` {
		t.Errorf("FailDetails=%q, want {\"k\":\"v\"}", string(o.FailDetails))
	}
}

func TestComputeViolationCarriesMessageAndDetails(t *testing.T) {
	o := Compute(OutcomeInputs{
		Fd3Violations: []Fd3Violation{
			{Reason: "event_line_too_large", Message: "fd3 success event (line 1) is 2048 bytes", Details: json.RawMessage(`{"fd3_line":1}`)},
			{Reason: "event_protocol_error", Message: "second", Details: json.RawMessage(`{"fd3_line":2}`)},
		},
		Fd3Final: &Fd3Final{Kind: "success", Return: json.RawMessage(`{"a":1}`)},
		ExitCode: 0,
	})
	if o.Outcome != "failed" || o.FailReason != "event_line_too_large" {
		t.Fatalf("outcome=%s reason=%s, want failed/event_line_too_large", o.Outcome, o.FailReason)
	}
	if o.FailMessage != "fd3 success event (line 1) is 2048 bytes" {
		t.Errorf("FailMessage=%q", o.FailMessage)
	}
	if string(o.FailDetails) != `{"fd3_line":1}` {
		t.Errorf("FailDetails=%s", o.FailDetails)
	}
	if !o.DropReturn {
		t.Error("DropReturn=false, want true")
	}
}

func TestComputeDaemonClassifiedFailuresCarryMessage(t *testing.T) {
	inputs := []OutcomeInputs{
		{ExternalKill: KillTimeout, ExitCode: -1, Signal: "TERM"},
		{ExternalKill: KillForceDelete, ExitCode: -1, Signal: "TERM"},
		{ExternalKill: KillLaneRemoved, ExitCode: -1, Signal: "TERM"},
		{ExternalKill: KillDugdaleShutdown, ExitCode: -1, Signal: "TERM"},
		{ExternalKill: KillByAPI, ExitCode: -1, Signal: "TERM"},
		{OOMDetected: true, ExitCode: 255},
		{Signal: "KILL", ExitCode: -1},
		{Signal: "BUS", ExitCode: -1},
		{Signal: "ILL", ExitCode: -1},
		{Signal: "FPE", ExitCode: -1},
		{Signal: "TRAP", ExitCode: -1},
		{Signal: "ABRT", ExitCode: -1},
		{Signal: "34", ExitCode: -1},
		{Fd3Final: &Fd3Final{Kind: "success"}, ExitCode: 3},
		{Fd3Final: &Fd3Final{Kind: "fail"}, ExitCode: 0},
		{ExitCode: 2},
	}
	for _, in := range inputs {
		o := Compute(in)
		if o.Outcome == "success" {
			t.Errorf("%+v: unexpected success", in)
			continue
		}
		if o.FailMessage == "" {
			t.Errorf("%+v: outcome=%s reason=%s has an empty fail_message", in, o.Outcome, o.FailReason)
		}
	}
}

func TestFormatTimeoutMs(t *testing.T) {
	cases := []struct {
		ms   int64
		want string
	}{
		{200, "200ms"},
		{1500, "1.5s"},
		{30000, "30s"},
		{90000, "1m30s"},
		{600000, "10m"},
		{3600000, "1h"},
		{5400000, "1h30m"},
		{3661000, "1h1m1s"},
	}
	for _, tc := range cases {
		if got := formatTimeoutMs(tc.ms); got != tc.want {
			t.Errorf("formatTimeoutMs(%d)=%q, want %q", tc.ms, got, tc.want)
		}
	}
}
