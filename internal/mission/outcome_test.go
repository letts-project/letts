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
	}{
		{
			name:        "external timeout overrides fd3 success",
			in:          OutcomeInputs{ExternalKill: KillTimeout, Signal: "TERM", ExitCode: -1, Fd3Final: &Fd3Final{Kind: "success", Return: json.RawMessage(`{"x":1}`)}},
			wantOutcome: "timeout",
			wantSignal:  "TERM",
			wantExit:    -1,
		},
		{
			name:        "external force_delete + fd3 success",
			in:          OutcomeInputs{ExternalKill: KillForceDelete, Signal: "KILL", ExitCode: -1, Fd3Final: &Fd3Final{Kind: "success"}},
			wantOutcome: "killed",
			wantReason:  "force_delete",
			wantSignal:  "KILL",
			wantExit:    -1,
		},
		{
			name:        "external lane_removed",
			in:          OutcomeInputs{ExternalKill: KillLaneRemoved, Signal: "TERM", ExitCode: -1},
			wantOutcome: "killed",
			wantReason:  "lane_removed",
			wantSignal:  "TERM",
			wantExit:    -1,
		},
		{
			name:        "external dugdale_shutdown",
			in:          OutcomeInputs{ExternalKill: KillDugdaleShutdown, Signal: "TERM", ExitCode: -1},
			wantOutcome: "killed",
			wantReason:  "dugdale_shutdown",
			wantSignal:  "TERM",
			wantExit:    -1,
		},
		{
			name:        "external killed_by_api",
			in:          OutcomeInputs{ExternalKill: KillByAPI, Signal: "TERM", ExitCode: -1},
			wantOutcome: "killed",
			wantReason:  "killed_by_api",
			wantSignal:  "TERM",
			wantExit:    -1,
		},
		{
			name:        "OOM detected",
			in:          OutcomeInputs{OOMDetected: true, ExitCode: -1, Signal: "KILL"},
			wantOutcome: "oom",
			wantReason:  "php_memory_limit",
			wantSignal:  "KILL",
			wantExit:    -1,
		},
		{
			name:        "SIGKILL without OOM proof",
			in:          OutcomeInputs{Signal: "KILL", ExitCode: -1},
			wantOutcome: "killed",
			wantReason:  "unknown_sigkill",
			wantSignal:  "KILL",
			wantExit:    -1,
		},
		{
			name:        "SIGKILL with fd3 success but no OOM",
			in:          OutcomeInputs{Signal: "KILL", ExitCode: -1, Fd3Final: &Fd3Final{Kind: "success"}},
			wantOutcome: "killed",
			wantReason:  "unknown_sigkill",
			wantSignal:  "KILL",
			wantExit:    -1,
		},
		{
			name:        "SEGV without fd3 final",
			in:          OutcomeInputs{Signal: "SEGV", ExitCode: -1},
			wantOutcome: "failed",
			wantReason:  "segfault",
			wantSignal:  "SEGV",
			wantExit:    -1,
		},
		{
			name:        "violation event_line_too_large overrides fd3 success",
			in:          OutcomeInputs{Fd3Violations: []string{"event_line_too_large"}, Fd3Final: &Fd3Final{Kind: "success"}, ExitCode: 0},
			wantOutcome: "failed",
			wantReason:  "event_line_too_large",
			wantDrop:    true,
		},
		{
			name:        "violation event_protocol_error",
			in:          OutcomeInputs{Fd3Violations: []string{"event_protocol_error"}, ExitCode: 1},
			wantOutcome: "failed",
			wantReason:  "event_protocol_error",
			wantExit:    1,
			wantDrop:    true,
		},
		{
			name:        "violation duplicate_final_event",
			in:          OutcomeInputs{Fd3Violations: []string{"duplicate_final_event"}, Fd3Final: &Fd3Final{Kind: "success"}, ExitCode: 0},
			wantOutcome: "failed",
			wantReason:  "duplicate_final_event",
			wantDrop:    true,
		},
		{
			name:        "violation too_many_output_files",
			in:          OutcomeInputs{Fd3Violations: []string{"too_many_output_files"}, Fd3Final: &Fd3Final{Kind: "success"}, ExitCode: 0},
			wantOutcome: "failed",
			wantReason:  "too_many_output_files",
			wantDrop:    true,
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
		},
		{
			name:        "fd3 fail + exit nonzero with reason",
			in:          OutcomeInputs{Fd3Final: &Fd3Final{Kind: "fail", Reason: "bad_arg", Message: "oops"}, ExitCode: 1},
			wantOutcome: "failed",
			wantReason:  "bad_arg",
			wantExit:    1,
		},
		{
			name:        "fd3 fail + exit nonzero default reason",
			in:          OutcomeInputs{Fd3Final: &Fd3Final{Kind: "fail", Message: "oops"}, ExitCode: 2},
			wantOutcome: "failed",
			wantReason:  "explicit",
			wantExit:    2,
		},
		{
			name:        "fd3 fail + exit 0",
			in:          OutcomeInputs{Fd3Final: &Fd3Final{Kind: "fail", Message: "oops", Reason: "bad"}, ExitCode: 0},
			wantOutcome: "failed",
			wantReason:  "fail_then_zero_exit",
			wantExit:    0,
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
