package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"letts/internal/ids"
	"letts/pkg/lettsconfig"
)

// TestExecFanOutHostListSameGroupID: two-host fan-out with no --group-id
// must auto-generate a single group_id shared by both dispatches, while
// each host receives a unique MissionID (Idempotency-Key).
func TestExecFanOutHostListSameGroupID(t *testing.T) {
	hs1 := newExecHostStub(t, "s1", execHostPlan{doneOutcome: "success", doneExitCode: 0})
	defer hs1.close()
	hs2 := newExecHostStub(t, "s2", execHostPlan{doneOutcome: "success", doneExitCode: 0})
	defer hs2.close()

	ac := stubExecMultiAppCtx(t, []*execHostStub{hs1, hs2})
	ef := &execFlags{lane: "light", host: "s1,s2", argv: []string{"uptime"}, outputFmt: "prefix"}
	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	var so, se strings.Builder
	cmd.SetOut(&so)
	cmd.SetErr(&se)
	_ = runExec(cmd, ac, ef, FormatText)

	if len(hs1.dispatched) != 1 || len(hs2.dispatched) != 1 {
		t.Fatalf("dispatch counts: s1=%d s2=%d", len(hs1.dispatched), len(hs2.dispatched))
	}
	g1, g2 := hs1.dispatched[0].GroupID, hs2.dispatched[0].GroupID
	if g1 == "" {
		t.Fatal("group_id empty for s1")
	}
	if g1 != g2 {
		t.Errorf("group_ids differ: s1=%q s2=%q", g1, g2)
	}
	m1, m2 := hs1.dispatched[0].MissionID, hs2.dispatched[0].MissionID
	if m1 == "" || m2 == "" {
		t.Fatalf("mission_id empty: s1=%q s2=%q", m1, m2)
	}
	if m1 == m2 {
		t.Errorf("mission_ids identical across hosts: %q (must be unique)", m1)
	}
}

// TestExecFanOutGroupIDOverride: --group-id passes verbatim to every host
// (user-supplied group_id wins over the auto-generated default).
func TestExecFanOutGroupIDOverride(t *testing.T) {
	hs1 := newExecHostStub(t, "s1", execHostPlan{doneOutcome: "success", doneExitCode: 0})
	defer hs1.close()
	hs2 := newExecHostStub(t, "s2", execHostPlan{doneOutcome: "success", doneExitCode: 0})
	defer hs2.close()

	ac := stubExecMultiAppCtx(t, []*execHostStub{hs1, hs2})
	ef := &execFlags{
		lane: "light", host: "s1,s2", argv: []string{"uptime"},
		outputFmt: "prefix", groupID: "custom-group-string",
	}
	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	var so, se strings.Builder
	cmd.SetOut(&so)
	cmd.SetErr(&se)
	_ = runExec(cmd, ac, ef, FormatText)

	for _, hs := range []*execHostStub{hs1, hs2} {
		if len(hs.dispatched) != 1 {
			t.Fatalf("%s dispatched %d, want 1", hs.id, len(hs.dispatched))
		}
		if hs.dispatched[0].GroupID != "custom-group-string" {
			t.Errorf("%s group_id=%q, want %q", hs.id, hs.dispatched[0].GroupID, "custom-group-string")
		}
	}
}

// TestExecFanOutRawRejected: --output=raw with N>1 hosts is a BadUsageError
// — raw is single-host only because there's no per-host disambiguation in
// the byte stream.
func TestExecFanOutRawRejected(t *testing.T) {
	hs1 := newExecHostStub(t, "s1", execHostPlan{doneOutcome: "success"})
	defer hs1.close()
	hs2 := newExecHostStub(t, "s2", execHostPlan{doneOutcome: "success"})
	defer hs2.close()
	ac := stubExecMultiAppCtx(t, []*execHostStub{hs1, hs2})
	ef := &execFlags{lane: "light", host: "s1,s2", argv: []string{"uptime"}, outputFmt: "raw"}
	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	var so, se strings.Builder
	cmd.SetOut(&so)
	cmd.SetErr(&se)
	err := runExec(cmd, ac, ef, FormatText)
	if mapErrorToExit(err) != 2 {
		t.Errorf("exit=%d, want 2", mapErrorToExit(err))
	}
	if err == nil || !strings.Contains(err.Error(), "raw") {
		t.Errorf("err=%v, want 'raw requires single host'", err)
	}
}

// TestExecFanOutMissionIDForbidden: --mission-id with N>1 hosts is rejected
// because a fan-out generates per-host mission_ids; user-supplied id would
// collide if applied to every host.
func TestExecFanOutMissionIDForbidden(t *testing.T) {
	hs1 := newExecHostStub(t, "s1", execHostPlan{doneOutcome: "success"})
	defer hs1.close()
	hs2 := newExecHostStub(t, "s2", execHostPlan{doneOutcome: "success"})
	defer hs2.close()
	ac := stubExecMultiAppCtx(t, []*execHostStub{hs1, hs2})
	ef := &execFlags{
		lane: "light", host: "s1,s2", argv: []string{"x"}, outputFmt: "prefix",
		missionID: "0192aaaa-0000-7000-8000-000000000001",
	}
	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	var so, se strings.Builder
	cmd.SetOut(&so)
	cmd.SetErr(&se)
	err := runExec(cmd, ac, ef, FormatText)
	if mapErrorToExit(err) != 2 {
		t.Errorf("exit=%d, want 2", mapErrorToExit(err))
	}
}

// TestExecFanOutPrefixLiveOutput verifies prefix mode wires through the live
// prefixedSink: per-host stdout lines arrive on cmd.OutOrStdout() tagged
// "[host] ..." while the run is in flight (not after post-aggregation).
// The fake hosts emit one stdout line each; both tags must appear.
func TestExecFanOutPrefixLiveOutput(t *testing.T) {
	hs1 := newExecHostStub(t, "s1", execHostPlan{
		doneOutcome: "success", doneExitCode: 0,
		stdoutBytes: "hello-s1\n",
	})
	defer hs1.close()
	hs2 := newExecHostStub(t, "s2", execHostPlan{
		doneOutcome: "success", doneExitCode: 0,
		stdoutBytes: "hello-s2\n",
	})
	defer hs2.close()

	ac := stubExecMultiAppCtx(t, []*execHostStub{hs1, hs2})
	ef := &execFlags{lane: "light", host: "s1,s2", argv: []string{"uptime"}, outputFmt: "prefix", outputBuffer: 4096}
	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	var so, se strings.Builder
	cmd.SetOut(&so)
	cmd.SetErr(&se)
	_ = runExec(cmd, ac, ef, FormatText)

	got := so.String()
	if !strings.Contains(got, "[s1] hello-s1") {
		t.Errorf("missing s1 line: %q", got)
	}
	if !strings.Contains(got, "[s2] hello-s2") {
		t.Errorf("missing s2 line: %q", got)
	}
}

// TestExecSingleHostJSONFormat verifies the single-host fan-out routing:
// a single-host run with --output=json routes through the fan-out
// machinery so the emitted payload has the same {results:[...]} shape as
// N>1 cases. Without the routing branch in runExec, single-host non-raw
// would either fall to runExecOne (which prints raw bytes, no envelope)
// or BadUsage.
func TestExecSingleHostJSONFormat(t *testing.T) {
	hs := newExecHostStub(t, "s1", execHostPlan{
		doneOutcome: "success", doneExitCode: 0, stdoutBytes: "ok\n",
	})
	defer hs.close()
	ac := stubExecAppCtx(t, hs)
	ef := &execFlags{lane: "light", host: "s1", argv: []string{"uptime"}, outputFmt: "json", outputBuffer: 4096}
	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	var so strings.Builder
	cmd.SetOut(&so)
	cmd.SetErr(&strings.Builder{})

	_ = runExec(cmd, ac, ef, FormatText)

	var got struct {
		Results []map[string]any `json:"results"`
	}
	if err := json.Unmarshal([]byte(so.String()), &got); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, so.String())
	}
	if len(got.Results) != 1 || got.Results[0]["host"] != "s1" {
		t.Errorf("results=%v", got.Results)
	}
}

// TestExecSingleHostJSONIncludesOriginalExitCode verifies the JSON payload
// preserves the raw exit_code from the done event even when the CLI's
// exit-code mapper collapses 124/125/255 into 125. Operators reading the
// JSON envelope must see the host's actual exit code, not the CLI's
// collision-avoidance mapping.
func TestExecSingleHostJSONIncludesOriginalExitCode(t *testing.T) {
	hs := newExecHostStub(t, "s1", execHostPlan{doneOutcome: "success", doneExitCode: 124})
	defer hs.close()
	ac := stubExecAppCtx(t, hs)
	ef := &execFlags{lane: "light", host: "s1", argv: []string{"x"}, outputFmt: "json", outputBuffer: 4096}
	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	var so strings.Builder
	cmd.SetOut(&so)
	cmd.SetErr(&strings.Builder{})
	err := runExec(cmd, ac, ef, FormatText)

	var got struct {
		Results []map[string]any `json:"results"`
	}
	if jerr := json.Unmarshal([]byte(so.String()), &got); jerr != nil {
		t.Fatalf("not JSON: %v\n%s", jerr, so.String())
	}
	if len(got.Results) == 0 {
		t.Fatalf("no results: %s", so.String())
	}
	if got.Results[0]["exit_code"] != float64(124) {
		t.Errorf("exit_code=%v, want 124 (raw passthrough in JSON)", got.Results[0]["exit_code"])
	}
	if mapErrorToExit(err) != 125 {
		t.Errorf("CLI exit=%d, want 125 (collision)", mapErrorToExit(err))
	}
}

// TestExecDetachMultiHostPrintsGroupID: with --detach across N>1 hosts and
// all dispatches succeeding, stdout must carry exactly the group_id (not
// any per-host exec_id) so callers can recover the whole batch. streamHang
// proves we don't wait on /events — the detach branch returns before the
// per-host goroutine would otherwise block reading events.
func TestExecDetachMultiHostPrintsGroupID(t *testing.T) {
	hs1 := newExecHostStub(t, "s1", execHostPlan{streamHang: true})
	defer hs1.close()
	hs2 := newExecHostStub(t, "s2", execHostPlan{streamHang: true})
	defer hs2.close()
	ac := stubExecMultiAppCtx(t, []*execHostStub{hs1, hs2})
	ef := &execFlags{lane: "light", host: "s1,s2", argv: []string{"x"}, outputFmt: "prefix", detach: true}
	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	var so strings.Builder
	cmd.SetOut(&so)
	cmd.SetErr(&strings.Builder{})
	err := runExec(cmd, ac, ef, FormatText)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	out := strings.TrimSpace(so.String())
	if !ids.ValidateUUIDv7(out) {
		t.Errorf("stdout=%q, want a single UUIDv7 group_id", out)
	}
	// Both hosts dispatched with same group_id:
	if hs1.dispatched[0].GroupID != out || hs2.dispatched[0].GroupID != out {
		t.Errorf("group_id mismatch: stdout=%q s1=%q s2=%q", out, hs1.dispatched[0].GroupID, hs2.dispatched[0].GroupID)
	}
}

// TestExecDetachMultiHostPartialFail: a partial dispatch failure still
// prints the group_id on stdout (the OK hosts are running and
// recoverable), prefixes each failed host with [FAIL] on stderr, and
// exits 255 so wrappers know not to trust the run as fully launched.
func TestExecDetachMultiHostPartialFail(t *testing.T) {
	hs1 := newExecHostStub(t, "s1", execHostPlan{streamHang: true})
	defer hs1.close()
	hs2 := newExecHostStub(t, "s2", execHostPlan{dispatchStatus: 500, dispatchBody: `{"error":"internal"}`})
	defer hs2.close()
	ac := stubExecMultiAppCtx(t, []*execHostStub{hs1, hs2})
	ef := &execFlags{lane: "light", host: "s1,s2", argv: []string{"x"}, outputFmt: "prefix", detach: true}
	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	var so, se strings.Builder
	cmd.SetOut(&so)
	cmd.SetErr(&se)
	err := runExec(cmd, ac, ef, FormatText)
	if mapErrorToExit(err) != 255 {
		t.Errorf("exit=%d, want 255", mapErrorToExit(err))
	}
	if strings.TrimSpace(so.String()) == "" {
		t.Error("stdout empty; expected group_id even on partial fail")
	}
	if !strings.Contains(se.String(), "[FAIL] s2") {
		t.Errorf("stderr missing [FAIL] s2: %q", se.String())
	}
}

// TestExecDetachMultiHostAllFail: when no host dispatches successfully,
// stdout stays empty (no group_id to recover — nothing is running) and
// every host shows up as a [FAIL] line on stderr. Exit 255.
func TestExecDetachMultiHostAllFail(t *testing.T) {
	hs1 := newExecHostStub(t, "s1", execHostPlan{dispatchStatus: 500, dispatchBody: `{}`})
	defer hs1.close()
	hs2 := newExecHostStub(t, "s2", execHostPlan{dispatchStatus: 500, dispatchBody: `{}`})
	defer hs2.close()
	ac := stubExecMultiAppCtx(t, []*execHostStub{hs1, hs2})
	ef := &execFlags{lane: "light", host: "s1,s2", argv: []string{"x"}, outputFmt: "prefix", detach: true}
	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	var so, se strings.Builder
	cmd.SetOut(&so)
	cmd.SetErr(&se)
	err := runExec(cmd, ac, ef, FormatText)
	if mapErrorToExit(err) != 255 {
		t.Errorf("exit=%d, want 255", mapErrorToExit(err))
	}
	if strings.TrimSpace(so.String()) != "" {
		t.Errorf("stdout=%q, want empty (no host OK)", so.String())
	}
	if !strings.Contains(se.String(), "[FAIL] s1") || !strings.Contains(se.String(), "[FAIL] s2") {
		t.Errorf("stderr missing both fails: %q", se.String())
	}
}

// TestExecScriptMultiHostUploadsPerDugdale is the fan-out counterpart:
// two staging-enabled dugdale stubs, single --script flag, each goroutine
// must independently call uploadOrReuse → each stub records exactly one
// stagingPut (dedupe is per-dugdale, not global). Both
// dispatched ExecRequests must carry a Script ref.
func TestExecScriptMultiHostUploadsPerDugdale(t *testing.T) {
	scriptPath := writeTempFile(t, "#!/bin/sh\necho hi\n")
	hs1 := newExecHostStubWithStaging(t, "s1", execHostPlan{
		doneOutcome: "success", doneExitCode: 0,
	})
	defer hs1.close()
	hs2 := newExecHostStubWithStaging(t, "s2", execHostPlan{
		doneOutcome: "success", doneExitCode: 0,
	})
	defer hs2.close()

	ac := stubExecMultiAppCtx(t, []*execHostStub{hs1, hs2})
	ef := &execFlags{
		lane: "light", host: "s1,s2",
		argv: []string{"bash", "$LETTS_SCRIPT"}, script: scriptPath,
		outputFmt: "prefix",
	}
	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	var so, se strings.Builder
	cmd.SetOut(&so)
	cmd.SetErr(&se)
	_ = runExec(cmd, ac, ef, FormatText)

	for _, hs := range []*execHostStub{hs1, hs2} {
		if len(hs.dispatched) != 1 {
			t.Fatalf("%s dispatched=%d, want 1", hs.id, len(hs.dispatched))
		}
		if hs.dispatched[0].Script == nil || hs.dispatched[0].Script.StagingID == "" {
			t.Errorf("%s: script ref missing: %+v", hs.id, hs.dispatched[0].Script)
		}
		if hs.stagingPuts != 1 {
			t.Errorf("%s stagingPuts=%d, want 1 (per-dugdale upload)", hs.id, hs.stagingPuts)
		}
	}
	// Per-dugdale staging IDs are minted independently (each host's goroutine
	// calls ids.NewUUIDv7), so the IDs MUST differ — guards against a future
	// regression where a single shared upload bypasses per-dugdale dedupe.
	if hs1.dispatched[0].Script.StagingID == hs2.dispatched[0].Script.StagingID {
		t.Errorf("staging_ids identical across hosts: %q (must be per-dugdale)",
			hs1.dispatched[0].Script.StagingID)
	}
}

// stubExecMultiAppCtx wires N fake dugdales (each its own httptest.Server)
// into a single appCtx via BaseURLForID — mirrors stubFanoutAppCtx in
// run_fanout_test.go but for exec scope (ExecToken set).
func stubExecMultiAppCtx(t *testing.T, stubs []*execHostStub) *appCtx {
	t.Helper()
	dugs := make([]lettsconfig.Dugdale, 0, len(stubs))
	baseURLs := map[string]string{}
	for _, hs := range stubs {
		dugs = append(dugs, lettsconfig.Dugdale{
			ID:        hs.id,
			Host:      "ignored",
			Labels:    []string{"prod"},
			Token:     "tok",
			ExecToken: "tok",
			Lanes:     map[string]lettsconfig.LaneCfg{"light": {Concurrency: 1}},
		})
		baseURLs[hs.id] = hs.srv.URL
	}
	return &appCtx{
		Config:       &lettsconfig.Config{Dugdales: dugs},
		Getenv:       func(string) (string, bool) { return "", false },
		BaseURLForID: baseURLs,
		clients:      map[clientKey]*hostClient{},
	}
}
