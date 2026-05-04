package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestIntegrationStubsCompile is the scaffolding smoke test: spin up the
// stub server and confirm every route the CLI uses is registered and
// returns the expected baseline shape.
func TestIntegrationStubsCompile(t *testing.T) {
	s := newStubDugdale(t)
	if s.URL() == "" {
		t.Fatal("newStubDugdale returned empty URL")
	}

	// Touch a handful of routes to ensure the mux is wired.
	resp, err := http.Get(s.URL() + "/v1/dugdale")
	if err != nil {
		t.Fatalf("GET /v1/dugdale: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET /v1/dugdale status = %d, want 200", resp.StatusCode)
	}

	resp, err = http.Get(s.URL() + "/v1/admin/state")
	if err != nil {
		t.Fatalf("GET /v1/admin/state: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET /v1/admin/state status = %d, want 200", resp.StatusCode)
	}

	// Apply a tiny state and confirm the recorder captured it.
	body := strings.NewReader(`{"lanes":{"normal":{"concurrency":1}}}`)
	resp, err = http.Post(s.URL()+"/v1/admin/apply", "application/json", body)
	if err != nil {
		t.Fatalf("POST /v1/admin/apply: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("POST /v1/admin/apply status = %d, want 200", resp.StatusCode)
	}
	if got := s.AppliedState(); len(got) == 0 {
		t.Error("AppliedState() empty after apply")
	}

	// Pause and continue toggle the in-memory flag.
	resp, err = http.Post(s.URL()+"/v1/admin/lanes/normal/pause", "", nil)
	if err != nil {
		t.Fatalf("POST .../pause: %v", err)
	}
	_ = resp.Body.Close()
	if !s.IsPaused("normal") {
		t.Error("IsPaused(normal) = false after pause")
	}
	resp, err = http.Post(s.URL()+"/v1/admin/lanes/normal/continue", "", nil)
	if err != nil {
		t.Fatalf("POST .../continue: %v", err)
	}
	_ = resp.Body.Close()
	if s.IsPaused("normal") {
		t.Error("IsPaused(normal) = true after continue")
	}
}

// TestIntegrationE2EHappyPath exercises the full apply → dispatch → events →
// run → run --output-file flow against the in-process stub dugdale. Each step
// drives the real cobra root command via SetArgs / SetOut / SetErr so the
// flag parsing, RunE plumbing, and output rendering paths all execute in the
// same goroutine the user would.
//
// Step 6 (--output-file) pre-seeds the mission record and staging entry so
// the stub's GetMission returns an Outputs map pointing at a staging_id with
// the expected bytes; the default event sequence (queued → running → done
// success) still drives the followed stream identically.
func TestIntegrationE2EHappyPath(t *testing.T) {
	stub := newStubDugdale(t)

	// Minimal letts.yaml: one dugdale pointing at the stub, one lane.
	// Plain tokens trigger CheckPermissions, so write the file with 0600.
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "letts.yaml")
	yamlBody := fmt.Sprintf(`dugdales:
  - id: s1
    url: %s
    token: dispatch-token
    admin_token: admin-token
    lanes:
      normal:
        concurrency: 1
`, stub.URL())
	if err := os.WriteFile(cfgPath, []byte(yamlBody), 0o600); err != nil {
		t.Fatalf("write letts.yaml: %v", err)
	}

	// runCLI drives the assembled cobra root in-process and returns the
	// captured stdout/stderr buffers plus any RunE error. A fresh root is
	// built per call so persistent-flag state (--config) is not leaked
	// across invocations.
	runCLI := func(args ...string) (stdout, stderr string, err error) {
		root := newRootCmd()
		var outBuf, errBuf bytes.Buffer
		root.SetOut(&outBuf)
		root.SetErr(&errBuf)
		root.SetIn(bytes.NewReader(nil))
		root.SetContext(context.Background())
		// Prepend --config so every subcommand discovers the test yaml.
		full := append([]string{"--config", cfgPath}, args...)
		root.SetArgs(full)
		err = root.Execute()
		return outBuf.String(), errBuf.String(), err
	}

	// 1. apply: POST /v1/admin/apply with the merged state from letts.yaml.
	if _, _, err := runCLI("apply", "-f", cfgPath); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if got := stub.AppliedState(); len(got) == 0 {
		t.Fatal("apply did not record state on the stub")
	}

	// 2. dispatch: id\tstatus on stdout, mission_id recorded by the stub.
	dispatchOut, _, err := runCLI(
		"dispatch",
		"--host=s1",
		"--lane=normal",
		"--mission=Test",
		`--input={"k":1}`,
	)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	parts := strings.Split(strings.TrimSpace(dispatchOut), "\t")
	if len(parts) < 2 {
		t.Fatalf("dispatch output should be id\\tstatus, got %q", dispatchOut)
	}
	missionID := parts[0]
	if missionID == "" {
		t.Fatalf("dispatch printed empty mission_id (out=%q)", dispatchOut)
	}
	if parts[1] != "queued" {
		t.Errorf("dispatch status = %q, want queued", parts[1])
	}

	// 3. events --follow: NDJSON stream must include a done event.
	eventsOut, _, err := runCLI("events", missionID, "--host=s1", "--follow")
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	if !strings.Contains(eventsOut, `"event":"done"`) {
		t.Errorf("events output missing done event:\n%s", eventsOut)
	}

	// 4. run: dispatches, follows, and prints the return body to stdout.
	// Stub's default done event carries return={}, which renders as "{}".
	runOut, _, err := runCLI(
		"run",
		"--host=s1",
		"--lane=normal",
		"--mission=Test",
		`--input={"k":1}`,
	)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if strings.TrimSpace(runOut) == "" {
		t.Errorf("run printed no return value (stdout empty)")
	}

	// 5. run --output-file: pre-seed a known mission with Outputs map and
	// the matching staging bytes, then dispatch with --mission-id so the
	// CLI replays into that same row. After the done event, downloadOutputs
	// fetches the mission and pulls the staging bytes to outPath.
	outPath := filepath.Join(tmp, "out.bin")
	const knownMissionID = "01900000-0000-7000-8000-0000000000aa"
	const stagingID = "01900000-0000-7000-8000-0000000000bb"
	const stagedContent = "the output bytes"
	stub.SetStagingFile(&stubStagingFile{
		StagingID: stagingID,
		Bytes:     []byte(stagedContent),
	})
	stub.SetMission(&stubMission{
		MissionID: knownMissionID,
		Kind:      "mission",
		Lane:      "normal",
		Status:    "done",
		Outcome:   "success",
		Outputs: map[string]stubMissionFile{
			"result": {
				Role:      "result",
				StagingID: stagingID,
				// Real sha256 of stagedContent — run --output-file now verifies
				// the downloaded bytes against the done event's declared digest.
				Sha256: fmt.Sprintf("%x", sha256.Sum256([]byte(stagedContent))),
				Size:   int64(len(stagedContent)),
			},
		},
	})

	if _, _, err := runCLI(
		"run",
		"--host=s1",
		"--lane=normal",
		"--mission=Test",
		"--mission-id="+knownMissionID,
		"--input={}",
		"--output-file=result="+outPath,
	); err != nil {
		t.Fatalf("run --output-file: %v", err)
	}
	data, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read %s: %v", outPath, err)
	}
	if string(data) != stagedContent {
		t.Errorf("output-file contents = %q, want %q", string(data), stagedContent)
	}
}

// --- failure-path integration tests and exit-code mapping ---

// writeMultiHostYAML writes a letts.yaml with the given (id → URL) map and
// a single shared lane. Permissions are 0600 so CheckPermissions accepts the
// plain-text tokens (mirrors what the happy-path helper does).
//
// The map iteration order is non-deterministic, but the tests below don't
// rely on dugdale ordering — they pin --host explicitly or use by-id fan-out
// which races all hosts.
func writeMultiHostYAML(t *testing.T, dir string, hostURLs map[string]string) string {
	t.Helper()
	cfgPath := filepath.Join(dir, "letts.yaml")
	var b strings.Builder
	b.WriteString("dugdales:\n")
	for id, url := range hostURLs {
		fmt.Fprintf(&b, `  - id: %s
    url: %s
    token: dispatch-token
    admin_token: admin-token
    lanes:
      normal:
        concurrency: 1
`, id, url)
	}
	if err := os.WriteFile(cfgPath, []byte(b.String()), 0o600); err != nil {
		t.Fatalf("write letts.yaml: %v", err)
	}
	return cfgPath
}

// execCLI builds a fresh root cobra command, wires the test stdout/stderr,
// and executes with --config pointing at cfgPath. Returns captured streams
// and the RunE error so the caller can assert on text and exit-code mapping.
func execCLI(t *testing.T, cfgPath string, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	root := newRootCmd()
	var outBuf, errBuf bytes.Buffer
	root.SetOut(&outBuf)
	root.SetErr(&errBuf)
	root.SetIn(bytes.NewReader(nil))
	root.SetContext(context.Background())
	full := append([]string{"--config", cfgPath}, args...)
	root.SetArgs(full)
	err = root.Execute()
	return outBuf.String(), errBuf.String(), err
}

// TestIntegrationApplyMixedSuccessFailure: one host returns 409 on
// apply, the other succeeds. Aggregate output must list both per-host rows
// ([OK] and [FAIL]) and the command must return a non-nil error so main()
// maps it to exit 1 (generic failure — not BadUsage/Config/Auth).
func TestIntegrationApplyMixedSuccessFailure(t *testing.T) {
	stubA := newStubDugdale(t)
	stubB := newStubDugdale(t)
	stubB.SetHooks(stubHooks{
		ApplyStatus: http.StatusConflict,
		ApplyBody:   `{"error":"conflict","message":"in_flight"}`,
	})

	tmp := t.TempDir()
	cfgPath := writeMultiHostYAML(t, tmp, map[string]string{
		"s1": stubA.URL(),
		"s2": stubB.URL(),
	})

	out, errBuf, err := execCLI(t, cfgPath, "apply", "-f", cfgPath)
	if err == nil {
		t.Fatalf("expected error from apply when one host returns 409, got nil; out=%q err=%q", out, errBuf)
	}
	combined := out + errBuf
	// Apply's text output writes both per-host rows to stdout regardless of
	// the failure; the aggregate "apply failed on at least one dugdale" error
	// then propagates up to main(). Per-host strings use "—" (em-dash).
	if !strings.Contains(combined, "[OK]   s1") {
		t.Errorf("missing [OK] s1 line:\n%s", combined)
	}
	if !strings.Contains(combined, "[FAIL] s2") {
		t.Errorf("missing [FAIL] s2 line:\n%s", combined)
	}
	if mapErrorToExit(err) != exitFailure {
		t.Errorf("expected exit code %d, got %d (err=%v)", exitFailure, mapErrorToExit(err), err)
	}
}

// TestIntegrationDispatchNoLanesConfigured: dispatch stub returns
// 412 `no_lanes_configured`. The CLI must surface the error verbatim (no
// retry on explicit 412) and exit non-zero with code 1 (HTTPError is not a
// typed CLI error).
func TestIntegrationDispatchNoLanesConfigured(t *testing.T) {
	stub := newStubDugdale(t)
	stub.SetHooks(stubHooks{
		DispatchStatus: http.StatusPreconditionFailed,
		DispatchBody:   `{"error":"no_lanes_configured","message":"apply lanes first"}`,
	})

	tmp := t.TempDir()
	cfgPath := writeMultiHostYAML(t, tmp, map[string]string{"s1": stub.URL()})

	_, _, err := execCLI(t, cfgPath,
		"dispatch",
		"--host=s1",
		"--lane=normal",
		"--mission=X",
		"--input={}",
	)
	if err == nil {
		t.Fatal("expected error from dispatch on 412, got nil")
	}
	if !strings.Contains(err.Error(), "no_lanes_configured") {
		t.Errorf("error should mention no_lanes_configured, got %v", err)
	}
	if mapErrorToExit(err) != exitFailure {
		t.Errorf("expected exit code %d, got %d (err=%v)", exitFailure, mapErrorToExit(err), err)
	}
}

// TestIntegrationRunMissionFailed: a mission completes with
// outcome=failed and fail_message=boom. The CLI must return a non-nil error
// whose text starts with "mission failed:" so main() maps it to exit 1. The
// fail_message is rendered via the streamed [event] line on stderr in text
// mode (not embedded in the returned error to avoid double-printing).
func TestIntegrationRunMissionFailed(t *testing.T) {
	stub := newStubDugdale(t)
	const missionID = "01900000-0000-7000-8000-0000000000c1"
	// Pre-seed a mission row so the events handler treats it as registered.
	stub.SetMission(&stubMission{
		MissionID: missionID,
		Kind:      "mission",
		Lane:      "normal",
		Status:    "queued",
	})
	// Script a done event with outcome=failed; the run path returns the
	// "mission failed: <msg>" error after printing the result.
	stub.ScriptMission(missionID, []string{
		`{"seq":1,"event":"queued","mission_id":"` + missionID + `"}`,
		`{"seq":2,"event":"done","outcome":"failed","fail_message":"boom"}`,
	})

	tmp := t.TempDir()
	cfgPath := writeMultiHostYAML(t, tmp, map[string]string{"s1": stub.URL()})

	_, _, err := execCLI(t, cfgPath,
		"run",
		"--host=s1",
		"--lane=normal",
		"--mission=Test",
		"--mission-id="+missionID,
		"--input={}",
	)
	if err == nil {
		t.Fatal("expected error from run when outcome=failed, got nil")
	}
	if !strings.HasPrefix(err.Error(), "mission failed") {
		t.Errorf("error should start with 'mission failed', got %v", err)
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Errorf("error should contain fail_message 'boom', got %v", err)
	}
	if mapErrorToExit(err) != exitFailure {
		t.Errorf("expected exit code %d, got %d (err=%v)", exitFailure, mapErrorToExit(err), err)
	}
}

// TestIntegrationRootVerbosePrintsDebug verifies the global -v flag turns on
// debug diagnostics on stderr (which dugdale URL/scope a request resolves to).
func TestIntegrationRootVerbosePrintsDebug(t *testing.T) {
	stub := newStubDugdale(t)
	tmp := t.TempDir()
	cfgPath := writeMultiHostYAML(t, tmp, map[string]string{"s1": stub.URL()})

	_, errBuf, err := execCLI(t, cfgPath, "-v", "ctl", "lanes", "list", "--host=s1")
	if err != nil {
		t.Fatalf("ctl lanes list -v: %v (stderr=%q)", err, errBuf)
	}
	if !strings.Contains(errBuf, "debug:") {
		t.Errorf("expected -v to print debug diagnostics on stderr, got %q", errBuf)
	}
}

// TestIntegrationRunRootQuietSuppressesEvents verifies the global -q flag
// suppresses run's informational event tailing on stderr.
func TestIntegrationRunRootQuietSuppressesEvents(t *testing.T) {
	stub := newStubDugdale(t)
	tmp := t.TempDir()
	cfgPath := writeMultiHostYAML(t, tmp, map[string]string{"s1": stub.URL()})

	runOnce := func(id string, extra ...string) string {
		stub.SetMission(&stubMission{MissionID: id, Kind: "mission", Lane: "normal", Status: "queued"})
		stub.ScriptMission(id, []string{
			`{"seq":1,"event":"queued","mission_id":"` + id + `"}`,
			`{"seq":2,"event":"done","outcome":"success","return":{"ok":true}}`,
		})
		args := append(append([]string{}, extra...),
			"run", "--host=s1", "--lane=normal", "--mission=Test",
			"--mission-id="+id, "--input={}")
		_, errBuf, err := execCLI(t, cfgPath, args...)
		if err != nil {
			t.Fatalf("run %v: %v (stderr=%q)", extra, err, errBuf)
		}
		return errBuf
	}

	// Without -q the [event] tail lines appear (proves the assertion below is meaningful).
	if loud := runOnce("01900000-0000-7000-8000-0000000000e1"); !strings.Contains(loud, "[event]") {
		t.Fatalf("expected [event] lines without -q, got stderr=%q", loud)
	}
	// With root -q they are suppressed.
	if quiet := runOnce("01900000-0000-7000-8000-0000000000e2", "-q"); strings.Contains(quiet, "[event]") {
		t.Errorf("root -q should suppress [event] lines, got stderr=%q", quiet)
	}
}

// TestIntegrationRunWaitTimeout: stub holds the events stream open
// indefinitely. With --wait-timeout=50ms the CLI's context fires, StreamEvents
// returns ctx.DeadlineExceeded, and runOne converts it to *WaitTimeoutError
// which mapErrorToExit maps to 124.
func TestIntegrationRunWaitTimeout(t *testing.T) {
	stub := newStubDugdale(t)
	const missionID = "01900000-0000-7000-8000-0000000000d1"
	stub.SetMission(&stubMission{
		MissionID: missionID,
		Kind:      "mission",
		Lane:      "normal",
		Status:    "queued",
	})
	stub.SetHooks(stubHooks{HangEvents: true})

	tmp := t.TempDir()
	cfgPath := writeMultiHostYAML(t, tmp, map[string]string{"s1": stub.URL()})

	_, _, err := execCLI(t, cfgPath,
		"run",
		"--host=s1",
		"--lane=normal",
		"--mission=Test",
		"--mission-id="+missionID,
		"--input={}",
		"--wait-timeout=50ms",
	)
	if err == nil {
		t.Fatal("expected error from run when stream hangs past --wait-timeout, got nil")
	}
	var wt *WaitTimeoutError
	if !errors.As(err, &wt) {
		t.Errorf("expected *WaitTimeoutError, got %T: %v", err, err)
	}
	if mapErrorToExit(err) != exitWaitTimeout {
		t.Errorf("expected exit code %d, got %d (err=%v)", exitWaitTimeout, mapErrorToExit(err), err)
	}
}

// TestIntegrationByIDFanOut: `ctl missions show <id>` without --host
// fans out across all 3 configured dugdales. Two stubs return 404; the third
// owns the mission and returns 200. The CLI must succeed and render the
// 200's mission body (FanOutByID picks the single non-404 winner).
func TestIntegrationByIDFanOut(t *testing.T) {
	const missionID = "found-mission"

	stubA := newStubDugdale(t)
	stubB := newStubDugdale(t) // owns the mission
	stubC := newStubDugdale(t)

	// stubA and stubC: every GET /v1/missions/{id} responds 404.
	stubA.SetHooks(stubHooks{MissionGet404: true})
	stubC.SetHooks(stubHooks{MissionGet404: true})

	// stubB: pre-seed the mission row so GetMission returns 200.
	stubB.SetMission(&stubMission{
		MissionID: missionID,
		Kind:      "mission",
		Lane:      "normal",
		Status:    "done",
		Outcome:   "success",
	})

	tmp := t.TempDir()
	cfgPath := writeMultiHostYAML(t, tmp, map[string]string{
		"sa": stubA.URL(),
		"sb": stubB.URL(),
		"sc": stubC.URL(),
	})

	out, _, err := execCLI(t, cfgPath,
		"ctl", "missions", "show", missionID,
	)
	if err != nil {
		t.Fatalf("ctl missions show: %v", err)
	}
	if !strings.Contains(out, `"mission_id": "`+missionID+`"`) {
		t.Errorf("output should contain mission_id=%q, got:\n%s", missionID, out)
	}
}

// TestIntegrationDestructiveByIDConflictExecutesNothing: a destructive
// command without --host on an id that TWO dugdales own must refuse with the
// "multiple hosts" conflict — and, critically, must not have executed the
// mutation anywhere. (The old fan-out ran the mutation on every host first
// and only then reported the conflict, so a restart spawned one new mission
// per owning host.) Each verb gets fresh stubs; both rows must be byte-for-
// byte untouched afterwards.
func TestIntegrationDestructiveByIDConflictExecutesNothing(t *testing.T) {
	const missionID = "dup-mission"
	verbs := []struct {
		name string
		args []string
		kind string
	}{
		{"missions restart", []string{"ctl", "missions", "restart", missionID}, "mission"},
		{"missions kill", []string{"ctl", "missions", "kill", missionID}, "mission"},
		{"missions delete", []string{"ctl", "missions", "delete", missionID}, "mission"},
		// ctl exec kill/delete delegate to the same runners; prove the
		// delegation inherits locate-then-act for exec records too.
		{"exec kill", []string{"ctl", "exec", "kill", missionID}, "exec"},
		{"exec delete", []string{"ctl", "exec", "delete", missionID}, "exec"},
	}
	for _, v := range verbs {
		t.Run(v.name, func(t *testing.T) {
			stubA := newStubDugdale(t)
			stubB := newStubDugdale(t)
			row := &stubMission{
				MissionID: missionID, Kind: v.kind, Lane: "normal",
				Status: "done", Outcome: "failed",
			}
			stubA.SetMission(row)
			stubB.SetMission(row)

			tmp := t.TempDir()
			cfgPath := writeMultiHostYAML(t, tmp, map[string]string{
				"sa": stubA.URL(),
				"sb": stubB.URL(),
			})

			_, _, err := execCLI(t, cfgPath, v.args...)
			if err == nil {
				t.Fatalf("%v: expected conflict error, got nil", v.args)
			}
			if !strings.Contains(err.Error(), "multiple hosts") || !strings.Contains(err.Error(), "--host") {
				t.Errorf("err=%q want 'multiple hosts' conflict demanding --host", err.Error())
			}
			for name, s := range map[string]*stubDugdale{"sa": stubA, "sb": stubB} {
				if n := s.MissionCount(); n != 1 {
					t.Errorf("%s: mission count=%d want 1 (restart must not have created a row)", name, n)
				}
				got := s.MissionRow(missionID)
				if got == nil || got.Status != "done" || got.Outcome != "failed" {
					t.Errorf("%s: row mutated despite conflict: %+v", name, got)
				}
			}
		})
	}
}

// TestIntegrationDestructiveByIDSingleOwner: with exactly one owning host,
// the destructive fan-out locates it and the mutation lands there — end-to-
// end through the cobra command, config discovery and locate-then-act.
func TestIntegrationDestructiveByIDSingleOwner(t *testing.T) {
	const missionID = "solo-mission"
	owner := newStubDugdale(t)
	other := newStubDugdale(t)
	owner.SetMission(&stubMission{
		MissionID: missionID, Kind: "mission", Lane: "normal",
		Status: "done", Outcome: "failed",
	})

	tmp := t.TempDir()
	cfgPath := writeMultiHostYAML(t, tmp, map[string]string{
		"sa": owner.URL(),
		"sb": other.URL(),
	})

	out, _, err := execCLI(t, cfgPath, "ctl", "missions", "restart", missionID)
	if err != nil {
		t.Fatalf("ctl missions restart: %v", err)
	}
	if strings.TrimSpace(out) == "" {
		t.Error("restart should print the new mission id")
	}
	if n := owner.MissionCount(); n != 2 {
		t.Errorf("owner mission count=%d want 2 (original + restarted)", n)
	}
	if n := other.MissionCount(); n != 0 {
		t.Errorf("non-owner mission count=%d want 0", n)
	}
}

// TestIntegrationApplyJSONFailureExitsNonZero: `apply -o json` with one
// failing host must print the parseable per-host summary on stdout AND
// return the aggregate error (exit 1) — structured output is no excuse for
// a lying exit code.
func TestIntegrationApplyJSONFailureExitsNonZero(t *testing.T) {
	stubA := newStubDugdale(t)
	stubB := newStubDugdale(t)
	stubB.SetHooks(stubHooks{
		ApplyStatus: http.StatusConflict,
		ApplyBody:   `{"error":"conflict","message":"in_flight"}`,
	})

	tmp := t.TempDir()
	cfgPath := writeMultiHostYAML(t, tmp, map[string]string{
		"s1": stubA.URL(),
		"s2": stubB.URL(),
	})

	out, _, err := execCLI(t, cfgPath, "-o", "json", "apply", "-f", cfgPath)
	if err == nil {
		t.Fatalf("expected error from apply -o json with a failing host, got nil; out=%q", out)
	}
	if !strings.Contains(err.Error(), "apply failed on at least one dugdale") {
		t.Errorf("err=%q want the same aggregate text as the text path", err.Error())
	}
	if mapErrorToExit(err) != exitFailure {
		t.Errorf("exit=%d want %d", mapErrorToExit(err), exitFailure)
	}
	var rows []struct {
		Host string `json:"host"`
		OK   bool   `json:"ok"`
	}
	if jerr := json.Unmarshal([]byte(out), &rows); jerr != nil {
		t.Fatalf("stdout is not the JSON summary: %v (%q)", jerr, out)
	}
	okByHost := map[string]bool{}
	for _, r := range rows {
		okByHost[r.Host] = r.OK
	}
	if !okByHost["s1"] || okByHost["s2"] {
		t.Errorf("rows=%+v want s1 ok / s2 failed", rows)
	}
}

// TestIntegrationStagingUploadRetries: the stub's PUT /v1/staging/{id}
// returns 503 on the first attempt and 201 on the second. The sticky-retry
// path in doStagingPut (added alongside this test) replays the seekable file
// body and succeeds on attempt #2. The CLI must print "<id>\t<sha>\t<size>"
// and exit zero.
func TestIntegrationStagingUploadRetries(t *testing.T) {
	stub := newStubDugdale(t)
	stub.SetHooks(stubHooks{Put503Times: 1})

	// Small payload — file size doesn't matter for retry coverage, only that
	// the file handle is seekable so doStagingPut can rewind it.
	tmp := t.TempDir()
	payloadPath := filepath.Join(tmp, "payload.bin")
	const payload = "retry-me"
	if err := os.WriteFile(payloadPath, []byte(payload), 0o600); err != nil {
		t.Fatalf("write payload: %v", err)
	}

	cfgPath := writeMultiHostYAML(t, tmp, map[string]string{"s1": stub.URL()})

	out, errBuf, err := execCLI(t, cfgPath,
		"ctl", "staging", "upload", payloadPath,
		"--host=s1",
	)
	if err != nil {
		t.Fatalf("staging upload should succeed via sticky retry: err=%v stderr=%s", err, errBuf)
	}
	// Expected text output: "<staging_id>\t<sha256>\t<size>\n".
	parts := strings.Split(strings.TrimSpace(out), "\t")
	if len(parts) != 3 {
		t.Fatalf("output should be id\\tsha\\tsize, got %q", out)
	}
	if parts[0] == "" {
		t.Errorf("staging_id should not be empty, got %q", out)
	}
	if want := fmt.Sprintf("%d", len(payload)); parts[2] != want {
		t.Errorf("size = %q, want %q", parts[2], want)
	}
}
