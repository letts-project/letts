package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/spf13/cobra"

	"letts/pkg/lettsclient"
)

// TestPrefixedSinkSerialisesConcurrentWrites verifies the mutex inside
// prefixedSink prevents interleaved bytes on the shared writer when two
// per-host writers emit lines concurrently. With race detector enabled,
// any unsynchronised access also surfaces here.
func TestPrefixedSinkSerialisesConcurrentWrites(t *testing.T) {
	var buf bytes.Buffer
	sink := newPrefixedSink(&buf)
	w1 := sink.WriterFor("s1", "")
	w2 := sink.WriterFor("s2", "[stderr]")

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_, _ = w1.Write([]byte("hello\n"))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_, _ = w2.Write([]byte("world\n"))
		}
	}()
	wg.Wait()

	// All lines must be complete (no torn interleaving).
	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	for _, l := range lines {
		if !strings.HasPrefix(l, "[s1] hello") && !strings.HasPrefix(l, "[s2][stderr] world") {
			t.Errorf("torn line: %q", l)
		}
	}
	if len(lines) != 200 {
		t.Errorf("got %d lines, want 200", len(lines))
	}
}

// TestPrefixedSinkPartialLineBuffered verifies a writer that receives bytes
// without a trailing newline holds them back until a subsequent write
// completes the line. Models tail-stream behaviour where one chunk arrives
// mid-line and the next finishes it.
func TestPrefixedSinkPartialLineBuffered(t *testing.T) {
	var buf bytes.Buffer
	sink := newPrefixedSink(&buf)
	w := sink.WriterFor("s1", "")
	_, _ = w.Write([]byte("hello "))
	// No newline yet — nothing should appear on the underlying writer.
	if buf.Len() != 0 {
		t.Errorf("buf=%q, want empty", buf.String())
	}
	_, _ = w.Write([]byte("world\n"))
	if buf.String() != "[s1] hello world\n" {
		t.Errorf("buf=%q", buf.String())
	}
}

// intPtr is a test helper to take the address of an int literal so test
// authors can populate Event.ExitCode (*int) inline.
func intPtr(i int) *int { return &i }

// TestFanOutJSONAggregateWellFormed: two-host fan-out emits a single
// {group_id, results:[...]} object with one success row per host. Verifies
// the sum-type encoding: outcome/exit_code are present on success rows,
// the JSON parses cleanly, and group_id round-trips verbatim.
func TestFanOutJSONAggregateWellFormed(t *testing.T) {
	var buf bytes.Buffer
	results := []execFanOutResult{
		{Host: "s1", ExecID: "0192-aaa", DoneEv: &lettsclient.Event{Event: "done", Outcome: "success", ExitCode: intPtr(0), DurationMs: 100}, Stdout: []byte("hi")},
		{Host: "s2", ExecID: "0192-bbb", DoneEv: &lettsclient.Event{Event: "done", Outcome: "failed", ExitCode: intPtr(1), DurationMs: 50}, Stderr: []byte("oops")},
	}
	if err := writeExecFanOutJSON(&buf, "group-x", results); err != nil {
		t.Fatal(err)
	}
	var got struct {
		GroupID string           `json:"group_id"`
		Results []map[string]any `json:"results"`
	}
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, buf.String())
	}
	if got.GroupID != "group-x" || len(got.Results) != 2 {
		t.Errorf("group_id=%q results=%d", got.GroupID, len(got.Results))
	}
	if got.Results[0]["outcome"] != "success" {
		t.Errorf("res0 outcome=%v", got.Results[0]["outcome"])
	}
	if got.Results[1]["exit_code"] != float64(1) {
		t.Errorf("res1 exit_code=%v", got.Results[1]["exit_code"])
	}
}

// TestFanOutJSONIncludesTransportError: a mixed batch (one success and
// one transport error) emits two rows; the error row carries error tag
// "transport" and lacks the "outcome" field (sum-type mutual exclusion).
// Empty group_id encodes as JSON null so consumers can distinguish "no
// group" from a missing field.
func TestFanOutJSONIncludesTransportError(t *testing.T) {
	var buf bytes.Buffer
	results := []execFanOutResult{
		{Host: "s1", ExecID: "0192-aaa", DoneEv: &lettsclient.Event{Event: "done", Outcome: "success", ExitCode: intPtr(0)}},
		{Host: "s2", HasErr: true, Err: &ExecTransportError{Inner: errors.New("dial: connection refused")}},
	}
	if err := writeExecFanOutJSON(&buf, "", results); err != nil {
		t.Fatal(err)
	}
	var got struct {
		GroupID any              `json:"group_id"`
		Results []map[string]any `json:"results"`
	}
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, buf.String())
	}
	if got.GroupID != nil {
		t.Errorf("group_id=%v, want null", got.GroupID)
	}
	if len(got.Results) != 2 {
		t.Fatalf("results=%d", len(got.Results))
	}
	if got.Results[1]["error"] != "transport" {
		t.Errorf("res1.error=%v, want transport", got.Results[1]["error"])
	}
	if _, ok := got.Results[1]["outcome"]; ok {
		t.Errorf("res1.outcome must be absent on error row: %v", got.Results[1]["outcome"])
	}
}

// TestFanOutJSONUnauthorizedFromHTTPError verifies the 401 → "unauthorized"
// classification when the wrapped error is *lettsclient.HTTPError (not the
// legacy *AuthError type): 401 must surface as error="unauthorized" with
// http_status=401.
func TestFanOutJSONUnauthorizedFromHTTPError(t *testing.T) {
	var buf bytes.Buffer
	httpErr := &lettsclient.HTTPError{Status: 401, Code: "unauthorized", Message: "invalid bearer token"}
	results := []execFanOutResult{
		{Host: "s1", HasErr: true, Err: &ExecTransportError{Inner: httpErr}},
	}
	if err := writeExecFanOutJSON(&buf, "", results); err != nil {
		t.Fatal(err)
	}
	var got struct {
		Results []map[string]any `json:"results"`
	}
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	row := got.Results[0]
	if row["error"] != "unauthorized" {
		t.Errorf("error=%v, want unauthorized", row["error"])
	}
	if row["http_status"] != float64(401) {
		t.Errorf("http_status=%v, want 401", row["http_status"])
	}
	if row["error_code"] != "unauthorized" {
		t.Errorf("error_code=%v, want unauthorized", row["error_code"])
	}
}

// TestFanOutJSONTransportEnrichmentFromHTTPError verifies that a 500
// wrapped in *ExecTransportError surfaces with http_status+error_code
// populated from the inner HTTPError, classified as "transport".
func TestFanOutJSONTransportEnrichmentFromHTTPError(t *testing.T) {
	var buf bytes.Buffer
	httpErr := &lettsclient.HTTPError{Status: 500, Code: "internal", Message: "db down"}
	results := []execFanOutResult{
		{Host: "s1", HasErr: true, Err: &ExecTransportError{Inner: httpErr}},
	}
	_ = writeExecFanOutJSON(&buf, "", results)
	var got struct {
		Results []map[string]any `json:"results"`
	}
	_ = json.Unmarshal(buf.Bytes(), &got)
	row := got.Results[0]
	if row["error"] != "transport" {
		t.Errorf("error=%v, want transport", row["error"])
	}
	if row["http_status"] != float64(500) {
		t.Errorf("http_status=%v, want 500", row["http_status"])
	}
	if row["error_code"] != "internal" {
		t.Errorf("error_code=%v, want internal", row["error_code"])
	}
}

// TestExecFanOutNDJSONLineCount verifies the live ndjson stream for a
// 2-host run: each host emits queued/running/done lifecycle events plus a
// stdout output line, all interleaved on one ndjsonSink. The output must
// be exclusively well-formed JSON lines (each carrying a "host" field) so
// downstream consumers can decode without a lookahead. Lower bound 6 = 2
// hosts × 3 lifecycle events; tail output lines push the actual count
// higher, but the count itself is non-deterministic under -race.
func TestExecFanOutNDJSONLineCount(t *testing.T) {
	hs1 := newExecHostStub(t, "s1", execHostPlan{
		doneOutcome: "success", doneExitCode: 0, stdoutBytes: "hi-s1\n",
	})
	defer hs1.close()
	hs2 := newExecHostStub(t, "s2", execHostPlan{
		doneOutcome: "success", doneExitCode: 0, stdoutBytes: "hi-s2\n",
	})
	defer hs2.close()
	ac := stubExecMultiAppCtx(t, []*execHostStub{hs1, hs2})
	ef := &execFlags{lane: "light", host: "s1,s2", argv: []string{"x"}, outputFmt: "ndjson", outputBuffer: 4096}
	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	var so strings.Builder
	cmd.SetOut(&so)
	cmd.SetErr(&strings.Builder{})
	_ = runExec(cmd, ac, ef, FormatText)

	lines := strings.Split(strings.TrimRight(so.String(), "\n"), "\n")
	// At least: 2 hosts × (queued, running, done) events = 6 lifecycle lines.
	// stdout output lines push the count higher but are race-timing-dependent.
	if len(lines) < 6 {
		t.Errorf("got %d lines, want >=6:\n%s", len(lines), so.String())
	}
	for _, l := range lines {
		var m map[string]any
		if err := json.Unmarshal([]byte(l), &m); err != nil {
			t.Errorf("bad JSON line: %q (%v)", l, err)
			continue
		}
		if _, ok := m["host"]; !ok {
			t.Errorf("line missing host: %q", l)
		}
	}
}

// TestExecFanOutNDJSONErrorLine verifies that a failed dispatch (no done
// event reached) surfaces one terminating {host, event:"error", ...}
// envelope on the ndjson stream. The classification is sourced from the
// shared classifyExecErr helper, so transport / 401 / etc. round-trip
// the same as the json aggregate's classification.
func TestExecFanOutNDJSONErrorLine(t *testing.T) {
	hs := newExecHostStub(t, "s7", execHostPlan{dispatchStatus: 500, dispatchBody: `{}`})
	defer hs.close()
	ac := stubExecMultiAppCtx(t, []*execHostStub{hs})
	ef := &execFlags{lane: "light", host: "s7", argv: []string{"x"}, outputFmt: "ndjson", outputBuffer: 4096}
	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	var so strings.Builder
	cmd.SetOut(&so)
	cmd.SetErr(&strings.Builder{})
	_ = runExec(cmd, ac, ef, FormatText)

	if !strings.Contains(so.String(), `"event":"error"`) {
		t.Errorf("ndjson missing error envelope: %s", so.String())
	}
	if !strings.Contains(so.String(), `"host":"s7"`) {
		t.Errorf("ndjson error envelope missing host=s7: %s", so.String())
	}
}

// TestFanOutJSONStdoutTruncation: a success row with StdoutTruncated=true
// surfaces stdout_truncated:true in JSON so downstream consumers can
// detect lost bytes without scanning the payload for the inline marker.
func TestFanOutJSONStdoutTruncation(t *testing.T) {
	r := execFanOutResult{
		Host: "s1", DoneEv: &lettsclient.Event{Event: "done", Outcome: "success", ExitCode: intPtr(0)},
		Stdout:          []byte("partial\n...[truncated, more bytes lost (client-side)]"),
		StdoutTruncated: true,
	}
	var buf bytes.Buffer
	if err := writeExecFanOutJSON(&buf, "g", []execFanOutResult{r}); err != nil {
		t.Fatal(err)
	}
	var got struct {
		Results []map[string]any `json:"results"`
	}
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, buf.String())
	}
	if got.Results[0]["stdout_truncated"] != true {
		t.Errorf("stdout_truncated=%v", got.Results[0]["stdout_truncated"])
	}
}
