package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

// happyEventsHandler emits queued → running → progress → done(success, return={k:1}).
// Used by all format tests to exercise the same stream shape against each
// --output mode.
func happyEventsHandler(w http.ResponseWriter, _ *http.Request) {
	writeNDJSON(w,
		`{"seq":1,"event":"queued"}`,
		`{"seq":2,"event":"running","pid":42}`,
		`{"seq":3,"event":"progress","value":0.5,"message":"halfway"}`,
		`{"seq":4,"event":"done","outcome":"success","exit_code":0,"return":{"k":1},"duration_ms":123}`,
	)
}

// runFormatHarness invokes runCore against the shared run stub with the given
// flags and format, returning captured stdout/stderr buffers. Centralising the
// boilerplate keeps each test focused on assertions over output channels.
func runFormatHarness(t *testing.T, rf *runFlags, f Format) (stdout, stderr bytes.Buffer) {
	t.Helper()
	rs := newRunStub(t)
	t.Cleanup(rs.close)
	rs.eventsHandler = happyEventsHandler
	ac := stubRunAppCtx(t, rs)

	cmd := newRunCmd()
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetIn(bytes.NewReader(nil))
	cmd.SetContext(context.Background())

	if rf.route == "" {
		rf.route = "normal"
	}
	if rf.mission == "" {
		rf.mission = "Smoke"
	}
	if err := runCore(cmd, ac, rf, f); err != nil {
		t.Fatalf("runCore: %v", err)
	}
	return stdout, stderr
}

// TestRunOutputTextSeparatesChannels verifies text mode: stdout holds
// only the pretty-printed return JSON, while events surface on stderr with
// the [event]/[progress] prefixes.
func TestRunOutputTextSeparatesChannels(t *testing.T) {
	out, errBuf := runFormatHarness(t, &runFlags{}, FormatText)

	// stdout must be one valid JSON object equal to {"k":1}.
	var got map[string]any
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("stdout not JSON: %v body=%q", err, out.String())
	}
	if v, _ := got["k"].(float64); v != 1 {
		t.Errorf("stdout.k = %v, want 1 (body=%q)", got["k"], out.String())
	}
	// stdout must NOT carry event prefixes — those belong on stderr.
	if strings.Contains(out.String(), "[event]") || strings.Contains(out.String(), "[progress]") {
		t.Errorf("stdout leaked event prefixes: %q", out.String())
	}

	// stderr should contain queued / running pid=42 / progress / done success.
	se := errBuf.String()
	wants := []string{
		"[event] queued",
		"[event] running pid=42",
		"[progress] 0.50 halfway",
		"[event] done outcome=success",
	}
	for _, w := range wants {
		if !strings.Contains(se, w) {
			t.Errorf("stderr missing %q\nfull stderr:\n%s", w, se)
		}
	}
}

// TestRunOutputJSONStdoutSingleObject verifies json mode: stdout is one
// JSON object describing the terminal state; stderr is silent.
func TestRunOutputJSONStdoutSingleObject(t *testing.T) {
	out, errBuf := runFormatHarness(t, &runFlags{}, FormatJSON)

	// stdout should be exactly one JSON object.
	var got map[string]any
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("stdout not JSON: %v body=%q", err, out.String())
	}
	if outcome, _ := got["outcome"].(string); outcome != "success" {
		t.Errorf("outcome = %v, want success", got["outcome"])
	}
	// return field should round-trip {"k":1}; json.RawMessage decodes as an inner object.
	ret, ok := got["return"].(map[string]any)
	if !ok {
		t.Fatalf("return is %T %v, want object", got["return"], got["return"])
	}
	if v, _ := ret["k"].(float64); v != 1 {
		t.Errorf("return.k = %v, want 1", ret["k"])
	}
	if d, _ := got["duration_ms"].(float64); d != 123 {
		t.Errorf("duration_ms = %v, want 123", got["duration_ms"])
	}

	// stderr is suppressed in json mode (no --logs).
	if errBuf.Len() != 0 {
		t.Errorf("stderr not empty in json mode: %q", errBuf.String())
	}
}

// TestRunOutputNDJSONStdoutAllEvents verifies ndjson mode: every event
// reaches stdout as a raw NDJSON line, with stderr silent.
func TestRunOutputNDJSONStdoutAllEvents(t *testing.T) {
	out, errBuf := runFormatHarness(t, &runFlags{}, FormatNDJSON)

	// Collect non-empty lines.
	var lines []string
	for _, l := range strings.Split(strings.TrimRight(out.String(), "\n"), "\n") {
		if l != "" {
			lines = append(lines, l)
		}
	}
	if len(lines) != 4 {
		t.Fatalf("ndjson stdout lines = %d, want 4\n%s", len(lines), out.String())
	}
	wantEvents := []string{"queued", "running", "progress", "done"}
	for i, l := range lines {
		var ev map[string]any
		if err := json.Unmarshal([]byte(l), &ev); err != nil {
			t.Fatalf("line %d not JSON: %v %q", i, err, l)
		}
		if e, _ := ev["event"].(string); e != wantEvents[i] {
			t.Errorf("line %d event = %v, want %s", i, ev["event"], wantEvents[i])
		}
	}

	// stderr is suppressed in ndjson mode.
	if errBuf.Len() != 0 {
		t.Errorf("stderr not empty in ndjson mode: %q", errBuf.String())
	}
}

// TestRunNoProgressSkipsProgressLines: --no-progress drops [progress] lines but
// keeps the other [event] markers in text mode.
func TestRunNoProgressSkipsProgressLines(t *testing.T) {
	_, errBuf := runFormatHarness(t, &runFlags{noProgress: true}, FormatText)
	se := errBuf.String()
	if strings.Contains(se, "[progress]") {
		t.Errorf("stderr should not contain [progress] when --no-progress\n%s", se)
	}
	// Sanity check: other event lines still emit.
	if !strings.Contains(se, "[event] running pid=42") {
		t.Errorf("stderr missing running event when --no-progress\n%s", se)
	}
}

// TestRunQuietSuppressesStderr: --quiet silences all stderr in text mode.
func TestRunQuietSuppressesStderr(t *testing.T) {
	out, errBuf := runFormatHarness(t, &runFlags{quiet: true}, FormatText)
	if errBuf.Len() != 0 {
		t.Errorf("stderr not empty under --quiet: %q", errBuf.String())
	}
	// stdout must still receive the return JSON.
	if !strings.Contains(out.String(), `"k"`) {
		t.Errorf("stdout missing return under --quiet: %q", out.String())
	}
}
