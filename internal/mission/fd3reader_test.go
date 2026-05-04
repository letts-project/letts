package mission

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"
)

func pipePair(t *testing.T) (*os.File, *os.File) {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	return r, w
}

func collectProgress(ch <-chan ProgressEvent) []ProgressEvent {
	var out []ProgressEvent
	for ev := range ch {
		out = append(out, ev)
	}
	return out
}

func hasViolation(state *Fd3State, name string) bool {
	state.mu.Lock()
	defer state.mu.Unlock()
	for _, v := range state.Violations {
		if v == name {
			return true
		}
	}
	return false
}

func TestReadFd3SingleProgress(t *testing.T) {
	r := strings.NewReader(`{"event":"progress","value":0.5,"message":"halfway"}` + "\n")
	progressCh := make(chan ProgressEvent, 1)
	state := &Fd3State{}
	go ReadFd3(context.Background(), r, Fd3Limits{}, progressCh, state)

	got := collectProgress(progressCh)
	if len(got) != 1 {
		t.Fatalf("got %d events, want 1", len(got))
	}
	if got[0].Value == nil || *got[0].Value != 0.5 {
		t.Errorf("Value=%v, want 0.5", got[0].Value)
	}
	if got[0].Message != "halfway" {
		t.Errorf("Message=%q, want halfway", got[0].Message)
	}
}

func TestReadFd3Success(t *testing.T) {
	r := strings.NewReader(`{"event":"success","return":{"answer":42}}` + "\n")
	progressCh := make(chan ProgressEvent, 4)
	state := &Fd3State{}
	ReadFd3(context.Background(), r, Fd3Limits{}, progressCh, state)
	if state.Final == nil {
		t.Fatal("Final not set")
	}
	if state.Final.Kind != "success" {
		t.Errorf("Kind=%q, want success", state.Final.Kind)
	}
	if string(state.Final.Return) != `{"answer":42}` {
		t.Errorf("Return=%q, want object", string(state.Final.Return))
	}
}

// TestReadFd3SuccessRejectsScalarReturn verifies a success
// event with a non-object/non-null return (array, scalar) is a protocol
// violation, not a valid mission outcome.
func TestReadFd3SuccessRejectsScalarReturn(t *testing.T) {
	cases := []string{
		`{"event":"success","return":42}`,
		`{"event":"success","return":"oops"}`,
		`{"event":"success","return":[1,2,3]}`,
		`{"event":"success","return":true}`,
	}
	for _, raw := range cases {
		state := &Fd3State{}
		ReadFd3(context.Background(), strings.NewReader(raw+"\n"),
			Fd3Limits{}, make(chan ProgressEvent, 4), state)
		if state.Final != nil {
			t.Errorf("%q: Final set, want nil (protocol violation)", raw)
		}
		if !hasViolation(state, "event_protocol_error") {
			t.Errorf("%q: violations=%v, want event_protocol_error", raw, state.Violations)
		}
	}
}

// TestReadFd3SuccessAcceptsNullReturn verifies null is treated as a valid
// (omitted) return.
func TestReadFd3SuccessAcceptsNullReturn(t *testing.T) {
	state := &Fd3State{}
	ReadFd3(context.Background(), strings.NewReader(`{"event":"success","return":null}`+"\n"),
		Fd3Limits{}, make(chan ProgressEvent, 4), state)
	if state.Final == nil || state.Final.Kind != "success" {
		t.Errorf("Final=%+v, want success", state.Final)
	}
}

func TestReadFd3DuplicateFinalEvent(t *testing.T) {
	r := strings.NewReader(`{"event":"success","return":{"v":1}}` + "\n" +
		`{"event":"success","return":{"v":2}}` + "\n")
	progressCh := make(chan ProgressEvent, 4)
	state := &Fd3State{}
	ReadFd3(context.Background(), r, Fd3Limits{}, progressCh, state)
	if state.Final == nil || string(state.Final.Return) != `{"v":1}` {
		t.Fatalf("Final=%+v, want first event", state.Final)
	}
	if !hasViolation(state, "duplicate_final_event") {
		t.Errorf("violations=%v, want duplicate_final_event", state.Violations)
	}
}

func TestReadFd3OversizeLineDroppedAndContinues(t *testing.T) {
	big := strings.Repeat("x", 2000)
	payload := `{"event":"progress","msg":"` + big + `"}`
	next := `{"event":"success","return":{"after":"oversize"}}`
	r := strings.NewReader(payload + "\n" + next + "\n")
	progressCh := make(chan ProgressEvent, 4)
	state := &Fd3State{}
	ReadFd3(context.Background(), r, Fd3Limits{MaxEventLineSize: 256}, progressCh, state)
	if !hasViolation(state, "event_line_too_large") {
		t.Errorf("violations=%v, want event_line_too_large", state.Violations)
	}
	if state.Final == nil || state.Final.Kind != "success" {
		t.Errorf("expected success after oversize line, got %+v", state.Final)
	}
}

func TestReadFd3VeryLongLineExceedingBufferDrained(t *testing.T) {
	// Larger than bufio's 64KB buffer to exercise isPrefix=true loop.
	big := strings.Repeat("y", 200*1024)
	r := strings.NewReader(big + "\n" + `{"event":"success"}` + "\n")
	progressCh := make(chan ProgressEvent, 4)
	state := &Fd3State{}
	ReadFd3(context.Background(), r, Fd3Limits{MaxEventLineSize: 1024}, progressCh, state)
	if !hasViolation(state, "event_line_too_large") {
		t.Errorf("violations=%v, want event_line_too_large", state.Violations)
	}
	if state.Final == nil || state.Final.Kind != "success" {
		t.Errorf("Final=%+v, want success after drained giant line", state.Final)
	}
}

func TestReadFd3OutputFilesAccumulate(t *testing.T) {
	var sb strings.Builder
	sb.WriteString(`{"event":"output_file","key":"a"}` + "\n")
	sb.WriteString(`{"event":"output_file","key":"b"}` + "\n")
	sb.WriteString(`{"event":"output_file","key":"a"}` + "\n") // dup ignored
	state := &Fd3State{}
	ReadFd3(context.Background(), strings.NewReader(sb.String()),
		Fd3Limits{MaxOutputFilesPerMsn: 32}, make(chan ProgressEvent, 4), state)
	if len(state.OutputFiles) != 2 {
		t.Errorf("OutputFiles=%d, want 2", len(state.OutputFiles))
	}
	if _, ok := state.OutputFiles["a"]; !ok {
		t.Error("missing key a")
	}
	if _, ok := state.OutputFiles["b"]; !ok {
		t.Error("missing key b")
	}
}

// TestReadFd3OutputFileRejectsInvalidKey enforces
// "key (string, regex ^[A-Za-z_][A-Za-z0-9_]{0,63}$)". The previous impl
// accepted arbitrary strings — empty, "..", slash-containing paths,
// reserved __ prefix — silently storing them. That bypassed the v1
// "no subdirectories" output-path contract: `openat(work/out,
// "a/b", O_NOFOLLOW)` follows through intermediate directories because
// O_NOFOLLOW only inspects the final path component, and the slash
// propagates into LETTS_IN_<role>=... env on downstream missions.
func TestReadFd3OutputFileRejectsInvalidKey(t *testing.T) {
	bad := []string{
		"",    // empty
		"..",  // dotdot
		"a/b", // slash → subdir bypass
		"-leading-dash",
		"with space",
		"__reserved", // reserved internal prefix
	}
	for _, k := range bad {
		t.Run("key="+k, func(t *testing.T) {
			line := []byte(`{"event":"output_file","key":` + jsonString(k) + `}` + "\n")
			state := &Fd3State{}
			ReadFd3(context.Background(), strings.NewReader(string(line)),
				Fd3Limits{MaxOutputFilesPerMsn: 32}, make(chan ProgressEvent, 4), state)
			if !hasViolation(state, "event_protocol_error") {
				t.Errorf("key %q: violations=%v, want event_protocol_error", k, state.Violations)
			}
			if _, ok := state.OutputFiles[k]; ok {
				t.Errorf("key %q stored despite validation failure", k)
			}
		})
	}
}

// jsonString encodes s as a JSON string literal (with quotes) — small
// helper for the test above so we can pass an empty string without
// hand-escaping.
func jsonString(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

func TestReadFd3TooManyOutputFiles(t *testing.T) {
	var sb strings.Builder
	sb.WriteString(`{"event":"output_file","key":"a"}` + "\n")
	sb.WriteString(`{"event":"output_file","key":"b"}` + "\n")
	sb.WriteString(`{"event":"output_file","key":"c"}` + "\n") // exceeds cap=2
	state := &Fd3State{}
	ReadFd3(context.Background(), strings.NewReader(sb.String()),
		Fd3Limits{MaxOutputFilesPerMsn: 2}, make(chan ProgressEvent, 4), state)
	if !hasViolation(state, "too_many_output_files") {
		t.Errorf("violations=%v, want too_many_output_files", state.Violations)
	}
	if len(state.OutputFiles) != 2 {
		t.Errorf("OutputFiles=%d, want 2 (extra rejected)", len(state.OutputFiles))
	}
}

// TestReadFd3ViolationsBoundedUnderGarbageFlood: a mission that accidentally
// pipes a data stream into fd 3 produces one violation per newline; the
// recorded list must stay capped so the reader is O(1) memory for the
// mission's lifetime. The reader keeps draining past the cap (the trailing
// final is still parsed) and the outcome still classifies as a protocol
// error from the earliest entries. The duplicate-final append goes through
// the same cap, so the second success line must not grow the slice either.
func TestReadFd3ViolationsBoundedUnderGarbageFlood(t *testing.T) {
	var sb strings.Builder
	for i := 0; i < maxRecordedViolations+40; i++ {
		sb.WriteString("definitely not json\n")
	}
	sb.WriteString(`{"event":"success"}` + "\n")
	sb.WriteString(`{"event":"success"}` + "\n") // duplicate final — capped too
	state := &Fd3State{}
	ReadFd3(context.Background(), strings.NewReader(sb.String()),
		Fd3Limits{}, make(chan ProgressEvent, 4), state)
	if len(state.Violations) != maxRecordedViolations {
		t.Errorf("len(Violations)=%d, want %d (cap)", len(state.Violations), maxRecordedViolations)
	}
	if state.Final == nil || state.Final.Kind != "success" {
		t.Errorf("Final=%+v, want success (reader must stay alive past the cap)", state.Final)
	}
	o := Compute(OutcomeInputs{ExitCode: 0, Fd3Final: state.Final, Fd3Violations: state.Violations})
	if o.Outcome != "failed" || o.FailReason != "event_protocol_error" {
		t.Errorf("outcome=%s reason=%s, want failed/event_protocol_error", o.Outcome, o.FailReason)
	}
}

// TestReadFd3FailRejectsNonObjectDetails enforces the fail.details schema:
// details is an optional OBJECT (or null) per the fd3 protocol, and
// downstream consumers type fail_details as a nullable map. A scalar/array
// must classify as a protocol violation instead of propagating into the
// public done event and the DB.
func TestReadFd3FailRejectsNonObjectDetails(t *testing.T) {
	cases := []string{
		`{"event":"fail","message":"x","details":42}`,
		`{"event":"fail","message":"x","details":"oops"}`,
		`{"event":"fail","message":"x","details":[1,2,3]}`,
		`{"event":"fail","message":"x","details":true}`,
	}
	for _, raw := range cases {
		state := &Fd3State{}
		ReadFd3(context.Background(), strings.NewReader(raw+"\n"),
			Fd3Limits{}, make(chan ProgressEvent, 4), state)
		if state.Final != nil {
			t.Errorf("%q: Final set, want nil (protocol violation)", raw)
		}
		if !hasViolation(state, "event_protocol_error") {
			t.Errorf("%q: violations=%v, want event_protocol_error", raw, state.Violations)
		}
		o := Compute(OutcomeInputs{ExitCode: 1, Fd3Final: state.Final, Fd3Violations: state.Violations})
		if o.Outcome != "failed" || o.FailReason != "event_protocol_error" {
			t.Errorf("%q: outcome=%s/%s, want failed/event_protocol_error", raw, o.Outcome, o.FailReason)
		}
	}
}

// TestReadFd3FailAcceptsObjectAndOmittedDetails pins the accepted shapes:
// object, explicit null, and omitted details all produce a fail final, same
// as before the schema check existed.
func TestReadFd3FailAcceptsObjectAndOmittedDetails(t *testing.T) {
	cases := []struct {
		name        string
		raw         string
		wantDetails string // "" means don't inspect (null/omitted encodings differ)
	}{
		{"object", `{"event":"fail","message":"x","reason":"r","details":{"k":"v"}}`, `{"k":"v"}`},
		{"null", `{"event":"fail","message":"x","reason":"r","details":null}`, ""},
		{"omitted", `{"event":"fail","message":"x","reason":"r"}`, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state := &Fd3State{}
			ReadFd3(context.Background(), strings.NewReader(tc.raw+"\n"),
				Fd3Limits{}, make(chan ProgressEvent, 4), state)
			if state.Final == nil || state.Final.Kind != "fail" {
				t.Fatalf("Final=%+v, want fail", state.Final)
			}
			if state.Final.Message != "x" || state.Final.Reason != "r" {
				t.Errorf("Message=%q Reason=%q", state.Final.Message, state.Final.Reason)
			}
			if tc.wantDetails != "" && string(state.Final.Details) != tc.wantDetails {
				t.Errorf("Details=%q, want %q", string(state.Final.Details), tc.wantDetails)
			}
			if hasViolation(state, "event_protocol_error") {
				t.Errorf("unexpected violation: %v", state.Violations)
			}
		})
	}
}

// TestReadFd3FinalRejectsWrongTypedField: a final event whose typed field
// fails to unmarshal (exit_code as a string, message as a number) is a
// malformed final and must classify exactly like other schema violations —
// never yield a half-populated final.
func TestReadFd3FinalRejectsWrongTypedField(t *testing.T) {
	cases := []string{
		`{"event":"fail","message":"x","exit_code":"abc"}`,
		`{"event":"fail","message":123}`,
	}
	for _, raw := range cases {
		state := &Fd3State{}
		ReadFd3(context.Background(), strings.NewReader(raw+"\n"),
			Fd3Limits{}, make(chan ProgressEvent, 4), state)
		if state.Final != nil {
			t.Errorf("%q: Final=%+v, want nil (half-populated final forbidden)", raw, state.Final)
		}
		if !hasViolation(state, "event_protocol_error") {
			t.Errorf("%q: violations=%v, want event_protocol_error", raw, state.Violations)
		}
	}
}

func TestReadFd3InvalidJSONIsNonTerminal(t *testing.T) {
	r := strings.NewReader("notjson\n" + `{"event":"success"}` + "\n")
	state := &Fd3State{}
	ReadFd3(context.Background(), r, Fd3Limits{}, make(chan ProgressEvent, 4), state)
	if !hasViolation(state, "event_protocol_error") {
		t.Errorf("violations=%v, want event_protocol_error", state.Violations)
	}
	if state.Final == nil || state.Final.Kind != "success" {
		t.Errorf("expected success after invalid JSON, got %+v", state.Final)
	}
}

func TestReadFd3UnknownEventIsProtocolError(t *testing.T) {
	r := strings.NewReader(`{"event":"weird"}` + "\n")
	state := &Fd3State{}
	ReadFd3(context.Background(), r, Fd3Limits{}, make(chan ProgressEvent, 4), state)
	if !hasViolation(state, "event_protocol_error") {
		t.Errorf("violations=%v, want event_protocol_error", state.Violations)
	}
}

func TestReadFd3ProgressDropOnFullChannel(t *testing.T) {
	r := strings.NewReader(`{"event":"progress","value":0.1}` + "\n" +
		`{"event":"progress","value":0.2}` + "\n")
	progressCh := make(chan ProgressEvent) // unbuffered, no consumer
	state := &Fd3State{}
	ReadFd3(context.Background(), r, Fd3Limits{}, progressCh, state)
	state.mu.Lock()
	drops := state.ProgressDrops
	state.mu.Unlock()
	if drops != 2 {
		t.Errorf("ProgressDrops=%d, want 2", drops)
	}
}

func TestReadFd3FailEvent(t *testing.T) {
	r := strings.NewReader(`{"event":"fail","message":"oops","reason":"bad","exit_code":7}` + "\n")
	state := &Fd3State{}
	ReadFd3(context.Background(), r, Fd3Limits{}, make(chan ProgressEvent, 4), state)
	if state.Final == nil || state.Final.Kind != "fail" {
		t.Fatalf("Final=%+v, want fail", state.Final)
	}
	if state.Final.Message != "oops" {
		t.Errorf("Message=%q, want oops", state.Final.Message)
	}
	if state.Final.Reason != "bad" {
		t.Errorf("Reason=%q, want bad", state.Final.Reason)
	}
	if state.Final.ExitHint != 7 {
		t.Errorf("ExitHint=%d, want 7", state.Final.ExitHint)
	}
}

func TestReadFd3FailDefaultExitHint(t *testing.T) {
	r := strings.NewReader(`{"event":"fail","message":"x"}` + "\n")
	state := &Fd3State{}
	ReadFd3(context.Background(), r, Fd3Limits{}, make(chan ProgressEvent, 4), state)
	if state.Final == nil {
		t.Fatal("Final not set")
	}
	if state.Final.ExitHint != 1 {
		t.Errorf("ExitHint=%d, want 1 (default)", state.Final.ExitHint)
	}
}

func TestReadFd3ContextCancelExitsCleanly(t *testing.T) {
	pr, pw := pipePair(t)
	defer func() { _ = pw.Close() }()
	defer func() { _ = pr.Close() }()
	progressCh := make(chan ProgressEvent, 4)
	state := &Fd3State{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		ReadFd3(ctx, pr, Fd3Limits{}, progressCh, state)
		close(done)
	}()
	// Write one line, then cancel before close.
	_, _ = pw.Write([]byte(`{"event":"progress","value":0.1}` + "\n"))
	// Drain the one event.
	<-progressCh
	cancel()
	_ = pw.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("ReadFd3 didn't return after cancel and EOF")
	}
}
