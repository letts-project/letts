package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"letts/pkg/lettsconfig"
)

// runStub spans both /v1/dispatch and the events stream. The events handler is
// pluggable so per-test we can choose: happy/failed/abnormal/hang.
type runStub struct {
	srv           *httptest.Server
	dispatchCalls atomic.Int64
	eventsCalls   atomic.Int64
	eventsHandler func(http.ResponseWriter, *http.Request)
}

func newRunStub(t *testing.T) *runStub {
	t.Helper()
	rs := &runStub{}
	rs.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "POST" && r.URL.Path == "/v1/dispatch":
			rs.dispatchCalls.Add(1)
			mid := r.Header.Get("Idempotency-Key")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			_, _ = io.WriteString(w, `{"mission_id":"`+mid+`","status":"queued"}`)
		case r.Method == "GET" && strings.HasSuffix(r.URL.Path, "/events"):
			rs.eventsCalls.Add(1)
			if rs.eventsHandler == nil {
				t.Errorf("events handler not set")
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			rs.eventsHandler(w, r)
		case r.Method == "GET" && strings.HasSuffix(r.URL.Path, "/output"):
			// `letts run` text-mode spawns output-tail goroutines that
			// GET /v1/missions/{id}/output?stream=*. Tests that don't care
			// about live tail just 404 — tailMissionStream silently
			// swallows the error, mission progress is unaffected.
			w.WriteHeader(http.StatusNotFound)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	return rs
}

func (rs *runStub) close() { rs.srv.Close() }

// stubRunAppCtx wires one dugdale at rs.srv with one "normal" lane.
func stubRunAppCtx(t *testing.T, rs *runStub) *appCtx {
	t.Helper()
	return &appCtx{
		Config: &lettsconfig.Config{
			Dugdales: []lettsconfig.Dugdale{
				{ID: "s1", Host: "ignored", Token: "tok",
					Lanes: map[string]lettsconfig.LaneCfg{"normal": {Concurrency: 1}}},
			},
			Routes: map[string]lettsconfig.Route{
				"normal": {Host: "s1", Lane: "normal"},
			},
		},
		Getenv:       func(k string) (string, bool) { return "", false },
		BaseURLForID: map[string]string{"s1": rs.srv.URL},
		clients:      map[clientKey]*hostClient{},
	}
}

// writeNDJSON writes one event per line; flushes between lines so the client
// scanner can see them progressively if needed.
func writeNDJSON(w http.ResponseWriter, lines ...string) {
	w.Header().Set("Content-Type", "application/x-ndjson")
	flusher, _ := w.(http.Flusher)
	for _, l := range lines {
		_, _ = io.WriteString(w, l+"\n")
		if flusher != nil {
			flusher.Flush()
		}
	}
}

// TestRunHappyPath: dispatch → queued/running/done(success, return={k:1}).
// runCore must return nil and write the JSON return body to stdout.
func TestRunHappyPath(t *testing.T) {
	rs := newRunStub(t)
	defer rs.close()
	rs.eventsHandler = func(w http.ResponseWriter, r *http.Request) {
		writeNDJSON(w,
			`{"seq":1,"event":"queued"}`,
			`{"seq":2,"event":"running","pid":42}`,
			`{"seq":3,"event":"done","outcome":"success","exit_code":0,"return":{"k":1}}`,
		)
	}
	ac := stubRunAppCtx(t, rs)

	cmd := newRunCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(io.Discard)
	cmd.SetIn(bytes.NewReader(nil))
	cmd.SetContext(context.Background())

	rf := &runFlags{route: "normal", mission: "Smoke"}
	if err := runCore(cmd, ac, rf, FormatText); err != nil {
		t.Fatalf("runCore: %v", err)
	}
	if rs.dispatchCalls.Load() != 1 {
		t.Errorf("dispatch calls = %d, want 1", rs.dispatchCalls.Load())
	}
	if rs.eventsCalls.Load() < 1 {
		t.Errorf("events calls = %d, want >=1", rs.eventsCalls.Load())
	}
	// The return body must surface on stdout as JSON.
	var got map[string]any
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("stdout not JSON: %v body=%q", err, out.String())
	}
	if v, _ := got["k"].(float64); v != 1 {
		t.Errorf("stdout.k = %v, want 1 (body=%q)", got["k"], out.String())
	}
}

// TestRunMissionFailed: done outcome=failed → runCore returns an error whose
// message includes "mission failed". mapErrorToExit on that error → exit 1.
func TestRunMissionFailed(t *testing.T) {
	rs := newRunStub(t)
	defer rs.close()
	rs.eventsHandler = func(w http.ResponseWriter, r *http.Request) {
		writeNDJSON(w,
			`{"seq":1,"event":"queued"}`,
			`{"seq":2,"event":"done","outcome":"failed","exit_code":1,"fail_message":"boom"}`,
		)
	}
	ac := stubRunAppCtx(t, rs)

	cmd := newRunCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetIn(bytes.NewReader(nil))
	cmd.SetContext(context.Background())

	rf := &runFlags{route: "normal", mission: "M"}
	err := runCore(cmd, ac, rf, FormatText)
	if err == nil {
		t.Fatal("expected error for failed outcome")
	}
	if !strings.Contains(err.Error(), "mission failed") {
		t.Errorf("error = %q, want contains 'mission failed'", err.Error())
	}
	// Must not map to one of the typed-error exit codes; falls through to 1.
	if got := mapErrorToExit(err); got != exitFailure {
		t.Errorf("exit code = %d, want %d", got, exitFailure)
	}
}

// TestRunMissionAbnormal: outcome=killed → MissionAbnormalError (exit 125).
func TestRunMissionAbnormal(t *testing.T) {
	for _, outcome := range []string{"killed", "timeout", "oom", "crashed", "lost"} {
		t.Run(outcome, func(t *testing.T) {
			rs := newRunStub(t)
			defer rs.close()
			body := `{"seq":1,"event":"done","outcome":"` + outcome + `"}`
			rs.eventsHandler = func(w http.ResponseWriter, r *http.Request) {
				writeNDJSON(w, body)
			}
			ac := stubRunAppCtx(t, rs)

			cmd := newRunCmd()
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)
			cmd.SetIn(bytes.NewReader(nil))
			cmd.SetContext(context.Background())

			rf := &runFlags{route: "normal", mission: "M"}
			err := runCore(cmd, ac, rf, FormatText)
			if err == nil {
				t.Fatalf("expected error for outcome=%s", outcome)
			}
			var ma *MissionAbnormalError
			if !errors.As(err, &ma) {
				t.Fatalf("got %T %v, want MissionAbnormalError", err, err)
			}
			if ma.Outcome != outcome {
				t.Errorf("Outcome = %q, want %q", ma.Outcome, outcome)
			}
			if got := mapErrorToExit(err); got != exitMissionAbnormal {
				t.Errorf("exit code = %d, want %d", got, exitMissionAbnormal)
			}
		})
	}
}

// TestRunWaitTimeout: --wait-timeout=10ms with a hanging events server →
// WaitTimeoutError (exit 124).
func TestRunWaitTimeout(t *testing.T) {
	rs := newRunStub(t)
	defer rs.close()
	hangDone := make(chan struct{})
	t.Cleanup(func() { close(hangDone) })
	rs.eventsHandler = func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		// Block the handler until the test ends so the client can't read a done.
		select {
		case <-hangDone:
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	}
	ac := stubRunAppCtx(t, rs)

	cmd := newRunCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetIn(bytes.NewReader(nil))
	cmd.SetContext(context.Background())

	rf := &runFlags{route: "normal", mission: "M", waitTimeout: "10ms"}
	err := runCore(cmd, ac, rf, FormatText)
	if err == nil {
		t.Fatal("expected WaitTimeoutError")
	}
	var wt *WaitTimeoutError
	if !errors.As(err, &wt) {
		t.Fatalf("got %T %v, want WaitTimeoutError", err, err)
	}
	if got := mapErrorToExit(err); got != exitWaitTimeout {
		t.Errorf("exit code = %d, want %d", got, exitWaitTimeout)
	}
}

// TestRunStreamsStdoutAndStderrWithPrefixes asserts text-mode live tail:
// runOne must spawn two output-tail goroutines that GET
// /v1/missions/{id}/output?stream=stdout|stderr&follow=true and emit each
// line on stderr with [stdout]/[stderr] prefixes, interleaved with the
// existing [progress]/[event] prefixes coming from the events stream.
//
// The stub server provides all three endpoints (dispatch, events, output)
// and emits queued/running/progress/done with small delays so the output
// stream has time to interleave. After runCore returns, the captured
// stderr buffer must contain all four prefix types as individual lines
// (no mid-line interleaving — the prefixedStderr mutex guarantees that).
func TestRunStreamsStdoutAndStderrWithPrefixes(t *testing.T) {
	mid := "01900000-0000-7000-8000-000000000001"

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/dispatch", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, `{"mission_id":"`+mid+`","status":"queued"}`)
	})
	mux.HandleFunc("GET /v1/missions/{id}/events", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.WriteHeader(200)
		f, _ := w.(http.Flusher)
		write := func(s string) {
			_, _ = io.WriteString(w, s+"\n")
			if f != nil {
				f.Flush()
			}
		}
		write(`{"seq":1,"event":"queued"}`)
		time.Sleep(20 * time.Millisecond)
		write(`{"seq":2,"event":"running","pid":42}`)
		time.Sleep(40 * time.Millisecond)
		write(`{"seq":3,"event":"progress","value":0.5,"message":"halfway"}`)
		time.Sleep(40 * time.Millisecond)
		write(`{"seq":4,"event":"done","outcome":"success","exit_code":0,"duration_ms":100,"time_finished":1000}`)
	})
	mux.HandleFunc("GET /v1/missions/{id}/output", func(w http.ResponseWriter, r *http.Request) {
		stream := r.URL.Query().Get("stream")
		w.WriteHeader(200)
		f, _ := w.(http.Flusher)
		switch stream {
		case "stdout":
			_, _ = io.WriteString(w, "hello from mission\n")
			if f != nil {
				f.Flush()
			}
			time.Sleep(50 * time.Millisecond)
			_, _ = io.WriteString(w, "another stdout line\n")
			if f != nil {
				f.Flush()
			}
		case "stderr":
			_, _ = io.WriteString(w, "warning: something\n")
			if f != nil {
				f.Flush()
			}
		}
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// Wire an appCtx directly at srv.URL using the same shape as
	// stubRunAppCtx (single dugdale "s1" with one "normal" lane).
	ac := &appCtx{
		Config: &lettsconfig.Config{
			Dugdales: []lettsconfig.Dugdale{
				{ID: "s1", Host: "ignored", Token: "tok",
					Lanes: map[string]lettsconfig.LaneCfg{"normal": {Concurrency: 1}}},
			},
			Routes: map[string]lettsconfig.Route{
				"normal": {Host: "s1", Lane: "normal"},
			},
		},
		Getenv:       func(k string) (string, bool) { return "", false },
		BaseURLForID: map[string]string{"s1": srv.URL},
		clients:      map[clientKey]*hostClient{},
	}

	cmd := newRunCmd()
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetIn(bytes.NewReader(nil))
	cmd.SetContext(context.Background())

	rf := &runFlags{route: "normal", mission: "Smoke"}
	if err := runCore(cmd, ac, rf, FormatText); err != nil {
		t.Fatalf("runCore: %v", err)
	}

	got := stderr.String()
	wantSubstrings := []string{
		"[event] queued",
		"[event] running pid=42",
		"[progress] 0.50 halfway",
		"[stdout] hello from mission",
		"[stdout] another stdout line",
		"[stderr] warning: something",
		"[event] done outcome=success",
	}
	for _, want := range wantSubstrings {
		if !strings.Contains(got, want) {
			t.Errorf("stderr missing %q\nfull stderr:\n%s", want, got)
		}
	}
}

// TestRunMissingMissionIsBadUsage covers the same validation as dispatch.
func TestRunMissingMissionIsBadUsage(t *testing.T) {
	rs := newRunStub(t)
	defer rs.close()
	ac := stubRunAppCtx(t, rs)

	cmd := newRunCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetIn(bytes.NewReader(nil))
	cmd.SetContext(context.Background())

	err := runCore(cmd, ac, &runFlags{route: "normal"}, FormatText)
	if err == nil {
		t.Fatal("expected BadUsageError")
	}
	var bue *BadUsageError
	if !errors.As(err, &bue) {
		t.Errorf("got %T %v", err, err)
	}
}
