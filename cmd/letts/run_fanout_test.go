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

	"letts/pkg/lettsconfig"
)

// fanoutHostStub spans /v1/dispatch and /v1/missions/{id}/events for a single
// host. The events handler is per-stub so each host can produce a different
// outcome in TestRunMultiHostMixedOutcomes.
type fanoutHostStub struct {
	id            string
	srv           *httptest.Server
	dispatchCalls atomic.Int64
	eventsCalls   atomic.Int64
	eventsHandler func(http.ResponseWriter, *http.Request)
}

func newFanoutHostStub(t *testing.T, id string) *fanoutHostStub {
	t.Helper()
	hs := &fanoutHostStub{id: id}
	hs.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "POST" && r.URL.Path == "/v1/dispatch":
			hs.dispatchCalls.Add(1)
			mid := r.Header.Get("Idempotency-Key")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			_, _ = io.WriteString(w, `{"mission_id":"`+mid+`","status":"queued"}`)
		case r.Method == "GET" && strings.HasSuffix(r.URL.Path, "/events"):
			hs.eventsCalls.Add(1)
			if hs.eventsHandler == nil {
				t.Errorf("[%s] events handler not set", hs.id)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			hs.eventsHandler(w, r)
		case r.Method == "GET" && strings.HasSuffix(r.URL.Path, "/output"):
			// Text-mode runOne spawns output-tail goroutines for
			// [stdout]/[stderr] prefixes. Fan-out tests don't exercise live
			// tail — 404 lets tailMissionStream exit silently.
			w.WriteHeader(http.StatusNotFound)
		default:
			t.Errorf("[%s] unexpected request: %s %s", hs.id, r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	return hs
}

func (hs *fanoutHostStub) close() { hs.srv.Close() }

// stubFanoutAppCtx wires one dugdale per stub, each with a "normal" lane.
// BaseURLForID points each id at its own httptest.Server URL.
func stubFanoutAppCtx(t *testing.T, stubs []*fanoutHostStub) *appCtx {
	t.Helper()
	dugs := make([]lettsconfig.Dugdale, 0, len(stubs))
	baseURLs := map[string]string{}
	for _, hs := range stubs {
		dugs = append(dugs, lettsconfig.Dugdale{
			ID: hs.id, Host: "ignored", Token: "tok",
			Lanes: map[string]lettsconfig.LaneCfg{"normal": {Concurrency: 1}},
		})
		baseURLs[hs.id] = hs.srv.URL
	}
	return &appCtx{
		Config:       &lettsconfig.Config{Dugdales: dugs},
		Getenv:       func(k string) (string, bool) { return "", false },
		BaseURLForID: baseURLs,
		clients:      map[clientKey]*hostClient{},
	}
}

// TestSplitHosts covers the comma-split helper in isolation: empty → nil,
// single → 1-entry slice, multi → trimmed list, junk whitespace dropped.
func TestSplitHosts(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"", nil},
		{"s1", []string{"s1"}},
		{"s1,s2", []string{"s1", "s2"}},
		{"s1, s2 ,s7", []string{"s1", "s2", "s7"}},
		{",,s1,", []string{"s1"}},
		// Dedup: repeated host collapses to one entry, first-occurrence order.
		{"s1,s1", []string{"s1"}},
		{"s1,s2,s1,s3,s2", []string{"s1", "s2", "s3"}},
		{" s1 , s1 ", []string{"s1"}},
	}
	for _, c := range cases {
		got := splitHosts(c.in)
		if len(got) != len(c.want) {
			t.Errorf("splitHosts(%q) = %v, want %v", c.in, got, c.want)
			continue
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Errorf("splitHosts(%q)[%d] = %q, want %q", c.in, i, got[i], c.want[i])
			}
		}
	}
}

// TestRunMultiHostFanOutTextSuccess: --host=s1,s2 both succeed, text mode
// aggregates per-host blocks and exits 0.
func TestRunMultiHostFanOutTextSuccess(t *testing.T) {
	s1 := newFanoutHostStub(t, "s1")
	s2 := newFanoutHostStub(t, "s2")
	defer s1.close()
	defer s2.close()
	s1.eventsHandler = func(w http.ResponseWriter, _ *http.Request) {
		writeNDJSON(w, `{"seq":1,"event":"done","outcome":"success","exit_code":0,"return":{"k":1}}`)
	}
	s2.eventsHandler = func(w http.ResponseWriter, _ *http.Request) {
		writeNDJSON(w, `{"seq":1,"event":"done","outcome":"success","exit_code":0,"return":{"k":2}}`)
	}
	ac := stubFanoutAppCtx(t, []*fanoutHostStub{s1, s2})

	cmd := newRunCmd()
	var out, errBuf bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errBuf)
	cmd.SetIn(bytes.NewReader(nil))
	cmd.SetContext(context.Background())

	rf := &runFlags{host: "s1,s2", lane: "normal", mission: "M"}
	if err := runCore(cmd, ac, rf, FormatText); err != nil {
		t.Fatalf("runCore: %v", err)
	}
	// Each host dispatched and streamed exactly once.
	if s1.dispatchCalls.Load() != 1 || s2.dispatchCalls.Load() != 1 {
		t.Errorf("dispatch calls s1=%d s2=%d, want 1 each",
			s1.dispatchCalls.Load(), s2.dispatchCalls.Load())
	}
	if s1.eventsCalls.Load() < 1 || s2.eventsCalls.Load() < 1 {
		t.Errorf("events calls s1=%d s2=%d, want >=1 each",
			s1.eventsCalls.Load(), s2.eventsCalls.Load())
	}
	// stdout must mention both host headers.
	body := out.String()
	for _, h := range []string{"== s1 ==", "== s2 =="} {
		if !strings.Contains(body, h) {
			t.Errorf("stdout missing %q\nfull:\n%s", h, body)
		}
	}
}

// TestRunMultiHostMixedOutcomes: s1=success, s2=failed → aggregate error
// "mission failed on s2", exit code = exitFailure.
func TestRunMultiHostMixedOutcomes(t *testing.T) {
	s1 := newFanoutHostStub(t, "s1")
	s2 := newFanoutHostStub(t, "s2")
	defer s1.close()
	defer s2.close()
	s1.eventsHandler = func(w http.ResponseWriter, _ *http.Request) {
		writeNDJSON(w, `{"seq":1,"event":"done","outcome":"success","exit_code":0,"return":{"k":1}}`)
	}
	s2.eventsHandler = func(w http.ResponseWriter, _ *http.Request) {
		writeNDJSON(w, `{"seq":1,"event":"done","outcome":"failed","exit_code":1,"fail_message":"boom"}`)
	}
	ac := stubFanoutAppCtx(t, []*fanoutHostStub{s1, s2})

	cmd := newRunCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetIn(bytes.NewReader(nil))
	cmd.SetContext(context.Background())

	rf := &runFlags{host: "s1,s2", lane: "normal", mission: "M"}
	err := runCore(cmd, ac, rf, FormatText)
	if err == nil {
		t.Fatal("expected aggregate error for mixed outcomes")
	}
	if !strings.Contains(err.Error(), "mission failed") || !strings.Contains(err.Error(), "s2") {
		t.Errorf("error = %q, want 'mission failed' mentioning s2", err.Error())
	}
	if got := mapErrorToExit(err); got != exitFailure {
		t.Errorf("exit code = %d, want %d", got, exitFailure)
	}
}

// TestRunMultiHostAbnormalWinsOverFailed: s1=failed, s2=killed → abnormal
// outranks failed; aggregate is MissionAbnormalError (exit 125).
func TestRunMultiHostAbnormalWinsOverFailed(t *testing.T) {
	s1 := newFanoutHostStub(t, "s1")
	s2 := newFanoutHostStub(t, "s2")
	defer s1.close()
	defer s2.close()
	s1.eventsHandler = func(w http.ResponseWriter, _ *http.Request) {
		writeNDJSON(w, `{"seq":1,"event":"done","outcome":"failed","exit_code":1,"fail_message":"x"}`)
	}
	s2.eventsHandler = func(w http.ResponseWriter, _ *http.Request) {
		writeNDJSON(w, `{"seq":1,"event":"done","outcome":"killed"}`)
	}
	ac := stubFanoutAppCtx(t, []*fanoutHostStub{s1, s2})

	cmd := newRunCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetIn(bytes.NewReader(nil))
	cmd.SetContext(context.Background())

	rf := &runFlags{host: "s1,s2", lane: "normal", mission: "M"}
	err := runCore(cmd, ac, rf, FormatText)
	if err == nil {
		t.Fatal("expected MissionAbnormalError")
	}
	var ma *MissionAbnormalError
	if !errors.As(err, &ma) {
		t.Fatalf("got %T %v, want MissionAbnormalError", err, err)
	}
	if got := mapErrorToExit(err); got != exitMissionAbnormal {
		t.Errorf("exit code = %d, want %d", got, exitMissionAbnormal)
	}
}

// TestRunMultiHostJSONResultsArray: --output=json aggregate is
// {"results": [{host, ok, outcome, return, ...}, ...]} with one row per host
// (order preserved per hosts slice).
func TestRunMultiHostJSONResultsArray(t *testing.T) {
	s1 := newFanoutHostStub(t, "s1")
	s2 := newFanoutHostStub(t, "s2")
	defer s1.close()
	defer s2.close()
	s1.eventsHandler = func(w http.ResponseWriter, _ *http.Request) {
		writeNDJSON(w, `{"seq":1,"event":"done","outcome":"success","exit_code":0,"return":{"k":1}}`)
	}
	s2.eventsHandler = func(w http.ResponseWriter, _ *http.Request) {
		writeNDJSON(w, `{"seq":1,"event":"done","outcome":"success","exit_code":0,"return":{"k":2}}`)
	}
	ac := stubFanoutAppCtx(t, []*fanoutHostStub{s1, s2})

	cmd := newRunCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(io.Discard)
	cmd.SetIn(bytes.NewReader(nil))
	cmd.SetContext(context.Background())

	rf := &runFlags{host: "s1,s2", lane: "normal", mission: "M"}
	if err := runCore(cmd, ac, rf, FormatJSON); err != nil {
		t.Fatalf("runCore: %v", err)
	}

	var top struct {
		Results []struct {
			Host    string          `json:"host"`
			OK      bool            `json:"ok"`
			Outcome string          `json:"outcome"`
			Return  json.RawMessage `json:"return"`
		} `json:"results"`
	}
	if err := json.Unmarshal(out.Bytes(), &top); err != nil {
		t.Fatalf("stdout not JSON: %v\n%s", err, out.String())
	}
	if len(top.Results) != 2 {
		t.Fatalf("results len = %d, want 2: %s", len(top.Results), out.String())
	}
	byHost := map[string]string{}
	for _, r := range top.Results {
		if !r.OK {
			t.Errorf("host %s ok=false, want true", r.Host)
		}
		if r.Outcome != "success" {
			t.Errorf("host %s outcome=%q, want success", r.Host, r.Outcome)
		}
		byHost[r.Host] = string(r.Return)
	}
	if byHost["s1"] == "" || byHost["s2"] == "" {
		t.Errorf("missing host rows; got %v", byHost)
	}
}

// TestRunMultiHostNDJSONHostPrefixed: each event line on stdout carries a
// top-level "host" field; counts match queued+running+done per host.
func TestRunMultiHostNDJSONHostPrefixed(t *testing.T) {
	s1 := newFanoutHostStub(t, "s1")
	s2 := newFanoutHostStub(t, "s2")
	defer s1.close()
	defer s2.close()
	stream := func(w http.ResponseWriter, _ *http.Request) {
		writeNDJSON(w,
			`{"seq":1,"event":"queued"}`,
			`{"seq":2,"event":"running","pid":7}`,
			`{"seq":3,"event":"done","outcome":"success","exit_code":0,"return":{"k":1}}`,
		)
	}
	s1.eventsHandler = stream
	s2.eventsHandler = stream
	ac := stubFanoutAppCtx(t, []*fanoutHostStub{s1, s2})

	cmd := newRunCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(io.Discard)
	cmd.SetIn(bytes.NewReader(nil))
	cmd.SetContext(context.Background())

	rf := &runFlags{host: "s1,s2", lane: "normal", mission: "M"}
	if err := runCore(cmd, ac, rf, FormatNDJSON); err != nil {
		t.Fatalf("runCore: %v", err)
	}

	counts := map[string]int{}
	for _, l := range strings.Split(strings.TrimRight(out.String(), "\n"), "\n") {
		if l == "" {
			continue
		}
		var ev map[string]any
		if err := json.Unmarshal([]byte(l), &ev); err != nil {
			t.Fatalf("ndjson line not JSON: %v\n%s", err, l)
		}
		h, _ := ev["host"].(string)
		if h == "" {
			t.Errorf("event line missing host field: %s", l)
		}
		counts[h]++
	}
	if counts["s1"] != 3 || counts["s2"] != 3 {
		t.Errorf("per-host event counts = %v, want s1=3 s2=3", counts)
	}
}

// TestRunMultiHostBlockedOutputFile: --output-file with multi-host hosts is
// rejected as a BadUsageError before any goroutine spawns.
func TestRunMultiHostBlockedOutputFile(t *testing.T) {
	s1 := newFanoutHostStub(t, "s1")
	s2 := newFanoutHostStub(t, "s2")
	defer s1.close()
	defer s2.close()
	// No event handler set; we expect this to fail before any HTTP call.
	ac := stubFanoutAppCtx(t, []*fanoutHostStub{s1, s2})

	cmd := newRunCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetIn(bytes.NewReader(nil))
	cmd.SetContext(context.Background())

	rf := &runFlags{
		host: "s1,s2", lane: "normal", mission: "M",
		outputFiles: []string{"result=/tmp/x"},
	}
	err := runCore(cmd, ac, rf, FormatText)
	if err == nil {
		t.Fatal("expected BadUsageError for --output-file with multi --host")
	}
	var bue *BadUsageError
	if !errors.As(err, &bue) {
		t.Errorf("got %T %v, want BadUsageError", err, err)
	}
	if s1.dispatchCalls.Load() != 0 || s2.dispatchCalls.Load() != 0 {
		t.Errorf("dispatch should not be called; got s1=%d s2=%d",
			s1.dispatchCalls.Load(), s2.dispatchCalls.Load())
	}
}

// TestRunMultiHostBlockedMissionID: --mission-id with multi-host is rejected
// to prevent reusing the same id across separate dugdales.
func TestRunMultiHostBlockedMissionID(t *testing.T) {
	s1 := newFanoutHostStub(t, "s1")
	s2 := newFanoutHostStub(t, "s2")
	defer s1.close()
	defer s2.close()
	ac := stubFanoutAppCtx(t, []*fanoutHostStub{s1, s2})

	cmd := newRunCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetIn(bytes.NewReader(nil))
	cmd.SetContext(context.Background())

	rf := &runFlags{
		host: "s1,s2", lane: "normal", mission: "M",
		missionID: "11111111-1111-7111-8111-111111111111",
	}
	err := runCore(cmd, ac, rf, FormatText)
	if err == nil {
		t.Fatal("expected BadUsageError for --mission-id with multi --host")
	}
	var bue *BadUsageError
	if !errors.As(err, &bue) {
		t.Errorf("got %T %v, want BadUsageError", err, err)
	}
}
