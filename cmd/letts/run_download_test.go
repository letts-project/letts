package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"letts/pkg/lettsconfig"
)

// runDownloadStub spans dispatch, events, and GET staging. The events handler
// is pluggable so per-test we can shape the done event's outputs map (the
// daemon emits staging_id inside the done event, so
// no follow-up GET /v1/missions/{id} is needed). The staging handler is
// pluggable for the success / missing-role / etc. shapes.
type runDownloadStub struct {
	srv             *httptest.Server
	dispatchCalls   atomic.Int64
	eventsCalls     atomic.Int64
	stagingGetCalls atomic.Int64
	eventsHandler   func(http.ResponseWriter, *http.Request)
	stagingHandler  func(http.ResponseWriter, *http.Request)
}

func newRunDownloadStub(t *testing.T) *runDownloadStub {
	t.Helper()
	rs := &runDownloadStub{}
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
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/v1/staging/"):
			rs.stagingGetCalls.Add(1)
			if rs.stagingHandler == nil {
				t.Errorf("staging handler not set")
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			rs.stagingHandler(w, r)
		case r.Method == "GET" && strings.HasSuffix(r.URL.Path, "/output"):
			// Text-mode runOne spawns output-tail goroutines for
			// [stdout]/[stderr] prefixes. Download tests don't exercise live
			// tail — 404 lets tailMissionStream exit silently.
			w.WriteHeader(http.StatusNotFound)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	return rs
}

func (rs *runDownloadStub) close() { rs.srv.Close() }

func stubDownloadAppCtx(t *testing.T, rs *runDownloadStub) *appCtx {
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

// TestRunOutputFileDownloads verifies the happy path: a successful mission
// whose done event carries an Outputs map[role]{staging_id} triggers a GET
// /v1/staging/{id} per --output-file role and writes the body to the
// configured local path.
func TestRunOutputFileDownloads(t *testing.T) {
	rs := newRunDownloadStub(t)
	defer rs.close()
	rs.eventsHandler = func(w http.ResponseWriter, _ *http.Request) {
		writeNDJSON(w,
			`{"seq":1,"event":"done","outcome":"success","exit_code":0,"return":{"ok":true},`+
				`"outputs":{"result":{"staging_id":"stg-123","sha256":"93a0b24644f2e0fd11d6b422c90275c482b0cc20be4a4e3f62148ed2932b4792","size":14}},`+
				`"time_finished":1714600045123,"duration_ms":1234}`,
		)
	}
	rs.stagingHandler = func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/stg-123") {
			t.Errorf("unexpected staging path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = io.WriteString(w, "binary content")
	}
	ac := stubDownloadAppCtx(t, rs)

	dst := filepath.Join(t.TempDir(), "out.bin")
	cmd := newRunCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetIn(bytes.NewReader(nil))
	cmd.SetContext(context.Background())

	rf := &runFlags{route: "normal", mission: "M", outputFiles: []string{"result=" + dst}}
	if err := runCore(cmd, ac, rf, FormatText); err != nil {
		t.Fatalf("runCore: %v", err)
	}

	if rs.stagingGetCalls.Load() != 1 {
		t.Errorf("staging GET calls = %d, want 1", rs.stagingGetCalls.Load())
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("read downloaded file: %v", err)
	}
	if string(got) != "binary content" {
		t.Errorf("downloaded body = %q, want %q", string(got), "binary content")
	}
}

// TestRunOutputFileShaMismatchAborts: when the downloaded bytes don't
// match the sha256 declared in the done event, runCore must error AND leave no
// file at the destination — the write is atomic (sidecar tmp → verify → rename),
// not a truncated/corrupt partial at the final path.
func TestRunOutputFileShaMismatchAborts(t *testing.T) {
	rs := newRunDownloadStub(t)
	defer rs.close()
	rs.eventsHandler = func(w http.ResponseWriter, _ *http.Request) {
		writeNDJSON(w,
			`{"seq":1,"event":"done","outcome":"success","exit_code":0,`+
				`"outputs":{"result":{"staging_id":"stg-123","sha256":"0000000000000000000000000000000000000000000000000000000000000000","size":14}}}`,
		)
	}
	rs.stagingHandler = func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = io.WriteString(w, "binary content") // real sha != declared zeros
	}
	ac := stubDownloadAppCtx(t, rs)

	dst := filepath.Join(t.TempDir(), "out.bin")
	cmd := newRunCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetIn(bytes.NewReader(nil))
	cmd.SetContext(context.Background())

	rf := &runFlags{route: "normal", mission: "M", outputFiles: []string{"result=" + dst}}
	err := runCore(cmd, ac, rf, FormatText)
	if err == nil {
		t.Fatal("expected integrity error on sha mismatch")
	}
	if _, statErr := os.Stat(dst); !os.IsNotExist(statErr) {
		t.Errorf("destination file exists after failed/corrupt download (err=%v); want no partial", statErr)
	}
}

// TestRunNDJSONPreservesZeroProgressValue: --output=ndjson must emit each
// event verbatim. Re-encoding the typed struct drops omitempty
// fields, so a progress event with value=0.0 loses its "value" key entirely —
// indistinguishable from "no value". The fix emits the captured raw line.
func TestRunNDJSONPreservesZeroProgressValue(t *testing.T) {
	rs := newRunDownloadStub(t)
	defer rs.close()
	rs.eventsHandler = func(w http.ResponseWriter, _ *http.Request) {
		writeNDJSON(w,
			`{"seq":1,"event":"progress","value":0,"message":"start"}`,
			`{"seq":2,"event":"done","outcome":"success","exit_code":0}`,
		)
	}
	ac := stubDownloadAppCtx(t, rs)

	var stdout bytes.Buffer
	cmd := newRunCmd()
	cmd.SetOut(&stdout)
	cmd.SetErr(io.Discard)
	cmd.SetIn(bytes.NewReader(nil))
	cmd.SetContext(context.Background())

	rf := &runFlags{route: "normal", mission: "M"}
	if err := runCore(cmd, ac, rf, FormatNDJSON); err != nil {
		t.Fatalf("runCore: %v", err)
	}
	if !strings.Contains(stdout.String(), `"value":0`) {
		t.Errorf("ndjson stdout dropped progress value=0:\n%s", stdout.String())
	}
}

// TestRunOutputFileMissingRole verifies that asking for a role not present in
// the done event's outputs map produces an error mentioning "no output with role".
func TestRunOutputFileMissingRole(t *testing.T) {
	rs := newRunDownloadStub(t)
	defer rs.close()
	rs.eventsHandler = func(w http.ResponseWriter, _ *http.Request) {
		writeNDJSON(w,
			`{"seq":1,"event":"done","outcome":"success","exit_code":0,`+
				`"outputs":{"other":{"staging_id":"stg-other","sha256":"abc","size":3}}}`,
		)
	}
	// staging handler should never be called; leave nil so a hit fails loudly.
	ac := stubDownloadAppCtx(t, rs)

	dst := filepath.Join(t.TempDir(), "x")
	cmd := newRunCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetIn(bytes.NewReader(nil))
	cmd.SetContext(context.Background())

	rf := &runFlags{route: "normal", mission: "M", outputFiles: []string{"missing=" + dst}}
	err := runCore(cmd, ac, rf, FormatText)
	if err == nil {
		t.Fatal("expected error for missing role")
	}
	if !strings.Contains(err.Error(), "no output with role") {
		t.Errorf("error = %q, want contains 'no output with role'", err.Error())
	}
	if rs.stagingGetCalls.Load() != 0 {
		t.Errorf("staging GET should not be called on missing role; got %d", rs.stagingGetCalls.Load())
	}
}

// TestRunOutputFileBadFormat verifies that a malformed --output-file value
// surfaces as a BadUsageError (exit 2 via mapErrorToExit).
func TestRunOutputFileBadFormat(t *testing.T) {
	rs := newRunDownloadStub(t)
	defer rs.close()
	rs.eventsHandler = func(w http.ResponseWriter, _ *http.Request) {
		writeNDJSON(w,
			`{"seq":1,"event":"done","outcome":"success","exit_code":0,"outputs":{}}`,
		)
	}
	ac := stubDownloadAppCtx(t, rs)

	cmd := newRunCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetIn(bytes.NewReader(nil))
	cmd.SetContext(context.Background())

	rf := &runFlags{route: "normal", mission: "M", outputFiles: []string{"nopath"}}
	err := runCore(cmd, ac, rf, FormatText)
	if err == nil {
		t.Fatal("expected BadUsageError")
	}
	var bue *BadUsageError
	if !errors.As(err, &bue) {
		t.Errorf("got %T %v, want BadUsageError", err, err)
	}
}
