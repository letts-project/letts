package main

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"testing"

	"letts/pkg/lettsclient"
)

// TestCtlExecOutputStreamStdout — `ctl exec output --stream=stdout` forwards
// the stream query param and copies the response body to stdout (delegates
// to runCtlMissionsOutput; this is a smoke test for the F3 wiring).
func TestCtlExecOutputStreamStdout(t *testing.T) {
	var gotPath, gotStream string
	ac, stop := stubAppCtx(t, "s1", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotStream = r.URL.Query().Get("stream")
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = io.WriteString(w, "stdout-bytes")
	}))
	defer stop()

	cmd := newCtlExecOutputCmd()
	cmd.SetContext(t.Context())
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(io.Discard)

	if err := runCtlMissionsOutput(cmd, ac, "0192aaaa-0000-7000-8000-000000000001", "s1", "stdout", false, nil); err != nil {
		t.Fatalf("runCtlMissionsOutput: %v", err)
	}
	if !strings.HasPrefix(gotPath, "/v1/missions/0192aaaa-0000-7000-8000-000000000001/output") {
		t.Errorf("path=%q", gotPath)
	}
	if gotStream != "stdout" {
		t.Errorf("stream=%q want stdout", gotStream)
	}
	if !strings.Contains(out.String(), "stdout-bytes") {
		t.Errorf("stdout=%q missing payload", out.String())
	}
}

// TestCtlExecRestartSuccess — the 201 response is rendered in text format
// as "<new_id>\t<status>\trestarted_from=<old_id>" so shell tooling can
// grab the new id with awk/cut without parsing JSON.
func TestCtlExecRestartSuccess(t *testing.T) {
	var gotMethod, gotPath string
	ac, stop := stubAppCtx(t, "s1", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{"mission_id":"0192bbbb-0000-7000-8000-000000000000","restarted_from":"0192aaaa-0000-7000-8000-000000000001","status":"queued"}`)
	}))
	defer stop()

	var out bytes.Buffer
	if err := runCtlExecRestart(ac, &out, "0192aaaa-0000-7000-8000-000000000001", "s1", FormatText); err != nil {
		t.Fatalf("runCtlExecRestart: %v", err)
	}
	if gotMethod != "POST" {
		t.Errorf("method=%q want POST", gotMethod)
	}
	if gotPath != "/v1/missions/0192aaaa-0000-7000-8000-000000000001/restart" {
		t.Errorf("path=%q", gotPath)
	}
	if !strings.Contains(out.String(), "0192bbbb-0000-7000-8000-000000000000") {
		t.Errorf("stdout=%q missing new id", out.String())
	}
	if !strings.Contains(out.String(), "restarted_from=0192aaaa-0000-7000-8000-000000000001") {
		t.Errorf("stdout=%q missing restarted_from", out.String())
	}
}

// TestCtlExecRestartArtifactsExpired — the daemon's 409 with code
// input_artifacts_expired must render the multi-line friendly message
// and surface a non-nil error so the CLI exits non-zero.
func TestCtlExecRestartArtifactsExpired(t *testing.T) {
	ac, stop := stubAppCtx(t, "s1", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_, _ = io.WriteString(w, `{"error":"input_artifacts_expired","message":"staging gone","details":{"staging_id":"0192cccc-0000-7000-8000-000000000000","role":"script"}}`)
	}))
	defer stop()

	var out bytes.Buffer
	err := runCtlExecRestart(ac, &out, "0192aaaa-0000-7000-8000-000000000001", "s1", FormatText)
	if err == nil {
		t.Fatal("expected error for 409 input_artifacts_expired")
	}
	body := out.String()
	if !strings.Contains(body, "input artifacts have expired") {
		t.Errorf("missing friendly headline: %q", body)
	}
	if !strings.Contains(body, "0192cccc-0000-7000-8000-000000000000") {
		t.Errorf("missing staging_id: %q", body)
	}
	if !strings.Contains(body, "script:") {
		t.Errorf("missing role label: %q", body)
	}
	if !strings.Contains(body, "exec_failed_ttl") {
		t.Errorf("missing retain advice: %q", body)
	}
}

// TestCtlExecListPassesKindExec — opts.Kind="exec" is threaded through to
// the daemon as ?kind=exec. Reuses runCtlMissionsList (the F2 list command
// pre-populates opts.Kind before delegating).
func TestCtlExecListPassesKindExec(t *testing.T) {
	var gotQuery string
	ac, stop := stubAppCtx(t, "s1", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"missions":[]}`)
	}))
	defer stop()

	var out bytes.Buffer
	if err := runCtlMissionsList(ac, &out, "s1",
		lettsclient.ListMissionsOpts{Kind: "exec"}, FormatText); err != nil {
		t.Fatalf("runCtlMissionsList: %v", err)
	}
	if !strings.Contains(gotQuery, "kind=exec") {
		t.Errorf("query=%q missing kind=exec", gotQuery)
	}
}

// TestCtlExecListWithGroupFilter — --group is wired into opts.GroupID and
// surfaces in the daemon query as ?group_id=...
func TestCtlExecListWithGroupFilter(t *testing.T) {
	var gotQuery string
	ac, stop := stubAppCtx(t, "s1", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"missions":[]}`)
	}))
	defer stop()

	gid := "0192aaaa-0000-7000-8000-000000000000"
	var out bytes.Buffer
	if err := runCtlMissionsList(ac, &out, "s1",
		lettsclient.ListMissionsOpts{Kind: "exec", GroupID: gid}, FormatText); err != nil {
		t.Fatalf("runCtlMissionsList: %v", err)
	}
	if !strings.Contains(gotQuery, "group_id="+gid) {
		t.Errorf("query=%q missing group_id", gotQuery)
	}
}

// TestCtlExecListShowsGroupIDColumn — exec listings carry per-row group_id
// and display_name, so the renderer widens to add GROUP_ID and DISPLAY_NAME
// columns. The header AND the values for the populated row must appear in
// the text output.
func TestCtlExecListShowsGroupIDColumn(t *testing.T) {
	ac, stop := stubAppCtx(t, "s1", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"missions":[
			{"mission_id":"0192aaaa-0000-7000-8000-000000000001","status":"done","outcome":"success","lane":"light","mission_name":"exec","time_created":1700000000000,"group_id":"0192bbbb-0000-7000-8000-000000000000","display_name":"uptime [+2 hosts]"}
		]}`)
	}))
	defer stop()

	var out bytes.Buffer
	if err := runCtlMissionsList(ac, &out, "s1",
		lettsclient.ListMissionsOpts{Kind: "exec"}, FormatText); err != nil {
		t.Fatalf("runCtlMissionsList: %v", err)
	}
	s := out.String()
	if !strings.Contains(s, "GROUP_ID") {
		t.Errorf("missing GROUP_ID header: %q", s)
	}
	if !strings.Contains(s, "DISPLAY_NAME") {
		t.Errorf("missing DISPLAY_NAME header: %q", s)
	}
	if !strings.Contains(s, "uptime [+2 hosts]") {
		t.Errorf("missing display_name value: %q", s)
	}
	if !strings.Contains(s, "0192bbbb-0000-7000-8000-000000000000") {
		t.Errorf("missing group_id value: %q", s)
	}
}

// TestCtlExecShowRejectsMissionKind — fetching a kind=mission record via
// `ctl exec show` must surface BadUsage so users don't silently get the
// mission renderer for a named job. Exit code is 2 (BadUsage).
func TestCtlExecShowRejectsMissionKind(t *testing.T) {
	ac, stop := stubAppCtx(t, "s1", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"mission_id":"0192aaaa-0000-7000-8000-000000000001","kind":"mission","mission_name":"job","status":"done"}`)
	}))
	defer stop()

	var out bytes.Buffer
	err := runCtlExecShow(ac, &out, "0192aaaa-0000-7000-8000-000000000001", "s1", nil, FormatText)
	if err == nil {
		t.Fatal("expected BadUsageError for mission kind")
	}
	if mapErrorToExit(err) != exitBadUsage {
		t.Errorf("exit=%d, want %d (badusage)", mapErrorToExit(err), exitBadUsage)
	}
	if !strings.Contains(err.Error(), "not an exec mission") {
		t.Errorf("err=%v, want 'not an exec mission'", err)
	}
}

// TestCtlExecShowSuccess — kind=exec records render normally via printMission
// (JSON by default for text mode since records don't tabulate cleanly).
func TestCtlExecShowSuccess(t *testing.T) {
	ac, stop := stubAppCtx(t, "s1", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"mission_id":"0192aaaa-0000-7000-8000-000000000002","kind":"exec","mission_name":"exec","status":"done","outcome":"success","exit_code":0}`)
	}))
	defer stop()

	var out bytes.Buffer
	if err := runCtlExecShow(ac, &out, "0192aaaa-0000-7000-8000-000000000002", "s1", nil, FormatText); err != nil {
		t.Fatalf("runCtlExecShow: %v", err)
	}
	if !strings.Contains(out.String(), `"exec"`) {
		t.Errorf("output=%q missing exec marker", out.String())
	}
}
