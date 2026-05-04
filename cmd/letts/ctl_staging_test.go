package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"letts/internal/ids"
	"letts/pkg/lettsclient"
)

// TestCtlStagingUpload — happy path: HEAD returns 404, PUT initial accepts
// the body; stdout has staging_id\tsha256\tsize and the printed staging_id
// is a valid UUIDv7.
func TestCtlStagingUpload(t *testing.T) {
	content := []byte("hello world")
	tmp := t.TempDir()
	path := filepath.Join(tmp, "src")
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatal(err)
	}

	var headCalls, putCalls atomic.Int64
	ac, stop := stubAppCtx(t, "s1", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "HEAD":
			headCalls.Add(1)
			w.WriteHeader(http.StatusNotFound)
		case "PUT":
			putCalls.Add(1)
			body, _ := io.ReadAll(r.Body)
			if string(body) != "hello world" {
				t.Errorf("PUT body=%q want %q", body, "hello world")
			}
			w.WriteHeader(http.StatusCreated)
		default:
			t.Errorf("unexpected method %s", r.Method)
		}
	}))
	defer stop()

	var out bytes.Buffer
	if err := runCtlStagingUpload(ac, &out, "s1", path); err != nil {
		t.Fatalf("runCtlStagingUpload: %v", err)
	}
	if headCalls.Load() != 1 || putCalls.Load() != 1 {
		t.Errorf("head=%d put=%d want 1/1", headCalls.Load(), putCalls.Load())
	}
	parts := strings.Split(strings.TrimRight(out.String(), "\n"), "\t")
	if len(parts) != 3 {
		t.Fatalf("stdout fields=%d want 3: %q", len(parts), out.String())
	}
	if !ids.ValidateUUIDv7(parts[0]) {
		t.Errorf("staging_id %q is not UUIDv7", parts[0])
	}
	if len(parts[1]) != 64 {
		t.Errorf("sha hex len=%d want 64: %q", len(parts[1]), parts[1])
	}
	if parts[2] != "11" {
		t.Errorf("size=%q want 11", parts[2])
	}
}

// TestCtlStagingUploadRequiresHost — invoking the cobra command without
// --host must surface a BadUsageError.
func TestCtlStagingUploadRequiresHost(t *testing.T) {
	cmd := newCtlStagingUploadCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"/tmp/whatever"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected BadUsageError for missing --host")
	}
	var bue *BadUsageError
	if !errors.As(err, &bue) {
		t.Errorf("got %T %v, want *BadUsageError", err, err)
	}
}

// TestCtlStagingDownloadToStdout — outPath="-" streams the body to w
// (cmd.OutOrStdout()) via io.Copy.
func TestCtlStagingDownloadToStdout(t *testing.T) {
	var gotPath string
	ac, stop := stubAppCtx(t, "s1", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = io.WriteString(w, "binary content")
	}))
	defer stop()

	var out bytes.Buffer
	if err := runCtlStagingDownload(ac, &out, "s1", "sid-abc", "-"); err != nil {
		t.Fatalf("runCtlStagingDownload: %v", err)
	}
	if gotPath != "/v1/staging/sid-abc" {
		t.Errorf("path=%q want /v1/staging/sid-abc", gotPath)
	}
	if out.String() != "binary content" {
		t.Errorf("stdout=%q want %q", out.String(), "binary content")
	}
}

// TestCtlStagingDownloadToFile — outPath=<file> writes the body to disk; the
// writer passed for "stdout" is unused and stays empty.
func TestCtlStagingDownloadToFile(t *testing.T) {
	ac, stop := stubAppCtx(t, "s1", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = io.WriteString(w, "binary content")
	}))
	defer stop()

	dest := filepath.Join(t.TempDir(), "out.bin")
	var out bytes.Buffer
	if err := runCtlStagingDownload(ac, &out, "s1", "sid-abc", dest); err != nil {
		t.Fatalf("runCtlStagingDownload: %v", err)
	}
	if out.Len() != 0 {
		t.Errorf("stdout should be empty when writing to file, got %q", out.String())
	}
	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatalf("read dest: %v", err)
	}
	if string(got) != "binary content" {
		t.Errorf("file=%q want %q", got, "binary content")
	}
}

// TestCtlStagingDelete — force=true must add ?force=true to the DELETE URL
// (DeleteStaging contract from staging.go).
func TestCtlStagingDelete(t *testing.T) {
	var gotMethod, gotPath, gotQuery string
	ac, stop := stubAppCtx(t, "s1", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotQuery = r.URL.RawQuery
		w.WriteHeader(http.StatusNoContent)
	}))
	defer stop()

	if err := runCtlStagingDelete(ac, "s1", "sid-abc", true); err != nil {
		t.Fatalf("runCtlStagingDelete: %v", err)
	}
	if gotMethod != "DELETE" {
		t.Errorf("method=%q want DELETE", gotMethod)
	}
	if gotPath != "/v1/staging/sid-abc" {
		t.Errorf("path=%q want /v1/staging/sid-abc", gotPath)
	}
	if gotQuery != "force=true" {
		t.Errorf("query=%q want force=true", gotQuery)
	}
}

// TestCtlStagingDeleteNoForceOmitsQuery — sanity-check the no-force path
// emits no query string.
func TestCtlStagingDeleteNoForceOmitsQuery(t *testing.T) {
	var gotQuery string
	ac, stop := stubAppCtx(t, "s1", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		w.WriteHeader(http.StatusNoContent)
	}))
	defer stop()

	if err := runCtlStagingDelete(ac, "s1", "sid-abc", false); err != nil {
		t.Fatalf("runCtlStagingDelete: %v", err)
	}
	if gotQuery != "" {
		t.Errorf("query=%q want empty", gotQuery)
	}
}

// TestCtlStagingList — text mode renders a header and one row per artifact and
// forwards mission_id as a query param.
func TestCtlStagingList(t *testing.T) {
	var gotQuery string
	ac, stop := stubAppCtx(t, "s1", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/staging" {
			t.Errorf("path=%q", r.URL.Path)
		}
		gotQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{
			"staging":[
				{"staging_id":"sid-1","sha256":"aaaa","state":"complete","ref_kind":"input","size":11},
				{"staging_id":"sid-2","sha256":"bbbb","state":"uploading","ref_kind":"output","size":42}
			],
			"next_cursor":"opaque-cursor"
		}`)
	}))
	defer stop()

	var out bytes.Buffer
	opts := lettsclient.ListStagingOpts{MissionID: "m1"}
	if err := runCtlStagingList(ac, &out, "s1", opts, FormatText); err != nil {
		t.Fatalf("runCtlStagingList: %v", err)
	}
	if !strings.Contains(gotQuery, "mission_id=m1") {
		t.Errorf("query=%q missing mission_id=m1", gotQuery)
	}
	s := out.String()
	for _, want := range []string{"STAGING_ID", "SIZE", "STATE", "REF_KIND", "sid-1", "sid-2", "complete", "uploading", "input", "output"} {
		if !strings.Contains(s, want) {
			t.Errorf("output missing %q: %q", want, s)
		}
	}
	if !strings.Contains(s, "cursor: opaque-cursor") {
		t.Errorf("cursor footer missing: %q", s)
	}
	rows := 0
	for _, line := range strings.Split(s, "\n") {
		if strings.HasPrefix(line, "sid-") {
			rows++
		}
	}
	if rows != 2 {
		t.Errorf("want 2 data rows, got %d in %q", rows, s)
	}
}

// TestCtlStagingListJSONFormat — JSON format round-trips ListStagingResponse
// from daemon to stdout.
func TestCtlStagingListJSONFormat(t *testing.T) {
	ac, stop := stubAppCtx(t, "s1", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{
			"staging":[{"staging_id":"sid-1","sha256":"aaaa","state":"complete","ref_kind":"input","size":11}],
			"next_cursor":""
		}`)
	}))
	defer stop()

	var out bytes.Buffer
	if err := runCtlStagingList(ac, &out, "s1", lettsclient.ListStagingOpts{}, FormatJSON); err != nil {
		t.Fatalf("runCtlStagingList: %v", err)
	}
	var got lettsclient.ListStagingResponse
	if err := json.NewDecoder(&out).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Staging) != 1 || got.Staging[0].StagingID != "sid-1" {
		t.Errorf("staging=%+v", got.Staging)
	}
}

// TestCtlStagingListNoCursorOmitsFooter — when next_cursor is empty the
// trailing "cursor: ..." line must not appear.
func TestCtlStagingListNoCursorOmitsFooter(t *testing.T) {
	ac, stop := stubAppCtx(t, "s1", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"staging":[],"next_cursor":""}`)
	}))
	defer stop()

	var out bytes.Buffer
	if err := runCtlStagingList(ac, &out, "s1", lettsclient.ListStagingOpts{}, FormatText); err != nil {
		t.Fatalf("runCtlStagingList: %v", err)
	}
	if strings.Contains(out.String(), "cursor:") {
		t.Errorf("did not expect cursor footer, got %q", out.String())
	}
}

// TestCtlStagingListRequiresHost — list without --host must surface BadUsageError.
func TestCtlStagingListRequiresHost(t *testing.T) {
	cmd := newCtlStagingListCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected BadUsageError for missing --host")
	}
	var bue *BadUsageError
	if !errors.As(err, &bue) {
		t.Errorf("got %T %v, want *BadUsageError", err, err)
	}
}

// TestCtlStagingDownloadRequiresHost — download without --host must surface
// BadUsageError.
func TestCtlStagingDownloadRequiresHost(t *testing.T) {
	cmd := newCtlStagingDownloadCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"sid-abc"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected BadUsageError for missing --host")
	}
	var bue *BadUsageError
	if !errors.As(err, &bue) {
		t.Errorf("got %T %v, want *BadUsageError", err, err)
	}
}

// TestCtlStagingDeleteRequiresHost — delete without --host must surface
// BadUsageError.
func TestCtlStagingDeleteRequiresHost(t *testing.T) {
	cmd := newCtlStagingDeleteCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"sid-abc"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected BadUsageError for missing --host")
	}
	var bue *BadUsageError
	if !errors.As(err, &bue) {
		t.Errorf("got %T %v, want *BadUsageError", err, err)
	}
}
