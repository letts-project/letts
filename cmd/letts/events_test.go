package main

import (
	"bytes"
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

// stubEventsAppCtx wires one dugdale at the given httptest server.
func stubEventsAppCtx(srvURL string) *appCtx {
	return &appCtx{
		Config: &lettsconfig.Config{
			Dugdales: []lettsconfig.Dugdale{
				{ID: "s1", Host: "ignored", Token: "tok"},
			},
		},
		Getenv:       func(k string) (string, bool) { return "", false },
		BaseURLForID: map[string]string{"s1": srvURL},
		clients:      map[clientKey]*hostClient{},
	}
}

// TestEventsArchived: stub returns 3 NDJSON events ending with done; all 3
// must be written to stdout (one per line).
func TestEventsArchived(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/missions/abc/events" {
			t.Errorf("path = %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = io.WriteString(w, `{"seq":1,"event":"queued"}`+"\n"+
			`{"seq":2,"event":"running"}`+"\n"+
			`{"seq":3,"event":"done","outcome":"success"}`+"\n")
	}))
	defer srv.Close()

	ac := stubEventsAppCtx(srv.URL)
	c, err := ac.ClientForHost("s1", lettsconfig.ScopeDispatch)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := streamEventsToStdout(t.Context(), c, "abc", false, 0, &buf); err != nil {
		t.Fatalf("streamEventsToStdout: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("got %d lines, want 3: %q", len(lines), buf.String())
	}
	// Each line must be a valid JSON event.
	for i, line := range lines {
		var ev map[string]any
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Errorf("line %d not valid JSON: %v (%q)", i, err, line)
		}
	}
	// Last line must be the done event.
	var last map[string]any
	_ = json.Unmarshal([]byte(lines[2]), &last)
	if last["event"] != "done" {
		t.Errorf("last event = %v, want done", last["event"])
	}
}

// TestEventsFromCursor: verify --from=5 query param is sent.
func TestEventsFromCursor(t *testing.T) {
	var gotFrom atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotFrom.Store(r.URL.Query().Get("from"))
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = io.WriteString(w, `{"seq":6,"event":"done","outcome":"success"}`+"\n")
	}))
	defer srv.Close()

	ac := stubEventsAppCtx(srv.URL)
	c, err := ac.ClientForHost("s1", lettsconfig.ScopeDispatch)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := streamEventsToStdout(t.Context(), c, "abc", false, 5, &buf); err != nil {
		t.Fatalf("streamEventsToStdout: %v", err)
	}
	if got, _ := gotFrom.Load().(string); got != "5" {
		t.Errorf("from query = %q, want 5", got)
	}
}

// TestEventsFollowReconnects: --follow drives a reconnect after the first
// non-terminal close, and the reconnect carries from=<last seq>.
func TestEventsFollowReconnects(t *testing.T) {
	var attempt atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		n := attempt.Add(1)
		if n == 1 {
			// First connection: emit two events, then close without done.
			_, _ = io.WriteString(w, `{"seq":1,"event":"queued"}`+"\n"+
				`{"seq":2,"event":"running"}`+"\n")
			return
		}
		// Second connection must carry from=2.
		if got := r.URL.Query().Get("from"); got != "2" {
			t.Errorf("reconnect from = %q, want 2", got)
		}
		_, _ = io.WriteString(w, `{"seq":3,"event":"done","outcome":"success"}`+"\n")
	}))
	defer srv.Close()

	ac := stubEventsAppCtx(srv.URL)
	c, err := ac.ClientForHost("s1", lettsconfig.ScopeDispatch)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := streamEventsToStdout(t.Context(), c, "abc", true, 0, &buf); err != nil {
		t.Fatalf("streamEventsToStdout: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("got %d lines, want 3: %q", len(lines), buf.String())
	}
	if attempt.Load() < 2 {
		t.Errorf("expected at least 2 attempts, got %d", attempt.Load())
	}
}

// TestEventsRequiresHost: invoking the command without --host returns a
// BadUsageError (by-id fan-out is not supported for events).
func TestEventsRequiresHost(t *testing.T) {
	// Wire an appCtx but command should reject before any HTTP call.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected HTTP request: %s %s", r.Method, r.URL.Path)
	}))
	defer srv.Close()

	cmd := newEventsCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetIn(bytes.NewReader(nil))

	err := runEvents(cmd, stubEventsAppCtx(srv.URL), "abc", "", false, 0)
	if err == nil {
		t.Fatal("expected error when --host is missing")
	}
	var bue *BadUsageError
	if !errors.As(err, &bue) {
		t.Errorf("got %T %v, want BadUsageError", err, err)
	}
}
