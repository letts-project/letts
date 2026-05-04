package handlers_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"letts/internal/eventfile"
	"letts/internal/ids"
	"letts/internal/server/handlers"
	"letts/internal/server/middleware"
	"letts/internal/storage"
)

// setupEventsTest creates a db, EventsHandler, and a writable events file
// for a given mission ID.
func setupEventsTest(t *testing.T) (*handlers.EventsHandler, string, string) {
	t.Helper()
	db := setupDB(t)
	dataDir := t.TempDir()

	missionID := ids.NewUUIDv7()

	// Create mission row.
	m := &storage.Mission{
		ID:               missionID,
		Kind:             storage.KindMission,
		Lane:             "default",
		MissionName:      "test-events",
		Status:           storage.StatusQueued,
		InputFingerprint: "fp",
		Input:            []byte("{}"),
		TimeCreatedMs:    time.Now().UnixMilli(),
	}
	if err := storage.InsertMission(context.Background(), db, m); err != nil {
		t.Fatalf("insert mission: %v", err)
	}

	// Compute shard path and create the directory.
	shard, err := ids.ShardPath(missionID)
	if err != nil {
		t.Fatalf("shard path: %v", err)
	}
	parentDir := filepath.Join(dataDir, "output", shard)
	if err := os.MkdirAll(parentDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	h := &handlers.EventsHandler{DataDir: dataDir, DB: db}
	return h, missionID, parentDir
}

// writeEvents creates an events file with queued+running+(opt progress)+done.
func writeEvents(t *testing.T, parentDir, missionID string, includeProgress bool) {
	t.Helper()
	w, err := eventfile.Create(parentDir, missionID)
	if err != nil {
		t.Fatalf("create events file: %v", err)
	}
	defer func() { _ = w.Close() }()
	_, _ = w.Append(eventfile.KindQueued, map[string]any{}, false)
	_, _ = w.Append(eventfile.KindRunning, map[string]any{}, false)
	if includeProgress {
		_, _ = w.Append(eventfile.KindProgress, map[string]any{"msg": "step1"}, false)
	}
	_ = w.AppendDoneIdempotent(map[string]any{"outcome": "success"}, 3)
}

// TestEventsStreamAllEvents verifies that GET /v1/missions/{id}/events returns all events.
func TestEventsStreamAllEvents(t *testing.T) {
	h, missionID, parentDir := setupEventsTest(t)
	writeEvents(t, parentDir, missionID, true)

	mux := http.NewServeMux()
	h.Register(mux)

	req := withScope(httptest.NewRequest("GET", "/v1/missions/"+missionID+"/events", nil), middleware.ScopeDispatch)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status: got %d, want 200; body=%s", w.Code, w.Body.String())
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/x-ndjson" {
		t.Errorf("Content-Type: got %q, want application/x-ndjson", ct)
	}

	// Count events.
	var count int
	scanner := bufio.NewScanner(w.Body)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var ev map[string]any
		if err := json.Unmarshal(line, &ev); err != nil {
			t.Fatalf("parse event: %v", err)
		}
		count++
	}
	if count != 4 { // queued + running + progress + done
		t.Errorf("event count: want 4, got %d", count)
	}
}

// TestEventsStreamDoneMissionWithoutTerminalEventDoesNotHang: a mission
// that is already done has an immutable events file, so follow=true must serve
// it as an archived read and close — even if the terminal done line is somehow
// missing. Without this, eventfile.Stream polls forever waiting for a done line
// that will never be written, hanging the follow client until its own timeout.
func TestEventsStreamDoneMissionWithoutTerminalEventDoesNotHang(t *testing.T) {
	h, missionID, parentDir := setupEventsTest(t)
	if _, err := h.DB.ExecContext(context.Background(),
		`UPDATE missions SET status='done', outcome='success' WHERE mission_id=?`, missionID); err != nil {
		t.Fatalf("set done: %v", err)
	}
	// Events file has lifecycle events but NO terminal done line.
	w, err := eventfile.Create(parentDir, missionID)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = w.Append(eventfile.KindQueued, map[string]any{}, false)
	_, _ = w.Append(eventfile.KindRunning, map[string]any{}, false)
	_ = w.Close()

	mux := http.NewServeMux()
	h.Register(mux)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	req := withScope(httptest.NewRequest("GET",
		"/v1/missions/"+missionID+"/events?follow=true", nil).WithContext(ctx), middleware.ScopeDispatch)
	rec := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		mux.ServeHTTP(rec, req)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(700 * time.Millisecond):
		t.Error("follow=true on a done mission did not return — hanging on missing terminal event")
		<-done // let the ctx timeout release the goroutine before returning
		return
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d", rec.Code)
	}
	if body := rec.Body.String(); !strings.Contains(body, `"running"`) {
		t.Errorf("archived events not served: %q", body)
	}
}

// TestEventsStreamMissionNotFound verifies 404 for unknown mission.
func TestEventsStreamMissionNotFound(t *testing.T) {
	db := setupDB(t)
	h := &handlers.EventsHandler{DataDir: t.TempDir(), DB: db}
	mux := http.NewServeMux()
	h.Register(mux)

	fakeID := ids.NewUUIDv7()
	req := withScope(httptest.NewRequest("GET", "/v1/missions/"+fakeID+"/events", nil), middleware.ScopeDispatch)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", w.Code)
	}
}

// TestEventsStreamDeletingMission verifies 410 for a mission with status='deleting'.
func TestEventsStreamDeletingMission(t *testing.T) {
	db := setupDB(t)
	missionID := ids.NewUUIDv7()
	m := &storage.Mission{
		ID:               missionID,
		Kind:             storage.KindMission,
		Lane:             "default",
		MissionName:      "deleting-test",
		Status:           storage.StatusDeleting,
		InputFingerprint: "fp",
		Input:            []byte("{}"),
		TimeCreatedMs:    time.Now().UnixMilli(),
	}
	if err := storage.InsertMission(context.Background(), db, m); err != nil {
		t.Fatalf("insert mission: %v", err)
	}

	h := &handlers.EventsHandler{DataDir: t.TempDir(), DB: db}
	mux := http.NewServeMux()
	h.Register(mux)

	req := withScope(httptest.NewRequest("GET", "/v1/missions/"+missionID+"/events", nil), middleware.ScopeDispatch)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusGone {
		t.Errorf("status: got %d, want 410", w.Code)
	}
}

// TestEventsStreamInvalidID verifies 400 for a non-UUIDv7 id.
func TestEventsStreamInvalidID(t *testing.T) {
	db := setupDB(t)
	h := &handlers.EventsHandler{DataDir: t.TempDir(), DB: db}
	mux := http.NewServeMux()
	h.Register(mux)

	req := withScope(httptest.NewRequest("GET", "/v1/missions/not-a-uuid/events", nil), middleware.ScopeDispatch)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", w.Code)
	}
}

// TestEventsStreamFollow verifies the follow mode streams new events as they arrive.
func TestEventsStreamFollow(t *testing.T) {
	h, missionID, parentDir := setupEventsTest(t)

	// Open a writer that will remain open (mission is running).
	w, err := eventfile.Create(parentDir, missionID)
	if err != nil {
		t.Fatalf("create writer: %v", err)
	}
	_, _ = w.Append(eventfile.KindQueued, map[string]any{}, false)
	_, _ = w.Append(eventfile.KindRunning, map[string]any{}, false)

	mux := http.NewServeMux()
	h.Register(mux)

	// Inject a Dispatch identity for the kind-gate check (server-side; the
	// real binary does this via middleware.Auth, which the test bypasses).
	wrapped := http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(rw, withScope(r, middleware.ScopeDispatch))
	})

	// Use a server that supports streaming (httptest.NewServer, not recorder).
	server := httptest.NewServer(wrapped)
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, "GET",
		server.URL+"/v1/missions/"+missionID+"/events?follow=true", nil)

	respCh := make(chan *http.Response, 1)
	errCh := make(chan error, 1)
	go func() {
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			errCh <- err
			return
		}
		respCh <- resp
	}()

	// Wait briefly, then write more events.
	time.Sleep(150 * time.Millisecond)
	_, _ = w.Append(eventfile.KindProgress, map[string]any{"msg": "in-progress"}, false)
	_ = w.AppendDoneIdempotent(map[string]any{"outcome": "success"}, 3)
	_ = w.Close()

	// Get the response.
	var resp *http.Response
	select {
	case resp = <-respCh:
	case err := <-errCh:
		t.Fatalf("request failed: %v", err)
	case <-ctx.Done():
		t.Fatal("timeout waiting for response headers")
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	// Read the response body within the context.
	scanner := bufio.NewScanner(resp.Body)
	var events []map[string]any
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var ev map[string]any
		if err := json.Unmarshal(line, &ev); err != nil {
			t.Fatalf("parse event: %v", err)
		}
		events = append(events, ev)
	}

	if len(events) != 4 {
		t.Errorf("want 4 events, got %d: %v", len(events), events)
	}
}

// TestEventsStreamKindGated: dispatch tokens hitting an
// exec mission and exec tokens hitting a normal mission both get 403
// forbidden_kind; admin sees both.
func TestEventsStreamKindGated(t *testing.T) {
	db := setupDB(t)
	dataDir := t.TempDir()

	// kind='mission' row
	mid := ids.NewUUIDv7()
	if err := storage.InsertMission(context.Background(), db, &storage.Mission{
		ID: mid, Kind: storage.KindMission, Lane: "x", MissionName: "m",
		Status: storage.StatusDone, InputFingerprint: "fp",
		Input: []byte("{}"), TimeCreatedMs: time.Now().UnixMilli(),
	}); err != nil {
		t.Fatalf("insert mission: %v", err)
	}
	// kind='exec' row
	xid := ids.NewUUIDv7()
	if err := storage.InsertMission(context.Background(), db, &storage.Mission{
		ID: xid, Kind: storage.KindExec, Lane: "x", MissionName: "x",
		Status: storage.StatusDone, InputFingerprint: "fp",
		Input: []byte("{}"), TimeCreatedMs: time.Now().UnixMilli(),
	}); err != nil {
		t.Fatalf("insert exec: %v", err)
	}
	// Pre-create an empty events file for both so reads don't error on missing files.
	for _, id := range []string{mid, xid} {
		shard, _ := ids.ShardPath(id)
		dir := filepath.Join(dataDir, "output", shard)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		ew, err := eventfile.Create(dir, id)
		if err != nil {
			t.Fatalf("create events: %v", err)
		}
		_, _ = ew.Append(eventfile.KindQueued, map[string]any{}, false)
		_ = ew.AppendDoneIdempotent(map[string]any{"outcome": "success"}, 1)
		_ = ew.Close()
	}

	h := &handlers.EventsHandler{DataDir: dataDir, DB: db}
	mux := http.NewServeMux()
	h.Register(mux)

	call := func(id string, scope middleware.Scope) *httptest.ResponseRecorder {
		req := withScope(httptest.NewRequest("GET", "/v1/missions/"+id+"/events", nil), scope)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		return w
	}

	// dispatch on exec → 403
	if w := call(xid, middleware.ScopeDispatch); w.Code != 403 {
		t.Errorf("dispatch on exec: code=%d, want 403; body=%s", w.Code, w.Body.String())
	}
	// exec on mission → 403
	if w := call(mid, middleware.ScopeExec); w.Code != 403 {
		t.Errorf("exec on mission: code=%d, want 403", w.Code)
	}
	// admin on either → 200
	if w := call(mid, middleware.ScopeAdmin); w.Code != 200 {
		t.Errorf("admin on mission: code=%d, want 200", w.Code)
	}
	if w := call(xid, middleware.ScopeAdmin); w.Code != 200 {
		t.Errorf("admin on exec: code=%d, want 200", w.Code)
	}
}

// nonFlusherRW is a ResponseWriter that does NOT implement http.Flusher.
// Used by TestEventsStreamLogsFlusherFallback to simulate a misbehaving
// middleware wrapper that swallows the underlying Flusher.
type nonFlusherRW struct {
	header http.Header
	body   bytes.Buffer
	status int
}

func (n *nonFlusherRW) Header() http.Header {
	if n.header == nil {
		n.header = http.Header{}
	}
	return n.header
}
func (n *nonFlusherRW) Write(b []byte) (int, error) { return n.body.Write(b) }
func (n *nonFlusherRW) WriteHeader(s int)           { n.status = s }

// TestEventsStreamLogsFlusherFallback verifies a warn-level log is emitted
// when the wrapped ResponseWriter doesn't implement http.Flusher. Without
// the log, an operator misconfiguring middleware to drop the Flusher
// interface would see "stuck" streams without any clue why.
func TestEventsStreamLogsFlusherFallback(t *testing.T) {
	h, missionID, parentDir := setupEventsTest(t)
	writeEvents(t, parentDir, missionID, false)

	// Capture warn-level slog output into a buffer via injected logger.
	var buf bytes.Buffer
	jsonLog := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	h.Logger = jsonLog

	w := &nonFlusherRW{}
	req := httptest.NewRequest("GET", "/v1/missions/"+missionID+"/events", nil)
	req.SetPathValue("id", missionID)
	ctx, cancel := context.WithTimeout(req.Context(), 500*time.Millisecond)
	defer cancel()
	req = req.WithContext(ctx)
	req = withScope(req, middleware.ScopeDispatch)
	h.Stream(w, req)

	if w.status != 0 && w.status != 200 {
		t.Errorf("status=%d, want 0 or 200", w.status)
	}
	logs := buf.String()
	if !strings.Contains(logs, "flusher_unavailable") {
		t.Errorf("missing flusher_unavailable warning; logs=%s", logs)
	}
}
