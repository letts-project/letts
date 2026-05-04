package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// stubDugdale is an in-memory fake of the dugdale daemon used by CLI
// integration tests. It serves every HTTP route the letts CLI touches with
// minimal but realistic semantics — just enough to confirm the CLI uses the
// wire protocol correctly. End-to-end coverage of the real daemon lives in
// the internal/server and internal/mission test packages.
//
// Lifecycle:
//
//	s := newStubDugdale(t)            // starts httptest.Server, registers cleanup
//	url := s.URL()                    // base URL for *appCtx.BaseURLForID
//	s.ScriptMission(id, events)       // optional: provide custom NDJSON events
//	s.SetMission(id, m)               // optional: prepopulate a mission row
//	s.SetMissionOutput(id, []byte)    // optional: prepopulate output bytes
//
// All operations are safe for concurrent test access.
type stubDugdale struct {
	mu sync.Mutex

	// Applied state (last successful POST /v1/admin/apply body).
	appliedState  json.RawMessage
	appliedSource string
	appliedAtMs   int64

	// Lane state — built from the applied state's lanes map, plus paused
	// overrides from POST /pause /continue.
	pausedLanes map[string]bool

	// Mission rows keyed by mission_id.
	missions map[string]*stubMission

	// Scripted NDJSON event lines per mission_id. If unset, a default
	// queued → running → done(success) sequence is emitted.
	missionEvents map[string][]string

	// Output bytes per mission_id (stream=combined). The stub treats the
	// same buffer as the response for any stream query.
	missionOutput map[string][]byte

	// Staging entries keyed by staging_id.
	staging map[string]*stubStagingFile

	// Optional hook callbacks let individual tests override per-call
	// behaviour (e.g. inject a 503 on the second PUT). Each hook is set
	// under s.mu; the handler reads under the same lock and runs the hook
	// outside the lock to avoid deadlocks on slow handlers.
	hooks stubHooks

	srv *httptest.Server
}

// stubMission is the subset of the real mission row that the stub stores.
// Field names mirror the daemon JSON shape so we can MarshalJSON straight
// through; missing optional fields use Go zero values (omitempty).
type stubMission struct {
	MissionID        string                     `json:"mission_id"`
	Kind             string                     `json:"kind,omitempty"`
	Lane             string                     `json:"lane,omitempty"`
	MissionName      string                     `json:"mission_name,omitempty"`
	DisplayName      string                     `json:"display_name,omitempty"`
	GroupID          string                     `json:"group_id,omitempty"`
	Status           string                     `json:"status"`
	Outcome          string                     `json:"outcome,omitempty"`
	ExitCode         *int                       `json:"exit_code,omitempty"`
	Signal           string                     `json:"signal,omitempty"`
	FailReason       string                     `json:"fail_reason,omitempty"`
	FailMessage      string                     `json:"fail_message,omitempty"`
	FailDetails      json.RawMessage            `json:"fail_details,omitempty"`
	Return           json.RawMessage            `json:"return,omitempty"`
	Input            json.RawMessage            `json:"input,omitempty"`
	InputFingerprint string                     `json:"input_fingerprint,omitempty"`
	Pid              int                        `json:"pid,omitempty"`
	TimeCreatedMs    int64                      `json:"time_created,omitempty"`
	TimeStartedMs    int64                      `json:"time_started,omitempty"`
	TimeFinishedMs   int64                      `json:"time_finished,omitempty"`
	DurationMs       int64                      `json:"duration_ms,omitempty"`
	TimeoutMs        int64                      `json:"timeout_ms,omitempty"`
	TruncatedStdout  bool                       `json:"truncated_stdout,omitempty"`
	TruncatedStderr  bool                       `json:"truncated_stderr,omitempty"`
	RestartedFrom    string                     `json:"restarted_from,omitempty"`
	Inputs           []stubMissionFile          `json:"inputs,omitempty"`
	Outputs          map[string]stubMissionFile `json:"outputs,omitempty"`
}

// stubMissionFile is one entry in inputs[] or outputs{}.
type stubMissionFile struct {
	Role      string `json:"role,omitempty"`
	StagingID string `json:"staging_id"`
	Sha256    string `json:"sha256,omitempty"`
	Size      int64  `json:"size,omitempty"`
}

// stubStagingFile is the in-memory staging entry. Bytes is the entire
// uploaded body; State follows the daemon's enum (uploading|complete|
// deleting).
type stubStagingFile struct {
	StagingID     string
	Sha256        string
	State         string
	RefKind       string
	Role          string
	Size          int64
	BytesReceived int64
	TimeCreated   int64
	MissionID     string // for ListStaging filter
	Bytes         []byte
}

// stubHooks lets tests inject failure or instrumentation without rewriting
// the handler. Each hook is invoked at the top of its handler; returning
// (true, …) short-circuits the default response.
//
// There are two flavours of knob:
//
//  1. Callback hooks (PutStaging, Dispatch, …) take full responsibility for
//     the response — return handled==true to opt out of the default path.
//  2. Convenience fields (ApplyStatus, MissionGet404, Put503Times, …) wrap
//     common failure-injection patterns so individual tests stay one-liners.
//     Convenience fields run before the callback hook of the same handler so
//     a test can opt into both (rare in practice).
type stubHooks struct {
	// PutStaging intercepts PUT /v1/staging/{id}. If handled==true the hook
	// fully owns the response; otherwise the default upload path runs.
	PutStaging func(w http.ResponseWriter, r *http.Request, id string) (handled bool)

	// Dispatch intercepts POST /v1/dispatch identically.
	Dispatch func(w http.ResponseWriter, r *http.Request) (handled bool)

	// Apply intercepts POST /v1/admin/apply identically.
	Apply func(w http.ResponseWriter, r *http.Request) (handled bool)

	// MissionGet intercepts GET /v1/missions/{id} identically.
	MissionGet func(w http.ResponseWriter, r *http.Request, id string) (handled bool)

	// MissionEvents intercepts GET /v1/missions/{id}/events identically.
	MissionEvents func(w http.ResponseWriter, r *http.Request, id string) (handled bool)

	// --- convenience knobs --------------------------------------------

	// ApplyStatus, if non-zero, short-circuits POST /v1/admin/apply with the
	// given status. ApplyBody (when non-empty) is the literal response body.
	ApplyStatus int
	ApplyBody   string

	// DispatchStatus, if non-zero, short-circuits POST /v1/dispatch with the
	// given status. DispatchBody (when non-empty) is the literal response body.
	DispatchStatus int
	DispatchBody   string

	// MissionGet404, when true, makes GET /v1/missions/{id} return a 404
	// `not_found` envelope without consulting the missions map. Used by the
	// by-id fan-out test to mark stubs that don't own the mission.
	MissionGet404 bool

	// HangEvents, when true, makes GET /v1/missions/{id}/events block until
	// the request context is cancelled (i.e. forever from the client's POV).
	// Pairs with `letts run --wait-timeout=...` to exercise WaitTimeoutError.
	HangEvents bool

	// Put503Times sets a counter: each PUT /v1/staging/{id} decrements it and
	// returns 503 while the counter is > 0, then the default upload path runs
	// on subsequent calls. Used to exercise sticky retry in doStagingPut.
	Put503Times int
}

// newStubDugdale starts an httptest.Server with stub handlers for every
// route the CLI uses, registers Close() via t.Cleanup, and returns the
// running stub. Tests typically call s.URL() and stitch it into appCtx via
// BaseURLForID.
func newStubDugdale(t *testing.T) *stubDugdale {
	t.Helper()
	s := &stubDugdale{
		pausedLanes:   map[string]bool{},
		missions:      map[string]*stubMission{},
		missionEvents: map[string][]string{},
		missionOutput: map[string][]byte{},
		staging:       map[string]*stubStagingFile{},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/admin/apply", s.handleApply)
	mux.HandleFunc("GET /v1/admin/state", s.handleState)
	mux.HandleFunc("POST /v1/admin/lanes/{name}/pause", s.handlePause)
	mux.HandleFunc("POST /v1/admin/lanes/{name}/continue", s.handleContinue)
	mux.HandleFunc("GET /v1/dugdale", s.handleDugdale)
	mux.HandleFunc("GET /v1/lanes", s.handleLanes)
	mux.HandleFunc("POST /v1/dispatch", s.handleDispatch)
	mux.HandleFunc("GET /v1/missions/{id}/events", s.handleMissionEvents)
	mux.HandleFunc("GET /v1/missions/{id}/output", s.handleMissionOutput)
	mux.HandleFunc("POST /v1/missions/{id}/restart", s.handleMissionRestart)
	mux.HandleFunc("POST /v1/missions/{id}/kill", s.handleMissionKill)
	mux.HandleFunc("DELETE /v1/missions/{id}", s.handleMissionDelete)
	mux.HandleFunc("GET /v1/missions/{id}", s.handleMissionGet)
	mux.HandleFunc("GET /v1/missions", s.handleMissionsList)
	mux.HandleFunc("POST /v1/missions/bulk-restart", s.handleBulkRestart)
	mux.HandleFunc("POST /v1/missions/bulk-delete", s.handleBulkDelete)
	mux.HandleFunc("PUT /v1/staging/{id}", s.handleStagingPut)
	mux.HandleFunc("HEAD /v1/staging/{id}", s.handleStagingHead)
	mux.HandleFunc("GET /v1/staging/{id}", s.handleStagingGet)
	mux.HandleFunc("DELETE /v1/staging/{id}", s.handleStagingDelete)
	mux.HandleFunc("GET /v1/staging/by-content/{sha}", s.handleStagingByContent)
	mux.HandleFunc("GET /v1/staging", s.handleStagingList)
	mux.HandleFunc("GET /v1/healthz", s.handleHealthz)
	mux.HandleFunc("GET /v1/readyz", s.handleHealthz)

	s.srv = httptest.NewServer(mux)
	t.Cleanup(s.srv.Close)
	return s
}

// URL returns the stub's base URL (no trailing slash). Pass into
// appCtx.BaseURLForID for the dugdale id under test.
func (s *stubDugdale) URL() string { return s.srv.URL }

// ScriptMission registers NDJSON event lines that the events handler will
// emit for the given mission id. Each line should already be a single-line
// JSON object (no trailing newline). The handler appends \n itself.
func (s *stubDugdale) ScriptMission(id string, lines []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.missionEvents[id] = lines
}

// SetMission prepopulates a mission row keyed by mission_id. Tests use this
// to seed responses for GET /v1/missions/{id} and the events stream
// without going through POST /v1/dispatch.
func (s *stubDugdale) SetMission(m *stubMission) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *m
	s.missions[m.MissionID] = &cp
}

// SetMissionOutput stores combined-stream output bytes for the mission.
func (s *stubDugdale) SetMissionOutput(id string, b []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := append([]byte(nil), b...)
	s.missionOutput[id] = cp
}

// SetStagingFile registers a staging entry as if a successful upload had
// completed. Useful for testing dispatch flows that reference pre-existing
// uploads.
func (s *stubDugdale) SetStagingFile(sf *stubStagingFile) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *sf
	cp.Bytes = append([]byte(nil), sf.Bytes...)
	if cp.State == "" {
		cp.State = "complete"
	}
	if cp.Sha256 == "" && len(cp.Bytes) > 0 {
		sum := sha256.Sum256(cp.Bytes)
		cp.Sha256 = hex.EncodeToString(sum[:])
	}
	if cp.Size == 0 {
		cp.Size = int64(len(cp.Bytes))
		cp.BytesReceived = cp.Size
	}
	s.staging[sf.StagingID] = &cp
}

// SetHooks installs the given hook table. Tests typically construct a fresh
// stubHooks with only the hook fields they need.
func (s *stubDugdale) SetHooks(h stubHooks) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hooks = h
}

// AppliedState returns the last raw apply body. Empty if no apply has been
// recorded.
func (s *stubDugdale) AppliedState() json.RawMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append(json.RawMessage(nil), s.appliedState...)
}

// IsPaused reports whether a pause-flag is stored for the named lane.
func (s *stubDugdale) IsPaused(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pausedLanes[name]
}

// MissionRow returns a copy of the stored mission row, or nil when absent.
// Tests use it to assert post-conditions of destructive commands (e.g. a
// refused fan-out delete must leave Status untouched).
func (s *stubDugdale) MissionRow(id string) *stubMission {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.missions[id]
	if !ok {
		return nil
	}
	cp := *m
	return &cp
}

// MissionCount returns the number of stored mission rows. Restart creates a
// new row, so tests can assert "nothing executed" by checking the count is
// unchanged after a refused destructive fan-out.
func (s *stubDugdale) MissionCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.missions)
}

// --- admin ---

func (s *stubDugdale) handleApply(w http.ResponseWriter, r *http.Request) {
	hooks := s.cloneHooks()
	if hooks.ApplyStatus != 0 {
		writeStubRaw(w, hooks.ApplyStatus, hooks.ApplyBody)
		return
	}
	if hooks.Apply != nil {
		if hooks.Apply(w, r) {
			return
		}
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeStubError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	// Parse minimally: we only need the lanes map to seed lane responses.
	var parsed struct {
		Lanes map[string]struct {
			Concurrency int  `json:"concurrency"`
			Paused      bool `json:"paused,omitempty"`
		} `json:"lanes"`
	}
	if len(body) > 0 {
		_ = json.Unmarshal(body, &parsed)
	}
	s.mu.Lock()
	s.appliedState = append(json.RawMessage(nil), body...)
	s.appliedAtMs = time.Now().UnixMilli()
	s.appliedSource = "apply"
	// Seed paused-state map from the applied lanes (preserving any explicit
	// runtime overrides set via /pause /continue).
	for name, cfg := range parsed.Lanes {
		if cfg.Paused {
			s.pausedLanes[name] = true
		}
	}
	s.mu.Unlock()
	started := []string{}
	for name := range parsed.Lanes {
		started = append(started, name)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"diff":    map[string]any{"reason": ""},
		"started": started,
	})
}

func (s *stubDugdale) handleState(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	resp := map[string]any{
		"applied_at": nil,
		"source":     s.appliedSource,
	}
	if s.appliedAtMs > 0 {
		resp["applied_at"] = s.appliedAtMs
	}
	if len(s.appliedState) > 0 {
		resp["state"] = json.RawMessage(s.appliedState)
	} else {
		resp["state"] = map[string]any{"lanes": map[string]any{}}
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *stubDugdale) handlePause(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	s.mu.Lock()
	s.pausedLanes[name] = true
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]string{"status": "paused"})
}

func (s *stubDugdale) handleContinue(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	s.mu.Lock()
	s.pausedLanes[name] = false
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]string{"status": "running"})
}

// --- inspect ---

func (s *stubDugdale) handleDugdale(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var queued, running int
	for _, m := range s.missions {
		switch m.Status {
		case "queued":
			queued++
		case "running":
			running++
		}
	}
	resp := map[string]any{
		"version":        "stub",
		"uptime_seconds": 1.0,
		"applied_at":     nil,
		"queue_summary":  map[string]int{"queued": queued, "running": running},
	}
	if s.appliedAtMs > 0 {
		resp["applied_at"] = s.appliedAtMs
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *stubDugdale) handleLanes(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Build lane list from applied state.
	var parsed struct {
		Lanes map[string]struct {
			Concurrency int  `json:"concurrency"`
			Paused      bool `json:"paused,omitempty"`
		} `json:"lanes"`
	}
	if len(s.appliedState) > 0 {
		_ = json.Unmarshal(s.appliedState, &parsed)
	}
	out := []map[string]any{}
	for name, cfg := range parsed.Lanes {
		paused := cfg.Paused || s.pausedLanes[name]
		var queued, running int
		for _, m := range s.missions {
			if m.Lane != name {
				continue
			}
			switch m.Status {
			case "queued":
				queued++
			case "running":
				running++
			}
		}
		out = append(out, map[string]any{
			"name":        name,
			"concurrency": cfg.Concurrency,
			"paused":      paused,
			"queued":      queued,
			"running":     running,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

// --- dispatch ---

func (s *stubDugdale) handleDispatch(w http.ResponseWriter, r *http.Request) {
	hooks := s.cloneHooks()
	if hooks.DispatchStatus != 0 {
		writeStubRaw(w, hooks.DispatchStatus, hooks.DispatchBody)
		return
	}
	if hooks.Dispatch != nil {
		if hooks.Dispatch(w, r) {
			return
		}
	}
	missionID := r.Header.Get("Idempotency-Key")
	if missionID == "" {
		writeStubError(w, http.StatusBadRequest, "bad_request", "Idempotency-Key required")
		return
	}
	var req struct {
		Mission string          `json:"mission"`
		Lane    string          `json:"lane"`
		Input   json.RawMessage `json:"input"`
		Files   []struct {
			Role      string `json:"role"`
			StagingID string `json:"staging_id"`
		} `json:"files"`
		Timeout string `json:"timeout"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeStubError(w, http.StatusBadRequest, "bad_request", "invalid JSON")
		return
	}
	s.mu.Lock()
	// Idempotency replay: if missionID exists, return existing status.
	if existing, ok := s.missions[missionID]; ok {
		status := existing.Status
		s.mu.Unlock()
		writeJSON(w, http.StatusOK, map[string]any{
			"mission_id": missionID,
			"status":     status,
		})
		return
	}
	m := &stubMission{
		MissionID:     missionID,
		Kind:          "mission",
		Lane:          req.Lane,
		MissionName:   req.Mission,
		Status:        "queued",
		Input:         append(json.RawMessage(nil), req.Input...),
		TimeCreatedMs: time.Now().UnixMilli(),
	}
	for _, f := range req.Files {
		m.Inputs = append(m.Inputs, stubMissionFile{Role: f.Role, StagingID: f.StagingID})
	}
	s.missions[missionID] = m
	s.mu.Unlock()
	writeJSON(w, http.StatusAccepted, map[string]any{
		"mission_id": missionID,
		"status":     "queued",
	})
}

// --- missions ---

func (s *stubDugdale) handleMissionGet(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	hooks := s.cloneHooks()
	if hooks.MissionGet404 {
		writeStubError(w, http.StatusNotFound, "not_found", "mission not found")
		return
	}
	if hooks.MissionGet != nil {
		if hooks.MissionGet(w, r, id) {
			return
		}
	}
	s.mu.Lock()
	m, ok := s.missions[id]
	s.mu.Unlock()
	if !ok {
		writeStubError(w, http.StatusNotFound, "not_found", "mission not found")
		return
	}
	resp := stubMissionToMap(m)
	writeJSON(w, http.StatusOK, resp)
}

func (s *stubDugdale) handleMissionsList(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	wantStatus := q.Get("status")
	wantOutcome := q.Get("outcome")
	wantLane := q.Get("lane")
	wantMission := q.Get("mission")
	wantKind := q.Get("kind")

	s.mu.Lock()
	out := make([]map[string]any, 0, len(s.missions))
	for _, m := range s.missions {
		if wantStatus != "" && m.Status != wantStatus {
			continue
		}
		if wantOutcome != "" && m.Outcome != wantOutcome {
			continue
		}
		if wantLane != "" && m.Lane != wantLane {
			continue
		}
		if wantMission != "" && m.MissionName != wantMission {
			continue
		}
		if wantKind != "" && m.Kind != wantKind {
			continue
		}
		out = append(out, stubMissionToMap(m))
	}
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"missions": out})
}

func (s *stubDugdale) handleMissionEvents(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	hooks := s.cloneHooks()
	if hooks.HangEvents {
		// Write headers so the client knows the stream is open, then block
		// on the request context until the client cancels (typically via
		// --wait-timeout firing). flusher.Flush() pushes the headers out
		// even when no body bytes are written.
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		<-r.Context().Done()
		return
	}
	if hooks.MissionEvents != nil {
		if hooks.MissionEvents(w, r, id) {
			return
		}
	}
	s.mu.Lock()
	m, exists := s.missions[id]
	scripted, hasScript := s.missionEvents[id]
	s.mu.Unlock()

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)

	lines := scripted
	if !hasScript {
		// Default: queued → running → done(success).
		if !exists {
			// Mission not registered — emit nothing terminal; the client will
			// see EOF without a done event.
			return
		}
		lines = defaultEventLines(id, m.Outputs)
	}
	for _, line := range lines {
		if _, err := io.WriteString(w, line); err != nil {
			return
		}
		if _, err := io.WriteString(w, "\n"); err != nil {
			return
		}
		if flusher != nil {
			flusher.Flush()
		}
	}
}

func (s *stubDugdale) handleMissionOutput(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	s.mu.Lock()
	out, ok := s.missionOutput[id]
	s.mu.Unlock()
	q := r.URL.Query()
	stream := q.Get("stream")
	switch stream {
	case "":
		writeStubError(w, http.StatusBadRequest, "bad_request", "stream required")
		return
	case "stdout", "stderr", "combined":
		// All three streams serve the same buffer in the stub.
	default:
		writeStubError(w, http.StatusBadRequest, "bad_request", "unknown stream")
		return
	}
	if stream == "combined" {
		w.Header().Set("Content-Type", "application/x-ndjson")
	} else {
		w.Header().Set("Content-Type", "application/octet-stream")
	}
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	if ok {
		_, _ = w.Write(out)
	}
}

func (s *stubDugdale) handleMissionRestart(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	s.mu.Lock()
	old, ok := s.missions[id]
	if !ok {
		s.mu.Unlock()
		writeStubError(w, http.StatusNotFound, "not_found", "mission not found")
		return
	}
	// Allocate a deterministic new id using nanos so tests can assert on a
	// non-empty UUID-like string without taking on the real ids package.
	newID := fmt.Sprintf("stub-restart-%d", time.Now().UnixNano())
	nm := *old
	nm.MissionID = newID
	nm.Status = "queued"
	nm.Outcome = ""
	nm.RestartedFrom = id
	nm.TimeCreatedMs = time.Now().UnixMilli()
	nm.TimeStartedMs = 0
	nm.TimeFinishedMs = 0
	s.missions[newID] = &nm
	s.mu.Unlock()
	writeJSON(w, http.StatusCreated, map[string]any{
		"mission_id":     newID,
		"restarted_from": id,
		"status":         "queued",
	})
}

func (s *stubDugdale) handleMissionKill(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	// Drain optional body — daemon accepts {"signal":"TERM"} for forward-compat.
	if r.Body != nil {
		_, _ = io.Copy(io.Discard, r.Body)
	}
	s.mu.Lock()
	m, ok := s.missions[id]
	if !ok {
		s.mu.Unlock()
		writeStubError(w, http.StatusNotFound, "not_found", "mission not found")
		return
	}
	switch m.Status {
	case "queued":
		m.Status = "done"
		m.Outcome = "killed"
		m.FailReason = "killed_by_api"
		ec := 0
		m.ExitCode = &ec
		m.TimeFinishedMs = time.Now().UnixMilli()
		s.mu.Unlock()
		writeJSON(w, http.StatusOK, map[string]string{"status": "killed"})
	case "running":
		s.mu.Unlock()
		writeJSON(w, http.StatusOK, map[string]string{"status": "kill_sent"})
	default:
		s.mu.Unlock()
		writeStubError(w, http.StatusConflict, "mission_done", "already done")
	}
}

func (s *stubDugdale) handleMissionDelete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	force := strings.EqualFold(r.URL.Query().Get("force"), "true")
	s.mu.Lock()
	m, ok := s.missions[id]
	if !ok {
		s.mu.Unlock()
		writeStubError(w, http.StatusNotFound, "not_found", "mission not found")
		return
	}
	if m.Status == "running" && !force {
		s.mu.Unlock()
		writeStubError(w, http.StatusConflict, "mission_running",
			"running mission cannot be deleted")
		return
	}
	m.Status = "deleting"
	s.mu.Unlock()
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "deletion_pending"})
}

func (s *stubDugdale) handleBulkRestart(w http.ResponseWriter, r *http.Request) {
	body, err := decodeBulkBody(r)
	if err != nil {
		writeStubError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	results := make([]map[string]any, 0, len(body.IDs))
	for _, id := range body.IDs {
		s.mu.Lock()
		old, ok := s.missions[id]
		if !ok {
			s.mu.Unlock()
			results = append(results, map[string]any{"id": id, "ok": false, "error": "not_found"})
			continue
		}
		newID := fmt.Sprintf("stub-restart-%d", time.Now().UnixNano())
		nm := *old
		nm.MissionID = newID
		nm.Status = "queued"
		nm.RestartedFrom = id
		nm.TimeCreatedMs = time.Now().UnixMilli()
		s.missions[newID] = &nm
		s.mu.Unlock()
		results = append(results, map[string]any{
			"id":         id,
			"ok":         true,
			"mission_id": newID,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"results": results})
}

func (s *stubDugdale) handleBulkDelete(w http.ResponseWriter, r *http.Request) {
	body, err := decodeBulkBody(r)
	if err != nil {
		writeStubError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	results := make([]map[string]any, 0, len(body.IDs))
	for _, id := range body.IDs {
		s.mu.Lock()
		m, ok := s.missions[id]
		if !ok {
			s.mu.Unlock()
			results = append(results, map[string]any{"id": id, "ok": false, "error": "not_found"})
			continue
		}
		if m.Status == "running" && !body.Force {
			s.mu.Unlock()
			results = append(results, map[string]any{"id": id, "ok": false, "error": "mission_running"})
			continue
		}
		m.Status = "deleting"
		s.mu.Unlock()
		results = append(results, map[string]any{
			"id":     id,
			"ok":     true,
			"status": "deletion_pending",
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"results": results})
}

// --- staging ---

func (s *stubDugdale) handleStagingPut(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	// Put503Times must be decremented under the mu lock so concurrent
	// retries land on monotonically decreasing counter values.
	s.mu.Lock()
	if s.hooks.Put503Times > 0 {
		s.hooks.Put503Times--
		s.mu.Unlock()
		// Drain the request body so the client's HTTP layer doesn't see
		// "connection reset" before the 503 line is written.
		_, _ = io.Copy(io.Discard, r.Body)
		writeStubError(w, http.StatusServiceUnavailable,
			"service_unavailable", "stub injected 503")
		return
	}
	hook := s.hooks.PutStaging
	s.mu.Unlock()
	if hook != nil {
		if hook(w, r, id) {
			return
		}
	}
	declared := strings.ToLower(r.Header.Get("X-Letts-Sha256"))
	if declared == "" {
		writeStubError(w, http.StatusBadRequest, "bad_request", "X-Letts-Sha256 required")
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeStubError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	// Optional Content-Range parse — for the stub a resume upload simply
	// overwrites the byte buffer.
	cr := r.Header.Get("Content-Range")
	total := r.ContentLength
	if cr != "" {
		// "bytes start-end/total" — extract total.
		if _, t, ok := parseContentRangeTotal(cr); ok {
			total = t
		}
	}
	sum := sha256.Sum256(body)
	actual := hex.EncodeToString(sum[:])
	if cr == "" && actual != declared {
		writeStubError(w, http.StatusConflict, "content_mismatch",
			fmt.Sprintf("computed %s != declared %s", actual, declared))
		return
	}
	if total < 0 {
		total = int64(len(body))
	}
	s.mu.Lock()
	if existing, ok := s.staging[id]; ok && existing.State == "complete" {
		// Re-PUT of complete file — no body consumption needed in real
		// daemon; stub still accepted body. Return 200.
		s.mu.Unlock()
		writeJSON(w, http.StatusOK, map[string]any{
			"staging_id":  id,
			"sha256":      existing.Sha256,
			"size":        existing.Size,
			"ttl_seconds": 3600,
		})
		return
	}
	s.staging[id] = &stubStagingFile{
		StagingID:     id,
		Sha256:        actual,
		State:         "complete",
		Size:          total,
		BytesReceived: total,
		Bytes:         body,
		TimeCreated:   time.Now().UnixMilli(),
	}
	s.mu.Unlock()
	writeJSON(w, http.StatusCreated, map[string]any{
		"staging_id":  id,
		"sha256":      actual,
		"size":        total,
		"ttl_seconds": 3600,
	})
}

func (s *stubDugdale) handleStagingHead(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	s.mu.Lock()
	sf, ok := s.staging[id]
	s.mu.Unlock()
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("X-Letts-Sha256", sf.Sha256)
	switch sf.State {
	case "complete":
		w.Header().Set("Content-Length", strconv.FormatInt(sf.Size, 10))
		w.Header().Set("X-Letts-Upload-Status", "complete")
	case "uploading":
		w.Header().Set("X-Letts-Upload-Status", "incomplete")
		w.Header().Set("X-Letts-Bytes-Received", strconv.FormatInt(sf.BytesReceived, 10))
		w.Header().Set("X-Letts-Total-Size", strconv.FormatInt(sf.Size, 10))
	default:
		w.Header().Set("X-Letts-Upload-Status", sf.State)
	}
	w.WriteHeader(http.StatusOK)
}

func (s *stubDugdale) handleStagingGet(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	s.mu.Lock()
	sf, ok := s.staging[id]
	s.mu.Unlock()
	if !ok {
		writeStubError(w, http.StatusNotFound, "not_found", "staging not found")
		return
	}
	if sf.State != "complete" {
		writeStubError(w, http.StatusConflict, "staging_uploading", "upload in progress")
		return
	}
	body := sf.Bytes
	// Minimal Range support — for resume tests. "bytes=N-" returns suffix.
	if rng := r.Header.Get("Range"); rng != "" {
		if start, ok := parseRangeStart(rng); ok && start >= 0 && start < int64(len(body)) {
			w.Header().Set("Content-Range",
				fmt.Sprintf("bytes %d-%d/%d", start, len(body)-1, len(body)))
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("X-Letts-Sha256", sf.Sha256)
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(body[start:])
			return
		}
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-Letts-Sha256", sf.Sha256)
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func (s *stubDugdale) handleStagingDelete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	s.mu.Lock()
	_, ok := s.staging[id]
	if ok {
		delete(s.staging, id)
	}
	s.mu.Unlock()
	if !ok {
		writeStubError(w, http.StatusNotFound, "not_found", "staging not found")
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "deletion_pending"})
}

func (s *stubDugdale) handleStagingList(w http.ResponseWriter, r *http.Request) {
	missionID := r.URL.Query().Get("mission_id")
	if missionID == "" {
		writeStubError(w, http.StatusBadRequest, "bad_request", "mission_id required")
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []map[string]any{}
	for _, sf := range s.staging {
		if sf.MissionID != missionID {
			continue
		}
		out = append(out, map[string]any{
			"staging_id":     sf.StagingID,
			"state":          sf.State,
			"sha256":         sf.Sha256,
			"size":           sf.Size,
			"bytes_received": sf.BytesReceived,
			"time_created":   sf.TimeCreated,
			"ref_kind":       sf.RefKind,
			"role":           sf.Role,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"staging": out})
}

func (s *stubDugdale) handleStagingByContent(w http.ResponseWriter, r *http.Request) {
	sha := strings.ToLower(r.PathValue("sha"))
	sizeStr := r.URL.Query().Get("size")
	if sizeStr == "" {
		writeStubError(w, http.StatusBadRequest, "bad_request", "size required")
		return
	}
	size, err := strconv.ParseInt(sizeStr, 10, 64)
	if err != nil || size <= 0 {
		writeStubError(w, http.StatusBadRequest, "bad_request", "invalid size")
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sf := range s.staging {
		if sf.Sha256 == sha && sf.Size == size && sf.State == "complete" {
			writeJSON(w, http.StatusOK, map[string]any{
				"staging_id": sf.StagingID,
				"sha256":     sf.Sha256,
				"size":       sf.Size,
			})
			return
		}
	}
	writeStubError(w, http.StatusNotFound, "not_found", "no match")
}

// --- health ---

func (s *stubDugdale) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// --- helpers ---

func (s *stubDugdale) cloneHooks() stubHooks {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hooks
}

// writeJSON serialises v and writes it with the given status. Uses the
// daemon's content-type ("application/json; charset=utf-8") so the CLI
// decodes responses identically.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeStubError matches the daemon's error envelope:
//
//	{"error": "<code>", "message": "<text>", "details": null}
//
// The stub uses an empty details object for simplicity.
func writeStubError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, map[string]any{
		"error":   code,
		"message": msg,
	})
}

// writeStubRaw writes a literal status/body pair. Used by the
// convenience hooks (ApplyStatus/ApplyBody etc.) so tests can supply the
// exact envelope they want without going through json.Marshal.
//
// If body is empty the response has no body. Content-Type is JSON if the
// body starts with `{` or `[`, otherwise text/plain.
func writeStubRaw(w http.ResponseWriter, status int, body string) {
	ct := "text/plain; charset=utf-8"
	trimmed := strings.TrimLeft(body, " \t\n\r")
	if strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[") {
		ct = "application/json; charset=utf-8"
	}
	w.Header().Set("Content-Type", ct)
	w.WriteHeader(status)
	if body != "" {
		_, _ = io.WriteString(w, body)
	}
}

// decodeBulkBody parses the {"ids":[...], "force":bool?} payload used by
// both bulk-restart and bulk-delete.
func decodeBulkBody(r *http.Request) (struct {
	IDs   []string `json:"ids"`
	Force bool     `json:"force,omitempty"`
}, error) {
	var body struct {
		IDs   []string `json:"ids"`
		Force bool     `json:"force,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		return body, err
	}
	if len(body.IDs) == 0 {
		return body, errors.New("ids required")
	}
	return body, nil
}

// stubMissionToMap converts a stubMission to the map shape used by the real
// daemon's buildMissionResponse, so CLI decoders see identical JSON.
func stubMissionToMap(m *stubMission) map[string]any {
	b, _ := json.Marshal(m)
	out := map[string]any{}
	_ = json.Unmarshal(b, &out)
	// inputs/outputs default to non-nil empty containers (matches daemon).
	if _, ok := out["inputs"]; !ok {
		out["inputs"] = []any{}
	}
	if _, ok := out["outputs"]; !ok {
		out["outputs"] = map[string]any{}
	}
	return out
}

// defaultEventLines emits the three-event sequence used when no test has
// scripted the mission: queued, running pid=1, done outcome=success. When
// outputs is non-empty, the done event carries the outputs map
// so --output-file can download via the done event
// without a follow-up GET /v1/missions/{id}.
func defaultEventLines(missionID string, outputs map[string]stubMissionFile) []string {
	now := time.Now().UnixMilli()
	queued := mustMarshalLine(map[string]any{
		"seq":          int64(1),
		"event":        "queued",
		"mission_id":   missionID,
		"time_created": now,
	})
	running := mustMarshalLine(map[string]any{
		"seq":          int64(2),
		"event":        "running",
		"time":         now,
		"pid":          1,
		"time_started": now,
	})
	doneFields := map[string]any{
		"seq":           int64(3),
		"event":         "done",
		"time_finished": now,
		"duration_ms":   int64(0),
		"outcome":       "success",
		"exit_code":     0,
		"return":        json.RawMessage(`{}`),
	}
	if len(outputs) > 0 {
		outsMap := make(map[string]map[string]any, len(outputs))
		for role, f := range outputs {
			outsMap[role] = map[string]any{
				"staging_id": f.StagingID,
				"sha256":     f.Sha256,
				"size":       f.Size,
			}
		}
		doneFields["outputs"] = outsMap
	}
	done := mustMarshalLine(doneFields)
	return []string{queued, running, done}
}

func mustMarshalLine(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// parseContentRangeTotal extracts the total bytes from a Content-Range
// header value of the form "bytes <start>-<end>/<total>". Returns
// (start, total, true) on success.
func parseContentRangeTotal(cr string) (int64, int64, bool) {
	cr = strings.TrimSpace(cr)
	if !strings.HasPrefix(cr, "bytes ") {
		return 0, 0, false
	}
	rest := strings.TrimPrefix(cr, "bytes ")
	slash := strings.LastIndex(rest, "/")
	if slash < 0 {
		return 0, 0, false
	}
	total, err := strconv.ParseInt(rest[slash+1:], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	dash := strings.Index(rest[:slash], "-")
	if dash < 0 {
		return 0, total, true
	}
	start, err := strconv.ParseInt(rest[:dash], 10, 64)
	if err != nil {
		return 0, total, true
	}
	return start, total, true
}

// parseRangeStart parses the start of a "bytes=N-" or "bytes=N-M" Range
// request header. Returns (start, true) on success.
func parseRangeStart(rng string) (int64, bool) {
	rng = strings.TrimSpace(rng)
	if !strings.HasPrefix(rng, "bytes=") {
		return 0, false
	}
	body := strings.TrimPrefix(rng, "bytes=")
	dash := strings.Index(body, "-")
	if dash < 0 {
		return 0, false
	}
	n, err := strconv.ParseInt(body[:dash], 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}
