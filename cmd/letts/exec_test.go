package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"letts/internal/ids"
	"letts/pkg/lettsclient"
	"letts/pkg/lettsconfig"
)

// execHostStub fakes a single dugdale that accepts POST /v1/exec/dispatch
// and serves GET /v1/missions/{id}/events?follow=true plus
// /v1/missions/{id}/output?stream=...&follow=true. Captures every dispatch
// payload (including the Idempotency-Key→MissionID echo) for assertions.
//
// When constructed via newExecHostStubWithStaging, also serves:
//   - GET /v1/staging/by-content/{sha}?size=N → 404 (forces upload)
//   - PUT /v1/staging/{id}                    → 201 (counts uploads)
//
// When constructed via newExecHostStubWithStagingAndOutputs, additionally:
//   - emits done event with outputs[role].staging_id for each declared role
//   - serves GET /v1/staging/{id} with the role's bytes (for download path)
type execHostStub struct {
	id          string
	srv         *httptest.Server
	mu          sync.Mutex
	dispatched  []lettsclient.ExecRequest
	plan        execHostPlan
	stagingPuts int // PUT /v1/staging/{id} counter (staging-enabled stubs only)

	// outputBytes / outputStagingID hold role→bytes and role→staging_id for
	// stubs that simulate a successful exec with --out role downloads.
	// outputStagingID is a stable per-role UUIDv7 minted at construction so
	// the done event and GET /v1/staging/{id} agree on the same id.
	outputBytes     map[string][]byte
	outputStagingID map[string]string
}

// execHostPlan tells the stub how to behave on dispatch/events/output. A
// zero plan = happy path (202 queued, events emits done immediately with
// outcome=success exit_code=0, empty output streams).
type execHostPlan struct {
	dispatchStatus int    // 0 → 202; any other → error body
	dispatchBody   string // body returned when dispatchStatus is set
	doneOutcome    string // "success"|"failed"|"killed"|... — required if !streamHang
	doneExitCode   int
	doneAfter      time.Duration // optional delay before done event
	stdoutBytes    string
	stderrBytes    string
	streamHang     bool // events stream never emits done — blocks until ctx cancel
}

// newExecHostStub spins a per-test httptest.Server. Caller must defer close().
func newExecHostStub(t *testing.T, id string, plan execHostPlan) *execHostStub {
	t.Helper()
	hs := &execHostStub{id: id, plan: plan}
	hs.srv = httptest.NewServer(buildExecHostMux(hs, plan, false))
	return hs
}

// newExecHostStubWithStaging is the staging variant: same as newExecHostStub but
// also serves the staging endpoints so single- and multi-host script
// upload flows can exercise uploadOrReuse end-to-end.
//
//   - GET /v1/staging/by-content/{sha}?size=N → 404 (forces upload — tests
//     covering the dedup-hit path stub the daemon directly)
//   - PUT /v1/staging/{id}                    → 201, increments stagingPuts
//
// HEAD /v1/staging/{id} also returns 404 so UploadFile takes the initial-PUT
// branch (PutStagingInitial). No partial-resume coverage here.
func newExecHostStubWithStaging(t *testing.T, id string, plan execHostPlan) *execHostStub {
	t.Helper()
	hs := &execHostStub{id: id, plan: plan}
	hs.srv = httptest.NewServer(buildExecHostMux(hs, plan, true))
	return hs
}

// newExecHostStubWithStagingAndOutputs is the outputs variant: same as
// newExecHostStubWithStaging plus simulates --out role downloads. For each
// (role → bytes) entry it (a) emits outputs[role].staging_id in the done
// event, and (b) serves GET /v1/staging/{id} with the role's bytes. The
// staging IDs are stable per-role UUIDv7s minted at construction time so
// the done event and GET endpoint agree.
//
// Used by tests that exercise the single-host --out download path
// end-to-end (refuse overwrite, empty file allowed, success path) via
// downloadAllAtomic.
func newExecHostStubWithStagingAndOutputs(t *testing.T, id string, outputs map[string][]byte, plan execHostPlan) *execHostStub {
	t.Helper()
	hs := &execHostStub{id: id, plan: plan}
	hs.outputBytes = make(map[string][]byte, len(outputs))
	hs.outputStagingID = make(map[string]string, len(outputs))
	for role, b := range outputs {
		hs.outputBytes[role] = b
		hs.outputStagingID[role] = ids.NewUUIDv7()
	}
	hs.srv = httptest.NewServer(buildExecHostMux(hs, plan, true))
	return hs
}

// newExecHostStubFailingDownload is the multi-host fan-out negative variant:
// the done event still advertises an outputs map (so the coordinator
// believes there is something to fetch), but GET /v1/staging/{id} returns
// 500 instead of the bytes. Used to drive the multi-host all-or-none
// coordinator into its first-pass failure branch — tests assert that no
// per-host final file leaks onto disk after rollback.
//
// `failKey` is the role whose download will 500; other roles still serve
// normally. In tests only the failing host's bytes matter, so passing the
// single declared key is enough.
func newExecHostStubFailingDownload(t *testing.T, id, failKey string, plan execHostPlan) *execHostStub {
	t.Helper()
	hs := &execHostStub{id: id, plan: plan}
	// Synthesise minimal outputs map: empty bytes is fine; the staging id
	// just needs to exist so the done event carries an outputs entry. The
	// failingStagingMux wrapper below short-circuits ALL GET /v1/staging/{id}
	// to 500 so the coordinator hits its first-pass download-failure branch.
	hs.outputBytes = map[string][]byte{failKey: nil}
	hs.outputStagingID = map[string]string{failKey: ids.NewUUIDv7()}
	hs.srv = httptest.NewServer(failingStagingMux(buildExecHostMux(hs, plan, true)))
	return hs
}

// failingStagingMux composes a wrapper that returns 500 for any GET
// /v1/staging/{id} (the bytes endpoint), regardless of which {id} is
// requested — overrides the happy-path handler installed by
// buildExecHostMux. The /v1/staging/by-content/ lookup path is left alone
// so upload-dedup logic still works unaffected.
func failingStagingMux(inner *http.ServeMux) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/staging/") &&
			!strings.HasPrefix(r.URL.Path, "/v1/staging/by-content/") {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"internal","message":"download failed"}`))
			return
		}
		inner.ServeHTTP(w, r)
	})
}

// buildExecHostMux assembles the per-stub routing. The withStaging flag adds
// the by-content and PUT endpoints (404 lookup → upload → 201). Kept as a
// single builder so the two constructors share dispatch/events/output bodies
// verbatim.
func buildExecHostMux(hs *execHostStub, plan execHostPlan, withStaging bool) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/exec/dispatch", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req lettsclient.ExecRequest
		_ = json.Unmarshal(body, &req)
		// MissionID is excluded from JSON body (json:"-"); recover via header.
		req.MissionID = r.Header.Get("Idempotency-Key")
		hs.mu.Lock()
		hs.dispatched = append(hs.dispatched, req)
		hs.mu.Unlock()

		if plan.dispatchStatus != 0 {
			w.WriteHeader(plan.dispatchStatus)
			_, _ = w.Write([]byte(plan.dispatchBody))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = fmt.Fprintf(w, `{"exec_id":%q,"status":"queued"}`, req.MissionID)
	})
	mux.HandleFunc("GET /v1/missions/{id}/events", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		flush := func() {
			if flusher != nil {
				flusher.Flush()
			}
		}
		// Emit queued and running so the stream looks like a real mission.
		_, _ = fmt.Fprintf(w, "{\"event\":\"queued\",\"seq\":1}\n")
		flush()
		_, _ = fmt.Fprintf(w, "{\"event\":\"running\",\"seq\":2}\n")
		flush()
		if plan.streamHang {
			<-r.Context().Done()
			return
		}
		if plan.doneAfter > 0 {
			time.Sleep(plan.doneAfter)
		}
		// When the stub was built with output bytes, emit an outputs map
		// keyed by role so the client's --out download loop can resolve
		// each role to a staging_id. Otherwise fall back to a minimal done.
		if len(hs.outputBytes) > 0 {
			doneEv := map[string]any{
				"event":     "done",
				"seq":       3,
				"outcome":   plan.doneOutcome,
				"exit_code": plan.doneExitCode,
				"outputs":   buildOutputsMap(hs),
			}
			buf, _ := json.Marshal(doneEv)
			_, _ = w.Write(buf)
			_, _ = w.Write([]byte("\n"))
		} else {
			_, _ = fmt.Fprintf(w, "{\"event\":\"done\",\"seq\":3,\"outcome\":%q,\"exit_code\":%d}\n",
				plan.doneOutcome, plan.doneExitCode)
		}
		flush()
	})
	mux.HandleFunc("GET /v1/missions/{id}/output", func(w http.ResponseWriter, r *http.Request) {
		stream := r.URL.Query().Get("stream")
		var data string
		switch stream {
		case "stdout":
			data = plan.stdoutBytes
		case "stderr":
			data = plan.stderrBytes
		}
		w.WriteHeader(http.StatusOK)
		if data != "" {
			_, _ = w.Write([]byte(data))
		}
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		// Keep the response body open until the client cancels; this matches
		// real /output?follow=true behaviour and lets io.Copy on the tail
		// goroutine block on EOF until cancellation.
		<-r.Context().Done()
	})
	if withStaging {
		mux.HandleFunc("GET /v1/staging/by-content/{sha}", func(w http.ResponseWriter, r *http.Request) {
			// 404 forces uploadOrReuse onto the upload branch so PutStagingInitial
			// runs (and stagingPuts increments). Hit-path coverage lives in
			// exec_staging_test.go.
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"not_found"}`))
		})
		mux.HandleFunc("HEAD /v1/staging/{id}", func(w http.ResponseWriter, r *http.Request) {
			// HEAD 404 → UploadFile picks the initial-PUT branch.
			w.WriteHeader(http.StatusNotFound)
		})
		mux.HandleFunc("PUT /v1/staging/{id}", func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			hs.mu.Lock()
			hs.stagingPuts++
			hs.mu.Unlock()
			w.WriteHeader(http.StatusCreated)
		})
		// GET /v1/staging/{id} serves output bytes for stubs constructed
		// via newExecHostStubWithStagingAndOutputs. Resolves staging_id back
		// to the role via the per-role map, then writes the role's bytes.
		// Unknown id → 404 so download tests can detect routing bugs.
		mux.HandleFunc("GET /v1/staging/{id}", func(w http.ResponseWriter, r *http.Request) {
			id := r.PathValue("id")
			for role, sid := range hs.outputStagingID {
				if sid == id {
					b := hs.outputBytes[role]
					w.Header().Set("Content-Type", "application/octet-stream")
					w.Header().Set("Content-Length", fmt.Sprintf("%d", len(b)))
					w.WriteHeader(http.StatusOK)
					if len(b) > 0 {
						_, _ = w.Write(b)
					}
					return
				}
			}
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"not_found"}`))
		})
	}
	return mux
}

// buildOutputsMap renders the per-role outputs slice for a done event.
// Each entry carries staging_id (the stub's per-role UUIDv7), sha256 of
// the bytes, and size. Tests only assert on staging_id and bytes-after-
// download so sha256/size fields exist purely for shape parity with
// production done events.
func buildOutputsMap(hs *execHostStub) map[string]map[string]any {
	out := make(map[string]map[string]any, len(hs.outputBytes))
	for role, b := range hs.outputBytes {
		sum := sha256.Sum256(b)
		out[role] = map[string]any{
			"staging_id": hs.outputStagingID[role],
			"sha256":     hex.EncodeToString(sum[:]),
			"size":       len(b),
		}
	}
	return out
}

func (hs *execHostStub) close() { hs.srv.Close() }

// stubExecAppCtx returns an appCtx wired to a single fake host using
// BaseURLForID (mirrors stubFanoutAppCtx in run_fanout_test.go). Lane
// "light" is preconfigured; an exec-scope token is set so ResolveToken
// succeeds.
func stubExecAppCtx(t *testing.T, hs *execHostStub) *appCtx {
	t.Helper()
	return &appCtx{
		Config: &lettsconfig.Config{
			Dugdales: []lettsconfig.Dugdale{
				{
					ID:        hs.id,
					Host:      "ignored",
					Labels:    []string{"prod"},
					Token:     "tok",
					ExecToken: "tok",
					Lanes:     map[string]lettsconfig.LaneCfg{"light": {Concurrency: 1}},
				},
			},
		},
		Getenv:       func(string) (string, bool) { return "", false },
		BaseURLForID: map[string]string{hs.id: hs.srv.URL},
		clients:      map[clientKey]*hostClient{},
	}
}

// runExecForTest wires a cobra.Command to scratch buffers and dispatches
// runExec, returning the captured stdout/stderr strings plus the error.
func runExecForTest(t *testing.T, hs *execHostStub, ef *execFlags) (string, string, error) {
	t.Helper()
	ac := stubExecAppCtx(t, hs)
	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	var so, se strings.Builder
	cmd.SetOut(&so)
	cmd.SetErr(&se)
	err := runExec(cmd, ac, ef, FormatText)
	return so.String(), se.String(), err
}

// mapExecErr is sugar so test bodies stay one-liner. mapErrorToExit lives
// in exitcode.go and is the production sink for all CLI errors.
func mapExecErr(t *testing.T, err error) int {
	t.Helper()
	return mapErrorToExit(err)
}

// TestExecArgvSingleHostSuccess is the happy path: argv-only request,
// success outcome, exit 0, stdout bytes streamed through verbatim.
//
// Note: runExecOne always returns a typed *ExecOutcomeError (even on
// success) so callers map via mapErrorToExit — one might expect
// `err == nil`, but the actual contract is exit-code parity.
func TestExecArgvSingleHostSuccess(t *testing.T) {
	hs := newExecHostStub(t, "s1", execHostPlan{
		doneOutcome:  "success",
		doneExitCode: 0,
		stdoutBytes:  "12:01:04 up 10 days\n",
	})
	defer hs.close()

	ac := stubExecAppCtx(t, hs)
	ef := &execFlags{lane: "light", host: "s1", argv: []string{"uptime"}, outputFmt: "raw"}
	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	var stdoutBuf, stderrBuf strings.Builder
	cmd.SetOut(&stdoutBuf)
	cmd.SetErr(&stderrBuf)
	err := runExec(cmd, ac, ef, FormatText)
	if got := mapExecErr(t, err); got != 0 {
		t.Fatalf("exit=%d, want 0; err=%v", got, err)
	}
	if !strings.Contains(stdoutBuf.String(), "up 10 days") {
		t.Errorf("stdout=%q, want substring 'up 10 days'", stdoutBuf.String())
	}
	if len(hs.dispatched) != 1 {
		t.Fatalf("dispatched %d, want 1", len(hs.dispatched))
	}
	got := hs.dispatched[0]
	if got.Lane != "light" || len(got.Command) != 1 || got.Command[0] != "uptime" {
		t.Errorf("dispatched: %+v", got)
	}
	if !ids.ValidateUUIDv7(got.MissionID) {
		t.Errorf("mission_id (Idempotency-Key) not a UUIDv7: %q", got.MissionID)
	}
}

// TestExecArgvSingleHostFailedExit42 verifies exit_code passthrough for
// outcome=failed when the remote exit_code is non-zero (no remap).
func TestExecArgvSingleHostFailedExit42(t *testing.T) {
	hs := newExecHostStub(t, "s1", execHostPlan{doneOutcome: "failed", doneExitCode: 42})
	defer hs.close()
	ef := &execFlags{lane: "light", host: "s1", argv: []string{"false"}, outputFmt: "raw"}
	_, _, err := runExecForTest(t, hs, ef)
	if got := mapExecErr(t, err); got != 42 {
		t.Errorf("exit=%d, want 42", got)
	}
}

// TestExecFailedOutcomeExitZeroMapsOne: a failed outcome with exit_code=0
// must surface as CLI 1 (treat as failure, not success).
func TestExecFailedOutcomeExitZeroMapsOne(t *testing.T) {
	hs := newExecHostStub(t, "s1", execHostPlan{doneOutcome: "failed", doneExitCode: 0})
	defer hs.close()
	ef := &execFlags{lane: "light", host: "s1", argv: []string{"x"}, outputFmt: "raw"}
	_, _, err := runExecForTest(t, hs, ef)
	if got := mapExecErr(t, err); got != 1 {
		t.Errorf("exit=%d, want 1 (failed+0 remap)", got)
	}
}

// TestExecArgvSingleHostExit124Collision verifies success+124 collapses to
// 125 so 124 stays reserved for CLI wait-timeout.
func TestExecArgvSingleHostExit124Collision(t *testing.T) {
	hs := newExecHostStub(t, "s1", execHostPlan{doneOutcome: "success", doneExitCode: 124})
	defer hs.close()
	ef := &execFlags{lane: "light", host: "s1", argv: []string{"x"}, outputFmt: "raw"}
	_, _, err := runExecForTest(t, hs, ef)
	if got := mapExecErr(t, err); got != 125 {
		t.Errorf("exit=%d, want 125", got)
	}
}

// TestExecArgvSingleHostExit125Collision pins the failed+125 path: even
// when the remote process literally returned 125, CLI returns 125 (not
// passthrough) because 125 is reserved for abnormal outcomes.
func TestExecArgvSingleHostExit125Collision(t *testing.T) {
	hs := newExecHostStub(t, "s1", execHostPlan{doneOutcome: "failed", doneExitCode: 125})
	defer hs.close()
	ef := &execFlags{lane: "light", host: "s1", argv: []string{"x"}, outputFmt: "raw"}
	_, _, err := runExecForTest(t, hs, ef)
	if got := mapExecErr(t, err); got != 125 {
		t.Errorf("exit=%d, want 125", got)
	}
}

// TestExecArgvSingleHostKilled covers the abnormal-outcome bucket: killed,
// timeout, oom, crashed, lost all collapse to 125 regardless of exit_code.
func TestExecArgvSingleHostKilled(t *testing.T) {
	hs := newExecHostStub(t, "s1", execHostPlan{doneOutcome: "killed", doneExitCode: 137})
	defer hs.close()
	ef := &execFlags{lane: "light", host: "s1", argv: []string{"x"}, outputFmt: "raw"}
	_, _, err := runExecForTest(t, hs, ef)
	if got := mapExecErr(t, err); got != 125 {
		t.Errorf("exit=%d, want 125", got)
	}
}

// TestExecArgvDetach verifies --detach prints the freshly-generated exec_id
// to stdout and returns nil immediately, even when the events stream hangs.
func TestExecArgvDetach(t *testing.T) {
	hs := newExecHostStub(t, "s1", execHostPlan{streamHang: true})
	defer hs.close()
	ef := &execFlags{
		lane: "light", host: "s1",
		argv:      []string{"sleep", "100"},
		outputFmt: "raw", detach: true,
	}
	so, _, err := runExecForTest(t, hs, ef)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	printed := strings.TrimSpace(so)
	if printed == "" {
		t.Fatalf("stdout empty; expected exec_id")
	}
	if !ids.ValidateUUIDv7(printed) {
		t.Errorf("stdout %q not a valid UUIDv7", printed)
	}
}

// TestExecMissionIDOverrideWithDetach: when both --mission-id and
// --detach are set, the user-supplied ID must be the one printed (not a
// freshly minted UUIDv7).
func TestExecMissionIDOverrideWithDetach(t *testing.T) {
	hs := newExecHostStub(t, "s1", execHostPlan{streamHang: true})
	defer hs.close()
	mid := "0192aaaa-0000-7000-8000-000000000001"
	ef := &execFlags{
		lane: "light", host: "s1",
		argv: []string{"x"}, outputFmt: "raw",
		detach: true, missionID: mid,
	}
	so, _, err := runExecForTest(t, hs, ef)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got := strings.TrimSpace(so); got != mid {
		t.Errorf("stdout=%q, want %q", got, mid)
	}
}

// TestExecArgvTransportError verifies a 500 from dispatch surfaces as a
// transport error mapped to exit 255.
func TestExecArgvTransportError(t *testing.T) {
	hs := newExecHostStub(t, "s1", execHostPlan{
		dispatchStatus: http.StatusInternalServerError,
		dispatchBody:   `{"error":"internal","message":"db gone"}`,
	})
	defer hs.close()
	ef := &execFlags{lane: "light", host: "s1", argv: []string{"x"}, outputFmt: "raw"}
	_, _, err := runExecForTest(t, hs, ef)
	if got := mapExecErr(t, err); got != 255 {
		t.Errorf("exit=%d, want 255 (transport)", got)
	}
}

// TestExecArgvAuthError verifies a 401 from dispatch also folds into the
// transport bucket (exit 255). The lettsclient does not synthesize an
// AuthError; the raw HTTPError is wrapped via wrapExecTransport.
func TestExecArgvAuthError(t *testing.T) {
	hs := newExecHostStub(t, "s1", execHostPlan{
		dispatchStatus: http.StatusUnauthorized,
		dispatchBody:   `{"error":"unauthorized","message":"bad token"}`,
	})
	defer hs.close()
	ef := &execFlags{lane: "light", host: "s1", argv: []string{"x"}, outputFmt: "raw"}
	_, _, err := runExecForTest(t, hs, ef)
	if got := mapExecErr(t, err); got != 255 {
		t.Errorf("exit=%d, want 255 (auth)", got)
	}
}

// TestExecConfigErrorMaps255 — all pre-terminal errors in the exec path
// map to CLI 255 (transport class). When the dugdale has no exec token,
// ClientForHost returns *ConfigError, which the runExec pipeline wraps in
// *ExecTransportError. mapErrorToExit checks *ExecTransportError BEFORE
// the inner-error chain so the wrapper classification wins.
func TestExecConfigErrorMaps255(t *testing.T) {
	cfg := &lettsconfig.Config{
		Dugdales: []lettsconfig.Dugdale{
			// No ExecToken set and no Auth.ExecToken fallback → ResolveToken fails.
			{
				ID: "s1", Host: "127.0.0.1", Port: 1,
				Lanes: map[string]lettsconfig.LaneCfg{"light": {Concurrency: 1}},
			},
		},
	}
	ac := &appCtx{
		Config:  cfg,
		Getenv:  func(string) (string, bool) { return "", false },
		clients: map[clientKey]*hostClient{},
	}
	ef := &execFlags{lane: "light", host: "s1", argv: []string{"x"}, outputFmt: "raw"}
	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	var so, se strings.Builder
	cmd.SetOut(&so)
	cmd.SetErr(&se)
	err := runExec(cmd, ac, ef, FormatText)
	if got := mapExecErr(t, err); got != 255 {
		t.Errorf("exit=%d, want 255 (all pre-terminal exec errors → transport)", got)
	}
}

// TestExecArgvSingleHostWaitTimeout verifies that a hung events stream
// triggers the wait-timeout path → exit 124. 100ms keeps the test fast.
func TestExecArgvSingleHostWaitTimeout(t *testing.T) {
	hs := newExecHostStub(t, "s1", execHostPlan{streamHang: true})
	defer hs.close()
	ef := &execFlags{
		lane: "light", host: "s1",
		argv: []string{"x"}, outputFmt: "raw",
		waitTimeout: "100ms",
	}
	_, _, err := runExecForTest(t, hs, ef)
	if got := mapExecErr(t, err); got != 124 {
		t.Errorf("exit=%d, want 124", got)
	}
}

// TestExecWaitTimeoutDefaultFromTimeout is a pure-function exercise of
// computeWaitDeadline: when --wait-timeout is unset but --timeout is "5m",
// the effective deadline should be now + 5m + 30s.
func TestExecWaitTimeoutDefaultFromTimeout(t *testing.T) {
	now := time.Now()
	d, err := computeWaitDeadline("", "5m", now)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	delta := d.Sub(now)
	if delta < 5*time.Minute+25*time.Second || delta > 5*time.Minute+35*time.Second {
		t.Errorf("delta=%v, want ~5m30s", delta)
	}
}

// TestExecWaitTimeoutInfiniteByDefault: neither flag set → infinite (zero time.Time).
func TestExecWaitTimeoutInfiniteByDefault(t *testing.T) {
	d, err := computeWaitDeadline("", "", time.Now())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !d.IsZero() {
		t.Errorf("deadline=%v, want zero (infinite)", d)
	}
}

// TestExecWaitTimeoutExplicitZeroIsInfinite: --wait-timeout=0 → infinite
// even when --timeout is set (explicit override of the auto-default).
func TestExecWaitTimeoutExplicitZeroIsInfinite(t *testing.T) {
	d, err := computeWaitDeadline("0", "5m", time.Now())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !d.IsZero() {
		t.Errorf("deadline=%v, want zero", d)
	}
}

// TestExecArgvShellFormBlockedNoFlag: bash -c '...' without --allow-shell
// must short-circuit with a BadUsageError → exit 2.
func TestExecArgvShellFormBlockedNoFlag(t *testing.T) {
	hs := newExecHostStub(t, "s1", execHostPlan{doneOutcome: "success"})
	defer hs.close()
	ef := &execFlags{
		lane: "light", host: "s1",
		argv:      []string{"bash", "-c", "uptime"},
		outputFmt: "raw",
	}
	_, _, err := runExecForTest(t, hs, ef)
	if got := mapExecErr(t, err); got != 2 {
		t.Errorf("exit=%d, want 2 (BadUsage)", got)
	}
}

// TestExecArgvShellFormAllowedWithFlag: same argv with --allow-shell goes
// through and yields exit 0 (success outcome).
func TestExecArgvShellFormAllowedWithFlag(t *testing.T) {
	hs := newExecHostStub(t, "s1", execHostPlan{doneOutcome: "success", doneExitCode: 0})
	defer hs.close()
	ef := &execFlags{
		lane: "light", host: "s1",
		argv:      []string{"bash", "-c", "uptime"},
		outputFmt: "raw", allowShell: true,
	}
	_, _, err := runExecForTest(t, hs, ef)
	if got := mapExecErr(t, err); got != 0 {
		t.Errorf("exit=%d, want 0; err=%v", got, err)
	}
}

// TestExecArgvLaneMissing: omitting --lane → BadUsage → exit 2.
func TestExecArgvLaneMissing(t *testing.T) {
	hs := newExecHostStub(t, "s1", execHostPlan{doneOutcome: "success"})
	defer hs.close()
	ef := &execFlags{host: "s1", argv: []string{"uptime"}, outputFmt: "raw"}
	_, _, err := runExecForTest(t, hs, ef)
	if got := mapExecErr(t, err); got != 2 {
		t.Errorf("exit=%d, want 2", got)
	}
}

// TestExecRequireExplicitTarget: --lane only (no host/match/all) → BadUsage
// with the "exactly one" guidance.
func TestExecRequireExplicitTarget(t *testing.T) {
	hs := newExecHostStub(t, "s1", execHostPlan{doneOutcome: "success"})
	defer hs.close()
	ef := &execFlags{lane: "light", argv: []string{"uptime"}, outputFmt: "raw"}
	_, _, err := runExecForTest(t, hs, ef)
	if got := mapExecErr(t, err); got != 2 {
		t.Errorf("exit=%d, want 2 (no target)", got)
	}
	if err == nil || !strings.Contains(err.Error(), "exactly one") {
		t.Errorf("err=%v, want 'exactly one' badusage", err)
	}
}

// TestExecRejectTwoTargets: --host and --all together is the canonical "exactly one"
// violation.
func TestExecRejectTwoTargets(t *testing.T) {
	hs := newExecHostStub(t, "s1", execHostPlan{doneOutcome: "success"})
	defer hs.close()
	ef := &execFlags{
		lane: "light", host: "s1", all: true,
		argv: []string{"uptime"}, outputFmt: "raw",
	}
	_, _, err := runExecForTest(t, hs, ef)
	if got := mapExecErr(t, err); got != 2 {
		t.Errorf("exit=%d, want 2 (two targets)", got)
	}
}

// TestExecScriptSingleHostUploaded is the single-host happy path: a
// --script local file is uploaded via uploadOrReuse and the
// resulting staging_id is threaded into the dispatched ExecRequest as
// .Script.StagingID. The stub serves 404 on by-content so uploadOrReuse
// falls through to PutStagingInitial (stagingPuts++); the dispatched payload
// MUST carry a non-empty Script ref so the daemon can resolve $LETTS_SCRIPT.
func TestExecScriptSingleHostUploaded(t *testing.T) {
	scriptPath := writeTempFile(t, "#!/bin/sh\necho hi\n")
	hs := newExecHostStubWithStaging(t, "s1", execHostPlan{
		doneOutcome: "success", doneExitCode: 0,
	})
	defer hs.close()
	ef := &execFlags{
		lane: "light", host: "s1",
		argv: []string{"bash", "$LETTS_SCRIPT"}, script: scriptPath,
		outputFmt: "raw",
	}
	_, _, err := runExecForTest(t, hs, ef)
	if got := mapExecErr(t, err); got != 0 {
		t.Fatalf("exit=%d, want 0; err=%v", got, err)
	}
	if len(hs.dispatched) != 1 {
		t.Fatalf("dispatched=%d, want 1", len(hs.dispatched))
	}
	if hs.dispatched[0].Script == nil || hs.dispatched[0].Script.StagingID == "" {
		t.Errorf("script ref missing: %+v", hs.dispatched[0].Script)
	}
	if !ids.ValidateUUIDv7(hs.dispatched[0].Script.StagingID) {
		t.Errorf("script staging_id %q not UUIDv7", hs.dispatched[0].Script.StagingID)
	}
	if hs.stagingPuts != 1 {
		t.Errorf("stagingPuts=%d, want 1 (one initial upload)", hs.stagingPuts)
	}
}

// TestExecInUploadedAndDispatched is the single-host happy path: a
// --in pdf=<file> pair is uploaded via uploadOrReuse and the resulting
// staging_id surfaces as ExecRequest.In[0] with the original key. The stub
// serves 404 on by-content so uploadOrReuse falls through to the upload
// branch; the dispatched payload must carry exactly one In ref.
func TestExecInUploadedAndDispatched(t *testing.T) {
	inPath := writeTempFile(t, "input-content")
	hs := newExecHostStubWithStaging(t, "s1", execHostPlan{
		doneOutcome: "success", doneExitCode: 0,
	})
	defer hs.close()
	ef := &execFlags{
		lane: "light", host: "s1",
		argv:      []string{"x"},
		in:        []string{"pdf=" + inPath},
		outputFmt: "raw",
	}
	_, _, err := runExecForTest(t, hs, ef)
	if got := mapExecErr(t, err); got != 0 {
		t.Fatalf("exit=%d, want 0; err=%v", got, err)
	}
	if len(hs.dispatched) != 1 {
		t.Fatalf("dispatched=%d, want 1", len(hs.dispatched))
	}
	if len(hs.dispatched[0].In) != 1 || hs.dispatched[0].In[0].Key != "pdf" {
		t.Errorf("In=%+v", hs.dispatched[0].In)
	}
	if !ids.ValidateUUIDv7(hs.dispatched[0].In[0].StagingID) {
		t.Errorf("in[0] staging_id %q not UUIDv7", hs.dispatched[0].In[0].StagingID)
	}
	if hs.stagingPuts != 1 {
		t.Errorf("stagingPuts=%d, want 1 (one initial upload)", hs.stagingPuts)
	}
}

// TestExecInKeyValidationFailsEarly: a reserved __ prefix on the --in key
// must short-circuit with a BadUsageError (exit 2) BEFORE any HTTP call —
// the stub's dispatched slice stays empty. Pre-upload validation should
// never let bad input touch the wire (would otherwise upload bytes only
// to be rejected by the daemon as bad_request).
func TestExecInKeyValidationFailsEarly(t *testing.T) {
	hs := newExecHostStubWithStaging(t, "s1", execHostPlan{})
	defer hs.close()
	ef := &execFlags{
		lane: "light", host: "s1",
		argv:      []string{"x"},
		in:        []string{"__internal=/tmp/x"},
		outputFmt: "raw",
	}
	_, _, err := runExecForTest(t, hs, ef)
	if got := mapExecErr(t, err); got != 2 {
		t.Errorf("exit=%d, want 2 (BadUsage)", got)
	}
	if len(hs.dispatched) != 0 {
		t.Errorf("server received %d dispatches; want 0 (validation should fail before HTTP)", len(hs.dispatched))
	}
	if hs.stagingPuts != 0 {
		t.Errorf("stagingPuts=%d, want 0 (no upload before validation)", hs.stagingPuts)
	}
}

// TestExecStdinSingleHostUploads is the single-host happy path: with
// --stdin=single, a piped stdinReader, and a single host, runExec must read
// the bytes once, upload them via uploadStdinToHost (uploadOrReuseBytes
// under the hood), and thread both Stdin="single" and a non-empty
// StdinStagingID into the dispatched ExecRequest. Overrides stdinReader
// (injected stdin) and isTerminalFD (forces non-TTY so the explicit
// --stdin=single passes the resolveStdinMode TTY guard).
func TestExecStdinSingleHostUploads(t *testing.T) {
	oldReader := stdinReader
	stdinReader = bytes.NewReader([]byte("input-data"))
	defer func() { stdinReader = oldReader }()
	oldTerm := isTerminalFD
	isTerminalFD = func(uintptr) bool { return false }
	defer func() { isTerminalFD = oldTerm }()

	hs := newExecHostStubWithStaging(t, "s1", execHostPlan{
		doneOutcome: "success", doneExitCode: 0,
	})
	defer hs.close()
	ef := &execFlags{
		lane: "light", host: "s1",
		argv:      []string{"cat"},
		stdin:     "single",
		outputFmt: "raw",
	}
	_, _, err := runExecForTest(t, hs, ef)
	if got := mapExecErr(t, err); got != 0 {
		t.Fatalf("exit=%d, want 0; err=%v", got, err)
	}
	if len(hs.dispatched) != 1 {
		t.Fatalf("dispatched=%d, want 1", len(hs.dispatched))
	}
	if hs.dispatched[0].Stdin != "single" {
		t.Errorf("Stdin=%q, want single", hs.dispatched[0].Stdin)
	}
	if hs.dispatched[0].StdinStagingID == "" {
		t.Errorf("StdinStagingID empty; want a staging_id from uploadStdinToHost")
	}
	if !ids.ValidateUUIDv7(hs.dispatched[0].StdinStagingID) {
		t.Errorf("StdinStagingID %q not UUIDv7", hs.dispatched[0].StdinStagingID)
	}
	if hs.stagingPuts != 1 {
		t.Errorf("stagingPuts=%d, want 1 (one stdin upload)", hs.stagingPuts)
	}
}

// TestExecAutoSingleEmptyStdinDowngradesToNone verifies the empty-payload
// auto-downgrade: when stdinMode auto-resolves to "single" (non-TTY, 1 host)
// but the piped stream is 0 bytes, runExec must NOT attempt a staging upload
// because the server's /v1/staging/by-content lookup rejects size=0 with a
// confusing "size must be a positive integer" 400. Instead the dispatch
// payload should look identical to --stdin=none: empty Stdin field, empty
// StdinStagingID, zero PUTs. This is the CI/cron/non-TTY shell shape where
// the operator never piped anything but the auto-detector picked "single"
// because stdin happens to be a pipe (e.g. nested subshell stdin).
func TestExecAutoSingleEmptyStdinDowngradesToNone(t *testing.T) {
	oldReader := stdinReader
	stdinReader = bytes.NewReader(nil) // 0 bytes piped
	defer func() { stdinReader = oldReader }()
	oldTerm := isTerminalFD
	isTerminalFD = func(uintptr) bool { return false } // non-TTY → auto "single"
	defer func() { isTerminalFD = oldTerm }()

	hs := newExecHostStubWithStaging(t, "s1", execHostPlan{
		doneOutcome: "success", doneExitCode: 0,
	})
	defer hs.close()
	ef := &execFlags{
		lane: "light", host: "s1",
		argv:      []string{"true"},
		outputFmt: "raw",
		// stdin: "" (auto)
	}
	_, _, err := runExecForTest(t, hs, ef)
	if got := mapExecErr(t, err); got != 0 {
		t.Fatalf("exit=%d, want 0; err=%v", got, err)
	}
	if len(hs.dispatched) != 1 {
		t.Fatalf("dispatched=%d, want 1", len(hs.dispatched))
	}
	if hs.dispatched[0].Stdin != "" {
		t.Errorf("Stdin=%q, want empty (downgraded to none)", hs.dispatched[0].Stdin)
	}
	if hs.dispatched[0].StdinStagingID != "" {
		t.Errorf("StdinStagingID=%q, want empty (no upload)", hs.dispatched[0].StdinStagingID)
	}
	if hs.stagingPuts != 0 {
		t.Errorf("stagingPuts=%d, want 0 (empty payload skipped)", hs.stagingPuts)
	}
}
