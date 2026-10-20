package main

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"letts/pkg/lettsclient"
	"letts/pkg/lettsconfig"
)

// fanoutByIDStub spins up N httptest servers and wires them as dugdales in an
// appCtx; each stub gets a status (and optional body) for GET /v1/missions/{id}.
type fanoutByIDStub struct {
	id     string
	labels []string
	status int
	body   string
}

// stubByIDAppCtx returns an *appCtx with one dugdale per stub. Stops via t.Cleanup.
func stubByIDAppCtx(t *testing.T, stubs []*fanoutByIDStub) *appCtx {
	t.Helper()
	dugs := make([]lettsconfig.Dugdale, 0, len(stubs))
	baseURLs := map[string]string{}
	for _, st := range stubs {
		st := st
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(st.status)
			if st.body != "" {
				_, _ = io.WriteString(w, st.body)
			}
		}))
		t.Cleanup(srv.Close)
		dugs = append(dugs, lettsconfig.Dugdale{
			ID: st.id, Host: "ignored", AdminToken: "atok", Labels: st.labels,
		})
		baseURLs[st.id] = srv.URL
	}
	return &appCtx{
		Config:       &lettsconfig.Config{Dugdales: dugs},
		Getenv:       func(k string) (string, bool) { return "", false },
		BaseURLForID: baseURLs,
		clients:      map[clientKey]*hostClient{},
	}
}

// TestFanOutByID_404plus200_returns200 — the canonical happy path: one host
// returns 404 (mission lives elsewhere), the other returns 200; the helper
// surfaces the 200's value and its host id.
func TestFanOutByID_404plus200_returns200(t *testing.T) {
	ac := stubByIDAppCtx(t, []*fanoutByIDStub{
		{id: "a", status: 404, body: `{"error":"not_found"}`},
		{id: "b", status: 200, body: `{"mission_id":"mid","status":"done","outcome":"success"}`},
	})
	m, host, err := FanOutByID(ac, nil, func(c *lettsclient.Client) (*lettsclient.Mission, error) {
		return lettsclient.GetMission(c, "mid")
	})
	if err != nil {
		t.Fatalf("FanOutByID: %v", err)
	}
	if host != "b" {
		t.Errorf("host=%q want %q", host, "b")
	}
	if m == nil || m.MissionID != "mid" {
		t.Errorf("mission=%+v", m)
	}
}

// TestFanOutByID_allMiss — every host returns 404 → "not found on any of N
// candidate" error.
func TestFanOutByID_allMiss(t *testing.T) {
	ac := stubByIDAppCtx(t, []*fanoutByIDStub{
		{id: "a", status: 404, body: `{"error":"not_found"}`},
		{id: "b", status: 404, body: `{"error":"not_found"}`},
	})
	_, _, err := FanOutByID(ac, nil, func(c *lettsclient.Client) (*lettsclient.Mission, error) {
		return lettsclient.GetMission(c, "mid")
	})
	if err == nil {
		t.Fatal("want error, got nil")
	}
	if !strings.Contains(err.Error(), "not found on any of 2 candidate") {
		t.Errorf("err=%q want substring 'not found on any of 2 candidate'", err.Error())
	}
}

// TestFanOutByID_authErrorWins — one host returns 401 (auth), another 404.
// 404 is skipped so the surviving non-404 (auth) error is returned.
func TestFanOutByID_authErrorWins(t *testing.T) {
	ac := stubByIDAppCtx(t, []*fanoutByIDStub{
		{id: "a", status: 401, body: `{"error":"unauthorized","message":"bad token"}`},
		{id: "b", status: 404, body: `{"error":"not_found"}`},
	})
	_, _, err := FanOutByID(ac, nil, func(c *lettsclient.Client) (*lettsclient.Mission, error) {
		return lettsclient.GetMission(c, "mid")
	})
	if err == nil {
		t.Fatal("want error, got nil")
	}
	var he *lettsclient.HTTPError
	if !errors.As(err, &he) {
		t.Fatalf("want *HTTPError, got %T: %v", err, err)
	}
	if he.Status != 401 {
		t.Errorf("status=%d want 401", he.Status)
	}
}

// When multiple hosts return distinct non-404 errors the
// previous implementation only kept the LAST one (overwrote in the for
// loop). An operator chasing a misconfigured cluster only saw one of
// several failures, hiding correlated issues like "two hosts have
// expired tokens AND one host's disk is full". Joined errors carry
// every non-404 plus the host that produced it, in a stable order.
func TestFanOutByID_multipleDistinctErrorsJoined(t *testing.T) {
	ac := stubByIDAppCtx(t, []*fanoutByIDStub{
		{id: "a", status: 401, body: `{"error":"unauthorized","message":"bad token"}`},
		{id: "b", status: 503, body: `{"error":"disk_quota_exceeded"}`},
		{id: "c", status: 404, body: `{"error":"not_found"}`}, // still skipped
	})
	_, _, err := FanOutByID(ac, nil, func(c *lettsclient.Client) (*lettsclient.Mission, error) {
		return lettsclient.GetMission(c, "mid")
	})
	if err == nil {
		t.Fatal("want error, got nil")
	}
	msg := err.Error()
	if !strings.Contains(msg, "a:") || !strings.Contains(msg, "401") {
		t.Errorf("expected host 'a' and 401 in error, got: %s", msg)
	}
	if !strings.Contains(msg, "b:") || !strings.Contains(msg, "503") {
		t.Errorf("expected host 'b' and 503 in error, got: %s", msg)
	}
}

// TestFanOutByID_conflict — multiple hosts return 200; helper refuses to pick
// and returns the "found on multiple hosts" error so callers can retry with
// --host.
func TestFanOutByID_conflict(t *testing.T) {
	ac := stubByIDAppCtx(t, []*fanoutByIDStub{
		{id: "a", status: 200, body: `{"mission_id":"mid"}`},
		{id: "b", status: 200, body: `{"mission_id":"mid"}`},
	})
	_, _, err := FanOutByID(ac, nil, func(c *lettsclient.Client) (*lettsclient.Mission, error) {
		return lettsclient.GetMission(c, "mid")
	})
	if err == nil {
		t.Fatal("want error, got nil")
	}
	if !strings.Contains(err.Error(), "multiple hosts") {
		t.Errorf("err=%q want substring 'multiple hosts'", err.Error())
	}
}

// TestFanOutByID_matchFilter — --match=foo narrows candidates; only matching
// hosts are queried, so a 200 from the non-matching host is invisible.
func TestFanOutByID_matchFilter(t *testing.T) {
	ac := stubByIDAppCtx(t, []*fanoutByIDStub{
		{id: "a", labels: []string{"prod"}, status: 200, body: `{"mission_id":"mid","status":"done"}`},
		{id: "b", labels: []string{"dev"}, status: 200, body: `{"mission_id":"mid","status":"done"}`},
	})
	m, host, err := FanOutByID(ac, []string{"prod"}, func(c *lettsclient.Client) (*lettsclient.Mission, error) {
		return lettsclient.GetMission(c, "mid")
	})
	if err != nil {
		t.Fatalf("FanOutByID: %v", err)
	}
	if host != "a" {
		t.Errorf("host=%q want %q", host, "a")
	}
	if m == nil {
		t.Errorf("mission nil")
	}
}

// stubByIDAppCtxWithTokens spins up N stub servers and wires them as dugdales
// with the given (dispatch, admin, exec) tokens — empty string means absent.
func stubByIDAppCtxWithTokens(t *testing.T, stubs []*fanoutByIDStub,
	dispatchTok, adminTok, execTok string,
) *appCtx {
	t.Helper()
	dugs := make([]lettsconfig.Dugdale, 0, len(stubs))
	baseURLs := map[string]string{}
	for _, st := range stubs {
		st := st
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(st.status)
			if st.body != "" {
				_, _ = io.WriteString(w, st.body)
			}
		}))
		t.Cleanup(srv.Close)
		dugs = append(dugs, lettsconfig.Dugdale{
			ID: st.id, Host: "ignored",
			Token: dispatchTok, AdminToken: adminTok, ExecToken: execTok,
			Labels: st.labels,
		})
		baseURLs[st.id] = srv.URL
	}
	return &appCtx{
		Config:       &lettsconfig.Config{Dugdales: dugs},
		Getenv:       func(k string) (string, bool) { return "", false },
		BaseURLForID: baseURLs,
		clients:      map[clientKey]*hostClient{},
	}
}

// TestFanOutByID_AdminOnly: FanOutByID never falls back to another scope
// (mutation commands must require admin).
func TestFanOutByID_AdminOnly(t *testing.T) {
	ac := stubByIDAppCtxWithTokens(t, []*fanoutByIDStub{
		{id: "a", status: 200, body: `{}`},
	}, "dtok", "", "")
	_, _, err := FanOutByID(ac, nil, func(c *lettsclient.Client) (*lettsclient.Mission, error) {
		return lettsclient.GetMission(c, "mid")
	})
	if err == nil {
		t.Error("expected error when admin token absent and only admin scope requested")
	}
}

// locateActStub fakes one dugdale for LocateThenActByID tests. GET
// /v1/missions/{id} (the locate probe) answers 200/404 from `owns`, or an
// explicit getStatus override for non-404 locate failures. Every non-GET
// request counts as a mutation, so tests can prove a refused fan-out really
// executed nothing.
type locateActStub struct {
	id        string
	owns      bool
	getStatus int    // 0 → derive from owns; else explicit (e.g. 401)
	getBody   string // body when getStatus is explicit
	mutStatus int    // mutation response status (0 → 200)
	mutBody   string // mutation response body

	mu        sync.Mutex
	mutations int
}

// Mutations returns how many non-GET requests this stub served.
func (st *locateActStub) Mutations() int {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.mutations
}

// stubLocateActAppCtx wires one dugdale per stub, mirroring stubByIDAppCtx.
func stubLocateActAppCtx(t *testing.T, stubs []*locateActStub) *appCtx {
	t.Helper()
	dugs := make([]lettsconfig.Dugdale, 0, len(stubs))
	baseURLs := map[string]string{}
	for _, st := range stubs {
		st := st
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			if r.Method == http.MethodGet {
				status, body := st.getStatus, st.getBody
				if status == 0 {
					if st.owns {
						status, body = 200, `{"mission_id":"mid","status":"done"}`
					} else {
						status, body = 404, `{"error":"not_found"}`
					}
				}
				w.WriteHeader(status)
				_, _ = io.WriteString(w, body)
				return
			}
			st.mu.Lock()
			st.mutations++
			st.mu.Unlock()
			status := st.mutStatus
			if status == 0 {
				status = 200
			}
			w.WriteHeader(status)
			if st.mutBody != "" {
				_, _ = io.WriteString(w, st.mutBody)
			}
		}))
		t.Cleanup(srv.Close)
		dugs = append(dugs, lettsconfig.Dugdale{
			ID: st.id, Host: "ignored", AdminToken: "atok",
		})
		baseURLs[st.id] = srv.URL
	}
	return &appCtx{
		Config:       &lettsconfig.Config{Dugdales: dugs},
		Getenv:       func(k string) (string, bool) { return "", false },
		BaseURLForID: baseURLs,
		clients:      map[clientKey]*hostClient{},
	}
}

// restartVia is the mutation closure shared by the LocateThenActByID tests:
// the same shape the ctl missions restart runner passes in.
func restartVia(id string) func(*lettsclient.Client) (*lettsclient.RestartResponse, error) {
	return func(c *lettsclient.Client) (*lettsclient.RestartResponse, error) {
		return lettsclient.RestartMission(c, id)
	}
}

// TestLocateThenActByID_conflictExecutesNothing — the core destructive-safety
// property: when 2+ hosts own the id, the helper must report the conflict
// WITHOUT issuing the mutation anywhere. (The parallel FanOutByID would have
// restarted the mission on every owning host and then reported a conflict
// implying nothing happened.)
func TestLocateThenActByID_conflictExecutesNothing(t *testing.T) {
	a := &locateActStub{id: "a", owns: true}
	b := &locateActStub{id: "b", owns: true}
	ac := stubLocateActAppCtx(t, []*locateActStub{a, b})

	_, _, err := LocateThenActByID(ac, "mid", nil, restartVia("mid"))
	if err == nil {
		t.Fatal("want conflict error, got nil")
	}
	if !strings.Contains(err.Error(), "multiple hosts") || !strings.Contains(err.Error(), "--host") {
		t.Errorf("err=%q want 'multiple hosts' conflict demanding --host", err.Error())
	}
	if a.Mutations() != 0 || b.Mutations() != 0 {
		t.Errorf("conflict must execute nothing: mutations a=%d b=%d, want 0/0", a.Mutations(), b.Mutations())
	}
}

// TestLocateThenActByID_singleOwnerActsOnlyThere — exactly one host owns the
// id: the mutation must hit that host (once) and no other.
func TestLocateThenActByID_singleOwnerActsOnlyThere(t *testing.T) {
	a := &locateActStub{id: "a", owns: true,
		mutStatus: 201, mutBody: `{"mission_id":"new","restarted_from":"mid","status":"queued"}`}
	b := &locateActStub{id: "b", owns: false}
	ac := stubLocateActAppCtx(t, []*locateActStub{a, b})

	resp, host, err := LocateThenActByID(ac, "mid", nil, restartVia("mid"))
	if err != nil {
		t.Fatalf("LocateThenActByID: %v", err)
	}
	if host != "a" {
		t.Errorf("host=%q want %q", host, "a")
	}
	if resp == nil || resp.MissionID != "new" {
		t.Errorf("resp=%+v want mission_id=new", resp)
	}
	if a.Mutations() != 1 || b.Mutations() != 0 {
		t.Errorf("mutations a=%d b=%d, want 1/0", a.Mutations(), b.Mutations())
	}
}

// TestLocateThenActByID_zeroOwnersNotFoundShape — every host 404s the locate
// probe: the existing "not found on any of N candidate" shape is preserved
// and nothing is mutated.
func TestLocateThenActByID_zeroOwnersNotFoundShape(t *testing.T) {
	a := &locateActStub{id: "a", owns: false}
	b := &locateActStub{id: "b", owns: false}
	ac := stubLocateActAppCtx(t, []*locateActStub{a, b})

	_, _, err := LocateThenActByID(ac, "mid", nil, restartVia("mid"))
	if err == nil {
		t.Fatal("want not-found error, got nil")
	}
	if !strings.Contains(err.Error(), "not found on any of 2 candidate") {
		t.Errorf("err=%q want 'not found on any of 2 candidate'", err.Error())
	}
	if a.Mutations() != 0 || b.Mutations() != 0 {
		t.Errorf("mutations a=%d b=%d, want 0/0", a.Mutations(), b.Mutations())
	}
}

// TestLocateThenActByID_locateErrorsJoined — non-404 locate failures keep
// the joined-error semantics of the read fan-out (host-prefixed, errors.As
// reachable), and still execute nothing.
func TestLocateThenActByID_locateErrorsJoined(t *testing.T) {
	a := &locateActStub{id: "a", getStatus: 401, getBody: `{"error":"unauthorized","message":"bad token"}`}
	b := &locateActStub{id: "b", owns: false}
	ac := stubLocateActAppCtx(t, []*locateActStub{a, b})

	_, _, err := LocateThenActByID(ac, "mid", nil, restartVia("mid"))
	if err == nil {
		t.Fatal("want error, got nil")
	}
	var he *lettsclient.HTTPError
	if !errors.As(err, &he) || he.Status != 401 {
		t.Errorf("want joined 401 HTTPError reachable via errors.As, got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), "a:") {
		t.Errorf("err=%q want host prefix 'a:'", err.Error())
	}
	if a.Mutations() != 0 || b.Mutations() != 0 {
		t.Errorf("mutations a=%d b=%d, want 0/0", a.Mutations(), b.Mutations())
	}
}

// TestLocateThenActByID_mutationNotFoundSurfaces — the row can expire
// between locate and act; the mutation's 404 must surface as a normal error
// (no retry, no remapping into the fan-out's not-found shape).
func TestLocateThenActByID_mutationNotFoundSurfaces(t *testing.T) {
	a := &locateActStub{id: "a", owns: true,
		mutStatus: 404, mutBody: `{"error":"not_found","message":"mission not found"}`}
	ac := stubLocateActAppCtx(t, []*locateActStub{a})

	_, _, err := LocateThenActByID(ac, "mid", nil, restartVia("mid"))
	if err == nil {
		t.Fatal("want error, got nil")
	}
	var he *lettsclient.HTTPError
	if !errors.As(err, &he) || he.Status != 404 {
		t.Errorf("want raw 404 HTTPError from the mutation, got %T: %v", err, err)
	}
	if strings.Contains(err.Error(), "candidate dugdales") {
		t.Errorf("mutation 404 must not be remapped into the fan-out not-found shape: %q", err.Error())
	}
	if a.Mutations() != 1 {
		t.Errorf("mutations=%d want 1", a.Mutations())
	}
}

// TestLocateThenActByID_matchFilterScopesLocate — the --match label filter
// applies to the locate step, so a host outside the filter is never probed
// nor mutated even if it owns the id.
func TestLocateThenActByID_matchFilterScopesLocate(t *testing.T) {
	a := &locateActStub{id: "a", owns: true,
		mutStatus: 201, mutBody: `{"mission_id":"new","restarted_from":"mid","status":"queued"}`}
	b := &locateActStub{id: "b", owns: true,
		mutStatus: 201, mutBody: `{"mission_id":"other","restarted_from":"mid","status":"queued"}`}
	ac := stubLocateActAppCtx(t, []*locateActStub{a, b})
	// Labels live on the config rows built by the helper; attach them here.
	ac.Config.Dugdales[0].Labels = []string{"prod"}
	ac.Config.Dugdales[1].Labels = []string{"dev"}

	resp, host, err := LocateThenActByID(ac, "mid", []string{"prod"}, restartVia("mid"))
	if err != nil {
		t.Fatalf("LocateThenActByID: %v", err)
	}
	if host != "a" || resp == nil || resp.MissionID != "new" {
		t.Errorf("host=%q resp=%+v want a/new", host, resp)
	}
	if a.Mutations() != 1 || b.Mutations() != 0 {
		t.Errorf("mutations a=%d b=%d, want 1/0", a.Mutations(), b.Mutations())
	}
}

// TestFanOutByID_noCandidates — --match excludes every host, helper returns
// a clear "no dugdales match labels" error before any HTTP traffic.
func TestFanOutByID_noCandidates(t *testing.T) {
	ac := stubByIDAppCtx(t, []*fanoutByIDStub{
		{id: "a", labels: []string{"prod"}, status: 200},
	})
	_, _, err := FanOutByID(ac, []string{"nope"}, func(c *lettsclient.Client) (*lettsclient.Mission, error) {
		return lettsclient.GetMission(c, "mid")
	})
	if err == nil {
		t.Fatal("want error, got nil")
	}
	if !strings.Contains(err.Error(), "no dugdales match labels") {
		t.Errorf("err=%q want substring 'no dugdales match labels'", err.Error())
	}
}

// kindGateAppCtx wires dugdales whose /output answers 200 only for the tokens
// in allowed (the daemon's kind gate) and 403 forbidden_kind for any other
// token, recording every token presented.
func kindGateAppCtx(t *testing.T, ids []string, allowed map[string]bool, dispatchTok, adminTok, execTok string) (*appCtx, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var seen []string
	dugs := make([]lettsconfig.Dugdale, 0, len(ids))
	baseURLs := map[string]string{}
	for _, id := range ids {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tok := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			mu.Lock()
			seen = append(seen, tok)
			mu.Unlock()
			if !allowed[tok] {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusForbidden)
				_, _ = io.WriteString(w, `{"error":"forbidden_kind","message":"token scope does not permit access to this mission kind"}`)
				return
			}
			_, _ = io.WriteString(w, "payload")
		}))
		t.Cleanup(srv.Close)
		dugs = append(dugs, lettsconfig.Dugdale{ID: id, Host: "ignored", Token: dispatchTok, AdminToken: adminTok, ExecToken: execTok})
		baseURLs[id] = srv.URL
	}
	ac := &appCtx{
		Config:       &lettsconfig.Config{Dugdales: dugs},
		Getenv:       func(k string) (string, bool) { return "", false },
		BaseURLForID: baseURLs,
		clients:      map[clientKey]*hostClient{},
	}
	return ac, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), seen...)
	}
}

func readOutput(t *testing.T, ac *appCtx, scopes []lettsconfig.Scope, host string) (string, error) {
	t.Helper()
	cmd := newLogsCmd()
	cmd.SetContext(t.Context())
	var out strings.Builder
	cmd.SetOut(&out)
	err := runCtlMissionsOutput(cmd, ac, FormatText, scopes, "mid", host, "stdout", false, nil)
	return out.String(), err
}

func TestOutputRead_ForbiddenKindMovesToNextScope(t *testing.T) {
	for _, host := range []string{"s1", ""} {
		ac, seen := kindGateAppCtx(t, []string{"s1"}, map[string]bool{"etok": true, "atok": true}, "dtok", "atok", "etok")
		got, err := readOutput(t, ac, readScopesMission, host)
		if err != nil || got != "payload" {
			t.Fatalf("host=%q: got %q err=%v", host, got, err)
		}
		if s := seen(); len(s) != 2 || s[0] != "dtok" || s[1] != "etok" {
			t.Errorf("host=%q: tokens tried = %v, want [dtok etok]", host, s)
		}
	}
}

func TestOutputRead_ExecScopesTryExecFirst(t *testing.T) {
	ac, seen := kindGateAppCtx(t, []string{"s1"}, map[string]bool{"etok": true}, "dtok", "atok", "etok")
	if got, err := readOutput(t, ac, readScopesExec, "s1"); err != nil || got != "payload" {
		t.Fatalf("got %q err=%v", got, err)
	}
	if s := seen(); len(s) != 1 || s[0] != "etok" {
		t.Errorf("tokens tried = %v, want [etok]", s)
	}
}

func TestOutputRead_ForbiddenKindSurfacesWhenNoOtherScope(t *testing.T) {
	ac, _ := kindGateAppCtx(t, []string{"s1"}, map[string]bool{}, "dtok", "", "")
	_, err := readOutput(t, ac, readScopesMission, "s1")
	var he *lettsclient.HTTPError
	if !errors.As(err, &he) || he.Code != "forbidden_kind" {
		t.Fatalf("err=%v, want forbidden_kind", err)
	}
}

func TestOutputRead_OtherErrorsDoNotRetry(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"error":"unauthorized","message":"bad token"}`)
	}))
	t.Cleanup(srv.Close)
	ac := &appCtx{
		Config:       &lettsconfig.Config{Dugdales: []lettsconfig.Dugdale{{ID: "s1", Host: "ignored", Token: "dtok", AdminToken: "atok", ExecToken: "etok"}}},
		Getenv:       func(k string) (string, bool) { return "", false },
		BaseURLForID: map[string]string{"s1": srv.URL},
		clients:      map[clientKey]*hostClient{},
	}
	_, err := readOutput(t, ac, readScopesMission, "s1")
	var he *lettsclient.HTTPError
	if !errors.As(err, &he) || he.Status != 401 || calls != 1 {
		t.Fatalf("err=%v calls=%d, want one 401", err, calls)
	}
}

func TestOutputRead_NoTokensIsConfigError(t *testing.T) {
	ac, _ := kindGateAppCtx(t, []string{"s1"}, map[string]bool{}, "", "", "")
	_, err := readOutput(t, ac, readScopesMission, "s1")
	var ce *ConfigError
	if !errors.As(err, &ce) {
		t.Fatalf("err=%v (%T), want ConfigError", err, err)
	}
}

func TestCtlExecShowNamedMissionGivesHint(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Header.Get("Authorization") == "Bearer etok" {
			w.WriteHeader(http.StatusForbidden)
			_, _ = io.WriteString(w, `{"error":"forbidden_kind","message":"no"}`)
			return
		}
		_, _ = io.WriteString(w, `{"mission_id":"mid","kind":"mission","status":"done"}`)
	}))
	t.Cleanup(srv.Close)
	ac := &appCtx{
		Config:       &lettsconfig.Config{Dugdales: []lettsconfig.Dugdale{{ID: "s1", Host: "ignored", Token: "dtok", AdminToken: "atok", ExecToken: "etok"}}},
		Getenv:       func(k string) (string, bool) { return "", false },
		BaseURLForID: map[string]string{"s1": srv.URL},
		clients:      map[clientKey]*hostClient{},
	}
	err := runCtlExecShow(ac, io.Discard, "mid", "s1", nil, FormatJSON)
	var bu *BadUsageError
	if !errors.As(err, &bu) || !strings.Contains(err.Error(), "ctl missions show") {
		t.Fatalf("err=%v, want BadUsage hint", err)
	}
}

func TestInspectPrefersAdminToken(t *testing.T) {
	var got []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = append(got, r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/v1/lanes" {
			_, _ = io.WriteString(w, `[]`)
			return
		}
		_, _ = io.WriteString(w, `{"version":"x"}`)
	}))
	t.Cleanup(srv.Close)
	for _, toks := range []struct{ d, a, e, want string }{
		{"dtok", "atok", "etok", "Bearer atok"},
		{"", "", "etok", "Bearer etok"},
	} {
		got = nil
		ac := &appCtx{
			Config:       &lettsconfig.Config{Dugdales: []lettsconfig.Dugdale{{ID: "s1", Host: "ignored", Token: toks.d, AdminToken: toks.a, ExecToken: toks.e}}},
			Getenv:       func(k string) (string, bool) { return "", false },
			BaseURLForID: map[string]string{"s1": srv.URL},
			clients:      map[clientKey]*hostClient{},
		}
		if err := runCtlLanesList(ac, io.Discard, "s1", FormatJSON); err != nil {
			t.Fatal(err)
		}
		if err := runCtlDugdalesInfo(ac, io.Discard, "s1", FormatJSON); err != nil {
			t.Fatal(err)
		}
		if len(got) != 2 || got[0] != toks.want || got[1] != toks.want {
			t.Errorf("tokens=%v, want %s for both", got, toks.want)
		}
	}
}
