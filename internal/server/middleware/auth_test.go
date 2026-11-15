package middleware_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"letts/internal/server/middleware"
)

func okHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

var testCfg = middleware.AuthConfig{
	Dispatch: []string{"dispatch-token"},
	Admin:    []string{"admin-token"},
}

func TestAuthMissingHeader(t *testing.T) {
	h := middleware.Auth(testCfg, middleware.ScopeDispatch, okHandler)
	req := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()
	h(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("want 401, got %d", w.Code)
	}
}

func TestAuthWrongScheme(t *testing.T) {
	h := middleware.Auth(testCfg, middleware.ScopeDispatch, okHandler)
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Authorization", "Basic dXNlcjpwYXNz")
	w := httptest.NewRecorder()
	h(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("want 401, got %d", w.Code)
	}
}

func TestAuthUnknownToken(t *testing.T) {
	h := middleware.Auth(testCfg, middleware.ScopeDispatch, okHandler)
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Authorization", "Bearer unknown-token")
	w := httptest.NewRecorder()
	h(w, req)
	// unknown token: not in any list → 401 (not 403 since we can't tell scope mismatch from unknown)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("want 401, got %d", w.Code)
	}
}

func TestAuthDispatchTokenOnDispatchEndpoint(t *testing.T) {
	h := middleware.Auth(testCfg, middleware.ScopeDispatch, okHandler)
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Authorization", "Bearer dispatch-token")
	w := httptest.NewRecorder()
	h(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("want 200, got %d", w.Code)
	}
}

func TestAuthDispatchTokenOnAdminEndpoint(t *testing.T) {
	h := middleware.Auth(testCfg, middleware.ScopeAdmin, okHandler)
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Authorization", "Bearer dispatch-token")
	w := httptest.NewRecorder()
	h(w, req)
	if w.Code != http.StatusForbidden {
		t.Errorf("want 403, got %d", w.Code)
	}
}

func TestAuthAdminTokenOnDispatchEndpoint(t *testing.T) {
	h := middleware.Auth(testCfg, middleware.ScopeDispatch, okHandler)
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Authorization", "Bearer admin-token")
	w := httptest.NewRecorder()
	h(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("want 200 (admin superset), got %d", w.Code)
	}
}

func TestAuthEmptyScopeListMeansDisabled(t *testing.T) {
	cfg := middleware.AuthConfig{
		Dispatch: []string{},
		Admin:    []string{},
	}
	h := middleware.Auth(cfg, middleware.ScopeDispatch, okHandler)
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Authorization", "Bearer anything")
	w := httptest.NewRecorder()
	h(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("want 401 for empty scope list, got %d", w.Code)
	}
}

func TestAuthIdentityInContext(t *testing.T) {
	var capturedID middleware.Identity
	var captured bool

	capture := func(w http.ResponseWriter, r *http.Request) {
		capturedID, captured = middleware.FromCtx(r.Context())
		w.WriteHeader(http.StatusOK)
	}

	h := middleware.Auth(testCfg, middleware.ScopeAdmin, capture)
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Authorization", "Bearer admin-token")
	w := httptest.NewRecorder()
	h(w, req)

	if !captured {
		t.Fatal("identity not in context")
	}
	if capturedID.Scope != middleware.ScopeAdmin {
		t.Errorf("scope: got %v, want ScopeAdmin", capturedID.Scope)
	}

	h = middleware.Auth(testCfg, middleware.ScopeDispatch, capture)
	req = httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Authorization", "Bearer dispatch-token")
	h(httptest.NewRecorder(), req)
	if capturedID.Scope != middleware.ScopeDispatch {
		t.Errorf("scope: got %v, want ScopeDispatch", capturedID.Scope)
	}
}

// TestAuthBruteForceBlocksAfter5Failures pins the brute-force backoff:
// 5 consecutive 401s from the same client IP trip the tracker; the 6th
// request returns 429 with Retry-After before the auth header is even parsed.
// Only admin endpoints are protected — dispatch endpoints don't have
// the tracker engaged.
func TestAuthBruteForceBlocksAfter5Failures(t *testing.T) {
	cfg := testCfg
	cfg.BruteForce = middleware.NewBruteForceTracker(time.Hour)
	h := middleware.Auth(cfg, middleware.ScopeAdmin, okHandler)

	makeReq := func() *http.Request {
		req := httptest.NewRequest("GET", "/v1/admin/state", nil)
		req.Header.Set("Authorization", "Bearer wrong-token")
		req.RemoteAddr = "10.0.0.1:5555"
		return req
	}

	// 5 failures → no backoff yet (backoff trips at >=5 with delay growing
	// from 100ms × 2^(count-5)).
	for i := 0; i < 5; i++ {
		w := httptest.NewRecorder()
		h(w, makeReq())
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("attempt %d: got %d, want 401", i+1, w.Code)
		}
	}

	// 6th attempt should be blocked → 429 with Retry-After.
	w := httptest.NewRecorder()
	h(w, makeReq())
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("6th attempt: got %d, want 429", w.Code)
	}
	if got := w.Header().Get("Retry-After"); got == "" {
		t.Error("missing Retry-After header on 429")
	}
}

// TestAuthBruteForceSkipsDispatchScope verifies that ScopeDispatch
// endpoints are NOT tracked — the brute-force feature is admin-only
// per the threat model (a busy CI clobbering its own dispatch
// token shouldn't trip backoff).
func TestAuthBruteForceSkipsDispatchScope(t *testing.T) {
	cfg := testCfg
	cfg.BruteForce = middleware.NewBruteForceTracker(time.Hour)
	h := middleware.Auth(cfg, middleware.ScopeDispatch, okHandler)

	for i := 0; i < 10; i++ {
		req := httptest.NewRequest("POST", "/v1/dispatch", nil)
		req.Header.Set("Authorization", "Bearer wrong-token")
		req.RemoteAddr = "10.0.0.2:5555"
		w := httptest.NewRecorder()
		h(w, req)
		// All 401, never 429.
		if w.Code != http.StatusUnauthorized {
			t.Errorf("attempt %d: got %d, want 401 (dispatch should not gate)", i+1, w.Code)
		}
	}
}

// TestAuthBruteForceSuccessResetsCounter — a successful auth between
// failures clears the counter so the next streak starts from 0 instead
// of compounding.
func TestAuthBruteForceSuccessResetsCounter(t *testing.T) {
	cfg := testCfg
	cfg.BruteForce = middleware.NewBruteForceTracker(time.Hour)
	h := middleware.Auth(cfg, middleware.ScopeAdmin, okHandler)

	mk := func(tok string) *http.Request {
		req := httptest.NewRequest("GET", "/v1/admin/state", nil)
		req.Header.Set("Authorization", "Bearer "+tok)
		req.RemoteAddr = "10.0.0.3:5555"
		return req
	}

	// 4 failures.
	for i := 0; i < 4; i++ {
		h(httptest.NewRecorder(), mk("wrong-token"))
	}
	// 1 success.
	w := httptest.NewRecorder()
	h(w, mk("admin-token"))
	if w.Code != http.StatusOK {
		t.Fatalf("success after failures: got %d, want 200", w.Code)
	}
	// 5 more failures should NOT trip the gate (counter reset).
	for i := 0; i < 5; i++ {
		w := httptest.NewRecorder()
		h(w, mk("wrong-token"))
		if w.Code == http.StatusTooManyRequests {
			t.Fatalf("attempt %d after reset: got 429; counter should have reset", i+1)
		}
	}
}

func TestFromCtxMissing(t *testing.T) {
	_, ok := middleware.FromCtx(context.Background())
	if ok {
		t.Error("expected false from empty context")
	}
}

// TestAuthAdminFailureIncrementsCounter verifies that
// letts_admin_auth_failures_total ticks once for every 401 returned from a
// ScopeAdmin endpoint, but stays untouched for 401s on ScopeDispatch.
func TestAuthAdminFailureIncrementsCounter(t *testing.T) {
	h := middleware.Auth(testCfg, middleware.ScopeAdmin, okHandler)
	before := readAuthCounter(t)

	// no Authorization header
	req := httptest.NewRequest("POST", "/v1/admin/apply", nil)
	rec := httptest.NewRecorder()
	h(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("got %d, want 401", rec.Code)
	}

	// wrong token
	req2 := httptest.NewRequest("POST", "/v1/admin/apply", nil)
	req2.Header.Set("Authorization", "Bearer not-a-real-token")
	rec2 := httptest.NewRecorder()
	h(rec2, req2)
	if rec2.Code != http.StatusUnauthorized {
		t.Fatalf("got %d, want 401", rec2.Code)
	}

	// dispatch-scope 401 must NOT increment admin counter
	hd := middleware.Auth(testCfg, middleware.ScopeDispatch, okHandler)
	req3 := httptest.NewRequest("POST", "/v1/dispatch", nil)
	req3.Header.Set("Authorization", "Bearer not-a-real-token")
	rec3 := httptest.NewRecorder()
	hd(rec3, req3)

	after := readAuthCounter(t)
	if got := after - before; got != 2 {
		t.Fatalf("admin_auth_failures_total delta = %v, want 2", got)
	}
}

func readAuthCounter(t *testing.T) float64 {
	t.Helper()
	mfs, _ := prometheus.DefaultGatherer.Gather()
	for _, mf := range mfs {
		if mf.GetName() != "letts_admin_auth_failures_total" {
			continue
		}
		var sum float64
		for _, m := range mf.GetMetric() {
			if m.Counter != nil {
				sum += m.Counter.GetValue()
			}
		}
		return sum
	}
	return 0
}
