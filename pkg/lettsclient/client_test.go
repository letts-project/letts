package lettsclient

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func newTestClient(t *testing.T, handler http.HandlerFunc) (*Client, func()) {
	t.Helper()
	srv := httptest.NewServer(handler)
	c, err := New(Options{BaseURL: srv.URL, Token: "test-token"})
	if err != nil {
		t.Fatal(err)
	}
	return c, srv.Close
}

func TestClientDoSuccess(t *testing.T) {
	c, stop := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Errorf("auth header = %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, `{"ok":true}`)
	})
	defer stop()

	var out struct {
		OK bool `json:"ok"`
	}
	if err := c.DoJSON("GET", "/v1/dugdale", nil, nil, &out); err != nil {
		t.Fatal(err)
	}
	if !out.OK {
		t.Error("ok = false")
	}
}

func TestClientDoErrorDecoded(t *testing.T) {
	c, stop := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(412)
		_, _ = io.WriteString(w, `{"error":"no_lanes_configured","message":"run apply first"}`)
	})
	defer stop()

	err := c.DoJSON("POST", "/v1/dispatch", nil, strings.NewReader(`{}`), nil)
	var he *HTTPError
	if !errors.As(err, &he) {
		t.Fatalf("expected HTTPError, got %v", err)
	}
	if he.Status != 412 || he.Code != "no_lanes_configured" {
		t.Errorf("got %+v", he)
	}
}

func TestClientDoNon200Non4xxIsUntyped(t *testing.T) {
	c, stop := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		_, _ = io.WriteString(w, `not json`)
	})
	defer stop()

	err := c.DoJSON("GET", "/v1/dugdale", nil, nil, nil)
	var he *HTTPError
	if !errors.As(err, &he) {
		t.Fatalf("got %v", err)
	}
	if he.Status != 500 {
		t.Errorf("status = %d", he.Status)
	}
}

// TestNewDisableKeepAlives locks in the fan-out fix: DisableKeepAlives must
// reach the transport, and the default (CLI / SSE) must keep connection reuse.
func TestNewDisableKeepAlives(t *testing.T) {
	c, err := New(Options{BaseURL: "http://x", DisableKeepAlives: true})
	if err != nil {
		t.Fatal(err)
	}
	tr, ok := c.hc.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport type = %T", c.hc.Transport)
	}
	if !tr.DisableKeepAlives {
		t.Error("DisableKeepAlives not propagated to transport")
	}

	d, err := New(Options{BaseURL: "http://x"})
	if err != nil {
		t.Fatal(err)
	}
	if d.hc.Transport.(*http.Transport).DisableKeepAlives {
		t.Error("default client must keep connection reuse (CLI / SSE)")
	}
}

// TestDisableKeepAlivesRequestsConnectionClose proves the behavior end-to-end:
// with keep-alives off the client tells the server to close after each reply
// (no pooled idle connection survives to be silently evicted by a NAT/firewall).
func TestDisableKeepAlivesRequestsConnectionClose(t *testing.T) {
	var mu sync.Mutex
	sawClose := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		if r.Close { // set when the client sent "Connection: close"
			sawClose = true
		}
		mu.Unlock()
		_, _ = io.WriteString(w, `{}`)
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL, DisableKeepAlives: true})
	for i := 0; i < 2; i++ {
		if err := c.DoJSON("GET", "/v1/healthz", nil, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if !sawClose {
		t.Error("expected the client to request Connection: close per request")
	}
}

// TestDoJSONRetriesIdempotentRead: with RetryReads, a 5xx on the first attempt
// is retried and the second (200) attempt succeeds.
func TestDoJSONRetriesIdempotentRead(t *testing.T) {
	var mu sync.Mutex
	n := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		n++
		first := n == 1
		mu.Unlock()
		if first {
			w.WriteHeader(503) // ambiguous server error → retryable
			return
		}
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL, RetryReads: true})
	var out struct {
		OK bool `json:"ok"`
	}
	if err := c.DoJSON("GET", "/v1/healthz", nil, nil, &out); err != nil {
		t.Fatalf("expected retry to succeed, got %v", err)
	}
	if !out.OK {
		t.Error("ok = false after retry")
	}
	mu.Lock()
	defer mu.Unlock()
	if n != 2 {
		t.Errorf("attempts = %d, want 2 (one retry)", n)
	}
}

// TestDoJSONDoesNotRetry4xx: a 4xx is definitive and must not be retried even
// when RetryReads is on.
func TestDoJSONDoesNotRetry4xx(t *testing.T) {
	var mu sync.Mutex
	n := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		n++
		mu.Unlock()
		w.WriteHeader(404)
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL, RetryReads: true})
	err := c.DoJSON("GET", "/v1/x", nil, nil, nil)
	var he *HTTPError
	if !errors.As(err, &he) || he.Status != 404 {
		t.Fatalf("want HTTPError 404, got %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if n != 1 {
		t.Errorf("4xx must not retry: attempts = %d, want 1", n)
	}
}

// TestDoJSONNoRetryWhenDisabled: the default client (RetryReads=false, e.g. the
// CLI) returns the first 5xx without retrying.
func TestDoJSONNoRetryWhenDisabled(t *testing.T) {
	var mu sync.Mutex
	n := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		n++
		mu.Unlock()
		w.WriteHeader(503)
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL}) // RetryReads defaults to false
	err := c.DoJSON("GET", "/v1/x", nil, nil, nil)
	var he *HTTPError
	if !errors.As(err, &he) || he.Status != 503 {
		t.Fatalf("want HTTPError 503, got %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if n != 1 {
		t.Errorf("retry disabled: attempts = %d, want 1", n)
	}
}

func TestClientDoIdempotencyKeyHeader(t *testing.T) {
	c, stop := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Idempotency-Key"); got != "abc-key" {
			t.Errorf("idem key = %q", got)
		}
		w.WriteHeader(202)
		_, _ = io.WriteString(w, `{}`)
	})
	defer stop()

	hdr := http.Header{"Idempotency-Key": []string{"abc-key"}}
	if err := c.DoJSON("POST", "/v1/dispatch", hdr, strings.NewReader(`{}`), nil); err != nil {
		t.Fatal(err)
	}
}
