package lettsclient

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func TestRetryTransient5xxThenSuccess(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		if n < 3 {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	rc := &RetryClient{
		Client:     c,
		MaxRetries: 3,
		BackoffMs:  []int{1, 1, 1},
	}
	var out struct{ OK bool }
	if err := rc.DoJSON("GET", "/v1/dugdale", nil, nil, &out); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 {
		t.Errorf("calls = %d, want 3", calls.Load())
	}
}

func TestRetryGivesUpAfterMax(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(503)
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	rc := &RetryClient{Client: c, MaxRetries: 3, BackoffMs: []int{1, 1, 1}}
	err := rc.DoJSON("GET", "/v1/dugdale", nil, nil, nil)
	var he *HTTPError
	if !errors.As(err, &he) || he.Status != 503 {
		t.Fatalf("expected 503 HTTPError, got %v", err)
	}
	if calls.Load() != 3 {
		t.Errorf("calls = %d, want 3", calls.Load())
	}
}

// TestRetryReBuffersRequestBody verifies the contract documented in
// DoJSON: each retry attempt sends the full original body. Without
// re-buffering (bytes.NewReader per attempt), the first attempt would
// consume the io.Reader and subsequent attempts would send an empty body
// — which the server might silently accept as a malformed POST.
func TestRetryReBuffersRequestBody(t *testing.T) {
	const wantBody = `{"hello":"world","n":42}`

	var (
		mu        sync.Mutex
		gotBodies []string
		calls     atomic.Int64
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		gotBodies = append(gotBodies, string(b))
		mu.Unlock()
		n := calls.Add(1)
		if n < 3 {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	rc := &RetryClient{Client: c, MaxRetries: 3, BackoffMs: []int{1, 1, 1}}

	var out struct{ OK bool }
	if err := rc.DoJSON("POST", "/v1/dispatch", nil, strings.NewReader(wantBody), &out); err != nil {
		t.Fatalf("DoJSON: %v", err)
	}
	if len(gotBodies) != 3 {
		t.Fatalf("server saw %d bodies, want 3: %#v", len(gotBodies), gotBodies)
	}
	for i, got := range gotBodies {
		if got != wantBody {
			t.Errorf("attempt %d body = %q, want %q", i+1, got, wantBody)
		}
	}
}

// TestRetrySkippedForBackpressure503 verifies that a 503 carrying an explicit
// backpressure error code (queue_full / draining / disk_quota_exceeded /
// lane_removing) is NOT sticky-retried — these are deliberate server
// rejections, not ambiguous failures, so the client must
// surface them immediately and let auto-select try another candidate, instead
// of hammering the same host for ~2.6s of pointless backoff.
func TestRetrySkippedForBackpressure503(t *testing.T) {
	for _, code := range []string{"queue_full", "draining", "disk_quota_exceeded", "lane_removing"} {
		t.Run(code, func(t *testing.T) {
			var calls atomic.Int64
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(503)
				_, _ = w.Write([]byte(`{"error":"` + code + `"}`))
			}))
			defer srv.Close()

			c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
			rc := &RetryClient{Client: c, MaxRetries: 3, BackoffMs: []int{1, 1, 1}}
			err := rc.DoJSON("POST", "/v1/dispatch", nil, nil, nil)
			var he *HTTPError
			if !errors.As(err, &he) || he.Status != 503 || he.Code != code {
				t.Fatalf("expected 503 %s HTTPError, got %v", code, err)
			}
			if calls.Load() != 1 {
				t.Errorf("calls = %d, want 1 (no retry on explicit backpressure 503)", calls.Load())
			}
		})
	}
}

// TestRetryStillRetriesCodelessTransient5xx guards that the fix for explicit
// backpressure does NOT stop retrying genuinely ambiguous 5xx (no error code —
// e.g. a bare proxy 503 or a half-written response), which remain sticky.
func TestRetryStillRetriesCodelessTransient5xx(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		if n < 3 {
			w.WriteHeader(503) // no body, no Code → ambiguous
			return
		}
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	rc := &RetryClient{Client: c, MaxRetries: 3, BackoffMs: []int{1, 1, 1}}
	var out struct{ OK bool }
	if err := rc.DoJSON("GET", "/v1/dugdale", nil, nil, &out); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 {
		t.Errorf("calls = %d, want 3 (codeless 5xx still retried)", calls.Load())
	}
}

func TestRetrySkippedFor4xx(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(401)
		_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	rc := &RetryClient{Client: c, MaxRetries: 3, BackoffMs: []int{1, 1, 1}}
	if err := rc.DoJSON("POST", "/v1/dispatch", nil, nil, nil); err == nil {
		t.Fatal("expected error")
	}
	if calls.Load() != 1 {
		t.Errorf("calls = %d, want 1 (no retry on 4xx)", calls.Load())
	}
}
