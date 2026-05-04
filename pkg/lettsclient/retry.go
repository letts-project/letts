// Package lettsclient implements sticky retry: ambiguous network errors
// (timeout / connection refused / 5xx without response) retry on the same host
// up to 3 attempts with jitter backoff 100ms/500ms/2s. 4xx propagates
// immediately — caller (subcommand) decides whether to surface or rewrite.
package lettsclient

import (
	"bytes"
	"errors"
	"io"
	"math/rand/v2"
	"net/http"
	"time"
)

// DefaultBackoffMs is the retry cadence: 100ms / 500ms / 2000ms with jitter.
var DefaultBackoffMs = []int{100, 500, 2000}

// Idempotent-read retry (Client.RetryReads): one extra attempt after a short
// pause. Kept deliberately small so a truly-down host is still marked
// unavailable promptly (worst case ≈ 2×Client.Timeout + this backoff).
const (
	readRetryAttempts = 2 // total attempts for an idempotent read (1 retry)
	readRetryBackoff  = 200 * time.Millisecond
)

// stickyRetry wraps c with the sticky-retry policy: 3 attempts on
// the same host with 100/500/2000ms jitter backoff, retrying network errors
// and 5xx, propagating 4xx. Used for the idempotency-keyed create endpoints
// (POST /v1/dispatch, POST /v1/exec/dispatch) where a retry is safe because the
// Idempotency-Key guarantees the server won't create a duplicate mission.
func stickyRetry(c *Client) *RetryClient {
	return &RetryClient{Client: c, MaxRetries: 3, BackoffMs: DefaultBackoffMs}
}

// RetryClient wraps Client with sticky retry on ambiguous failures.
type RetryClient struct {
	*Client
	MaxRetries int   // total attempts including the first
	BackoffMs  []int // index = attempt number (first retry sleeps BackoffMs[0])
}

// definitiveServerCodes are server error codes that represent a *deliberate*
// rejection rather than ambiguous server-side failure. These
// are not sticky: the mission was not created, so retrying the same host just
// wastes backoff and defeats auto-select's "try another candidate" path. A 5xx
// with no code (or an unknown code) is still treated as ambiguous and retried.
var definitiveServerCodes = map[string]bool{
	"queue_full":          true, // backpressure: lane/global queue limit
	"draining":            true, // graceful shutdown: host asked to be left alone
	"disk_quota_exceeded": true, // soft disk quota tripped
	"lane_removing":       true, // lane mid force-prune
	"no_lanes_configured": true, // bootstrap (412, but guard regardless of status)
}

// DoJSON retries on network errors and 5xx (which are server-side ambiguity).
// 4xx and explicit no-retry conditions propagate immediately.
//
// The body is buffered up-front so it can be re-sent on retry (the underlying
// Client takes an io.Reader which is consumed by the first attempt).
func (r *RetryClient) DoJSON(method, relPath string, header http.Header, body io.Reader, out any) error {
	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = io.ReadAll(body)
		if err != nil {
			return err
		}
	}
	attempts := r.MaxRetries
	if attempts < 1 {
		attempts = 1
	}
	var lastErr error
	for i := 0; i < attempts; i++ {
		var reqBody io.Reader
		if bodyBytes != nil {
			reqBody = bytes.NewReader(bodyBytes)
		}
		err := r.Client.DoJSON(method, relPath, header, reqBody, out)
		if err == nil {
			return nil
		}
		var he *HTTPError
		if errors.As(err, &he) {
			// 4xx is a client-side condition; never retry.
			// 5xx with a known definitive code is a deliberate server
			// rejection (backpressure/draining) — not ambiguous, so
			// surface it immediately and let the caller fail over.
			if he.Status < 500 || definitiveServerCodes[he.Code] {
				return err
			}
		}
		lastErr = err
		if i+1 < attempts {
			sleepWithJitter(r.backoffFor(i))
		}
	}
	return lastErr
}

func (r *RetryClient) backoffFor(attempt int) time.Duration {
	cadence := r.BackoffMs
	if len(cadence) == 0 {
		cadence = DefaultBackoffMs
	}
	if attempt >= len(cadence) {
		attempt = len(cadence) - 1
	}
	return time.Duration(cadence[attempt]) * time.Millisecond
}

// sleepWithJitter sleeps for d ± up-to-20%. Tests use 1ms backoffs which would
// make int64(d)/5 = 0; rand.Int64N panics on 0, so we floor at 1 (effectively
// no jitter at that granularity).
func sleepWithJitter(d time.Duration) {
	if d <= 0 {
		return
	}
	bound := int64(d) / 5
	if bound < 1 {
		bound = 1
	}
	jitter := time.Duration(rand.Int64N(bound))
	if rand.IntN(2) == 0 {
		jitter = -jitter
	}
	time.Sleep(d + jitter)
}
