// Package lettsclient is the HTTP client used by the letts CLI to talk to
// dugdale daemons. One Client instance per (host, scope-token) — share
// across subcommands via cmd/letts/runtime.go pool.
package lettsclient

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Options configures Client construction.
type Options struct {
	BaseURL   string
	Token     string
	Timeout   time.Duration
	UserAgent string
	// DisableKeepAlives dials a fresh connection per request instead of pooling
	// idle keep-alives. Set this for low-volume, long-idle-gap callers (arby's
	// fan-out reads) that talk to dugdales across a stateful NAT/firewall: such
	// intermediaries silently evict idle flows without RST, and a pooled request
	// that reuses an evicted connection black-holes until Client.Timeout fires
	// ("awaiting headers"). Fresh-dial-per-request behaves like curl and sidesteps
	// it. Leave false for the CLI and SSE streams, where reuse is worth keeping.
	DisableKeepAlives bool
	// RetryReads retries idempotent reads (GET/HEAD, no body) once on an ambiguous
	// failure — a network error, timeout, or 5xx. Set this for arby's fan-out so a
	// single transient hiccup on one attempt re-dials and usually succeeds, instead
	// of flapping a healthy dugdale to "unavailable". 4xx is definitive and never
	// retried; mutations (POST/DELETE) are never retried.
	RetryReads bool
}

// Client is a thin wrapper around http.Client that knows letts conventions.
type Client struct {
	base       *url.URL
	token      string
	ua         string
	timeout    time.Duration
	hc         *http.Client
	retryReads bool
}

// HTTPError wraps a non-2xx response with the decoded error shape.
type HTTPError struct {
	Status  int
	Code    string
	Message string
	Details any
	URL     string
}

func (e *HTTPError) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("dugdale %s %d: %s — %s", e.URL, e.Status, e.Code, e.Message)
	}
	return fmt.Sprintf("dugdale %s %d", e.URL, e.Status)
}

// New constructs a Client from Options.
func New(opts Options) (*Client, error) {
	u, err := url.Parse(opts.BaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse BaseURL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("BaseURL scheme must be http or https, got %q", u.Scheme)
	}
	ua := opts.UserAgent
	if ua == "" {
		ua = "letts/dev"
	}
	tr := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		DisableKeepAlives:     opts.DisableKeepAlives,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   10,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: 60 * time.Second,
	}
	return &Client{
		base:       u,
		token:      opts.Token,
		ua:         ua,
		timeout:    opts.Timeout,
		hc:         &http.Client{Transport: tr, Timeout: opts.Timeout},
		retryReads: opts.RetryReads,
	}, nil
}

// BaseURL exposes the resolved base for diagnostics.
func (c *Client) BaseURL() string { return c.base.String() }

func (c *Client) urlFor(relativePath string) string {
	u := *c.base
	path, query, hasQuery := strings.Cut(relativePath, "?")
	u.Path = strings.TrimRight(u.Path, "/") + path
	if hasQuery {
		u.RawQuery = query
	}
	return u.String()
}

func (c *Client) newRequest(method, relativePath string, header http.Header, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, c.urlFor(relativePath), body)
	if err != nil {
		return nil, err
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	req.Header.Set("User-Agent", c.ua)
	for k, vs := range header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	return req, nil
}

// DoJSON sends a request and decodes a JSON response into out (if non-nil).
// On non-2xx, returns *HTTPError. body may be nil for GET/DELETE.
//
// When the client opts in (Options.RetryReads) an idempotent read (GET/HEAD with
// no body) is retried once on an ambiguous failure — a network error, timeout,
// or 5xx — re-dialing fresh. A 4xx is definitive and returned immediately;
// requests with a body (mutations) are never retried here.
func (c *Client) DoJSON(method, relativePath string, header http.Header, body io.Reader, out any) error {
	attempts := 1
	if c.retryReads && body == nil && (method == http.MethodGet || method == http.MethodHead) {
		attempts = readRetryAttempts
	}
	var lastErr error
	for i := 0; i < attempts; i++ {
		err := c.doJSONOnce(method, relativePath, header, body, out)
		if err == nil {
			return nil
		}
		var he *HTTPError
		if errors.As(err, &he) && he.Status < 500 {
			return err // 4xx (and other <500): definitive, do not retry
		}
		lastErr = err
		if i+1 < attempts {
			sleepWithJitter(readRetryBackoff)
		}
	}
	return lastErr
}

// doJSONOnce performs a single DoJSON attempt (no retry).
func (c *Client) doJSONOnce(method, relativePath string, header http.Header, body io.Reader, out any) error {
	req, err := c.newRequest(method, relativePath, header, body)
	if err != nil {
		return err
	}
	if body != nil && header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode/100 != 2 {
		return decodeHTTPError(resp, req.URL.String())
	}
	if out == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// Do sends a request and returns the response. Caller owns resp.Body
// (must Close it). Use for streaming endpoints.
func (c *Client) Do(method, relativePath string, header http.Header, body io.Reader) (*http.Response, error) {
	req, err := c.newRequest(method, relativePath, header, body)
	if err != nil {
		return nil, err
	}
	return c.hc.Do(req)
}

func decodeHTTPError(resp *http.Response, requestURL string) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	var errResp struct {
		Error   string `json:"error"`
		Message string `json:"message"`
		Details any    `json:"details"`
	}
	_ = json.Unmarshal(body, &errResp)
	return &HTTPError{
		Status:  resp.StatusCode,
		Code:    errResp.Error,
		Message: errResp.Message,
		Details: errResp.Details,
		URL:     requestURL,
	}
}
