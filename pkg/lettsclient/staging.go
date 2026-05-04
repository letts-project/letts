package lettsclient

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// StagingUploadStatus enumerates HEAD outcomes.
type StagingUploadStatus int

const (
	// StagingNotFound is returned when the daemon responds with 404
	// (no staging row for this id).
	StagingNotFound StagingUploadStatus = iota
	// StagingComplete means the upload has finished and the body is ready
	// to download via GET.
	StagingComplete
	// StagingIncomplete means an upload is in progress; the caller can
	// resume by sending PUT with Content-Range starting at BytesReceived.
	StagingIncomplete
)

// StagingHead is the parsed HEAD response. Headers are forwarded into typed
// fields so callers don't have to touch http.Header.
type StagingHead struct {
	Status        StagingUploadStatus
	SHA256        string
	TotalSize     int64
	BytesReceived int64
}

// ListStagingOpts is the filter for ListStaging. MissionID is required by the
// daemon (orphan listing is handled by the cleanup goroutine).
type ListStagingOpts struct {
	MissionID string
	RefKind   string
	Cursor    string
	Limit     int
}

// StagingFile is one element of a ListStaging response. Field tags match
// daemon handlers/staging.go List() exactly.
type StagingFile struct {
	StagingID     string `json:"staging_id"`
	SHA256        string `json:"sha256"`
	State         string `json:"state"`
	RefKind       string `json:"ref_kind"`
	Role          string `json:"role"`
	Size          int64  `json:"size"`
	BytesReceived int64  `json:"bytes_received"`
	TimeCreated   int64  `json:"time_created"`
	TimeExpires   *int64 `json:"time_expires,omitempty"`
}

// ListStagingResponse mirrors `{"staging":[...], "next_cursor":"..."}`.
// NextCursor is empty when there are no more pages.
type ListStagingResponse struct {
	Staging    []StagingFile `json:"staging"`
	NextCursor string        `json:"next_cursor,omitempty"`
}

// HeadStaging issues HEAD /v1/staging/{id} and parses the X-Letts-* headers
// into a StagingHead struct. A 404 maps to StagingNotFound (not an error).
// Any other non-2xx response returns *HTTPError.
func HeadStaging(c *Client, id string) (*StagingHead, error) {
	req, err := c.newRequest("HEAD", "/v1/staging/"+url.PathEscape(id), nil, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusNotFound {
		return &StagingHead{Status: StagingNotFound}, nil
	}
	if resp.StatusCode/100 != 2 {
		return nil, decodeHTTPError(resp, req.URL.String())
	}

	head := &StagingHead{
		SHA256: resp.Header.Get("X-Letts-Sha256"),
	}
	switch resp.Header.Get("X-Letts-Upload-Status") {
	case "complete":
		head.Status = StagingComplete
		// Complete uploads carry total size in Content-Length.
		if cl := resp.Header.Get("Content-Length"); cl != "" {
			if n, err := strconv.ParseInt(cl, 10, 64); err == nil {
				head.TotalSize = n
			}
		}
	case "incomplete":
		head.Status = StagingIncomplete
		if v := resp.Header.Get("X-Letts-Bytes-Received"); v != "" {
			if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				head.BytesReceived = n
			}
		}
		if v := resp.Header.Get("X-Letts-Total-Size"); v != "" {
			if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				head.TotalSize = n
			}
		}
	default:
		// "deleting" or any other state — surface as not-found so callers
		// won't try to resume; treat these as "not usable".
		head.Status = StagingNotFound
	}
	return head, nil
}

// PutStagingInitial uploads the entire body in a single request. Sets
// X-Letts-Sha256 and Content-Type: application/octet-stream and lets net/http
// emit Content-Length = total via the explicit req.ContentLength assignment.
// No Content-Range is sent — that signals "initial PUT" to the daemon.
//
// body may be either a plain io.Reader or an io.ReadSeeker. Seekable bodies
// (typically *os.File or *bytes.Reader) opt into the sticky retry path
// in doStagingPut: a transient 5xx replays from offset 0.
func PutStagingInitial(c *Client, id, sha256hex string, total int64, body io.Reader) error {
	hdr := http.Header{
		"X-Letts-Sha256": []string{sha256hex},
		"Content-Type":   []string{"application/octet-stream"},
	}
	return doStagingPut(c, "PUT", "/v1/staging/"+url.PathEscape(id), hdr, body, total)
}

// PutStagingResume continues a partially uploaded staging file. Sends
// Content-Range: bytes <offset>-<total-1>/<total> and sets ContentLength to
// the suffix size (total-offset). Errors if offset>=total.
func PutStagingResume(c *Client, id, sha256hex string, total, offset int64, body io.Reader) error {
	if offset >= total {
		return fmt.Errorf("PutStagingResume: offset %d must be < total %d", offset, total)
	}
	if offset < 0 {
		return fmt.Errorf("PutStagingResume: offset %d must be >= 0", offset)
	}
	hdr := http.Header{
		"X-Letts-Sha256": []string{sha256hex},
		"Content-Type":   []string{"application/octet-stream"},
		"Content-Range":  []string{fmt.Sprintf("bytes %d-%d/%d", offset, total-1, total)},
	}
	return doStagingPut(c, "PUT", "/v1/staging/"+url.PathEscape(id), hdr, body, total-offset)
}

// doStagingPut runs a PUT request and discards a 2xx response body. Non-2xx
// returns *HTTPError.
//
// Sticky retry: a 5xx (server-ambiguous) response triggers up to
// stagingPutMaxAttempts total attempts with the backoff cadence. The
// retry only kicks in when the body either is nil or implements io.Seeker
// (so we can rewind it for a fresh PUT) — for non-seekable bodies the
// original behaviour (fail on first 5xx) is preserved.
//
// 4xx propagates immediately; the caller (UploadFile) decides whether to
// HEAD-then-resume on 409/412/etc.
func doStagingPut(c *Client, method, relPath string, header http.Header, body io.Reader, contentLength int64) error {
	seeker, _ := body.(io.Seeker)
	canRetry := body == nil || seeker != nil

	// Wrap an *os.File (or any other ReadCloser body) in NopCloser so the
	// HTTP transport's req.Body.Close() doesn't close our underlying
	// file/handle — we need it open for the seek+retry below. Plain
	// io.Readers are already NopCloser-wrapped by http.NewRequest.
	var sendBody io.Reader
	if body != nil {
		sendBody = io.NopCloser(body)
	}

	attempts := stagingPutMaxAttempts
	if !canRetry {
		attempts = 1
	}
	var lastErr error
	for i := 0; i < attempts; i++ {
		if i > 0 && seeker != nil {
			if _, serr := seeker.Seek(0, io.SeekStart); serr != nil {
				return serr
			}
		}
		req, err := c.newRequest(method, relPath, header, sendBody)
		if err != nil {
			return err
		}
		req.ContentLength = contentLength
		resp, err := c.hc.Do(req)
		if err != nil {
			lastErr = err
			if i+1 < attempts {
				sleepWithJitter(stagingPutBackoffFor(i))
				continue
			}
			return err
		}
		if resp.StatusCode/100 == 2 {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			return nil
		}
		httpErr := decodeHTTPError(resp, req.URL.String())
		_ = resp.Body.Close()
		if resp.StatusCode < 500 {
			return httpErr
		}
		lastErr = httpErr
		if i+1 < attempts {
			sleepWithJitter(stagingPutBackoffFor(i))
		}
	}
	return lastErr
}

// stagingPutMaxAttempts is the cadence: 3 total attempts.
const stagingPutMaxAttempts = 3

// stagingPutBackoffFor mirrors RetryClient.backoffFor but uses the package
// DefaultBackoffMs directly so tests don't have to thread a custom cadence.
func stagingPutBackoffFor(attempt int) time.Duration {
	cadence := DefaultBackoffMs
	if attempt >= len(cadence) {
		attempt = len(cadence) - 1
	}
	return time.Duration(cadence[attempt]) * time.Millisecond
}

// GetStaging downloads /v1/staging/{id}. Pass rangeHeader = "" for a full
// download; pass e.g. "bytes=4-9" to resume. Returns the body, the
// Content-Length the server reported (-1 if unknown), and any error. Caller
// must Close the returned ReadCloser.
func GetStaging(c *Client, id, rangeHeader string) (io.ReadCloser, int64, error) {
	var hdr http.Header
	if rangeHeader != "" {
		hdr = http.Header{"Range": []string{rangeHeader}}
	}
	req, err := c.newRequest("GET", "/v1/staging/"+url.PathEscape(id), hdr, nil)
	if err != nil {
		return nil, 0, err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, 0, err
	}
	if resp.StatusCode/100 != 2 {
		err := decodeHTTPError(resp, req.URL.String())
		_ = resp.Body.Close()
		return nil, 0, err
	}
	return resp.Body, resp.ContentLength, nil
}

// DeleteStaging issues DELETE /v1/staging/{id}. With force=true appends
// ?force=true to cascade-delete dependent missions.
func DeleteStaging(c *Client, id string, force bool) error {
	path := "/v1/staging/" + url.PathEscape(id)
	if force {
		path += "?force=true"
	}
	return c.DoJSON("DELETE", path, nil, nil, nil)
}

// ListStaging queries GET /v1/staging. MissionID is required; the other
// fields are optional and omitted when zero.
func ListStaging(c *Client, opts ListStagingOpts) (*ListStagingResponse, error) {
	q := url.Values{}
	if opts.MissionID != "" {
		q.Set("mission_id", opts.MissionID)
	}
	if opts.RefKind != "" {
		q.Set("ref_kind", opts.RefKind)
	}
	if opts.Cursor != "" {
		q.Set("cursor", opts.Cursor)
	}
	if opts.Limit > 0 {
		q.Set("limit", strconv.Itoa(opts.Limit))
	}
	path := "/v1/staging"
	if enc := q.Encode(); enc != "" {
		path += "?" + enc
	}
	var out ListStagingResponse
	if err := c.DoJSON("GET", path, nil, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// StagingByContent looks up a staging artifact by sha256 and size on this
// dugdale. Returns (stagingID, true, nil) on 200, ("", false, nil) on 404,
// error otherwise. The size parameter is REQUIRED by the daemon.
func StagingByContent(c *Client, sha256hex string, size int64) (string, bool, error) {
	q := url.Values{}
	q.Set("size", strconv.FormatInt(size, 10))
	rel := "/v1/staging/by-content/" + sha256hex + "?" + q.Encode()
	var out struct {
		StagingID string `json:"staging_id"`
	}
	err := c.DoJSON("GET", rel, nil, nil, &out)
	if err == nil {
		return out.StagingID, true, nil
	}
	var he *HTTPError
	if errors.As(err, &he) && he.Status == http.StatusNotFound {
		return "", false, nil
	}
	return "", false, err
}
