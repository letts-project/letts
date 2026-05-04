package lettsclient

import (
	"context"
	"fmt"
	"io"
	"net/url"
)

// OutputOpts configures OpenOutput.
type OutputOpts struct {
	Stream string // stdout|stderr|combined (required)
	Follow bool
}

// OpenOutput returns an io.ReadCloser for /v1/missions/{id}/output.
// Caller must Close. Errors out if stream is empty.
func OpenOutput(ctx context.Context, c *Client, missionID string, opts OutputOpts) (io.ReadCloser, error) {
	if opts.Stream == "" {
		return nil, fmt.Errorf("OpenOutput requires opts.Stream")
	}
	q := url.Values{}
	q.Set("stream", opts.Stream)
	if opts.Follow {
		q.Set("follow", "true")
	}
	path := "/v1/missions/" + url.PathEscape(missionID) + "/output?" + q.Encode()
	req, err := c.newRequest("GET", path, nil, nil)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		err := decodeHTTPError(resp, req.URL.String())
		_ = resp.Body.Close()
		return nil, err
	}
	return resp.Body, nil
}
