package lettsclient

import "net/url"

// PauseLane: POST /v1/admin/lanes/{name}/pause.
func PauseLane(c *Client, name string) error {
	return c.DoJSON("POST", "/v1/admin/lanes/"+url.PathEscape(name)+"/pause", nil, nil, nil)
}

// ContinueLane: POST /v1/admin/lanes/{name}/continue.
func ContinueLane(c *Client, name string) error {
	return c.DoJSON("POST", "/v1/admin/lanes/"+url.PathEscape(name)+"/continue", nil, nil, nil)
}
