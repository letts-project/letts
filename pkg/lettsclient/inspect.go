package lettsclient

// DugdaleInfo mirrors GET /v1/dugdale response shape (handlers.Dugdale).
type DugdaleInfo struct {
	Version       string       `json:"version"`
	UptimeSeconds float64      `json:"uptime_seconds"`
	AppliedAt     *int64       `json:"applied_at"`
	QueueSummary  QueueSummary `json:"queue_summary"`
}

// QueueSummary holds aggregated queued/running mission counts.
type QueueSummary struct {
	Queued  int `json:"queued"`
	Running int `json:"running"`
}

// LaneInfo mirrors one element of GET /v1/lanes response.
type LaneInfo struct {
	Name        string `json:"name"`
	Concurrency int    `json:"concurrency"`
	Paused      bool   `json:"paused"`
	Queued      int    `json:"queued"`
	Running     int    `json:"running"`
}

// GetDugdaleInfo: GET /v1/dugdale.
func GetDugdaleInfo(c *Client) (*DugdaleInfo, error) {
	var out DugdaleInfo
	if err := c.DoJSON("GET", "/v1/dugdale", nil, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ListLanes: GET /v1/lanes.
func ListLanes(c *Client) ([]LaneInfo, error) {
	var out []LaneInfo
	if err := c.DoJSON("GET", "/v1/lanes", nil, nil, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// Healthz: GET /v1/healthz. Returns nil if 200.
func Healthz(c *Client) error {
	return c.DoJSON("GET", "/v1/healthz", nil, nil, nil)
}

// Readyz: GET /v1/readyz. Returns nil if 200, *HTTPError on 503.
func Readyz(c *Client) error {
	return c.DoJSON("GET", "/v1/readyz", nil, nil, nil)
}
