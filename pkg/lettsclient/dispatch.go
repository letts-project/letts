package lettsclient

import (
	"bytes"
	"encoding/json"
	"net/http"
)

// DispatchRequest is the body of POST /v1/dispatch. MissionID is sent as the
// Idempotency-Key header, not in the JSON body.
type DispatchRequest struct {
	MissionID string           `json:"-"`
	Mission   string           `json:"mission"`
	Lane      string           `json:"lane"`
	Input     json.RawMessage  `json:"input,omitempty"`
	Files     []DispatchedFile `json:"files,omitempty"`
	Timeout   string           `json:"timeout,omitempty"`
}

// DispatchedFile is one entry in DispatchRequest.Files.
type DispatchedFile struct {
	Role      string `json:"role"`
	StagingID string `json:"staging_id"`
}

// DispatchResponse mirrors the 202 (or 200 replay) response body from
// POST /v1/dispatch.
type DispatchResponse struct {
	MissionID string `json:"mission_id"`
	Status    string `json:"status"`
}

// Dispatch posts a mission. The MissionID field is used as the
// Idempotency-Key header and is not serialized into the body.
func Dispatch(c *Client, req DispatchRequest) (*DispatchResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	hdr := http.Header{"Idempotency-Key": []string{req.MissionID}}
	var out DispatchResponse
	// Sticky retry — safe because the Idempotency-Key dedups.
	if err := stickyRetry(c).DoJSON("POST", "/v1/dispatch", hdr, bytes.NewReader(body), &out); err != nil {
		return nil, err
	}
	return &out, nil
}
