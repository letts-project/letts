package main

import (
	"bytes"
	"encoding/json"
	"net/url"

	"letts/internal/apply"
	"letts/pkg/lettsclient"
)

// ApplyOptions mirrors server query params on POST /v1/admin/apply.
type ApplyOptions struct {
	Force      bool
	Prune      bool
	ForcePrune bool
}

// ApplyResult mirrors apply.Result.
type ApplyResult = apply.Result

// Apply: POST /v1/admin/apply. Lives in cmd/letts (not pkg/lettsclient)
// because apply.AppliedState/apply.Result drag the daemon-side apply closure
// (lane/mission/storage), which must not enter an arby-importable package.
func Apply(c *lettsclient.Client, desired apply.AppliedState, opts ApplyOptions) (*ApplyResult, error) {
	body, err := json.Marshal(desired)
	if err != nil {
		return nil, err
	}
	q := url.Values{}
	if opts.Force {
		q.Set("force", "true")
	}
	if opts.Prune {
		q.Set("prune", "true")
	}
	if opts.ForcePrune {
		q.Set("force_prune", "true")
	}
	path := "/v1/admin/apply"
	if q.Encode() != "" {
		path += "?" + q.Encode()
	}
	var out ApplyResult
	if err := c.DoJSON("POST", path, nil, bytes.NewReader(body), &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// StateResponse mirrors GET /v1/admin/state.
type StateResponse struct {
	AppliedAt *int64             `json:"applied_at"`
	Source    string             `json:"source,omitempty"`
	State     apply.AppliedState `json:"state"`
}

// GetState: GET /v1/admin/state.
func GetState(c *lettsclient.Client) (*StateResponse, error) {
	var out StateResponse
	if err := c.DoJSON("GET", "/v1/admin/state", nil, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
