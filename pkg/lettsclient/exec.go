package lettsclient

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
)

// uuidV7Re mirrors internal/ids.ValidateUUIDv7, inlined so pkg/lettsclient has
// zero letts/internal dependencies and is importable by external modules (arby).
var uuidV7Re = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

// ExecRequest mirrors the POST /v1/exec/dispatch body schema. JSON tags
// MUST match internal/server/handlers/exec_dispatch.go ExecRequest
// byte-for-byte. MissionID is excluded from body (json:"-") and used as
// the Idempotency-Key header.
type ExecRequest struct {
	MissionID      string         `json:"-"`
	Lane           string         `json:"lane"`
	Command        []string       `json:"command"`
	Script         *ExecScriptRef `json:"script,omitempty"`
	In             []ExecFileRef  `json:"in,omitempty"`
	Out            []ExecOutKey   `json:"out,omitempty"`
	Stdin          string         `json:"stdin,omitempty"`
	StdinStagingID string         `json:"stdin_staging_id,omitempty"`
	Timeout        string         `json:"timeout,omitempty"`
	GroupID        string         `json:"group_id,omitempty"`
	DisplayName    string         `json:"display_name,omitempty"`
}

// ExecScriptRef references the script staging file.
type ExecScriptRef struct {
	StagingID string `json:"staging_id"`
}

// ExecFileRef is one entry in the in[] array.
type ExecFileRef struct {
	Key       string `json:"key"`
	StagingID string `json:"staging_id"`
}

// ExecOutKey is one entry in the out[] array.
type ExecOutKey struct {
	Key string `json:"key"`
}

// ExecResponse is the 202 body from POST /v1/exec/dispatch.
type ExecResponse struct {
	ExecID string `json:"exec_id"`
	Status string `json:"status"`
}

// Exec dispatches an exec request. The caller MUST supply a valid UUIDv7
// MissionID — it serves as the Idempotency-Key header.
func Exec(c *Client, req ExecRequest) (*ExecResponse, error) {
	if !uuidV7Re.MatchString(req.MissionID) {
		return nil, fmt.Errorf("lettsclient.Exec: req.MissionID must be a valid UUIDv7, got %q", req.MissionID)
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	hdr := http.Header{}
	hdr.Set("Content-Type", "application/json")
	hdr.Set("Idempotency-Key", req.MissionID)
	var resp ExecResponse
	// Sticky retry — safe because the Idempotency-Key dedups.
	if err := stickyRetry(c).DoJSON("POST", "/v1/exec/dispatch", hdr, bytes.NewReader(body), &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}
