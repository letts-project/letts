package lettsclient

import (
	"bytes"
	"encoding/json"
	"net/url"
	"strconv"
)

// Mission mirrors the JSON object returned by GET /v1/missions/{id} and the
// items inside GET /v1/missions. Field tags match daemon
// handlers.buildMissionResponse exactly. Optional fields use Go zero values
// since the daemon omits absent keys.
type Mission struct {
	MissionID        string          `json:"mission_id"`
	Kind             string          `json:"kind,omitempty"`
	Lane             string          `json:"lane,omitempty"`
	MissionName      string          `json:"mission_name,omitempty"`
	DisplayName      string          `json:"display_name,omitempty"`
	GroupID          string          `json:"group_id,omitempty"`
	Status           string          `json:"status"`
	Outcome          string          `json:"outcome,omitempty"`
	ExitCode         *int            `json:"exit_code,omitempty"`
	Signal           string          `json:"signal,omitempty"`
	FailReason       string          `json:"fail_reason,omitempty"`
	FailMessage      string          `json:"fail_message,omitempty"`
	FailDetails      json.RawMessage `json:"fail_details,omitempty"`
	Return           json.RawMessage `json:"return,omitempty"`
	Input            json.RawMessage `json:"input,omitempty"`
	InputFingerprint string          `json:"input_fingerprint,omitempty"`
	Pid              int             `json:"pid,omitempty"`
	TimeCreatedMs    int64           `json:"time_created,omitempty"`
	TimeStartedMs    int64           `json:"time_started,omitempty"`
	TimeFinishedMs   int64           `json:"time_finished,omitempty"`
	DurationMs       int64           `json:"duration_ms,omitempty"`
	TimeoutMs        int64           `json:"timeout_ms,omitempty"`
	TruncatedStdout  bool            `json:"truncated_stdout,omitempty"`
	TruncatedStderr  bool            `json:"truncated_stderr,omitempty"`
	RestartedFrom    string          `json:"restarted_from,omitempty"`
	Inputs           []MissionFile   `json:"inputs,omitempty"`
	// Outputs is a map keyed by role.
	Outputs map[string]MissionFile `json:"outputs,omitempty"`
}

// MissionFile is one staging artifact attached to a mission. Used for both
// the "inputs" array and the "outputs" map; the outputs entries omit "role"
// (the role is the map key).
type MissionFile struct {
	Role      string `json:"role,omitempty"`
	StagingID string `json:"staging_id"`
	Sha256    string `json:"sha256,omitempty"`
	Size      int64  `json:"size,omitempty"`
}

// GetMission fetches a single mission resource.
func GetMission(c *Client, id string) (*Mission, error) {
	var m Mission
	if err := c.DoJSON("GET", "/v1/missions/"+url.PathEscape(id), nil, nil, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// ListMissionsOpts holds the query filter for GET /v1/missions. All fields
// are optional; zero values are omitted from the query string.
type ListMissionsOpts struct {
	Status        string
	Outcome       string
	Lane          string
	Mission       string // substring match on mission name
	MissionPrefix string // anchored prefix match on mission name
	Kind          string
	GroupID       string // exec group filter
	Order         string // "" / "created" (default) | "finished"
	SinceMs       int64
	UntilMs       int64
	Cursor        string
	Limit         int
}

// ListMissionsResponse mirrors `{"missions":[...], "next_cursor":"..."}`.
// NextCursor is empty when no more pages.
type ListMissionsResponse struct {
	Missions   []Mission `json:"missions"`
	NextCursor string    `json:"next_cursor,omitempty"`
}

func (o ListMissionsOpts) toQuery() string {
	q := url.Values{}
	if o.Status != "" {
		q.Set("status", o.Status)
	}
	if o.Outcome != "" {
		q.Set("outcome", o.Outcome)
	}
	if o.Lane != "" {
		q.Set("lane", o.Lane)
	}
	if o.Mission != "" {
		q.Set("mission", o.Mission)
	}
	if o.MissionPrefix != "" {
		q.Set("mission_prefix", o.MissionPrefix)
	}
	if o.Kind != "" {
		q.Set("kind", o.Kind)
	}
	if o.GroupID != "" {
		q.Set("group_id", o.GroupID)
	}
	if o.SinceMs > 0 {
		q.Set("since", strconv.FormatInt(o.SinceMs, 10))
	}
	if o.UntilMs > 0 {
		q.Set("until", strconv.FormatInt(o.UntilMs, 10))
	}
	if o.Order != "" {
		q.Set("order", o.Order)
	}
	if o.Cursor != "" {
		q.Set("cursor", o.Cursor)
	}
	if o.Limit > 0 {
		q.Set("limit", strconv.Itoa(o.Limit))
	}
	return q.Encode()
}

// ListMissions queries GET /v1/missions with the given filter.
func ListMissions(c *Client, opts ListMissionsOpts) (*ListMissionsResponse, error) {
	path := "/v1/missions"
	if q := opts.toQuery(); q != "" {
		path += "?" + q
	}
	var out ListMissionsResponse
	if err := c.DoJSON("GET", path, nil, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// RestartResponse mirrors the 201 body from POST /v1/missions/{id}/restart.
type RestartResponse struct {
	MissionID     string `json:"mission_id"`
	RestartedFrom string `json:"restarted_from"`
	Status        string `json:"status"`
}

// RestartMission posts a restart. The new mission id is in the response.
func RestartMission(c *Client, id string) (*RestartResponse, error) {
	var out RestartResponse
	if err := c.DoJSON("POST", "/v1/missions/"+url.PathEscape(id)+"/restart", nil, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// KillMission posts a kill request. signal defaults to "TERM" if empty.
// The daemon currently treats signal as advisory but accepts it for forward
// compat (see handlers/missions_kill.go).
func KillMission(c *Client, id, signal string) error {
	if signal == "" {
		signal = "TERM"
	}
	body, _ := json.Marshal(map[string]string{"signal": signal})
	return c.DoJSON("POST", "/v1/missions/"+url.PathEscape(id)+"/kill", nil, bytes.NewReader(body), nil)
}

// DeleteMission issues DELETE /v1/missions/{id}. force=true adds ?force=true
// (required to delete a running mission).
func DeleteMission(c *Client, id string, force bool) error {
	path := "/v1/missions/" + url.PathEscape(id)
	if force {
		path += "?force=true"
	}
	return c.DoJSON("DELETE", path, nil, nil, nil)
}

// BulkResult is one entry in BulkResponse.Results. MissionID is set on
// restart success (new mission id); Status is set on delete success
// ("deletion_pending"). Details may be present on per-id failures (e.g.
// idempotency conflict hints).
type BulkResult struct {
	ID        string         `json:"id"`
	OK        bool           `json:"ok"`
	Error     string         `json:"error,omitempty"`
	MissionID string         `json:"mission_id,omitempty"`
	Status    string         `json:"status,omitempty"`
	Details   map[string]any `json:"details,omitempty"`
}

// BulkResponse mirrors `{"results":[...]}` returned by bulk-restart and
// bulk-delete.
type BulkResponse struct {
	Results []BulkResult `json:"results"`
}

// BulkRestart issues POST /v1/missions/bulk-restart.
func BulkRestart(c *Client, ids []string) (*BulkResponse, error) {
	body, _ := json.Marshal(map[string][]string{"ids": ids})
	var out BulkResponse
	if err := c.DoJSON("POST", "/v1/missions/bulk-restart", nil, bytes.NewReader(body), &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// BulkDelete issues POST /v1/missions/bulk-delete. force=true is added to
// the body only when true (matches daemon's `omitempty` parsing).
func BulkDelete(c *Client, ids []string, force bool) (*BulkResponse, error) {
	body := map[string]any{"ids": ids}
	if force {
		body["force"] = true
	}
	b, _ := json.Marshal(body)
	var out BulkResponse
	if err := c.DoJSON("POST", "/v1/missions/bulk-delete", nil, bytes.NewReader(b), &out); err != nil {
		return nil, err
	}
	return &out, nil
}
