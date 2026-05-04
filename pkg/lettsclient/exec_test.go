package lettsclient

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestExecRejectsEmptyMissionID(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("server reached despite empty MissionID")
	}))
	defer srv.Close()
	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	_, err := Exec(c, ExecRequest{Lane: "light", Command: []string{"uptime"}})
	if err == nil {
		t.Fatal("expected error for empty MissionID")
	}
	if !strings.Contains(err.Error(), "MissionID") {
		t.Errorf("error %q does not mention MissionID", err)
	}
}

func TestExecSendsIdempotencyKeyAndBody(t *testing.T) {
	var gotKey, gotPath, gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.Header.Get("Idempotency-Key")
		gotPath = r.URL.Path
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"exec_id":"0192aaaa-0000-7000-8000-000000000000","status":"queued"}`))
	}))
	defer srv.Close()
	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	mid := "0192aaaa-0000-7000-8000-000000000001"
	resp, err := Exec(c, ExecRequest{
		MissionID: mid, Lane: "light", Command: []string{"uptime"},
		DisplayName: "uptime",
	})
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if gotKey != mid {
		t.Errorf("Idempotency-Key=%q, want %q", gotKey, mid)
	}
	if gotPath != "/v1/exec/dispatch" {
		t.Errorf("path=%q", gotPath)
	}
	if !strings.Contains(gotBody, `"command":["uptime"]`) {
		t.Errorf("body missing command: %s", gotBody)
	}
	if !strings.Contains(gotBody, `"display_name":"uptime"`) {
		t.Errorf("body missing display_name: %s", gotBody)
	}
	if strings.Contains(gotBody, `"mission_id"`) {
		t.Errorf("body should NOT contain mission_id (json:\"-\"): %s", gotBody)
	}
	if resp.ExecID == "" || resp.Status != "queued" {
		t.Errorf("resp=%+v", resp)
	}
}

// TestExecRequestSchemaMatchesServer round-trips a fully-populated request
// through both the lettsclient.ExecRequest and a struct mirroring the
// server-side shape via field-by-field json key comparison.
func TestExecRequestSchemaMatchesServer(t *testing.T) {
	req := ExecRequest{
		MissionID:      "0192aaaa-0000-7000-8000-000000000001",
		Lane:           "light",
		Command:        []string{"bash", "-c", "echo hi"},
		Script:         &ExecScriptRef{StagingID: "0192bbbb-0000-7000-8000-000000000000"},
		In:             []ExecFileRef{{Key: "pdf", StagingID: "0192cccc-0000-7000-8000-000000000000"}},
		Out:            []ExecOutKey{{Key: "png"}},
		Stdin:          "single",
		StdinStagingID: "0192dddd-0000-7000-8000-000000000000",
		Timeout:        "5m",
		GroupID:        "0192eeee-0000-7000-8000-000000000000",
		DisplayName:    "echo hi",
	}
	raw, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	// Re-decode via server-side keys (one of each):
	var server struct {
		Lane    string   `json:"lane"`
		Command []string `json:"command"`
		Script  *struct {
			StagingID string `json:"staging_id"`
		} `json:"script"`
		In []struct {
			Key       string `json:"key"`
			StagingID string `json:"staging_id"`
		} `json:"in"`
		Out []struct {
			Key string `json:"key"`
		} `json:"out"`
		Stdin          string `json:"stdin"`
		StdinStagingID string `json:"stdin_staging_id"`
		Timeout        string `json:"timeout"`
		GroupID        string `json:"group_id"`
		DisplayName    string `json:"display_name"`
	}
	if err := json.Unmarshal(raw, &server); err != nil {
		t.Fatal(err)
	}
	if server.Lane != "light" || server.Command[0] != "bash" ||
		server.Script == nil || server.Script.StagingID != "0192bbbb-0000-7000-8000-000000000000" ||
		len(server.In) != 1 || server.In[0].Key != "pdf" ||
		len(server.Out) != 1 || server.Out[0].Key != "png" ||
		server.Stdin != "single" || server.Timeout != "5m" ||
		server.GroupID != "0192eeee-0000-7000-8000-000000000000" ||
		server.DisplayName != "echo hi" {
		t.Fatalf("schema mismatch — server decode = %+v", server)
	}
	// Negative: the inserted In[].StagingID field name must round-trip.
	// Re-encode server-shape and verify our struct still parses it.
	srvRaw, _ := json.Marshal(server)
	var roundTrip ExecRequest
	if err := json.Unmarshal(srvRaw, &roundTrip); err != nil {
		t.Fatal(err)
	}
	if roundTrip.In[0].StagingID == "" {
		t.Errorf("In[0].StagingID lost on round-trip (json tag mismatch)")
	}
}
