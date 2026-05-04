package lettsclient

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGetMissionDecodesAllFields(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || r.URL.Path != "/v1/missions/01900000-0000-7000-8000-000000000001" {
			t.Errorf("got %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{
			"mission_id":"01900000-0000-7000-8000-000000000001",
			"kind":"mission",
			"lane":"normal",
			"mission_name":"DemoMission",
			"status":"done",
			"outcome":"success",
			"exit_code":0,
			"input_fingerprint":"fp123",
			"input":{"a":1},
			"pid":12345,
			"time_created":1714600000000,
			"time_started":1714600001000,
			"time_finished":1714600002000,
			"duration_ms":1000,
			"return":{"ok":true},
			"restarted_from":"01900000-0000-7000-8000-000000000000"
		}`)
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	m, err := GetMission(c, "01900000-0000-7000-8000-000000000001")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if m.MissionID != "01900000-0000-7000-8000-000000000001" {
		t.Errorf("mission_id=%q", m.MissionID)
	}
	if m.Kind != "mission" {
		t.Errorf("kind=%q", m.Kind)
	}
	if m.Lane != "normal" {
		t.Errorf("lane=%q", m.Lane)
	}
	if m.MissionName != "DemoMission" {
		t.Errorf("mission_name=%q", m.MissionName)
	}
	if m.Status != "done" {
		t.Errorf("status=%q", m.Status)
	}
	if m.Outcome != "success" {
		t.Errorf("outcome=%q", m.Outcome)
	}
	if m.ExitCode == nil || *m.ExitCode != 0 {
		t.Errorf("exit_code=%v", m.ExitCode)
	}
	if m.InputFingerprint != "fp123" {
		t.Errorf("input_fingerprint=%q", m.InputFingerprint)
	}
	if string(m.Input) != `{"a":1}` {
		t.Errorf("input=%s", m.Input)
	}
	if m.Pid != 12345 {
		t.Errorf("pid=%d", m.Pid)
	}
	if m.TimeCreatedMs != 1714600000000 {
		t.Errorf("time_created=%d", m.TimeCreatedMs)
	}
	if m.TimeStartedMs != 1714600001000 {
		t.Errorf("time_started=%d", m.TimeStartedMs)
	}
	if m.TimeFinishedMs != 1714600002000 {
		t.Errorf("time_finished=%d", m.TimeFinishedMs)
	}
	if m.DurationMs != 1000 {
		t.Errorf("duration_ms=%d", m.DurationMs)
	}
	if string(m.Return) != `{"ok":true}` {
		t.Errorf("return=%s", m.Return)
	}
	if m.RestartedFrom != "01900000-0000-7000-8000-000000000000" {
		t.Errorf("restarted_from=%q", m.RestartedFrom)
	}
}

func TestGetMissionPropagates404(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `{"error":"not_found","message":"mission not found"}`)
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	_, err := GetMission(c, "01900000-0000-7000-8000-000000000099")
	if err == nil {
		t.Fatalf("expected error")
	}
	he, ok := err.(*HTTPError)
	if !ok || he.Status != 404 || he.Code != "not_found" {
		t.Errorf("got %+v", err)
	}
}

func TestListMissionsBuildsQuery(t *testing.T) {
	var gotPath, gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"missions":[],"next_cursor":""}`)
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	_, err := ListMissions(c, ListMissionsOpts{
		Status:  "done",
		Outcome: "success",
		Lane:    "normal",
		Mission: "Demo",
		SinceMs: 1714600000000,
		UntilMs: 1714700000000,
		Cursor:  "abc",
		Limit:   50,
	})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if gotPath != "/v1/missions" {
		t.Errorf("path=%q", gotPath)
	}
	// Verify each query key is present.
	required := []string{
		"status=done",
		"outcome=success",
		"lane=normal",
		"mission=Demo",
		"since=1714600000000",
		"until=1714700000000",
		"cursor=abc",
		"limit=50",
	}
	for _, kv := range required {
		if !strings.Contains(gotQuery, kv) {
			t.Errorf("query %q missing %q", gotQuery, kv)
		}
	}
}

func TestListMissionsOmitsZeroValues(t *testing.T) {
	var gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"missions":[]}`)
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	_, err := ListMissions(c, ListMissionsOpts{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if gotQuery != "" {
		t.Errorf("empty opts should produce no query, got %q", gotQuery)
	}
}

func TestListMissionsOptsEncodesGroupID(t *testing.T) {
	var gotURL string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotURL = r.URL.String()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"missions":[]}`))
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	_, err := ListMissions(c, ListMissionsOpts{GroupID: "0192aaaa-0000-7000-8000-000000000000"})
	if err != nil {
		t.Fatalf("ListMissions: %v", err)
	}
	if !strings.Contains(gotURL, "group_id=0192aaaa-0000-7000-8000-000000000000") {
		t.Errorf("URL %q missing group_id", gotURL)
	}
}

func TestListMissionsOptsEncodesMissionPrefix(t *testing.T) {
	var gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"missions":[]}`))
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	if _, err := ListMissions(c, ListMissionsOpts{MissionPrefix: "deploy"}); err != nil {
		t.Fatalf("ListMissions: %v", err)
	}
	if !strings.Contains(gotQuery, "mission_prefix=deploy") {
		t.Errorf("query %q missing mission_prefix=deploy", gotQuery)
	}
}

func TestListMissionsDecodesPaginatedResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{
			"missions":[
				{"mission_id":"01900000-0000-7000-8000-000000000001","status":"done","mission_name":"A","lane":"normal","time_created":1},
				{"mission_id":"01900000-0000-7000-8000-000000000002","status":"queued","mission_name":"B","lane":"normal","time_created":2}
			],
			"next_cursor":"opaque-cursor-value"
		}`)
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	out, err := ListMissions(c, ListMissionsOpts{Limit: 2})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(out.Missions) != 2 {
		t.Fatalf("missions len=%d", len(out.Missions))
	}
	if out.Missions[0].MissionID != "01900000-0000-7000-8000-000000000001" || out.Missions[0].MissionName != "A" {
		t.Errorf("missions[0]=%+v", out.Missions[0])
	}
	if out.NextCursor != "opaque-cursor-value" {
		t.Errorf("next_cursor=%q", out.NextCursor)
	}
}

func TestRestartMissionPOSTAndDecode(t *testing.T) {
	var gotMethod, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{
			"mission_id":"01900000-0000-7000-8000-000000000010",
			"restarted_from":"01900000-0000-7000-8000-000000000009",
			"status":"queued"
		}`)
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	out, err := RestartMission(c, "01900000-0000-7000-8000-000000000009")
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	if gotMethod != "POST" {
		t.Errorf("method=%q", gotMethod)
	}
	if gotPath != "/v1/missions/01900000-0000-7000-8000-000000000009/restart" {
		t.Errorf("path=%q", gotPath)
	}
	if out.MissionID != "01900000-0000-7000-8000-000000000010" {
		t.Errorf("mission_id=%q", out.MissionID)
	}
	if out.RestartedFrom != "01900000-0000-7000-8000-000000000009" {
		t.Errorf("restarted_from=%q", out.RestartedFrom)
	}
	if out.Status != "queued" {
		t.Errorf("status=%q", out.Status)
	}
}

func TestKillMissionSendsSignalBody(t *testing.T) {
	var gotMethod, gotPath string
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"status":"kill_sent"}`)
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	if err := KillMission(c, "01900000-0000-7000-8000-000000000020", "KILL"); err != nil {
		t.Fatalf("kill: %v", err)
	}
	if gotMethod != "POST" {
		t.Errorf("method=%q", gotMethod)
	}
	if gotPath != "/v1/missions/01900000-0000-7000-8000-000000000020/kill" {
		t.Errorf("path=%q", gotPath)
	}
	var sent map[string]any
	if err := json.Unmarshal(gotBody, &sent); err != nil {
		t.Fatalf("body unmarshal: %v body=%s", err, gotBody)
	}
	if sent["signal"] != "KILL" {
		t.Errorf("signal=%v", sent["signal"])
	}
}

func TestKillMissionDefaultsSignalTERM(t *testing.T) {
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"status":"kill_sent"}`)
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	if err := KillMission(c, "01900000-0000-7000-8000-000000000021", ""); err != nil {
		t.Fatalf("kill: %v", err)
	}
	var sent map[string]any
	if err := json.Unmarshal(gotBody, &sent); err != nil {
		t.Fatalf("body unmarshal: %v", err)
	}
	if sent["signal"] != "TERM" {
		t.Errorf("default signal=%v", sent["signal"])
	}
}

func TestDeleteMissionUsesDELETEAndForceQuery(t *testing.T) {
	var gotMethod, gotPath, gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, `{"status":"deletion_pending"}`)
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	if err := DeleteMission(c, "01900000-0000-7000-8000-000000000030", false); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if gotMethod != "DELETE" {
		t.Errorf("method=%q", gotMethod)
	}
	if gotPath != "/v1/missions/01900000-0000-7000-8000-000000000030" {
		t.Errorf("path=%q", gotPath)
	}
	if gotQuery != "" {
		t.Errorf("force=false should not add query, got %q", gotQuery)
	}

	if err := DeleteMission(c, "01900000-0000-7000-8000-000000000031", true); err != nil {
		t.Fatalf("delete force: %v", err)
	}
	if gotQuery != "force=true" {
		t.Errorf("force=true query=%q", gotQuery)
	}
}

func TestBulkRestartSendsIDs(t *testing.T) {
	var gotMethod, gotPath string
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"results":[
			{"id":"01900000-0000-7000-8000-000000000040","ok":true},
			{"id":"01900000-0000-7000-8000-000000000041","ok":false,"error":"mission_not_done"}
		]}`)
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	out, err := BulkRestart(c, []string{
		"01900000-0000-7000-8000-000000000040",
		"01900000-0000-7000-8000-000000000041",
	})
	if err != nil {
		t.Fatalf("bulk-restart: %v", err)
	}
	if gotMethod != "POST" {
		t.Errorf("method=%q", gotMethod)
	}
	if gotPath != "/v1/missions/bulk-restart" {
		t.Errorf("path=%q", gotPath)
	}
	var sent map[string]any
	if err := json.Unmarshal(gotBody, &sent); err != nil {
		t.Fatalf("body unmarshal: %v body=%s", err, gotBody)
	}
	ids, _ := sent["ids"].([]any)
	if len(ids) != 2 {
		t.Errorf("ids len=%d", len(ids))
	}
	if len(out.Results) != 2 {
		t.Fatalf("results len=%d", len(out.Results))
	}
	if out.Results[0].ID != "01900000-0000-7000-8000-000000000040" || !out.Results[0].OK {
		t.Errorf("results[0]=%+v", out.Results[0])
	}
	if out.Results[1].OK || out.Results[1].Error != "mission_not_done" {
		t.Errorf("results[1]=%+v", out.Results[1])
	}
}

func TestBulkDeleteSendsForceFlagOnlyWhenTrue(t *testing.T) {
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"results":[]}`)
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})

	if _, err := BulkDelete(c, []string{"01900000-0000-7000-8000-000000000050"}, false); err != nil {
		t.Fatalf("bulk-delete: %v", err)
	}
	var sent map[string]any
	if err := json.Unmarshal(gotBody, &sent); err != nil {
		t.Fatalf("body unmarshal: %v body=%s", err, gotBody)
	}
	if _, ok := sent["force"]; ok {
		t.Errorf("force=false should be omitted, got %v", sent["force"])
	}

	if _, err := BulkDelete(c, []string{"01900000-0000-7000-8000-000000000051"}, true); err != nil {
		t.Fatalf("bulk-delete force: %v", err)
	}
	if err := json.Unmarshal(gotBody, &sent); err != nil {
		t.Fatalf("body unmarshal: %v", err)
	}
	if force, _ := sent["force"].(bool); !force {
		t.Errorf("force=true expected, got %v", sent["force"])
	}
}

func TestBulkDeleteDecodesResultsWithStatusField(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"results":[
			{"id":"01900000-0000-7000-8000-000000000060","ok":true,"status":"deletion_pending"}
		]}`)
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	out, err := BulkDelete(c, []string{"01900000-0000-7000-8000-000000000060"}, false)
	if err != nil {
		t.Fatalf("bulk-delete: %v", err)
	}
	if len(out.Results) != 1 || !out.Results[0].OK || out.Results[0].Status != "deletion_pending" {
		t.Errorf("results=%+v", out.Results)
	}
}
