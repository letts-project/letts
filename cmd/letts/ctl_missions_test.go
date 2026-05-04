package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"letts/pkg/lettsclient"
	"letts/pkg/lettsconfig"
)

// TestCtlMissionsListJSONFormat verifies that --output=json round-trips the
// ListMissionsResponse from daemon to stdout intact.
func TestCtlMissionsListJSONFormat(t *testing.T) {
	ac, stop := stubAppCtx(t, "s1", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/missions" {
			t.Errorf("path=%q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{
			"missions":[
				{"mission_id":"01900000-0000-7000-8000-000000000001","status":"done","outcome":"success","lane":"normal","mission_name":"A","time_created":1714600000000},
				{"mission_id":"01900000-0000-7000-8000-000000000002","status":"queued","lane":"normal","mission_name":"B","time_created":1714600000500}
			],
			"next_cursor":"opaque-cursor"
		}`)
	}))
	defer stop()

	var out bytes.Buffer
	if err := runCtlMissionsList(ac, &out, "s1", lettsclient.ListMissionsOpts{}, FormatJSON); err != nil {
		t.Fatalf("runCtlMissionsList: %v", err)
	}
	var resp lettsclient.ListMissionsResponse
	if err := json.NewDecoder(&out).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Missions) != 2 {
		t.Fatalf("missions len=%d", len(resp.Missions))
	}
	if resp.Missions[0].MissionID != "01900000-0000-7000-8000-000000000001" {
		t.Errorf("missions[0].id=%q", resp.Missions[0].MissionID)
	}
	if resp.NextCursor != "opaque-cursor" {
		t.Errorf("next_cursor=%q", resp.NextCursor)
	}
}

// TestCtlMissionsListTextFormat verifies the tab/space-aligned table has a
// header row and one row per mission. Cursor is appended when present.
func TestCtlMissionsListTextFormat(t *testing.T) {
	ac, stop := stubAppCtx(t, "s1", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{
			"missions":[
				{"mission_id":"01900000-0000-7000-8000-000000000001","status":"done","outcome":"success","lane":"normal","mission_name":"A","time_created":1714600000000},
				{"mission_id":"01900000-0000-7000-8000-000000000002","status":"queued","lane":"normal","mission_name":"B","time_created":1714600000500}
			],
			"next_cursor":"opaque-cursor"
		}`)
	}))
	defer stop()

	var out bytes.Buffer
	if err := runCtlMissionsList(ac, &out, "s1", lettsclient.ListMissionsOpts{}, FormatText); err != nil {
		t.Fatalf("runCtlMissionsList: %v", err)
	}
	s := out.String()
	if !strings.Contains(s, "MISSION_ID") || !strings.Contains(s, "STATUS") || !strings.Contains(s, "TIME_CREATED") {
		t.Errorf("header missing: %q", s)
	}
	if !strings.Contains(s, "01900000-0000-7000-8000-000000000001") || !strings.Contains(s, "01900000-0000-7000-8000-000000000002") {
		t.Errorf("data rows missing: %q", s)
	}
	if !strings.Contains(s, "cursor: opaque-cursor") {
		t.Errorf("cursor footer missing: %q", s)
	}
	// Two data rows + header + cursor lines.
	dataLines := 0
	for _, line := range strings.Split(s, "\n") {
		if strings.HasPrefix(line, "01900000") {
			dataLines++
		}
	}
	if dataLines != 2 {
		t.Errorf("want 2 data rows, got %d in %q", dataLines, s)
	}
}

// TestCtlMissionsListOmitsGroupIDWhenAbsent — when no row carries a
// group_id (or display_name), the renderer must NOT widen the table with
// the optional columns. Plain mission listings stay narrow.
func TestCtlMissionsListOmitsGroupIDWhenAbsent(t *testing.T) {
	ac, stop := stubAppCtx(t, "s1", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"missions":[
			{"mission_id":"01900000-0000-7000-8000-000000000001","status":"done","outcome":"success","lane":"normal","mission_name":"NamedMission","time_created":1700000000000}
		]}`)
	}))
	defer stop()

	var out bytes.Buffer
	if err := runCtlMissionsList(ac, &out, "s1",
		lettsclient.ListMissionsOpts{Kind: "mission"}, FormatText); err != nil {
		t.Fatalf("runCtlMissionsList: %v", err)
	}
	s := out.String()
	if strings.Contains(s, "GROUP_ID") {
		t.Errorf("should NOT show GROUP_ID when no row has one: %q", s)
	}
	if strings.Contains(s, "DISPLAY_NAME") {
		t.Errorf("should NOT show DISPLAY_NAME when no row has one: %q", s)
	}
}

// TestCtlMissionsListNoCursorOmitsFooter — when next_cursor is empty,
// the trailing "cursor: ..." line must not appear.
func TestCtlMissionsListNoCursorOmitsFooter(t *testing.T) {
	ac, stop := stubAppCtx(t, "s1", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"missions":[],"next_cursor":""}`)
	}))
	defer stop()

	var out bytes.Buffer
	if err := runCtlMissionsList(ac, &out, "s1", lettsclient.ListMissionsOpts{}, FormatText); err != nil {
		t.Fatalf("runCtlMissionsList: %v", err)
	}
	if strings.Contains(out.String(), "cursor:") {
		t.Errorf("did not expect cursor footer, got %q", out.String())
	}
}

// TestCtlMissionsListFilters asserts query parameters are forwarded.
func TestCtlMissionsListFilters(t *testing.T) {
	var gotQuery string
	ac, stop := stubAppCtx(t, "s1", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"missions":[],"next_cursor":""}`)
	}))
	defer stop()

	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	sinceMs, err := parseSinceTime("-1h", now)
	if err != nil {
		t.Fatalf("parseSinceTime: %v", err)
	}
	opts := lettsclient.ListMissionsOpts{
		Status:  "done",
		Outcome: "success",
		Lane:    "normal",
		Mission: "Demo",
		SinceMs: sinceMs,
		Limit:   25,
	}
	var out bytes.Buffer
	if err := runCtlMissionsList(ac, &out, "s1", opts, FormatJSON); err != nil {
		t.Fatalf("runCtlMissionsList: %v", err)
	}
	for _, kv := range []string{
		"status=done",
		"outcome=success",
		"lane=normal",
		"mission=Demo",
		"limit=25",
	} {
		if !strings.Contains(gotQuery, kv) {
			t.Errorf("query %q missing %q", gotQuery, kv)
		}
	}
	if !strings.Contains(gotQuery, "since=") {
		t.Errorf("query %q missing since=", gotQuery)
	}
}

// TestCtlMissionsListRequiresHost — invoking the cobra command without
// --host must surface a BadUsageError so main() maps to exitBadUsage.
func TestCtlMissionsListRequiresHost(t *testing.T) {
	cmd := newCtlMissionsListCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected BadUsageError for missing --host")
	}
	var bue *BadUsageError
	if !errors.As(err, &bue) {
		t.Errorf("got %T %v, want *BadUsageError", err, err)
	}
}

// TestCtlMissionsShowExplicitHost — with --host, runCtlMissionsShow goes
// straight to the named dugdale and renders the mission JSON.
func TestCtlMissionsShowExplicitHost(t *testing.T) {
	ac, stop := stubAppCtx(t, "s1", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/missions/mid" {
			t.Errorf("path=%q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"mission_id":"mid","status":"done","outcome":"success"}`)
	}))
	defer stop()

	var out bytes.Buffer
	if err := runCtlMissionsShow(ac, &out, "mid", "s1", nil, FormatJSON); err != nil {
		t.Fatalf("runCtlMissionsShow: %v", err)
	}
	var got lettsclient.Mission
	if err := json.NewDecoder(&out).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.MissionID != "mid" || got.Outcome != "success" {
		t.Errorf("got %+v", got)
	}
}

// TestCtlMissionsShowByIDFanOut — no --host, two configured dugdales: the
// first 404s, the second returns 200; the output is the 200's body. Verifies
// the show subcommand wires FanOutByID correctly.
func TestCtlMissionsShowByIDFanOut(t *testing.T) {
	ac := stubByIDAppCtx(t, []*fanoutByIDStub{
		{id: "a", status: 404, body: `{"error":"not_found"}`},
		{id: "b", status: 200, body: `{"mission_id":"mid","status":"done","outcome":"success"}`},
	})

	var out bytes.Buffer
	if err := runCtlMissionsShow(ac, &out, "mid", "", nil, FormatJSON); err != nil {
		t.Fatalf("runCtlMissionsShow: %v", err)
	}
	if !strings.Contains(out.String(), `"mission_id"`) || !strings.Contains(out.String(), `"success"`) {
		t.Errorf("body missing mission fields: %q", out.String())
	}
}

// TestCtlMissionsOutputStreamsToStdout — runCtlMissionsOutput copies the
// raw response body for /output to stdout. Uses an explicit --host so we
// exercise the singlehost path; fan-out logic is covered by the helper tests.
func TestCtlMissionsOutputStreamsToStdout(t *testing.T) {
	ac, stop := stubAppCtx(t, "s1", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/v1/missions/mid/output") {
			t.Errorf("path=%q", r.URL.Path)
		}
		if r.URL.Query().Get("stream") != "stdout" {
			t.Errorf("stream=%q want stdout", r.URL.Query().Get("stream"))
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = io.WriteString(w, "line one\nline two\n")
	}))
	defer stop()

	cmd := newCtlMissionsOutputCmd()
	cmd.SetContext(t.Context())
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(io.Discard)

	if err := runCtlMissionsOutput(cmd, ac, "mid", "s1", "stdout", false, nil); err != nil {
		t.Fatalf("runCtlMissionsOutput: %v", err)
	}
	if !strings.Contains(out.String(), "line one") || !strings.Contains(out.String(), "line two") {
		t.Errorf("body missing: %q", out.String())
	}
}

// TestCtlMissionsRestartExplicitHost — with --host, runCtlMissionsRestart
// POSTs /restart and prints the new mission id (text format).
func TestCtlMissionsRestartExplicitHost(t *testing.T) {
	var gotMethod, gotPath string
	ac, stop := stubAppCtx(t, "s1", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{"mission_id":"new","restarted_from":"mid","status":"queued"}`)
	}))
	defer stop()

	var out bytes.Buffer
	if err := runCtlMissionsRestart(ac, &out, "mid", "s1", nil, FormatText); err != nil {
		t.Fatalf("runCtlMissionsRestart: %v", err)
	}
	if gotMethod != "POST" {
		t.Errorf("method=%q want POST", gotMethod)
	}
	if gotPath != "/v1/missions/mid/restart" {
		t.Errorf("path=%q", gotPath)
	}
	if strings.TrimSpace(out.String()) != "new" {
		t.Errorf("stdout=%q want 'new'", out.String())
	}
}

// TestCtlMissionsRestartJSONFormat — JSON output renders the full
// RestartResponse so callers can grab restarted_from / status too.
func TestCtlMissionsRestartJSONFormat(t *testing.T) {
	ac, stop := stubAppCtx(t, "s1", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{"mission_id":"new","restarted_from":"mid","status":"queued"}`)
	}))
	defer stop()

	var out bytes.Buffer
	if err := runCtlMissionsRestart(ac, &out, "mid", "s1", nil, FormatJSON); err != nil {
		t.Fatalf("runCtlMissionsRestart: %v", err)
	}
	var resp lettsclient.RestartResponse
	if err := json.NewDecoder(&out).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.MissionID != "new" || resp.RestartedFrom != "mid" || resp.Status != "queued" {
		t.Errorf("resp=%+v", resp)
	}
}

// TestCtlMissionsKillSendsSignal — --signal=KILL is forwarded in the POST body.
func TestCtlMissionsKillSendsSignal(t *testing.T) {
	var gotMethod, gotPath string
	var gotBody []byte
	ac, stop := stubAppCtx(t, "s1", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"status":"kill_sent"}`)
	}))
	defer stop()

	if err := runCtlMissionsKill(ac, "mid", "s1", "KILL", nil); err != nil {
		t.Fatalf("runCtlMissionsKill: %v", err)
	}
	if gotMethod != "POST" {
		t.Errorf("method=%q want POST", gotMethod)
	}
	if gotPath != "/v1/missions/mid/kill" {
		t.Errorf("path=%q", gotPath)
	}
	var sent map[string]any
	if err := json.Unmarshal(gotBody, &sent); err != nil {
		t.Fatalf("body unmarshal: %v body=%s", err, gotBody)
	}
	if sent["signal"] != "KILL" {
		t.Errorf("signal=%v want KILL", sent["signal"])
	}
}

// TestCtlMissionsDeleteForce — --force=true adds ?force=true to the DELETE URL.
func TestCtlMissionsDeleteForce(t *testing.T) {
	var gotMethod, gotPath, gotQuery string
	ac, stop := stubAppCtx(t, "s1", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, `{"status":"deletion_pending"}`)
	}))
	defer stop()

	if err := runCtlMissionsDelete(ac, "mid", "s1", true, nil); err != nil {
		t.Fatalf("runCtlMissionsDelete: %v", err)
	}
	if gotMethod != "DELETE" {
		t.Errorf("method=%q want DELETE", gotMethod)
	}
	if gotPath != "/v1/missions/mid" {
		t.Errorf("path=%q", gotPath)
	}
	if gotQuery != "force=true" {
		t.Errorf("query=%q want force=true", gotQuery)
	}
}

// TestCtlMissionsDeleteNoForceOmitsQuery — without --force, the URL must have
// no query string (the daemon distinguishes by presence, not value).
func TestCtlMissionsDeleteNoForceOmitsQuery(t *testing.T) {
	var gotQuery string
	ac, stop := stubAppCtx(t, "s1", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, `{"status":"deletion_pending"}`)
	}))
	defer stop()

	if err := runCtlMissionsDelete(ac, "mid", "s1", false, nil); err != nil {
		t.Fatalf("runCtlMissionsDelete: %v", err)
	}
	if gotQuery != "" {
		t.Errorf("query=%q want empty", gotQuery)
	}
}

// TestCtlMissionsRestartByIDFanOut — exercises the no-host fan-out path:
// host "a" 404s, host "b" 201s; we must print "b"'s new mission id.
func TestCtlMissionsRestartByIDFanOut(t *testing.T) {
	ac := stubByIDAppCtx(t, []*fanoutByIDStub{
		{id: "a", status: 404, body: `{"error":"not_found"}`},
		{id: "b", status: 201, body: `{"mission_id":"new","restarted_from":"mid","status":"queued"}`},
	})
	var out bytes.Buffer
	if err := runCtlMissionsRestart(ac, &out, "mid", "", nil, FormatText); err != nil {
		t.Fatalf("runCtlMissionsRestart: %v", err)
	}
	if strings.TrimSpace(out.String()) != "new" {
		t.Errorf("stdout=%q want 'new'", out.String())
	}
}

// bulkStubHandler builds an http.HandlerFunc that serves two routes for
// bulk subcommand tests:
//   - GET  /v1/missions          → listBody (records the URL.RawQuery)
//   - POST /v1/missions/bulk-X   → bulkBody (records hit and body)
//
// hits & bulkBody are returned via pointers so the test can assert them.
type bulkRecorder struct {
	listQuery string
	bulkHits  int
	bulkBody  []byte
}

func bulkStubHandler(t *testing.T, bulkPath, listBody, bulkBody string) (http.HandlerFunc, *bulkRecorder) {
	t.Helper()
	rec := &bulkRecorder{}
	h := func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "GET" && r.URL.Path == "/v1/missions":
			rec.listQuery = r.URL.RawQuery
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, listBody)
		case r.Method == "POST" && r.URL.Path == bulkPath:
			rec.bulkHits++
			rec.bulkBody, _ = io.ReadAll(r.Body)
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, bulkBody)
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.String())
			http.Error(w, "unexpected", http.StatusNotImplemented)
		}
	}
	return h, rec
}

// TestBulkRestartDryRun — --dry-run lists matching missions and prints the
// "would restart" preamble and ids; it must NOT issue the bulk POST.
func TestBulkRestartDryRun(t *testing.T) {
	h, rec := bulkStubHandler(t, "/v1/missions/bulk-restart",
		`{"missions":[{"mission_id":"m1","status":"done"},{"mission_id":"m2","status":"done"}],"next_cursor":""}`,
		`{"results":[]}`)
	ac, stop := stubAppCtx(t, "s1", h)
	defer stop()

	var out, errOut bytes.Buffer
	if err := runCtlMissionsBulkRestart(ac, &out, &errOut, strings.NewReader(""), "s1", "outcome=failed", 0, true /*dryRun*/, false /*yes*/); err != nil {
		t.Fatalf("runCtlMissionsBulkRestart: %v", err)
	}
	if rec.bulkHits != 0 {
		t.Errorf("dry-run should not hit bulk endpoint, hits=%d", rec.bulkHits)
	}
	s := out.String()
	if !strings.Contains(s, "would restart 2 missions") {
		t.Errorf("preamble missing: %q", s)
	}
	if !strings.Contains(s, "m1") || !strings.Contains(s, "m2") {
		t.Errorf("ids missing: %q", s)
	}
	if !strings.Contains(rec.listQuery, "outcome=failed") {
		t.Errorf("list query missing outcome=failed: %q", rec.listQuery)
	}
}

// TestBulkRestartConfirmYes — --yes skips the prompt and POSTs all listed
// ids; successes print to stdout, partial failures to stderr. A partial
// failure also surfaces as an aggregate "N of M restart operations failed"
// error so the exit code is non-zero (plain failure, not BadUsage).
func TestBulkRestartConfirmYes(t *testing.T) {
	h, rec := bulkStubHandler(t, "/v1/missions/bulk-restart",
		`{"missions":[{"mission_id":"m1"},{"mission_id":"m2"}],"next_cursor":""}`,
		`{"results":[
			{"id":"m1","ok":true,"mission_id":"new1"},
			{"id":"m2","ok":false,"error":"mission_not_done"}
		]}`)
	ac, stop := stubAppCtx(t, "s1", h)
	defer stop()

	var out, errOut bytes.Buffer
	err := runCtlMissionsBulkRestart(ac, &out, &errOut, strings.NewReader(""), "s1", "outcome=failed,since=-1h", 0, false /*dryRun*/, true /*yes*/)
	if err == nil || !strings.Contains(err.Error(), "1 of 2 restart operations failed") {
		t.Fatalf("want aggregate partial-failure error, got %v", err)
	}
	if mapErrorToExit(err) != exitFailure {
		t.Errorf("exit=%d want %d", mapErrorToExit(err), exitFailure)
	}
	if rec.bulkHits != 1 {
		t.Fatalf("bulk hits=%d want 1", rec.bulkHits)
	}
	var sent map[string]any
	if err := json.Unmarshal(rec.bulkBody, &sent); err != nil {
		t.Fatalf("body unmarshal: %v body=%s", err, rec.bulkBody)
	}
	ids, _ := sent["ids"].([]any)
	if len(ids) != 2 || ids[0] != "m1" || ids[1] != "m2" {
		t.Errorf("sent ids=%v want [m1 m2]", ids)
	}
	if !strings.Contains(out.String(), "new1") {
		t.Errorf("new mission id missing from stdout: %q", out.String())
	}
	if !strings.Contains(errOut.String(), "FAIL m2") || !strings.Contains(errOut.String(), "mission_not_done") {
		t.Errorf("FAIL row missing from stderr: %q", errOut.String())
	}
}

// TestBulkRestartConfirmAborted — without --yes, stdin reading "n\n"
// aborts: the bulk POST must NOT fire and the runner returns a generic
// "aborted" error (NOT BadUsageError, so the user exits non-zero cleanly).
func TestBulkRestartConfirmAborted(t *testing.T) {
	h, rec := bulkStubHandler(t, "/v1/missions/bulk-restart",
		`{"missions":[{"mission_id":"m1"}],"next_cursor":""}`,
		`{"results":[]}`)
	ac, stop := stubAppCtx(t, "s1", h)
	defer stop()

	var out, errOut bytes.Buffer
	err := runCtlMissionsBulkRestart(ac, &out, &errOut, strings.NewReader("n\n"), "s1", "outcome=failed", 0, false, false)
	if err == nil || !strings.Contains(err.Error(), "aborted") {
		t.Fatalf("want 'aborted' error, got %v", err)
	}
	if rec.bulkHits != 0 {
		t.Errorf("aborted run must not hit bulk endpoint, hits=%d", rec.bulkHits)
	}
	if !strings.Contains(errOut.String(), "restart 1 missions?") {
		t.Errorf("prompt missing from stderr: %q", errOut.String())
	}
	// Sanity: aborted is a generic error, not a BadUsageError.
	var bue *BadUsageError
	if errors.As(err, &bue) {
		t.Errorf("aborted should not be BadUsageError, got %T", err)
	}
}

// TestBulkRestartLimitOverridesSelector — --limit replaces any cap the
// selector would have set (--limit is the user-facing
// knob). Selector parsing has no `limit=` key today, so this guards the
// override semantics for when one is added.
func TestBulkRestartLimitOverridesSelector(t *testing.T) {
	h, rec := bulkStubHandler(t, "/v1/missions/bulk-restart",
		`{"missions":[],"next_cursor":""}`, `{"results":[]}`)
	ac, stop := stubAppCtx(t, "s1", h)
	defer stop()

	var out, errOut bytes.Buffer
	if err := runCtlMissionsBulkRestart(ac, &out, &errOut, strings.NewReader(""), "s1", "outcome=failed", 50, false, true); err != nil {
		t.Fatalf("runCtlMissionsBulkRestart: %v", err)
	}
	if !strings.Contains(rec.listQuery, "limit=50") {
		t.Errorf("list query missing limit=50: %q", rec.listQuery)
	}
}

// TestBulkDeleteForwardsForce — --force becomes body.force=true in the
// bulk-delete POST.
func TestBulkDeleteForwardsForce(t *testing.T) {
	h, rec := bulkStubHandler(t, "/v1/missions/bulk-delete",
		`{"missions":[{"mission_id":"m1"}],"next_cursor":""}`,
		`{"results":[{"id":"m1","ok":true,"status":"deletion_pending"}]}`)
	ac, stop := stubAppCtx(t, "s1", h)
	defer stop()

	var out, errOut bytes.Buffer
	if err := runCtlMissionsBulkDelete(ac, &out, &errOut, strings.NewReader(""), "s1", "outcome=success,since=-7d", 0, true /*force*/, false, true /*yes*/); err != nil {
		t.Fatalf("runCtlMissionsBulkDelete: %v", err)
	}
	if rec.bulkHits != 1 {
		t.Fatalf("bulk hits=%d want 1", rec.bulkHits)
	}
	var sent map[string]any
	if err := json.Unmarshal(rec.bulkBody, &sent); err != nil {
		t.Fatalf("body unmarshal: %v body=%s", err, rec.bulkBody)
	}
	if force, _ := sent["force"].(bool); !force {
		t.Errorf("force=true expected, got %v body=%s", sent["force"], rec.bulkBody)
	}
	if !strings.Contains(out.String(), "m1") {
		t.Errorf("deleted id missing from stdout: %q", out.String())
	}
}

// TestBulkDeleteDryRun — preamble verb is "delete"; no POST hits.
func TestBulkDeleteDryRun(t *testing.T) {
	h, rec := bulkStubHandler(t, "/v1/missions/bulk-delete",
		`{"missions":[{"mission_id":"m1"},{"mission_id":"m2"}],"next_cursor":""}`,
		`{"results":[]}`)
	ac, stop := stubAppCtx(t, "s1", h)
	defer stop()

	var out, errOut bytes.Buffer
	if err := runCtlMissionsBulkDelete(ac, &out, &errOut, strings.NewReader(""), "s1", "outcome=success", 0, false, true /*dryRun*/, false); err != nil {
		t.Fatalf("runCtlMissionsBulkDelete: %v", err)
	}
	if rec.bulkHits != 0 {
		t.Errorf("dry-run should not hit bulk endpoint, hits=%d", rec.bulkHits)
	}
	if !strings.Contains(out.String(), "would delete 2 missions") {
		t.Errorf("preamble missing: %q", out.String())
	}
}

// TestBulkSelectorListingPinsKindMission — the listing request both bulk
// runners build from a --selector must carry kind=mission. Without the pin
// the daemon returns exec rows too (shared id namespace), so a bulk restart
// would re-execute ad-hoc exec commands and a bulk delete would wipe exec
// history. There is deliberately no bulk surface for exec records — they are
// managed individually via `ctl exec`.
func TestBulkSelectorListingPinsKindMission(t *testing.T) {
	runners := []struct {
		name     string
		bulkPath string
		run      func(ac *appCtx, out, errOut *bytes.Buffer) error
	}{
		{"restart", "/v1/missions/bulk-restart", func(ac *appCtx, out, errOut *bytes.Buffer) error {
			return runCtlMissionsBulkRestart(ac, out, errOut, strings.NewReader(""), "s1", "outcome=failed", 0, false, true /*yes*/)
		}},
		{"delete", "/v1/missions/bulk-delete", func(ac *appCtx, out, errOut *bytes.Buffer) error {
			return runCtlMissionsBulkDelete(ac, out, errOut, strings.NewReader(""), "s1", "outcome=failed", 0, false, false, true /*yes*/)
		}},
	}
	for _, r := range runners {
		t.Run(r.name, func(t *testing.T) {
			h, rec := bulkStubHandler(t, r.bulkPath,
				`{"missions":[],"next_cursor":""}`, `{"results":[]}`)
			ac, stop := stubAppCtx(t, "s1", h)
			defer stop()

			var out, errOut bytes.Buffer
			if err := r.run(ac, &out, &errOut); err != nil {
				t.Fatalf("bulk %s: %v", r.name, err)
			}
			if !strings.Contains(rec.listQuery, "kind=mission") {
				t.Errorf("list query %q missing kind=mission", rec.listQuery)
			}
		})
	}
}

// TestBulkDeleteSelectorSkipsExecRows — behavior-level proof against the
// full stub daemon: with one named mission and one exec record both matching
// the selector, bulk delete must touch only the mission. The exec row's
// status stays intact because the kind=mission listing never returned its id.
func TestBulkDeleteSelectorSkipsExecRows(t *testing.T) {
	stub := newStubDugdale(t)
	stub.SetMission(&stubMission{
		MissionID: "named-1", Kind: "mission", Lane: "normal",
		Status: "done", Outcome: "failed",
	})
	stub.SetMission(&stubMission{
		MissionID: "exec-1", Kind: "exec", Lane: "normal",
		Status: "done", Outcome: "failed",
	})
	ac := &appCtx{
		Config: &lettsconfig.Config{Dugdales: []lettsconfig.Dugdale{{
			ID: "s1", Host: "ignored", AdminToken: "atok",
		}}},
		Getenv:       func(k string) (string, bool) { return "", false },
		BaseURLForID: map[string]string{"s1": stub.URL()},
		clients:      map[clientKey]*hostClient{},
	}

	var out, errOut bytes.Buffer
	if err := runCtlMissionsBulkDelete(ac, &out, &errOut, strings.NewReader(""), "s1", "outcome=failed", 0, false, false, true /*yes*/); err != nil {
		t.Fatalf("runCtlMissionsBulkDelete: %v (stderr=%q)", err, errOut.String())
	}
	if got := stub.MissionRow("named-1"); got == nil || got.Status != "deleting" {
		t.Errorf("named mission should be deleting, got %+v", got)
	}
	if got := stub.MissionRow("exec-1"); got == nil || got.Status != "done" {
		t.Errorf("exec record must be untouched by bulk delete, got %+v", got)
	}
	if !strings.Contains(out.String(), "named-1") || strings.Contains(out.String(), "exec-1") {
		t.Errorf("stdout should list only the named mission: %q", out.String())
	}
}

// TestBulkDeleteAllFailedReturnsError — when every per-id result is a
// failure the runner must not exit 0; the FAIL lines stay on stderr and the
// aggregate error names the failure ratio and verb.
func TestBulkDeleteAllFailedReturnsError(t *testing.T) {
	h, _ := bulkStubHandler(t, "/v1/missions/bulk-delete",
		`{"missions":[{"mission_id":"m1"},{"mission_id":"m2"}],"next_cursor":""}`,
		`{"results":[
			{"id":"m1","ok":false,"error":"mission_running"},
			{"id":"m2","ok":false,"error":"not_found"}
		]}`)
	ac, stop := stubAppCtx(t, "s1", h)
	defer stop()

	var out, errOut bytes.Buffer
	err := runCtlMissionsBulkDelete(ac, &out, &errOut, strings.NewReader(""), "s1", "outcome=success", 0, false, false, true /*yes*/)
	if err == nil || !strings.Contains(err.Error(), "2 of 2 delete operations failed") {
		t.Fatalf("want aggregate failure error, got %v", err)
	}
	if mapErrorToExit(err) != exitFailure {
		t.Errorf("exit=%d want %d", mapErrorToExit(err), exitFailure)
	}
	if !strings.Contains(errOut.String(), "FAIL m1") || !strings.Contains(errOut.String(), "FAIL m2") {
		t.Errorf("per-id FAIL lines must stay on stderr: %q", errOut.String())
	}
	if strings.TrimSpace(out.String()) != "" {
		t.Errorf("stdout must stay clean when no id succeeded: %q", out.String())
	}
}

// TestBulkRestartCmdRejectsSelectorWithPositional — mixing the two modes
// must surface BadUsageError before any HTTP call.
func TestBulkRestartCmdRejectsSelectorWithPositional(t *testing.T) {
	cmd := newCtlMissionsRestartCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"mid", "--selector=outcome=failed"})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected BadUsageError")
	}
	var bue *BadUsageError
	if !errors.As(err, &bue) {
		t.Errorf("got %T %v, want *BadUsageError", err, err)
	}
}

// TestBulkRestartCmdRejectsMissingHost — --selector without --host is a
// BadUsageError; selectors don't fan out (each daemon has its own data).
func TestBulkRestartCmdRejectsMissingHost(t *testing.T) {
	cmd := newCtlMissionsRestartCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"--selector=outcome=failed"})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected BadUsageError")
	}
	var bue *BadUsageError
	if !errors.As(err, &bue) {
		t.Errorf("got %T %v, want *BadUsageError", err, err)
	}
}

// TestBulkRestartCmdRejectsNoModeChosen — restart with neither a positional
// id nor --selector must be rejected with BadUsageError.
func TestBulkRestartCmdRejectsNoModeChosen(t *testing.T) {
	cmd := newCtlMissionsRestartCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected BadUsageError")
	}
	var bue *BadUsageError
	if !errors.As(err, &bue) {
		t.Errorf("got %T %v, want *BadUsageError", err, err)
	}
}

// TestParseSinceTime exercises every accepted format and verifies error
// reporting for malformed input.
func TestParseSinceTime(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		in   string
		want int64
		err  bool
	}{
		{"-1h", now.Add(-time.Hour).UnixMilli(), false},
		{"-30m", now.Add(-30 * time.Minute).UnixMilli(), false},
		{"-7d", now.Add(-7 * 24 * time.Hour).UnixMilli(), false},
		{"1714600000123", 1714600000123, false},
		{"-bogus", 0, true},
		{"nope", 0, true},
	}
	for _, tc := range cases {
		got, err := parseSinceTime(tc.in, now)
		if tc.err {
			if err == nil {
				t.Errorf("%q: want error, got %d", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("%q: unexpected err %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("%q: got %d want %d", tc.in, got, tc.want)
		}
	}
}

// TestCtlMissionsListDefaultKindMission verifies that opts.Kind="mission" lands
// as `kind=mission` in the upstream GET /v1/missions query string. Mirrors the
// default the cobra binding installs so default invocations stay scoped to
// missions and don't leak exec rows into the listing.
func TestCtlMissionsListDefaultKindMission(t *testing.T) {
	var gotQuery string
	ac, stop := stubAppCtx(t, "s1", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"missions":[],"next_cursor":""}`)
	}))
	defer stop()

	var out bytes.Buffer
	if err := runCtlMissionsList(ac, &out, "s1", lettsclient.ListMissionsOpts{Kind: "mission"}, FormatText); err != nil {
		t.Fatalf("runCtlMissionsList: %v", err)
	}
	if !strings.Contains(gotQuery, "kind=mission") {
		t.Errorf("query=%q, want kind=mission default", gotQuery)
	}
}

// TestCtlMissionsListKindAllOmits verifies that an empty opts.Kind (which the
// command-builder uses to encode `--kind=all`) leaves no `kind=` segment in
// the query — letting the daemon list both missions and exec rows.
func TestCtlMissionsListKindAllOmits(t *testing.T) {
	var gotQuery string
	ac, stop := stubAppCtx(t, "s1", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"missions":[],"next_cursor":""}`)
	}))
	defer stop()

	var out bytes.Buffer
	if err := runCtlMissionsList(ac, &out, "s1", lettsclient.ListMissionsOpts{}, FormatText); err != nil {
		t.Fatalf("runCtlMissionsList: %v", err)
	}
	if strings.Contains(gotQuery, "kind=") {
		t.Errorf("query=%q, should omit kind for 'all' mode", gotQuery)
	}
}

// TestCtlMissionsListKindUnknownRejected drives the command through cobra so
// the --kind validation gate actually fires; unknown values must surface as
// BadUsageError (exit 2) before any HTTP call or config load happens.
func TestCtlMissionsListKindUnknownRejected(t *testing.T) {
	cmd := newCtlMissionsListCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"--host", "s1", "--kind", "garbage"})
	err := cmd.Execute()
	if mapErrorToExit(err) != 2 {
		t.Errorf("exit=%d, want 2 (badusage), err=%v", mapErrorToExit(err), err)
	}
}
