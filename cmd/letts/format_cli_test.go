package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// formatStubServer answers the read and single-mutation endpoints the
// format tests drive.
func formatStubServer(t *testing.T) *httptest.Server {
	t.Helper()
	mid := "01900000-0000-7000-8000-000000000001"
	mission := `{"mission_id":"` + mid + `","mission_name":"report","status":"done","kind":"mission","time_created":1760000000000,"input":{"a":1}}`
	mux := http.NewServeMux()
	reply := func(body string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, body)
		}
	}
	mux.HandleFunc("GET /v1/lanes", reply(`[{"name":"normal","concurrency":1},{"name":"heavy","concurrency":2}]`))
	mux.HandleFunc("GET /v1/dugdale", reply(`{"version":"x","uptime_seconds":1,"queue_summary":{"queued":0,"running":0}}`))
	mux.HandleFunc("GET /v1/missions", reply(`{"missions":[`+mission+`,`+mission+`],"next_cursor":"c2"}`))
	mux.HandleFunc("GET /v1/missions/{id}", reply(mission))
	mux.HandleFunc("POST /v1/missions/{id}/restart", reply(`{"mission_id":"new","restarted_from":"`+mid+`","status":"queued"}`))
	mux.HandleFunc("POST /v1/dispatch", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, `{"mission_id":"`+mid+`","status":"queued"}`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestCommandsHonourNDJSON(t *testing.T) {
	srv := formatStubServer(t)
	cfgPath := writeMultiHostYAML(t, t.TempDir(), map[string]string{"s1": srv.URL})
	mid := "01900000-0000-7000-8000-000000000001"
	for _, tc := range []struct {
		args  []string
		lines int
	}{
		{[]string{"ctl", "lanes", "list", "--host=s1"}, 2},
		{[]string{"ctl", "dugdales", "info", "--host=s1"}, 1},
		{[]string{"ctl", "dugdales", "list"}, 1},
		{[]string{"ctl", "missions", "list", "--host=s1"}, 2},
		{[]string{"ctl", "missions", "show", mid, "--host=s1"}, 1},
		{[]string{"ctl", "missions", "restart", mid, "--host=s1"}, 1},
		{[]string{"dispatch", "--host=s1", "--lane=normal", "--mission=M", "--input={}"}, 1},
		{[]string{"version"}, 1},
	} {
		stdout, stderr, err := execCLI(t, cfgPath, append(tc.args, "--output=ndjson")...)
		if err != nil {
			t.Errorf("%v: %v (stderr %q)", tc.args, err, stderr)
			continue
		}
		lines := strings.Split(strings.TrimSuffix(stdout, "\n"), "\n")
		if len(lines) != tc.lines {
			t.Errorf("%v: %d lines, want %d:\n%s", tc.args, len(lines), tc.lines, stdout)
		}
		for _, l := range lines {
			if !json.Valid([]byte(l)) || !strings.HasPrefix(l, "{") {
				t.Errorf("%v: line is not a JSON object: %q", tc.args, l)
			}
		}
		if strings.HasPrefix(strings.Join(tc.args, " "), "ctl missions list") && !strings.Contains(stderr, "cursor: c2") {
			t.Errorf("%v: cursor not reported on stderr: %q", tc.args, stderr)
		}
	}
}

func TestCommandsYAMLUseAPIKeys(t *testing.T) {
	srv := formatStubServer(t)
	cfgPath := writeMultiHostYAML(t, t.TempDir(), map[string]string{"s1": srv.URL})
	mid := "01900000-0000-7000-8000-000000000001"
	for _, tc := range []struct {
		args []string
		want []string
	}{
		{[]string{"ctl", "missions", "show", mid, "--host=s1"}, []string{"mission_id: " + mid, "time_created: 1760000000000", "input:\n  a: 1"}},
		{[]string{"ctl", "missions", "list", "--host=s1"}, []string{"next_cursor: c2", "- mission_id: "}},
		{[]string{"ctl", "missions", "restart", mid, "--host=s1"}, []string{"restarted_from: " + mid}},
		{[]string{"ctl", "dugdales", "info", "--host=s1"}, []string{"uptime_seconds: 1", "queue_summary:"}},
	} {
		stdout, _, err := execCLI(t, cfgPath, append(tc.args, "--output=yaml")...)
		if err != nil {
			t.Errorf("%v: %v", tc.args, err)
			continue
		}
		for _, w := range tc.want {
			if !strings.Contains(stdout, w) {
				t.Errorf("%v: missing %q in:\n%s", tc.args, w, stdout)
			}
		}
	}
}
