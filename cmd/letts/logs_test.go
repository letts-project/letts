package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestLogsStreamsCombinedByDefault: invoking the top-level `letts logs <id>`
// command with --host=s1 must hit /v1/missions/{id}/output?stream=combined
// (the default) and write the decoded output text to stdout.
func TestLogsStreamsCombinedByDefault(t *testing.T) {
	mux := http.NewServeMux()
	mid := "01900000-0000-7000-8000-000000000001"
	gotQuery := ""
	mux.HandleFunc("GET /v1/missions/{id}/output", func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = io.WriteString(w, `{"t":1,"stream":"stdout","data":"line1\n"}`+"\n"+`{"t":2,"stream":"stderr","data":"line2\n"}`+"\n")
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	cfgPath := writeMultiHostYAML(t, t.TempDir(), map[string]string{"s1": srv.URL})

	stdout, _, err := execCLI(t, cfgPath, "logs", mid, "--host=s1")
	if err != nil {
		t.Fatalf("logs: %v", err)
	}
	if stdout != "line1\nline2\n" {
		t.Errorf("stdout = %q, want decoded combined text", stdout)
	}
	if !strings.Contains(gotQuery, "stream=combined") {
		t.Errorf("expected stream=combined default; got query %q", gotQuery)
	}
}

// TestLogsRespectsStreamAndFollow: --stream=stderr --follow must propagate
// through runCtlMissionsOutput → OpenOutput → the /output query string.
func TestLogsRespectsStreamAndFollow(t *testing.T) {
	mux := http.NewServeMux()
	mid := "01900000-0000-7000-8000-000000000001"
	gotQuery := ""
	mux.HandleFunc("GET /v1/missions/{id}/output", func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		_, _ = io.WriteString(w, "x")
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	cfgPath := writeMultiHostYAML(t, t.TempDir(), map[string]string{"s1": srv.URL})

	if _, _, err := execCLI(t, cfgPath, "logs", mid, "--host=s1", "--stream=stderr", "--follow"); err != nil {
		t.Fatalf("logs: %v", err)
	}
	if !strings.Contains(gotQuery, "stream=stderr") || !strings.Contains(gotQuery, "follow=true") {
		t.Errorf("expected stream=stderr&follow=true; got %q", gotQuery)
	}
}

// TestLogsHonoursOutputFormat: the global --output flag reaches the renderer
// for `logs`, `ctl missions output` and `ctl exec output`.
func TestLogsHonoursOutputFormat(t *testing.T) {
	mux := http.NewServeMux()
	mid := "01900000-0000-7000-8000-000000000001"
	frames := `{"t":1,"stream":"stdout","data":"a\n"}` + "\n" + `{"t":2,"stream":"stderr","data":"b\n"}` + "\n"
	mux.HandleFunc("GET /v1/missions/{id}/output", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = io.WriteString(w, frames)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	cfgPath := writeMultiHostYAML(t, t.TempDir(), map[string]string{"s1": srv.URL})

	for _, argv := range [][]string{
		{"logs", mid},
		{"ctl", "missions", "output", mid},
		{"ctl", "exec", "output", mid},
	} {
		name := strings.Join(argv[:len(argv)-1], " ")
		run := func(format string) string {
			t.Helper()
			stdout, _, err := execCLI(t, cfgPath, append(argv, "--host=s1", "--output="+format)...)
			if err != nil {
				t.Fatalf("%s --output=%s: %v", name, format, err)
			}
			return stdout
		}
		if got := run("text"); got != "a\nb\n" {
			t.Errorf("%s text: got %q", name, got)
		}
		if got := run("ndjson"); got != frames {
			t.Errorf("%s ndjson: got %q, want frames verbatim", name, got)
		}
		var arr []outputFrame
		if err := json.Unmarshal([]byte(run("json")), &arr); err != nil || len(arr) != 2 || arr[1].Stream != "stderr" {
			t.Errorf("%s json: frames=%+v err=%v", name, arr, err)
		}
		if got := run("yaml"); !strings.Contains(got, "stream: stderr") {
			t.Errorf("%s yaml: got %q", name, got)
		}
	}
}
