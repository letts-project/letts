package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestLogsStreamsCombinedByDefault: invoking the top-level `letts logs <id>`
// command with --host=s1 must hit /v1/missions/{id}/output?stream=combined
// (the default) and write the streamed bytes to stdout.
func TestLogsStreamsCombinedByDefault(t *testing.T) {
	mux := http.NewServeMux()
	mid := "01900000-0000-7000-8000-000000000001"
	gotQuery := ""
	mux.HandleFunc("GET /v1/missions/{id}/output", func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		_, _ = io.WriteString(w, "line1\nline2\n")
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	cfgPath := writeMultiHostYAML(t, t.TempDir(), map[string]string{"s1": srv.URL})

	stdout, _, err := execCLI(t, cfgPath, "logs", mid, "--host=s1")
	if err != nil {
		t.Fatalf("logs: %v", err)
	}
	if !strings.Contains(stdout, "line1\nline2") {
		t.Errorf("expected stdout to contain streamed bytes; got %q", stdout)
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
