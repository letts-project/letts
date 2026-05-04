package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"letts/internal/apply"
	"letts/pkg/lettsconfig"
)

func TestApplyHitsAllHostsInParallel(t *testing.T) {
	var calls atomic.Int64
	mk := func() *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			b, _ := io.ReadAll(r.Body)
			var st apply.AppliedState
			_ = json.Unmarshal(b, &st)
			w.WriteHeader(200)
			_, _ = io.WriteString(w, `{"diff":{"reason":""}}`)
		}))
	}
	a := mk()
	b := mk()
	defer a.Close()
	defer b.Close()

	ac := &appCtx{
		Config: &lettsconfig.Config{
			Dugdales: []lettsconfig.Dugdale{
				{ID: "s1", Host: "x", AdminToken: "atok", Lanes: map[string]lettsconfig.LaneCfg{"normal": {Concurrency: 1}}},
				{ID: "s2", Host: "x", AdminToken: "atok", Lanes: map[string]lettsconfig.LaneCfg{"normal": {Concurrency: 2}}},
			},
		},
		Getenv:       func(k string) (string, bool) { return "", false },
		BaseURLForID: map[string]string{"s1": a.URL, "s2": b.URL},
		clients:      map[clientKey]*hostClient{},
	}

	var out bytes.Buffer
	if err := runApply(ac, &out, []string{}, []string{}, false, false, false, FormatText); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Errorf("calls = %d, want 2", calls.Load())
	}
}

func TestApplyDryRunSkipsPOST(t *testing.T) {
	var posts atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			posts.Add(1)
			t.Errorf("POST should not happen in dry-run")
		}
		_, _ = io.WriteString(w, `{"state":{"lanes":{}}}`)
	}))
	defer srv.Close()

	ac := &appCtx{
		Config: &lettsconfig.Config{
			Dugdales: []lettsconfig.Dugdale{{ID: "s1", Host: "x", AdminToken: "t", Lanes: map[string]lettsconfig.LaneCfg{"new": {Concurrency: 1}}}},
		},
		Getenv:       func(k string) (string, bool) { return "", false },
		BaseURLForID: map[string]string{"s1": srv.URL},
		clients:      map[clientKey]*hostClient{},
	}

	var out bytes.Buffer
	if err := runApplyDryRun(ac, &out, []string{}, []string{}, FormatText); err != nil {
		t.Fatal(err)
	}
	if posts.Load() != 0 {
		t.Errorf("posts = %d, want 0", posts.Load())
	}
}

// TestFormatApplyResultsFailureExitsNonZeroAllFormats — a failed host must
// produce the aggregate "apply failed on at least one dugdale" error in
// EVERY output format. JSON/YAML used to return nil after printing the
// summary, so `letts apply -o json` exited 0 while reporting ok=false rows.
// The summary is still printed in full before the error is returned.
func TestFormatApplyResultsFailureExitsNonZeroAllFormats(t *testing.T) {
	results := []applyResult{
		{ID: "s1", Res: &ApplyResult{}},
		{ID: "s2", Err: errors.New("connect: refused")},
	}
	for _, f := range []Format{FormatText, FormatJSON, FormatYAML} {
		var out bytes.Buffer
		err := formatApplyResults(&out, results, f)
		if err == nil || !strings.Contains(err.Error(), "apply failed on at least one dugdale") {
			t.Errorf("format %v: want aggregate failure error, got %v", f, err)
		}
		// Both per-host rows must still be rendered (stdout), regardless of
		// the non-nil error (stderr via main).
		if !strings.Contains(out.String(), "s1") || !strings.Contains(out.String(), "s2") {
			t.Errorf("format %v: summary missing host rows: %q", f, out.String())
		}
	}

	// JSON specifically must stay machine-parseable with ok flags intact.
	var out bytes.Buffer
	_ = formatApplyResults(&out, results, FormatJSON)
	var rows []struct {
		Host  string `json:"host"`
		OK    bool   `json:"ok"`
		Error string `json:"error"`
	}
	if err := json.Unmarshal(out.Bytes(), &rows); err != nil {
		t.Fatalf("JSON summary not parseable: %v (%q)", err, out.String())
	}
	if len(rows) != 2 || !rows[0].OK || rows[1].OK || rows[1].Error == "" {
		t.Errorf("rows=%+v want s1 ok / s2 failed with error text", rows)
	}
}

// TestFormatApplyResultsAllOKReturnsNil — no failures, no error, in every
// format (guards against the aggregate check misfiring on success).
func TestFormatApplyResultsAllOKReturnsNil(t *testing.T) {
	results := []applyResult{{ID: "s1", Res: &ApplyResult{}}}
	for _, f := range []Format{FormatText, FormatJSON, FormatYAML} {
		var out bytes.Buffer
		if err := formatApplyResults(&out, results, f); err != nil {
			t.Errorf("format %v: want nil, got %v", f, err)
		}
	}
}

func TestApplyHostFilter(t *testing.T) {
	var calls atomic.Int64
	mk := func() *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			w.WriteHeader(200)
			_, _ = io.WriteString(w, `{}`)
		}))
	}
	a, b := mk(), mk()
	defer a.Close()
	defer b.Close()
	ac := &appCtx{
		Config: &lettsconfig.Config{
			Dugdales: []lettsconfig.Dugdale{
				{ID: "s1", Host: "x", AdminToken: "t"},
				{ID: "s2", Host: "x", AdminToken: "t"},
			},
		},
		Getenv:       func(k string) (string, bool) { return "", false },
		BaseURLForID: map[string]string{"s1": a.URL, "s2": b.URL},
		clients:      map[clientKey]*hostClient{},
	}
	var out bytes.Buffer
	if err := runApply(ac, &out, []string{"s1"}, nil, false, false, false, FormatText); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Errorf("calls = %d, want 1 (only s1)", calls.Load())
	}
}
