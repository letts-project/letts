package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"letts/pkg/lettsconfig"
)

// stubAppCtx returns an *appCtx wired to one httptest server.
func stubAppCtx(t *testing.T, dugdaleID string, h http.Handler) (*appCtx, func()) {
	t.Helper()
	srv := httptest.NewServer(h)
	cfg := &lettsconfig.Config{
		Dugdales: []lettsconfig.Dugdale{{
			ID: dugdaleID, Host: "ignored", Token: "tok", AdminToken: "atok",
		}},
	}
	return &appCtx{
		Config:       cfg,
		Getenv:       func(k string) (string, bool) { return "", false },
		BaseURLForID: map[string]string{dugdaleID: srv.URL},
		clients:      map[clientKey]*hostClient{},
	}, srv.Close
}

func TestCtlDugdalesList(t *testing.T) {
	ac, stop := stubAppCtx(t, "s1", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer stop()

	var out bytes.Buffer
	if err := runCtlDugdalesList(ac, &out, FormatJSON); err != nil {
		t.Fatal(err)
	}
	var got []map[string]any
	if err := json.NewDecoder(&out).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0]["id"] != "s1" {
		t.Errorf("got %+v", got)
	}
}

// TestCtlDugdalesListLanesSorted ensures lanes are emitted in deterministic
// alphabetical order regardless of map iteration. A naïve `for n := range`
// would otherwise produce flaky JSON output.
func TestCtlDugdalesListLanesSorted(t *testing.T) {
	cfg := &lettsconfig.Config{
		Dugdales: []lettsconfig.Dugdale{{
			ID: "s1", Host: "h", Token: "tok",
			Lanes: map[string]lettsconfig.LaneCfg{
				"zeta":  {Concurrency: 1},
				"alpha": {Concurrency: 1},
				"mu":    {Concurrency: 1},
			},
		}},
	}
	ac := &appCtx{
		Config:       cfg,
		Getenv:       func(k string) (string, bool) { return "", false },
		BaseURLForID: map[string]string{},
		clients:      map[clientKey]*hostClient{},
	}
	want := []string{"alpha", "mu", "zeta"}
	// Run many times to guard against random map order flakes.
	for i := 0; i < 20; i++ {
		var out bytes.Buffer
		if err := runCtlDugdalesList(ac, &out, FormatJSON); err != nil {
			t.Fatal(err)
		}
		var got []struct {
			Lanes []string `json:"lanes"`
		}
		if err := json.NewDecoder(&out).Decode(&got); err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 {
			t.Fatalf("rows = %d, want 1", len(got))
		}
		if len(got[0].Lanes) != len(want) {
			t.Fatalf("lanes len = %d, want %d", len(got[0].Lanes), len(want))
		}
		for j := range want {
			if got[0].Lanes[j] != want[j] {
				t.Errorf("iteration %d: lanes[%d] = %q, want %q (full: %v)",
					i, j, got[0].Lanes[j], want[j], got[0].Lanes)
				break
			}
		}
	}
}

func TestCtlDugdalesInfo(t *testing.T) {
	ac, stop := stubAppCtx(t, "s1", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/dugdale" {
			_, _ = io.WriteString(w, `{"version":"v","uptime_seconds":1,"queue_summary":{}}`)
		}
	}))
	defer stop()

	var out bytes.Buffer
	if err := runCtlDugdalesInfo(ac, &out, "s1", FormatJSON); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), `"version"`) {
		t.Errorf("got %q", out.String())
	}
}

func TestCtlDugdalesConfig(t *testing.T) {
	ac, stop := stubAppCtx(t, "s1", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/admin/state" {
			_, _ = io.WriteString(w, `{"applied_at":1,"state":{"mission_dir":"/x"}}`)
		}
	}))
	defer stop()

	var out bytes.Buffer
	if err := runCtlDugdalesConfig(ac, &out, "s1", FormatJSON); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), `mission_dir`) {
		t.Errorf("got %q", out.String())
	}
}
