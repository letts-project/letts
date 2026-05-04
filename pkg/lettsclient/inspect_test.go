package lettsclient

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGetDugdaleInfo(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/dugdale" {
			t.Errorf("path = %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"version":"v0.0.0","uptime_seconds":42.5,"applied_at":1714600000123,"queue_summary":{"queued":1,"running":2}}`))
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	info, err := GetDugdaleInfo(c)
	if err != nil {
		t.Fatal(err)
	}
	if info.Version != "v0.0.0" || info.QueueSummary.Running != 2 {
		t.Errorf("got %+v", info)
	}
}

func TestListLanes(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/lanes" {
			t.Errorf("path = %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"name":"normal","concurrency":2,"paused":false,"queued":1,"running":0},{"name":"high","concurrency":5,"paused":true,"queued":3,"running":2}]`))
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	lanes, err := ListLanes(c)
	if err != nil {
		t.Fatal(err)
	}
	if len(lanes) != 2 || lanes[0].Name != "normal" {
		t.Fatalf("got %+v", lanes)
	}
}

func TestHealthz(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/healthz":
			w.WriteHeader(200)
		case "/v1/readyz":
			w.WriteHeader(503)
		}
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL})
	if err := Healthz(c); err != nil {
		t.Errorf("healthz: %v", err)
	}
	if err := Readyz(c); err == nil {
		t.Error("readyz: expected error on 503")
	}
}
