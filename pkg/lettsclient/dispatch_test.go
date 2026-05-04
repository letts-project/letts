package lettsclient

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// TestDispatchRetriesTransient5xx verifies Dispatch uses the sticky-
// retry client so a transient 5xx / network blip retries on the same
// host (safe — the Idempotency-Key dedups), instead of failing on the first
// attempt. Pre-fix Dispatch called the bare Client.DoJSON and RetryClient was
// dead code.
func TestDispatchRetriesTransient5xx(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) < 2 {
			w.WriteHeader(503)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(202)
		_, _ = w.Write([]byte(`{"mission_id":"m","status":"queued"}`))
	}))
	defer srv.Close()
	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	if _, err := Dispatch(c, DispatchRequest{
		MissionID: "0192a8b3-d2c1-7abc-bad0-1234567890ab", Mission: "X", Lane: "normal",
	}); err != nil {
		t.Fatalf("Dispatch should retry the transient 503 and succeed: %v", err)
	}
	if calls.Load() != 2 {
		t.Errorf("calls = %d, want 2 (sticky retry)", calls.Load())
	}
}

func TestDispatchSetsIdempotencyKeyHeaderAndBody(t *testing.T) {
	var gotMethod, gotPath, gotKey, gotContentType string
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotKey = r.Header.Get("Idempotency-Key")
		gotContentType = r.Header.Get("Content-Type")
		gotBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, `{"mission_id":"01900000-0000-7000-8000-000000000001","status":"queued"}`)
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	req := DispatchRequest{
		MissionID: "01900000-0000-7000-8000-000000000001",
		Mission:   "Smoke",
		Lane:      "normal",
		Input:     json.RawMessage(`{"k":"v"}`),
		Files: []DispatchedFile{
			{Role: "config", StagingID: "01900000-0000-7000-8000-000000000002"},
		},
		Timeout: "30s",
	}
	resp, err := Dispatch(c, req)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	if gotMethod != "POST" {
		t.Errorf("method=%q", gotMethod)
	}
	if gotPath != "/v1/dispatch" {
		t.Errorf("path=%q", gotPath)
	}
	if gotKey != req.MissionID {
		t.Errorf("Idempotency-Key=%q want %q", gotKey, req.MissionID)
	}
	if gotContentType == "" {
		t.Errorf("Content-Type not set")
	}
	if resp.MissionID != req.MissionID {
		t.Errorf("resp.MissionID=%q", resp.MissionID)
	}
	if resp.Status != "queued" {
		t.Errorf("resp.Status=%q", resp.Status)
	}

	// Body should be JSON containing mission, lane, input, files, timeout.
	var sent map[string]any
	if err := json.Unmarshal(gotBody, &sent); err != nil {
		t.Fatalf("body unmarshal: %v body=%s", err, gotBody)
	}
	if sent["mission"] != "Smoke" {
		t.Errorf("body.mission=%v", sent["mission"])
	}
	if sent["lane"] != "normal" {
		t.Errorf("body.lane=%v", sent["lane"])
	}
	if sent["timeout"] != "30s" {
		t.Errorf("body.timeout=%v", sent["timeout"])
	}
	// MissionID is the Idempotency-Key, not body.
	if _, ok := sent["mission_id"]; ok {
		t.Errorf("body should not include mission_id field")
	}
	if _, ok := sent["MissionID"]; ok {
		t.Errorf("body should not include MissionID field")
	}
	files, _ := sent["files"].([]any)
	if len(files) != 1 {
		t.Fatalf("files len=%d", len(files))
	}
	first := files[0].(map[string]any)
	if first["role"] != "config" || first["staging_id"] != "01900000-0000-7000-8000-000000000002" {
		t.Errorf("files[0]=%v", first)
	}
}

func TestDispatchOmitsEmptyOptionalFields(t *testing.T) {
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, `{"mission_id":"x","status":"queued"}`)
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	_, err := Dispatch(c, DispatchRequest{
		MissionID: "01900000-0000-7000-8000-000000000003",
		Mission:   "M",
		Lane:      "normal",
	})
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	var sent map[string]any
	if err := json.Unmarshal(gotBody, &sent); err != nil {
		t.Fatalf("body unmarshal: %v body=%s", err, gotBody)
	}
	if _, ok := sent["input"]; ok {
		t.Errorf("empty input should be omitted, got %v", sent["input"])
	}
	if _, ok := sent["files"]; ok {
		t.Errorf("empty files should be omitted, got %v", sent["files"])
	}
	if _, ok := sent["timeout"]; ok {
		t.Errorf("empty timeout should be omitted, got %v", sent["timeout"])
	}
}

func TestDispatchHandles200ReplayBody(t *testing.T) {
	// Idempotent replay returns 200 with the same body shape.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"mission_id":"01900000-0000-7000-8000-000000000004","status":"running"}`)
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	resp, err := Dispatch(c, DispatchRequest{
		MissionID: "01900000-0000-7000-8000-000000000004",
		Mission:   "M", Lane: "normal",
	})
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if resp.Status != "running" {
		t.Errorf("status=%q", resp.Status)
	}
}

func TestDispatchSurfacesHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = io.WriteString(w, `{"error":"idempotency_conflict","message":"fingerprint mismatch"}`)
	}))
	defer srv.Close()

	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	_, err := Dispatch(c, DispatchRequest{
		MissionID: "01900000-0000-7000-8000-000000000005",
		Mission:   "M", Lane: "normal",
	})
	if err == nil {
		t.Fatalf("expected error on 409")
	}
	he, ok := err.(*HTTPError)
	if !ok {
		t.Fatalf("err is not *HTTPError: %T %v", err, err)
	}
	if he.Status != http.StatusConflict || he.Code != "idempotency_conflict" {
		t.Errorf("got %+v", he)
	}
}
