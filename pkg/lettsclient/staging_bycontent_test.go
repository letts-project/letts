package lettsclient

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestStagingByContentHit(t *testing.T) {
	var gotURL string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotURL = r.URL.String()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"staging_id":"0192aaaa-0000-7000-8000-000000000000"}`))
	}))
	defer srv.Close()
	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	sha := strings.Repeat("a", 64)
	id, found, err := StagingByContent(c, sha, 100)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !found {
		t.Error("found=false, want true")
	}
	if id != "0192aaaa-0000-7000-8000-000000000000" {
		t.Errorf("id=%q", id)
	}
	if !strings.Contains(gotURL, "/v1/staging/by-content/"+sha) {
		t.Errorf("URL %q missing path", gotURL)
	}
	if !strings.Contains(gotURL, "size=100") {
		t.Errorf("URL %q missing size", gotURL)
	}
}

func TestStagingByContentMiss(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"error":"not_found","message":"not found"}`))
	}))
	defer srv.Close()
	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	id, found, err := StagingByContent(c, strings.Repeat("b", 64), 50)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if found {
		t.Error("found=true, want false")
	}
	if id != "" {
		t.Errorf("id=%q, want empty", id)
	}
}

func TestStagingByContentServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer srv.Close()
	c, _ := New(Options{BaseURL: srv.URL, Token: "t"})
	_, _, err := StagingByContent(c, strings.Repeat("c", 64), 1)
	if err == nil {
		t.Fatal("expected error on 500")
	}
}
