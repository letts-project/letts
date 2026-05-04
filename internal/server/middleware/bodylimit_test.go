package middleware_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"letts/internal/server/middleware"
)

func bodyReadHandler(w http.ResponseWriter, r *http.Request) {
	_, _ = io.ReadAll(r.Body)
	w.WriteHeader(http.StatusOK)
}

func TestBodyLimitOversizedContentLength(t *testing.T) {
	const limit = 100
	h := middleware.BodyLimit(limit, bodyReadHandler)

	body := bytes.Repeat([]byte("x"), limit+1)
	req := httptest.NewRequest("POST", "/", bytes.NewReader(body))
	req.ContentLength = int64(len(body))
	w := httptest.NewRecorder()
	h(w, req)

	if w.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("want 413, got %d", w.Code)
	}
	var resp map[string]any
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp["error"] != "payload_too_large" {
		t.Errorf("error field: got %q, want payload_too_large", resp["error"])
	}
}

func TestBodyLimitAtExactLimit(t *testing.T) {
	const limit = 100
	h := middleware.BodyLimit(limit, bodyReadHandler)

	body := strings.Repeat("x", limit)
	req := httptest.NewRequest("POST", "/", strings.NewReader(body))
	req.ContentLength = int64(len(body))
	w := httptest.NewRecorder()
	h(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("want 200 at limit, got %d", w.Code)
	}
}

func TestBodyLimitUnderLimit(t *testing.T) {
	const limit = 100
	h := middleware.BodyLimit(limit, bodyReadHandler)

	body := strings.Repeat("x", limit-1)
	req := httptest.NewRequest("POST", "/", strings.NewReader(body))
	req.ContentLength = int64(len(body))
	w := httptest.NewRecorder()
	h(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("want 200 under limit, got %d", w.Code)
	}
}

func TestBodyLimitNoContentLength(t *testing.T) {
	// When ContentLength is unknown (-1), the limit is enforced at read time.
	const limit = 10
	h := middleware.BodyLimit(limit, bodyReadHandler)

	body := strings.Repeat("x", limit+1)
	req := httptest.NewRequest("POST", "/", strings.NewReader(body))
	req.ContentLength = -1 // unknown
	w := httptest.NewRecorder()
	h(w, req)

	// Handler reads the body via io.ReadAll; MaxBytesReader will cause a
	// MaxBytesError. Since bodyReadHandler discards the error, it still
	// returns 200 — but the body is truncated. This test just verifies
	// the handler runs (no panic, no 413 before read).
	if w.Code != http.StatusOK {
		t.Errorf("want handler to run for unknown ContentLength, got %d", w.Code)
	}
}
