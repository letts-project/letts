package server_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"letts/internal/server"
)

func TestWriteError(t *testing.T) {
	w := httptest.NewRecorder()
	server.WriteError(w, http.StatusBadRequest, "bad_request", "something went wrong", map[string]any{"field": "x"})

	resp := w.Result()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json; charset=utf-8" {
		t.Errorf("Content-Type: got %q, want %q", ct, "application/json; charset=utf-8")
	}

	var got server.ErrorResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if got.Error != "bad_request" {
		t.Errorf("error field: got %q, want %q", got.Error, "bad_request")
	}
	if got.Message != "something went wrong" {
		t.Errorf("message field: got %q, want %q", got.Message, "something went wrong")
	}
	if got.Details == nil {
		t.Error("details should be non-nil")
	}
}

func TestWriteErrorOmitEmpty(t *testing.T) {
	w := httptest.NewRecorder()
	server.WriteError(w, http.StatusUnauthorized, "unauthorized", "", nil)

	var got map[string]any
	if err := json.NewDecoder(w.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, ok := got["message"]; ok {
		t.Error("message should be omitted when empty")
	}
	if _, ok := got["details"]; ok {
		t.Error("details should be omitted when nil")
	}
}

func TestWriteJSON(t *testing.T) {
	w := httptest.NewRecorder()
	server.WriteJSON(w, http.StatusCreated, map[string]string{"status": "ok"})

	resp := w.Result()
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("status: got %d, want %d", resp.StatusCode, http.StatusCreated)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json; charset=utf-8" {
		t.Errorf("Content-Type: got %q, want %q", ct, "application/json; charset=utf-8")
	}

	var got map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got["status"] != "ok" {
		t.Errorf("status field: got %q, want %q", got["status"], "ok")
	}
}
