package handlers_test

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"letts/internal/ids"
	"letts/internal/server/handlers"
	"letts/internal/storage"
)

func doHead(h *handlers.StagingHandler, id string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("HEAD", "/v1/staging/"+id, nil)
	r.SetPathValue("id", id)
	w := httptest.NewRecorder()
	h.Head(w, r)
	return w
}

func TestStagingHeadNotFound(t *testing.T) {
	h, _, _ := setupStagingPut(t)
	w := doHead(h, ids.NewUUIDv7())
	if w.Code != 404 {
		t.Errorf("status=%d", w.Code)
	}
}

func TestStagingHeadInvalidIDReturns400(t *testing.T) {
	h, _, _ := setupStagingPut(t)
	w := doHead(h, "not-a-uuid")
	if w.Code != 400 {
		t.Errorf("status=%d", w.Code)
	}
}

func TestStagingHeadCompleteHeaders(t *testing.T) {
	h, db, _ := setupStagingPut(t)
	id := ids.NewUUIDv7()
	now := time.Now().UnixMilli()
	_ = storage.InsertStaging(context.Background(), db, &storage.StagingFile{
		StagingID:     id,
		State:         storage.StagingComplete,
		Sha256:        "deadbeef",
		Size:          1024,
		BytesReceived: 1024,
		Path:          "x",
		TimeCreatedMs: now, TimeUpdatedMs: now, TimeExpiresMs: now + 60_000,
	})

	w := doHead(h, id)
	if w.Code != 200 {
		t.Fatalf("status=%d", w.Code)
	}
	if w.Header().Get("Content-Length") != "1024" {
		t.Errorf("Content-Length=%q", w.Header().Get("Content-Length"))
	}
	if w.Header().Get("X-Letts-Sha256") != "deadbeef" {
		t.Errorf("Sha256=%q", w.Header().Get("X-Letts-Sha256"))
	}
	if w.Header().Get("X-Letts-Upload-Status") != "complete" {
		t.Errorf("Status=%q", w.Header().Get("X-Letts-Upload-Status"))
	}
}

func TestStagingHeadIncompleteHeaders(t *testing.T) {
	h, db, _ := setupStagingPut(t)
	id := ids.NewUUIDv7()
	now := time.Now().UnixMilli()
	_ = storage.InsertStaging(context.Background(), db, &storage.StagingFile{
		StagingID:     id,
		State:         storage.StagingUploading,
		Sha256:        "abcdef",
		Size:          2048,
		BytesReceived: 512,
		Path:          "x",
		TimeCreatedMs: now, TimeUpdatedMs: now, TimeExpiresMs: now + 60_000,
	})

	w := doHead(h, id)
	if w.Code != 200 {
		t.Fatalf("status=%d", w.Code)
	}
	if w.Header().Get("X-Letts-Upload-Status") != "incomplete" {
		t.Errorf("Status=%q", w.Header().Get("X-Letts-Upload-Status"))
	}
	if w.Header().Get("X-Letts-Bytes-Received") != "512" {
		t.Errorf("BytesReceived=%q", w.Header().Get("X-Letts-Bytes-Received"))
	}
	if w.Header().Get("X-Letts-Total-Size") != "2048" {
		t.Errorf("TotalSize=%q", w.Header().Get("X-Letts-Total-Size"))
	}
	if w.Header().Get("X-Letts-Sha256") != "abcdef" {
		t.Errorf("Sha256=%q", w.Header().Get("X-Letts-Sha256"))
	}
	// No Content-Length (response body is empty for HEAD; total reported via custom header).
	if w.Header().Get("Content-Length") != "" {
		t.Errorf("Content-Length should be unset, got %q", w.Header().Get("Content-Length"))
	}
}

func TestStagingHeadDeletingState(t *testing.T) {
	h, db, _ := setupStagingPut(t)
	id := ids.NewUUIDv7()
	now := time.Now().UnixMilli()
	_ = storage.InsertStaging(context.Background(), db, &storage.StagingFile{
		StagingID:     id,
		State:         storage.StagingDeleting,
		Sha256:        "x",
		Size:          1,
		BytesReceived: 1,
		Path:          "x",
		TimeCreatedMs: now, TimeUpdatedMs: now, TimeExpiresMs: now + 60_000,
	})
	w := doHead(h, id)
	if w.Code != 200 {
		t.Errorf("status=%d", w.Code)
	}
	if w.Header().Get("X-Letts-Upload-Status") != "deleting" {
		t.Errorf("Status=%q", w.Header().Get("X-Letts-Upload-Status"))
	}
}
