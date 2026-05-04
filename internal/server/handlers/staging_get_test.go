package handlers_test

import (
	"bytes"
	"context"
	"database/sql"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"letts/internal/ids"
	"letts/internal/server/handlers"
	"letts/internal/storage"
)

func setupStagingGet(t *testing.T, state storage.StagingState, content []byte) (*handlers.StagingHandler, *sql.DB, string) {
	t.Helper()
	h, db, dataDir := setupStagingPut(t)
	id := ids.NewUUIDv7()
	shard, _ := ids.ShardPath(id)
	relPath := filepath.Join("staging", shard, id)
	abs := filepath.Join(dataDir, relPath)
	_ = os.MkdirAll(filepath.Dir(abs), 0o755)
	if content != nil {
		_ = os.WriteFile(abs, content, 0o600)
	}
	now := time.Now().UnixMilli()
	_ = storage.InsertStaging(context.Background(), db, &storage.StagingFile{
		StagingID: id, State: state, Sha256: "sha", Size: int64(len(content)),
		BytesReceived: int64(len(content)), Path: relPath,
		TimeCreatedMs: now, TimeUpdatedMs: now, TimeExpiresMs: now + 60_000,
	})
	return h, db, id
}

func doGet(h *handlers.StagingHandler, id, rangeHeader string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("GET", "/v1/staging/"+id, nil)
	r.SetPathValue("id", id)
	if rangeHeader != "" {
		r.Header.Set("Range", rangeHeader)
	}
	w := httptest.NewRecorder()
	h.Get(w, r)
	return w
}

// TestStagingGetFallsBackToTombstoneOnRenameRace: if the GC renames the
// file to tombstone/<id> in the window between the handler reading the (still
// complete) row and os.Open, the download must serve from the tombstone during
// the grace window instead of returning a spurious 500.
func TestStagingGetFallsBackToTombstoneOnRenameRace(t *testing.T) {
	payload := []byte("tombstoned bytes")
	h, _, id := setupStagingGet(t, storage.StagingComplete, payload)
	shard, _ := ids.ShardPath(id)
	src := filepath.Join(h.DataDir, "staging", shard, id)
	tombDir := filepath.Join(h.DataDir, "tombstone")
	if err := os.MkdirAll(tombDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(src, filepath.Join(tombDir, id)); err != nil {
		t.Fatalf("simulate tombstone rename: %v", err)
	}
	w := doGet(h, id, "")
	if w.Code != 200 {
		t.Fatalf("status=%d body=%s, want 200 (serve from tombstone during grace)", w.Code, w.Body.String())
	}
	if !bytes.Equal(w.Body.Bytes(), payload) {
		t.Errorf("body=%q, want %q", w.Body.String(), payload)
	}
}

func TestStagingGetFullDownload(t *testing.T) {
	payload := []byte("hello world")
	h, _, id := setupStagingGet(t, storage.StagingComplete, payload)
	w := doGet(h, id, "")
	if w.Code != 200 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if !bytes.Equal(w.Body.Bytes(), payload) {
		t.Errorf("body=%q", w.Body.String())
	}
	if w.Header().Get("Content-Type") != "application/octet-stream" {
		t.Errorf("Content-Type=%q", w.Header().Get("Content-Type"))
	}
	if w.Header().Get("X-Letts-Sha256") != "sha" {
		t.Errorf("Sha256=%q", w.Header().Get("X-Letts-Sha256"))
	}
}

func TestStagingGetRangeReturns206(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 1000)
	h, _, id := setupStagingGet(t, storage.StagingComplete, payload)
	w := doGet(h, id, "bytes=0-99")
	if w.Code != 206 {
		t.Fatalf("status=%d", w.Code)
	}
	if w.Body.Len() != 100 {
		t.Errorf("body len=%d, want 100", w.Body.Len())
	}
}

func TestStagingGetSetsDownloadedAtOnFullGet(t *testing.T) {
	payload := []byte("hello")
	h, db, id := setupStagingGet(t, storage.StagingComplete, payload)
	doGet(h, id, "")
	sf, _ := storage.GetStaging(context.Background(), db, id)
	if !sf.DownloadedAt.Valid {
		t.Errorf("downloaded_at not set: %+v", sf.DownloadedAt)
	}
}

func TestStagingGetDoesNotSetDownloadedAtOnPartialGet(t *testing.T) {
	payload := bytes.Repeat([]byte("y"), 500)
	h, db, id := setupStagingGet(t, storage.StagingComplete, payload)
	w := doGet(h, id, "bytes=0-49")
	if w.Code != 206 {
		t.Fatalf("status=%d", w.Code)
	}
	sf, _ := storage.GetStaging(context.Background(), db, id)
	if sf.DownloadedAt.Valid {
		t.Errorf("downloaded_at unexpectedly set after 206")
	}
}

func TestStagingGetDoesNotOverwriteDownloadedAt(t *testing.T) {
	payload := []byte("hello")
	h, db, id := setupStagingGet(t, storage.StagingComplete, payload)
	// Pre-set downloaded_at.
	preset := int64(1000)
	_, err := db.ExecContext(context.Background(),
		`UPDATE staging_files SET downloaded_at=? WHERE staging_id=?`, preset, id)
	if err != nil {
		t.Fatal(err)
	}
	doGet(h, id, "")
	sf, _ := storage.GetStaging(context.Background(), db, id)
	if !sf.DownloadedAt.Valid || sf.DownloadedAt.Int64 != preset {
		t.Errorf("downloaded_at changed: %+v", sf.DownloadedAt)
	}
}

func TestStagingGetUploadingReturns409(t *testing.T) {
	h, _, id := setupStagingGet(t, storage.StagingUploading, []byte("partial"))
	w := doGet(h, id, "")
	if w.Code != 409 {
		t.Errorf("status=%d", w.Code)
	}
	if parseJSON(t, w.Body.Bytes())["error"] != "staging_uploading" {
		t.Errorf("body=%s", w.Body.String())
	}
}

func TestStagingGetDeletingReturns410(t *testing.T) {
	h, _, id := setupStagingGet(t, storage.StagingDeleting, []byte("x"))
	w := doGet(h, id, "")
	if w.Code != 410 {
		t.Errorf("status=%d", w.Code)
	}
}

func TestStagingGetMissingReturns404(t *testing.T) {
	h, _, _ := setupStagingPut(t)
	w := doGet(h, ids.NewUUIDv7(), "")
	if w.Code != 404 {
		t.Errorf("status=%d", w.Code)
	}
}

func TestStagingGetInvalidIDReturns400(t *testing.T) {
	h, _, _ := setupStagingPut(t)
	w := doGet(h, "bad-id", "")
	if w.Code != 400 {
		t.Errorf("status=%d", w.Code)
	}
}
