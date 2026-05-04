package handlers_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"letts/internal/config"
	"letts/internal/ids"
	"letts/internal/server/handlers"
	"letts/internal/stagingstore"
	"letts/internal/storage"
)

// cancelOnReadBody delivers its full payload in the first Read and then cancels
// the request context, reproducing the disconnect race: the client disconnects right
// after the last byte, so the request ctx is Done by the time the handler tries
// to mark the (fully-written, sha-verified) upload complete.
type cancelOnReadBody struct {
	data   []byte
	done   bool
	cancel context.CancelFunc
}

func (b *cancelOnReadBody) Read(p []byte) (int, error) {
	if b.done {
		return 0, io.EOF
	}
	n := copy(p, b.data)
	b.done = true
	if b.cancel != nil {
		b.cancel()
		b.cancel = nil
	}
	return n, nil
}

func (b *cancelOnReadBody) Close() error { return nil }

// TestStagingPutCompletesDespiteRequestCtxCancel: a fully-received,
// sha-verified upload must reach state='complete' even if the request context
// is cancelled in the window after the body is read. Binding
// MarkStagingComplete to the request ctx would let a client disconnect leave
// the row stuck 'uploading' with a complete file on disk (a silently lost
// upload).
func TestStagingPutCompletesDespiteRequestCtxCancel(t *testing.T) {
	h, db, _ := setupStagingPut(t)
	id := ids.NewUUIDv7()
	payload := []byte("payload-bytes-13")

	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	body := &cancelOnReadBody{data: payload, cancel: cancel}

	r := httptest.NewRequest("PUT", "/v1/staging/"+id, body).WithContext(parent)
	r.SetPathValue("id", id)
	r.Header.Set("X-Letts-Sha256", sha256Hex(payload))
	r.ContentLength = int64(len(payload))
	w := httptest.NewRecorder()
	h.Put(w, r)

	if w.Code != 201 {
		t.Fatalf("status=%d body=%s, want 201 (completion must survive request-ctx cancel)", w.Code, w.Body.String())
	}
	sf, err := storage.GetStaging(context.Background(), db, id)
	if err != nil {
		t.Fatalf("get staging: %v", err)
	}
	if sf.State != storage.StagingComplete {
		t.Errorf("state=%q, want complete (row stuck uploading = lost upload)", sf.State)
	}
}

// TestStagingPutContentRangeEndMustMatchChunkLength: when Content-Range is
// present, its end must equal start+ContentLength-1. Parsing end but then
// ignoring it would drive the write loop purely off the denominator (total),
// letting a client claim a small chunk (0-2) while streaming the whole file.
func TestStagingPutContentRangeEndMustMatchChunkLength(t *testing.T) {
	h, _, _ := setupStagingPut(t)
	id := ids.NewUUIDv7()
	payload := []byte("payload") // 7 bytes, ContentLength=7
	// Content-Range claims bytes 0-2 (a 3-byte chunk) of a 7-byte total,
	// but the body/ContentLength is 7 — a lie the server must reject.
	w := doPut(h, id, payload, sha256Hex(payload), "bytes 0-2/7")
	if w.Code != 400 {
		t.Fatalf("status=%d body=%s, want 400 (Content-Range end != chunk length)", w.Code, w.Body.String())
	}
}

// TestStagingPutDeclaredTotalOverDataDirQuota503: a declared total that
// alone exceeds max_data_dir_size can never complete, so the PUT must be
// rejected up front instead of streaming until a mid-stream quota
// re-check trips. This static check needs no DiskUsage wiring.
func TestStagingPutDeclaredTotalOverDataDirQuota503(t *testing.T) {
	h, _, _ := setupStagingPut(t)
	h.Cfg.Limits.MaxDataDirSize = 10
	h.Cfg.Limits.MaxStagingUploadSize = 0 // ensure the data-dir gate is what fires
	id := ids.NewUUIDv7()
	payload := []byte("this body is way over ten bytes")
	w := doPut(h, id, payload, sha256Hex(payload), "")
	if w.Code != 503 {
		t.Fatalf("status=%d body=%s, want 503 (declared total > max_data_dir_size)", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "disk_quota_exceeded") {
		t.Errorf("body=%s missing disk_quota_exceeded", w.Body.String())
	}
}

func setupStagingPut(t *testing.T) (*handlers.StagingHandler, *sql.DB, string) {
	t.Helper()
	db := setupDB(t)
	dataDir := t.TempDir()
	cfg := &config.DugdaleConfig{
		DataDir: dataDir,
		Cleanup: config.CleanupConfig{StagingTTL: time.Hour},
		Limits:  config.LimitsConfig{MaxStagingUploadSize: 1024 * 1024},
	}
	lock := stagingstore.NewUploadLock(time.Minute, nil)
	return &handlers.StagingHandler{DB: db, Cfg: cfg, DataDir: dataDir, UploadLock: lock}, db, dataDir
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func doPut(h *handlers.StagingHandler, id string, body []byte, sha string, contentRange string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("PUT", "/v1/staging/"+id, bytes.NewReader(body))
	r.SetPathValue("id", id)
	r.Header.Set("X-Letts-Sha256", sha)
	r.ContentLength = int64(len(body))
	if contentRange != "" {
		r.Header.Set("Content-Range", contentRange)
	}
	w := httptest.NewRecorder()
	h.Put(w, r)
	return w
}

// TestStagingPutIncompleteUploadsLimit503 enforces
// max_incomplete_staging_uploads (default 128, but tunable via config)
// must reject new PUTs once the count of in-flight uploading rows
// reaches the cap. This is listed alongside
// upload_idle_timeout and max_incomplete_staging_bytes as the
// slowloris/abandonment defences.
func TestStagingPutIncompleteUploadsLimit503(t *testing.T) {
	h, db, _ := setupStagingPut(t)
	// Cap at 2 concurrent uploads for testability.
	h.Cfg.Limits.MaxIncompleteUploads = 2

	// Insert two synthetic uploading rows to consume the budget.
	nowMs := time.Now().UnixMilli()
	for i := 0; i < 2; i++ {
		sid := ids.NewUUIDv7()
		if err := storage.InsertStaging(context.Background(), db, &storage.StagingFile{
			StagingID: sid, State: storage.StagingUploading,
			Sha256: "sha", Size: 1024, BytesReceived: 0,
			Path:          "staging/xx/yy/" + sid,
			TimeCreatedMs: nowMs, TimeUpdatedMs: nowMs, TimeExpiresMs: nowMs + 3600_000,
		}); err != nil {
			t.Fatal(err)
		}
	}

	// Third PUT should 503 with incomplete_uploads_full.
	id := ids.NewUUIDv7()
	payload := []byte("payload")
	w := doPut(h, id, payload, sha256Hex(payload), "")
	if w.Code != 503 {
		t.Errorf("status=%d body=%s, want 503", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "incomplete_uploads_full") {
		t.Errorf("body=%s missing incomplete_uploads_full", w.Body.String())
	}
}

// TestStagingPutIncompleteBytesLimit503 verifies the byte-sum variant of
// the same defence: max_incomplete_staging_bytes capping the sum of
// bytes_received over uploading rows.
func TestStagingPutIncompleteBytesLimit503(t *testing.T) {
	h, db, _ := setupStagingPut(t)
	// 100 bytes total in-flight budget.
	h.Cfg.Limits.MaxIncompleteBytes = 100

	nowMs := time.Now().UnixMilli()
	sid := ids.NewUUIDv7()
	if err := storage.InsertStaging(context.Background(), db, &storage.StagingFile{
		StagingID: sid, State: storage.StagingUploading,
		Sha256: "sha", Size: 200, BytesReceived: 150, // exceeds cap alone
		Path:          "staging/xx/yy/" + sid,
		TimeCreatedMs: nowMs, TimeUpdatedMs: nowMs, TimeExpiresMs: nowMs + 3600_000,
	}); err != nil {
		t.Fatal(err)
	}

	id := ids.NewUUIDv7()
	payload := []byte("payload")
	w := doPut(h, id, payload, sha256Hex(payload), "")
	if w.Code != 503 {
		t.Errorf("status=%d body=%s, want 503", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "incomplete_uploads_full") {
		t.Errorf("body=%s missing incomplete_uploads_full", w.Body.String())
	}
}

func TestStagingPutInitialFullUpload(t *testing.T) {
	h, db, dataDir := setupStagingPut(t)
	id := ids.NewUUIDv7()
	payload := []byte("hello world")
	sha := sha256Hex(payload)

	w := doPut(h, id, payload, sha, "")
	if w.Code != 201 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	got := parseJSON(t, w.Body.Bytes())
	if got["staging_id"] != id || got["sha256"] != sha {
		t.Errorf("body=%v", got)
	}

	sf, err := storage.GetStaging(context.Background(), db, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if sf.State != storage.StagingComplete || sf.Size != int64(len(payload)) || sf.Sha256 != sha {
		t.Errorf("row=%+v", sf)
	}

	shard, _ := ids.ShardPath(id)
	abs := filepath.Join(dataDir, "staging", shard, id)
	b, err := os.ReadFile(abs)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if !bytes.Equal(b, payload) {
		t.Errorf("file content=%q, want %q", b, payload)
	}
}

func TestStagingPutResumeFromOffset(t *testing.T) {
	h, db, dataDir := setupStagingPut(t)
	id := ids.NewUUIDv7()
	full := bytes.Repeat([]byte("a"), 100)
	sha := sha256Hex(full)

	// Pre-populate row and half the file as if a prior PUT was interrupted.
	shard, _ := ids.ShardPath(id)
	relPath := filepath.Join("staging", shard, id)
	abs := filepath.Join(dataDir, relPath)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(abs, full[:60], 0o600); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixMilli()
	if err := storage.InsertStaging(context.Background(), db, &storage.StagingFile{
		StagingID: id, State: storage.StagingUploading,
		Sha256: sha, Size: 100, BytesReceived: 60,
		Path: relPath, TimeCreatedMs: now, TimeUpdatedMs: now, TimeExpiresMs: now + 60_000,
	}); err != nil {
		t.Fatal(err)
	}

	// Resume PUT for bytes 60-99/100.
	w := doPut(h, id, full[60:], sha, "bytes 60-99/100")
	if w.Code != 201 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	sf, _ := storage.GetStaging(context.Background(), db, id)
	if sf.State != storage.StagingComplete {
		t.Errorf("state=%q", sf.State)
	}
	b, _ := os.ReadFile(abs)
	if !bytes.Equal(b, full) {
		t.Errorf("file %d bytes, want %d; first byte=%c", len(b), len(full), b[0])
	}
}

func TestStagingPutRetryOnCompleteSkipsBody(t *testing.T) {
	h, db, _ := setupStagingPut(t)
	id := ids.NewUUIDv7()
	payload := []byte("xyz")
	sha := sha256Hex(payload)
	doPut(h, id, payload, sha, "")

	// Second PUT with the same content — should 200 without re-reading body.
	w := doPut(h, id, []byte("garbage"), sha, "") // wrong body but matches declared sha+size
	// Actually contentLength=7 differs from existing.size=3 → expect 409 mismatch.
	if w.Code != 409 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if parseJSON(t, w.Body.Bytes())["error"] != "content_mismatch" {
		t.Errorf("body=%s", w.Body.String())
	}

	// Second PUT with same body+sha → 200, Connection: close.
	w = doPut(h, id, payload, sha, "")
	if w.Code != 200 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if w.Header().Get("Connection") != "close" {
		t.Errorf("missing Connection: close header")
	}
	sf, _ := storage.GetStaging(context.Background(), db, id)
	if sf.State != storage.StagingComplete {
		t.Errorf("state changed: %q", sf.State)
	}
}

func TestStagingPutWrongShaReturns409(t *testing.T) {
	h, db, _ := setupStagingPut(t)
	id := ids.NewUUIDv7()
	payload := []byte("hello")
	wrongSha := sha256Hex([]byte("different"))

	w := doPut(h, id, payload, wrongSha, "")
	if w.Code != 409 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if parseJSON(t, w.Body.Bytes())["error"] != "content_mismatch" {
		t.Errorf("body=%s", w.Body.String())
	}
	sf, err := storage.GetStaging(context.Background(), db, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if sf.State != storage.StagingDeleting {
		t.Errorf("state=%q, want deleting after sha mismatch", sf.State)
	}
}

func TestStagingPutWrongRangeStartReturns416(t *testing.T) {
	h, db, dataDir := setupStagingPut(t)
	id := ids.NewUUIDv7()
	full := bytes.Repeat([]byte("z"), 50)
	sha := sha256Hex(full)
	shard, _ := ids.ShardPath(id)
	relPath := filepath.Join("staging", shard, id)
	abs := filepath.Join(dataDir, relPath)
	_ = os.MkdirAll(filepath.Dir(abs), 0o755)
	_ = os.WriteFile(abs, full[:20], 0o600)
	now := time.Now().UnixMilli()
	_ = storage.InsertStaging(context.Background(), db, &storage.StagingFile{
		StagingID: id, State: storage.StagingUploading,
		Sha256: sha, Size: 50, BytesReceived: 20,
		Path: relPath, TimeCreatedMs: now, TimeUpdatedMs: now, TimeExpiresMs: now + 60_000,
	})

	// Resume from wrong offset.
	w := doPut(h, id, full[10:], sha, "bytes 10-49/50")
	if w.Code != 416 {
		t.Fatalf("status=%d", w.Code)
	}
}

func TestStagingPutOversizedReturns413(t *testing.T) {
	h, _, _ := setupStagingPut(t)
	h.Cfg.Limits.MaxStagingUploadSize = 10
	id := ids.NewUUIDv7()
	payload := bytes.Repeat([]byte("x"), 100)
	sha := sha256Hex(payload)
	w := doPut(h, id, payload, sha, "")
	if w.Code != 413 {
		t.Errorf("status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestStagingPutInvalidIDReturns400(t *testing.T) {
	h, _, _ := setupStagingPut(t)
	w := doPut(h, "not-a-uuid", []byte("x"), sha256Hex([]byte("x")), "")
	if w.Code != 400 {
		t.Errorf("status=%d", w.Code)
	}
}

func TestStagingPutInvalidShaReturns400(t *testing.T) {
	h, _, _ := setupStagingPut(t)
	id := ids.NewUUIDv7()
	w := doPut(h, id, []byte("x"), "not-hex", "")
	if w.Code != 400 {
		t.Errorf("status=%d", w.Code)
	}
}

func TestStagingPutNoUploadStartedRangeReturns416(t *testing.T) {
	h, _, _ := setupStagingPut(t)
	id := ids.NewUUIDv7()
	// 5-byte chunk so Content-Range "bytes 5-9/10" is internally consistent
	// (end == start+len-1); this isolates the "no upload started → 416" path
	// rather than tripping the chunk-length validation.
	payload := []byte("xxxxx")
	w := doPut(h, id, payload, sha256Hex(payload), "bytes 5-9/10")
	if w.Code != 416 {
		t.Errorf("status=%d", w.Code)
	}
}

func TestStagingPutMalformedRangeReturns400(t *testing.T) {
	h, _, _ := setupStagingPut(t)
	id := ids.NewUUIDv7()
	w := doPut(h, id, []byte("x"), sha256Hex([]byte("x")), "garbage")
	if w.Code != 400 {
		t.Errorf("status=%d", w.Code)
	}
}

// TestStagingPutIdleAbortResetsTimeExpires verifies that when the
// UploadLock janitor fires onIdle for an abandoned upload, the matching
// staging_files row's time_expires is flipped to "now" so the cleanup
// GC sweeps the partial file on its NEXT cycle instead of waiting the
// full staging_ttl.
func TestStagingPutIdleAbortResetsTimeExpires(t *testing.T) {
	h, db, _ := setupStagingPut(t)
	id := ids.NewUUIDv7()

	// Seed an uploading row with a far-future time_expires so the value
	// change is unambiguous when the abort fires.
	future := time.Now().Add(24 * time.Hour).UnixMilli()
	if err := storage.InsertStaging(context.Background(), db, &storage.StagingFile{
		StagingID:     id,
		State:         storage.StagingUploading,
		Sha256:        "abc",
		Size:          100,
		BytesReceived: 10,
		Path:          "staging/" + id,
		TimeCreatedMs: 1, TimeUpdatedMs: 1, TimeExpiresMs: future,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Invoke the idle-abort callback directly with a no-op cancel. The
	// handler builds this closure per PUT; here we ask the handler to
	// build it the same way for testability.
	cancelCalled := false
	cancel := func() { cancelCalled = true }
	onIdle := handlers.MakeIdleAbortFnForTest(h, id, cancel)
	onIdle()

	if !cancelCalled {
		t.Error("idle-abort did not invoke cancel")
	}
	got, err := storage.GetStaging(context.Background(), db, id)
	if err != nil {
		t.Fatalf("get staging: %v", err)
	}
	now := time.Now().UnixMilli()
	if got.TimeExpiresMs > now+1000 {
		t.Errorf("time_expires not reset: got %d, now %d (still future by %dms)",
			got.TimeExpiresMs, now, got.TimeExpiresMs-now)
	}
	if got.TimeExpiresMs < now-5000 {
		t.Errorf("time_expires unexpectedly low: got %d, now %d", got.TimeExpiresMs, now)
	}
}

func TestStagingPutSecondConcurrentReturns409(t *testing.T) {
	h, _, _ := setupStagingPut(t)
	id := ids.NewUUIDv7()

	// Manually acquire the lock to simulate an in-flight PUT.
	rel, ok := h.UploadLock.TryAcquire(id, nil)
	if !ok {
		t.Fatal("setup failed")
	}
	defer rel()

	w := doPut(h, id, []byte("x"), sha256Hex([]byte("x")), "")
	if w.Code != 409 {
		t.Errorf("status=%d", w.Code)
	}
	if parseJSON(t, w.Body.Bytes())["error"] != "upload_in_progress" {
		t.Errorf("body=%s", w.Body.String())
	}
}

func TestStagingPutShortBodyReturns400(t *testing.T) {
	h, db, _ := setupStagingPut(t)
	id := ids.NewUUIDv7()
	full := bytes.Repeat([]byte("a"), 100)
	sha := sha256Hex(full)

	// Lie about Content-Length to claim 100 bytes but only send 30.
	r := httptest.NewRequest("PUT", "/v1/staging/"+id, bytes.NewReader(full[:30]))
	r.SetPathValue("id", id)
	r.Header.Set("X-Letts-Sha256", sha)
	r.ContentLength = 100
	w := httptest.NewRecorder()
	h.Put(w, r)

	if w.Code != 400 {
		t.Errorf("status=%d body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "incomplete_upload") {
		t.Errorf("body=%s", w.Body.String())
	}
	sf, _ := storage.GetStaging(context.Background(), db, id)
	if sf.State != storage.StagingUploading {
		t.Errorf("state=%q, want uploading after partial body", sf.State)
	}
}
