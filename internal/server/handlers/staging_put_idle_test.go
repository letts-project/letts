package handlers_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"letts/internal/config"
	"letts/internal/ids"
	"letts/internal/server/handlers"
	"letts/internal/server/middleware"
	"letts/internal/stagingstore"
	"letts/internal/storage"
)

// newIdleTestServer boots a real HTTP server around the staging routes,
// wrapped in the production RequestLog middleware. ResponseController read
// deadlines need a real connection (httptest.ResponseRecorder reports
// ErrNotSupported), and going through the middleware proves the wrapper chain
// forwards deadline controls instead of swallowing them.
func newIdleTestServer(t *testing.T, idle time.Duration) (*httptest.Server, *handlers.StagingHandler) {
	t.Helper()
	db := setupDB(t)
	dataDir := t.TempDir()
	cfg := &config.DugdaleConfig{
		DataDir: dataDir,
		Cleanup: config.CleanupConfig{StagingTTL: time.Hour},
		Limits: config.LimitsConfig{
			MaxStagingUploadSize: 1 << 20,
			UploadIdleTimeout:    idle,
		},
	}
	h := &handlers.StagingHandler{
		DB: db, Cfg: cfg, DataDir: dataDir,
		// Janitor not started: these tests isolate the read-deadline path.
		UploadLock: stagingstore.NewUploadLock(idle, nil),
	}
	mux := http.NewServeMux()
	h.Register(mux)
	srv := httptest.NewServer(middleware.RequestLog(newTestLogger(), mux))
	t.Cleanup(srv.Close)
	return srv, h
}

func doServerPut(t *testing.T, srv *httptest.Server, id string, body io.Reader, total int64, sha string) (*http.Response, error) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), "PUT", srv.URL+"/v1/staging/"+id, body)
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = total
	req.Header.Set("X-Letts-Sha256", sha)
	client := &http.Client{Timeout: 30 * time.Second} // CI guard, not an assertion bound
	return client.Do(req)
}

// TestStagingPutIdleBodyAbortsWith408: a client that opens a PUT, sends a few
// bytes and then goes silent must not park the handler goroutine forever.
// The per-request read deadline turns the stall into a 408
// upload_idle_timeout, the row flips to deleting and the partial file is
// removed — instead of the goroutine and fd being pinned until process exit.
func TestStagingPutIdleBodyAbortsWith408(t *testing.T) {
	const idle = 500 * time.Millisecond
	srv, h := newIdleTestServer(t, idle)
	id := ids.NewUUIDv7()

	payload := []byte("0123456789abcdef") // 16 bytes delivered...
	const total = 1024                    // ...of a declared 1 KiB

	pr, pw := io.Pipe()
	defer func() { _ = pw.Close() }()
	go func() {
		_, _ = pw.Write(payload)
		// Then: silence. The pipe stays open so the body read genuinely
		// blocks rather than returning EOF.
	}()

	start := time.Now()
	resp, err := doServerPut(t, srv, id, pr, total, sha256Hex(payload))
	if err != nil {
		t.Fatalf("request failed instead of returning 408: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	elapsed := time.Since(start)

	if resp.StatusCode != http.StatusRequestTimeout {
		t.Fatalf("status=%d, want 408", resp.StatusCode)
	}
	respBody, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(respBody), "upload_idle_timeout") {
		t.Errorf("body=%s missing upload_idle_timeout", respBody)
	}
	// Generous bounds: the abort must come from the idle deadline (≥ idle),
	// not hang unboundedly (the 30s client timeout already guards that).
	if elapsed < idle {
		t.Errorf("aborted after %v, before the %v idle timeout", elapsed, idle)
	}

	sf, err := storage.GetStaging(context.Background(), h.DB, id)
	if err != nil {
		t.Fatalf("get staging: %v", err)
	}
	if sf.State != storage.StagingDeleting {
		t.Errorf("state=%q, want deleting (partial must be discarded)", sf.State)
	}
	shard, _ := ids.ShardPath(id)
	if _, err := os.Stat(filepath.Join(h.DataDir, "staging", shard, id)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("partial file still on disk (stat err=%v)", err)
	}
}

// TestStagingPutSlowProgressingUploadOutlivesIdleTimeout: the deadline is
// pushed forward on every accepted chunk, so an upload that keeps making
// progress must complete even when its TOTAL duration exceeds
// upload_idle_timeout. Only silence is idleness.
func TestStagingPutSlowProgressingUploadOutlivesIdleTimeout(t *testing.T) {
	const idle = 1 * time.Second
	srv, h := newIdleTestServer(t, idle)
	id := ids.NewUUIDv7()

	const chunks = 15
	chunk := []byte("aaaaaaaa") // 8 bytes × 15 = 120 bytes over ~1.5s total
	full := make([]byte, 0, len(chunk)*chunks)
	for i := 0; i < chunks; i++ {
		full = append(full, chunk...)
	}

	pr, pw := io.Pipe()
	go func() {
		for i := 0; i < chunks; i++ {
			if _, err := pw.Write(chunk); err != nil {
				return
			}
			time.Sleep(100 * time.Millisecond) // well under idle per chunk
		}
		_ = pw.Close()
	}()

	resp, err := doServerPut(t, srv, id, pr, int64(len(full)), sha256Hex(full))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusCreated {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d body=%s, want 201 (progress must push the idle deadline forward)",
			resp.StatusCode, respBody)
	}
	sf, err := storage.GetStaging(context.Background(), h.DB, id)
	if err != nil {
		t.Fatalf("get staging: %v", err)
	}
	if sf.State != storage.StagingComplete {
		t.Errorf("state=%q, want complete", sf.State)
	}
}
