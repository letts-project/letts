package server_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"testing"

	"letts/internal/server"
	"letts/internal/storage"
)

func setupTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dir := t.TempDir()
	db, err := storage.Open(filepath.Join(dir, "state.db"), storage.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := storage.Migrate(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	return db
}

// startServer spins up a Server on a random port and returns the base URL and
// a cancel func that gracefully stops it.
func startServer(t *testing.T, db *sql.DB) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	deps := server.Deps{
		DB:       db,
		Listener: ln,
		Logger:   slog.Default(),
	}
	srv := server.NewServer(deps)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return "http://" + ln.Addr().String()
}

func TestServerHealthz(t *testing.T) {
	base := startServer(t, setupTestDB(t))

	resp, err := http.Get(base + "/v1/healthz")
	if err != nil {
		t.Fatalf("GET /v1/healthz: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", resp.StatusCode)
	}
}

func TestServerReadyzNoConfig(t *testing.T) {
	base := startServer(t, setupTestDB(t))

	resp, err := http.Get(base + "/v1/readyz")
	if err != nil {
		t.Fatalf("GET /v1/readyz: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("want 503, got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got["error"] != "awaiting_apply" {
		t.Errorf("error field: got %v, want awaiting_apply", got["error"])
	}
}

func TestServerVersion(t *testing.T) {
	base := startServer(t, setupTestDB(t))

	resp, err := http.Get(base + "/v1/version")
	if err != nil {
		t.Fatalf("GET /v1/version: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", resp.StatusCode)
	}
	var got map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	for _, field := range []string{"version", "commit", "built_at"} {
		if _, ok := got[field]; !ok {
			t.Errorf("missing field %q in version response", field)
		}
	}
}
