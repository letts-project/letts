package handlers_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"letts/internal/criticalerr"
	"letts/internal/server/handlers"
	"letts/internal/storage"
	"letts/internal/version"
)

func newTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func setupDB(t *testing.T) *sql.DB {
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

func TestHealthz(t *testing.T) {
	db := setupDB(t)
	h := &handlers.Health{DB: db}
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest("GET", "/v1/healthz", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status: got %d, want 200", w.Code)
	}
	var got map[string]string
	if err := json.NewDecoder(w.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got["status"] != "ok" {
		t.Errorf("status field: got %q, want ok", got["status"])
	}
}

func TestReadyzNoConfig(t *testing.T) {
	db := setupDB(t)
	h := &handlers.Health{DB: db}
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest("GET", "/v1/readyz", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status: got %d, want 503", w.Code)
	}
	var got map[string]any
	if err := json.NewDecoder(w.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got["error"] != "awaiting_apply" {
		t.Errorf("error field: got %q, want awaiting_apply", got["error"])
	}
}

func TestReadyzWithConfig(t *testing.T) {
	db := setupDB(t)
	if err := storage.SetAppliedConfig(context.Background(), db, storage.AppliedConfig{
		Data:      []byte(`{}`),
		AppliedAt: 1700000000000,
		Source:    sql.NullString{String: "test", Valid: true},
	}); err != nil {
		t.Fatal(err)
	}

	h := &handlers.Health{DB: db}
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest("GET", "/v1/readyz", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status: got %d, want 200", w.Code)
	}
	var got map[string]any
	if err := json.NewDecoder(w.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got["status"] != "ok" {
		t.Errorf("status field: got %q, want ok", got["status"])
	}
}

// TestReadyzFlipsTo503DuringDrain verifies the readyz behavior:
// IsDraining returning true must produce 503 awaiting_drain with a
// Retry-After header, even when an applied config exists. LBs polling
// /readyz then stop routing new traffic before the dispatch path also
// starts 503'ing.
func TestReadyzFlipsTo503DuringDrain(t *testing.T) {
	db := setupDB(t)
	if err := storage.SetAppliedConfig(context.Background(), db, storage.AppliedConfig{
		Data:      []byte(`{}`),
		AppliedAt: 1700000000000,
		Source:    sql.NullString{String: "test", Valid: true},
	}); err != nil {
		t.Fatal(err)
	}
	h := &handlers.Health{DB: db, IsDraining: func() bool { return true }}
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest("GET", "/v1/readyz", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status: got %d, want 503", w.Code)
	}
	if got := w.Header().Get("Retry-After"); got != "30" {
		t.Errorf("Retry-After=%q, want 30", got)
	}
	var got map[string]any
	if err := json.NewDecoder(w.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got["error"] != "awaiting_drain" {
		t.Errorf("error=%q, want awaiting_drain", got["error"])
	}
}

// TestReadyzReportsCriticalErrorWhenTripped: when
// commitFinalize or repair observes ErrTerminalEventConflict, the
// criticalerr flag trips and /v1/readyz must return 503
// awaiting_manual_repair until operator resolves. Sticky across
// requests.
func TestReadyzReportsCriticalErrorWhenTripped(t *testing.T) {
	db := setupDB(t)
	if err := storage.SetAppliedConfig(context.Background(), db, storage.AppliedConfig{
		Data:      []byte(`{}`),
		AppliedAt: 1700000000000,
		Source:    sql.NullString{String: "test", Valid: true},
	}); err != nil {
		t.Fatal(err)
	}

	criticalerr.Reset()
	t.Cleanup(criticalerr.Reset)
	criticalerr.Trip(criticalerr.Detail{
		Kind: "terminal_event_conflict", MissionID: "abc", Op: "test",
	})

	h := &handlers.Health{DB: db}
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest("GET", "/v1/readyz", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status: got %d, want 503", w.Code)
	}
	var got map[string]any
	if err := json.NewDecoder(w.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got["error"] != "awaiting_manual_repair" {
		t.Errorf("error=%q, want awaiting_manual_repair", got["error"])
	}
	details, _ := got["details"].(map[string]any)
	if details["mission_id"] != "abc" {
		t.Errorf("details.mission_id=%v, want abc", details["mission_id"])
	}
}

func TestVersion(t *testing.T) {
	db := setupDB(t)
	h := &handlers.Health{DB: db}
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest("GET", "/v1/version", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status: got %d, want 200", w.Code)
	}
	var got map[string]string
	if err := json.NewDecoder(w.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got["version"] != version.Version {
		t.Errorf("version: got %q, want %q", got["version"], version.Version)
	}
	if _, ok := got["commit"]; !ok {
		t.Error("missing commit field")
	}
	if _, ok := got["built_at"]; !ok {
		t.Error("missing built_at field")
	}
}
