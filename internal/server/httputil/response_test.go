package httputil

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// SQL error strings often quote table and column names
// ("no such table: foo_v2", "constraint failed: missions.unique_idx").
// WriteDBError must NOT echo those into the response body while still
// logging the raw err at server level so operators can debug.
func TestWriteDBErrorOmitsSQLTextFromResponse(t *testing.T) {
	var logBuf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	rec := httptest.NewRecorder()
	sqlErr := errors.New("no such table: missions_v3")
	WriteDBError(rec, 500, "test-op", sqlErr)

	if rec.Code != 500 {
		t.Errorf("status=%d want 500", rec.Code)
	}
	body := rec.Body.String()
	if strings.Contains(body, "missions_v3") || strings.Contains(body, "no such table") {
		t.Errorf("response body leaks SQL text: %s", body)
	}
	var resp ErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response not JSON: %v body=%s", err, body)
	}
	if resp.Error != "db_error" {
		t.Errorf("error=%q want db_error", resp.Error)
	}
	if resp.Details != nil {
		t.Errorf("details should be nil to avoid leak; got %v", resp.Details)
	}
	// But the slog output MUST contain the raw error so operators
	// can grep for it after a 500 lands in production.
	logged := logBuf.String()
	if !strings.Contains(logged, "missions_v3") {
		t.Errorf("server log should contain raw error for debugging; got %s", logged)
	}
	if !strings.Contains(logged, "test-op") {
		t.Errorf("server log should tag op; got %s", logged)
	}
}

// TestWriteIOErrorOmitsAbsolutePathFromResponse verifies filesystem
// errors echoed via err.Error() leak the absolute data_dir path
// (`open /var/lib/letts/prod/staging/...: no such file or directory`),
// which is info disclosure for anyone holding a known-id read capability.
// Like WriteDBError, log the raw err but ship a generic body.
func TestWriteIOErrorOmitsAbsolutePathFromResponse(t *testing.T) {
	var logBuf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	rec := httptest.NewRecorder()
	ioErr := &os.PathError{Op: "open", Path: "/var/lib/letts/prod/staging/aa/bb/leaked", Err: errors.New("permission denied")}
	WriteIOError(rec, 500, "staging.Get/open", ioErr)

	body := rec.Body.String()
	if strings.Contains(body, "/var/lib/letts") || strings.Contains(body, "permission denied") {
		t.Errorf("response body leaks fs path/err: %s", body)
	}
	var resp ErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response not JSON: %v body=%s", err, body)
	}
	if resp.Error != "io_error" {
		t.Errorf("error=%q want io_error", resp.Error)
	}
	logged := logBuf.String()
	if !strings.Contains(logged, "/var/lib/letts/prod/staging/aa/bb/leaked") {
		t.Errorf("server log should contain raw path for debugging; got %s", logged)
	}
}

// TestWriteIOErrorNotExistMapsToGone exercises the os.ErrNotExist
// branch — staging tombstone race with GET should surface 410 Gone,
// consistent with state='deleting' branch.
func TestWriteIOErrorNotExistMapsToGone(t *testing.T) {
	rec := httptest.NewRecorder()
	WriteIOError(rec, 500, "test", os.ErrNotExist)
	if rec.Code != 410 {
		t.Errorf("status=%d, want 410 Gone for os.ErrNotExist", rec.Code)
	}
}
