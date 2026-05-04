package config

import (
	"os"
	"path/filepath"
	"testing"
)

// TestValidateConfigPathsAcceptsWritableDataDir verifies the happy path:
// data_dir exists and is writable.
func TestValidateConfigPathsAcceptsWritableDataDir(t *testing.T) {
	dir := t.TempDir()
	c := &DugdaleConfig{DataDir: dir}
	if err := ValidateConfigPaths(c); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestValidateConfigPathsAcceptsMissingDataDirWithWritableParent verifies
// that a yet-to-be-created data_dir is accepted when its parent is writable.
// This is the common case: `dugdale --check-config` runs before MkdirAll.
func TestValidateConfigPathsAcceptsMissingDataDirWithWritableParent(t *testing.T) {
	parent := t.TempDir()
	c := &DugdaleConfig{DataDir: filepath.Join(parent, "not-yet")}
	if err := ValidateConfigPaths(c); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestValidateConfigPathsRejectsUnreachableDataDirParent verifies that a
// data_dir under a nonexistent grandparent surfaces as an error rather
// than blowing up at first MkdirAll.
func TestValidateConfigPathsRejectsUnreachableDataDirParent(t *testing.T) {
	c := &DugdaleConfig{DataDir: "/nonexistent-grandparent-abc/letts/data"}
	if err := ValidateConfigPaths(c); err == nil {
		t.Error("expected error for unreachable parent")
	}
}

// TestValidateConfigPathsChecksLogOutputWritability verifies log.output
// pointing at an unwritable target produces an error.
func TestValidateConfigPathsChecksLogOutputWritability(t *testing.T) {
	c := &DugdaleConfig{
		DataDir: t.TempDir(),
		Log:     LogConfig{Output: "/nonexistent-log-parent-zzz/dugdale.log"},
	}
	if err := ValidateConfigPaths(c); err == nil {
		t.Error("expected error for unreachable log.output parent")
	}
}

// TestValidateConfigPathsAcceptsStdoutStderr verifies the special log.output
// strings "stdout", "stderr", and "-" don't trigger file writability checks.
func TestValidateConfigPathsAcceptsStdoutStderr(t *testing.T) {
	for _, out := range []string{"stdout", "stderr", "-"} {
		t.Run(out, func(t *testing.T) {
			c := &DugdaleConfig{DataDir: t.TempDir(), Log: LogConfig{Output: out}}
			if err := ValidateConfigPaths(c); err != nil {
				t.Errorf("Output=%q: unexpected error: %v", out, err)
			}
		})
	}
}

// TestValidateConfigPathsAcceptsExistingWritableLog verifies an existing
// writable log file is accepted (file exists check and write probe).
func TestValidateConfigPathsAcceptsExistingWritableLog(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "dugdale.log")
	if err := os.WriteFile(logPath, []byte("preexisting"), 0o600); err != nil {
		t.Fatalf("seed log: %v", err)
	}
	c := &DugdaleConfig{DataDir: dir, Log: LogConfig{Output: logPath}}
	if err := ValidateConfigPaths(c); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}
