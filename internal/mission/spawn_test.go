package mission

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestSpawnFd3Write verifies that the mission can write to fd 3 and the parent
// receives the data via Fd3Reader.
func TestSpawnFd3Write(t *testing.T) {
	dir := t.TempDir()

	var stdout, stderr bytes.Buffer
	result, err := Spawn(
		[]string{"/bin/sh", "-c", "echo hi >&3"},
		[]string{"PATH=/usr/bin:/bin"},
		dir,
		&stdout,
		&stderr,
		"", // no stdin file
		0,  // no wait-delay bound — these children leave no survivors holding stdout/stderr
	)
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	defer result.Cmd.Wait() //nolint:errcheck

	data, err := io.ReadAll(result.Fd3Reader)
	if err != nil {
		t.Fatalf("read fd3: %v", err)
	}
	_ = result.Fd3Reader.Close()

	if err := result.Cmd.Wait(); err != nil {
		t.Fatalf("cmd wait: %v", err)
	}

	got := strings.TrimRight(string(data), "\n")
	if got != "hi" {
		t.Errorf("fd3 data: got %q, want %q", got, "hi")
	}
}

// TestSpawnEchoExitCode verifies a simple echo exits cleanly with no fd3 traffic.
func TestSpawnEchoExitCode(t *testing.T) {
	dir := t.TempDir()

	var stdout bytes.Buffer
	result, err := Spawn(
		[]string{"/bin/echo", "something"},
		[]string{"PATH=/usr/bin:/bin"},
		dir,
		&stdout,
		io.Discard,
		"",
		0,
	)
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}

	// fd3 should yield EOF immediately after process finishes.
	data, err := io.ReadAll(result.Fd3Reader)
	if err != nil {
		t.Fatalf("read fd3: %v", err)
	}
	_ = result.Fd3Reader.Close()

	if err := result.Cmd.Wait(); err != nil {
		t.Fatalf("cmd wait: %v", err)
	}

	if len(data) != 0 {
		t.Errorf("expected empty fd3, got %q", data)
	}
	if !strings.Contains(stdout.String(), "something") {
		t.Errorf("stdout: %q", stdout.String())
	}
}

// TestSpawnSetpgid verifies SysProcAttr.Setpgid is set on the Cmd.
func TestSpawnSetpgid(t *testing.T) {
	dir := t.TempDir()

	result, err := Spawn(
		[]string{"/bin/echo", "pgid-test"},
		nil,
		dir,
		io.Discard,
		io.Discard,
		"",
		0,
	)
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	io.ReadAll(result.Fd3Reader) //nolint:errcheck
	_ = result.Fd3Reader.Close()
	result.Cmd.Wait() //nolint:errcheck

	if result.Cmd.SysProcAttr == nil {
		t.Fatal("SysProcAttr is nil")
	}
	if !result.Cmd.SysProcAttr.Setpgid {
		t.Error("Setpgid should be true")
	}
}

// TestSpawnEmptyArgv verifies that empty argv returns an error.
func TestSpawnEmptyArgv(t *testing.T) {
	dir := t.TempDir()

	_, err := Spawn(nil, nil, dir, io.Discard, io.Discard, "", 0)
	if err == nil {
		t.Fatal("expected error for empty argv, got nil")
	}
}

// TestSpawnWithStdin verifies that stdinPath is wired correctly.
func TestSpawnWithStdin(t *testing.T) {
	dir := t.TempDir()

	stdinFile := filepath.Join(dir, "input.txt")
	if err := os.WriteFile(stdinFile, []byte("hello from stdin\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	var stdout bytes.Buffer
	result, err := Spawn(
		[]string{"/bin/cat"},
		[]string{"PATH=/usr/bin:/bin"},
		dir,
		&stdout,
		io.Discard,
		stdinFile,
		0,
	)
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	io.ReadAll(result.Fd3Reader) //nolint:errcheck
	_ = result.Fd3Reader.Close()

	if err := result.Cmd.Wait(); err != nil {
		t.Fatalf("cmd wait: %v", err)
	}

	if !strings.Contains(stdout.String(), "hello from stdin") {
		t.Errorf("stdout: %q", stdout.String())
	}
}
