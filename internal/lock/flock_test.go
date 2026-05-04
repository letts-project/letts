package lock

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestAcquireOnce(t *testing.T) {
	dir := t.TempDir()
	l, err := Acquire(filepath.Join(dir, "dugdale.lock"), Info{Pid: 1, Host: "h"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Release() }()
}

func TestAcquireConflict(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dugdale.lock")
	l1, err := Acquire(path, Info{Pid: 1, Host: "h"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l1.Release() }()
	if _, err := Acquire(path, Info{Pid: 2, Host: "h"}); !errors.Is(err, ErrLocked) {
		t.Errorf("expected ErrLocked, got %v", err)
	}
}

func TestReleaseAllowsReacquire(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dugdale.lock")
	l1, _ := Acquire(path, Info{Pid: 1, Host: "h"})
	_ = l1.Release()
	l2, err := Acquire(path, Info{Pid: 2, Host: "h"})
	if err != nil {
		t.Fatalf("reacquire: %v", err)
	}
	defer func() { _ = l2.Release() }()
}

// flock is per-inode, so the lock survives the on-disk
// dirent being removed. If an admin runs `rm <data_dir>/dugdale.lock`
// while dugdale is running, a second dugdale start re-creates the
// file at a NEW inode and acquires its own flock — two daemons land
// on the same data_dir. A periodic Verify() call from a watchdog
// goroutine detects the inode mismatch so the original daemon can
// shut down cleanly.
func TestVerifyDetectsLockFileRemoved(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dugdale.lock")
	l, err := Acquire(path, Info{Pid: 1, Host: "h"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Release() }()

	if err := l.Verify(); err != nil {
		t.Fatalf("Verify on fresh lock: %v", err)
	}

	// Simulate admin rm.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := l.Verify(); err == nil {
		t.Error("Verify after rm: want error, got nil")
	}
}

func TestVerifyDetectsLockFileReplaced(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dugdale.lock")
	l, err := Acquire(path, Info{Pid: 1, Host: "h"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Release() }()

	// After an admin rm, a second dugdale re-creates the file at a new inode.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("pid=99\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := l.Verify(); err == nil {
		t.Error("Verify after replace: want inode-mismatch error, got nil")
	}
}

// TestVerifyReleaseConcurrent runs Verify in a goroutine while Release
// fires from another. The mutex inside Lock serializes both around the
// f pointer; -race must stay clean.
func TestVerifyReleaseConcurrent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dugdale.lock")
	l, err := Acquire(path, Info{Pid: 1, Host: "h"})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			_ = l.Verify()
		}
		close(done)
	}()
	_ = l.Release()
	<-done
}
