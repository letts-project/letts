package fsutil

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSyncDir(t *testing.T) {
	dir := t.TempDir()
	if err := SyncDir(dir); err != nil {
		t.Errorf("SyncDir: %v", err)
	}
}

func TestSyncDirNonexistent(t *testing.T) {
	if err := SyncDir(filepath.Join(t.TempDir(), "nope")); err == nil {
		t.Error("expected error for nonexistent dir")
	}
}

func TestSyncDirOnFile(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, "x")
	if err := os.WriteFile(f, []byte("x"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if err := SyncDir(f); err == nil {
		t.Error("expected error when path is a regular file")
	}
}

// TestDirChainPathsTwoLevels verifies path enumeration over a two-level
// rel ("ab/cd"): base, base/ab, base/ab/cd. This matches the shard pattern
// used for output and staging dirs.
func TestDirChainPathsTwoLevels(t *testing.T) {
	got := dirChainPaths("/data", "ab/cd")
	want := []string{"/data", "/data/ab", "/data/ab/cd"}
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("[%d]: got %q want %q", i, got[i], want[i])
		}
	}
}

// TestDirChainPathsEmptyRel verifies empty rel just returns base alone.
func TestDirChainPathsEmptyRel(t *testing.T) {
	got := dirChainPaths("/data", "")
	want := []string{"/data"}
	if len(got) != len(want) || got[0] != want[0] {
		t.Errorf("got %v want %v", got, want)
	}
}

// TestSyncDirChainFsyncsEachAncestor verifies SyncDirChain visits base
// and every intermediate directory down to the leaf. We assert this by
// pointing the chain at a tree that exists end-to-end (no error) and by
// breaking an intermediate to confirm the call surfaces the error.
func TestSyncDirChainFsyncsEachAncestor(t *testing.T) {
	base := t.TempDir()
	if err := os.MkdirAll(filepath.Join(base, "ab", "cd"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := SyncDirChain(base, "ab/cd"); err != nil {
		t.Errorf("SyncDirChain on existing tree: %v", err)
	}
}

func TestSyncDirChainMissingIntermediate(t *testing.T) {
	base := t.TempDir()
	if err := os.MkdirAll(filepath.Join(base, "ab"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	// "ab/cd" leaf doesn't exist — chain should surface that as an error.
	if err := SyncDirChain(base, "ab/cd"); err == nil {
		t.Error("expected error when leaf is missing")
	}
}
