package fsutil

import (
	"os"
	"path/filepath"
	"testing"
)

func TestContainedPathAccepts(t *testing.T) {
	parent := t.TempDir()
	child := filepath.Join(parent, "ok.txt")
	if err := os.WriteFile(child, []byte{}, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	got, err := ContainedPath(parent, child)
	if err != nil {
		t.Fatalf("ContainedPath: %v", err)
	}
	resolvedParent, _ := filepath.EvalSymlinks(parent)
	if !filepath.IsAbs(got) {
		t.Errorf("got = %s, expected absolute", got)
	}
	if got != filepath.Clean(filepath.Join(resolvedParent, "ok.txt")) {
		t.Errorf("got %s, want under %s", got, resolvedParent)
	}
}

func TestContainedPathRejectsEscape(t *testing.T) {
	parent := t.TempDir()
	other := t.TempDir()
	target := filepath.Join(other, "secret")
	if err := os.WriteFile(target, []byte{}, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	link := filepath.Join(parent, "escape")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink unsupported: %v", err)
	}
	if _, err := ContainedPath(parent, link); err == nil {
		t.Error("expected error for escape symlink")
	}
}

func TestContainedPathParentDoesNotExist(t *testing.T) {
	if _, err := ContainedPath("/nonexistent-letts-test", "/nonexistent-letts-test/x"); err == nil {
		t.Error("expected error when parent missing")
	}
}
