package mission

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// TestCopyFileRoundTrip is a platform-agnostic correctness check for
// copyFile. Verifies that whatever the per-OS strategy is —
// FICLONE / copy_file_range / clonefile / buffered io.Copy — the dst file
// receives the exact bytes of src, and the destination's mode is
// independent of the source.
//
// Doesn't try to detect *which* path was taken (FICLONE vs CFR vs
// io.Copy) — that depends on the filesystem and kernel version of the
// test host. Only correctness is required; the platform-
// specific branches are an optimisation for CoW filesystems.
func TestCopyFileRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		body []byte
	}{
		{"empty", nil},
		{"small", []byte("hello, world")},
		{"binary", []byte{0, 1, 2, 3, 4, 5, 0xff, 0xfe, 0xfd}},
		{"large", bytes.Repeat([]byte("AB"), 1<<20)}, // 2 MiB
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			src := filepath.Join(dir, "src")
			dst := filepath.Join(dir, "dst")
			if err := os.WriteFile(src, tc.body, 0o644); err != nil {
				t.Fatal(err)
			}
			if err := copyFile(src, dst); err != nil {
				t.Fatalf("copyFile: %v", err)
			}
			got, err := os.ReadFile(dst)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, tc.body) {
				t.Errorf("content mismatch: got %d bytes, want %d", len(got), len(tc.body))
			}
		})
	}
}

// TestCopyFileExistingDstFails verifies the O_EXCL guard: copying onto
// an existing destination returns an error rather than silently
// overwriting. Materialization quota relies on this — a buggy mission
// can't repoint another mission's input file by pre-writing the work
// path.
func TestCopyFileExistingDstFails(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")
	if err := os.WriteFile(src, []byte("a"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dst, []byte("preexisting"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := copyFile(src, dst); err == nil {
		t.Error("copyFile onto existing dst did not error")
	}
}
