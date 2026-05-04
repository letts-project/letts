// Package fsutil provides POSIX-correct filesystem helpers for letts:
// fsync(parent_dir), symlink-containment, atomic rename helpers.
package fsutil

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// SyncDir opens dir, fsyncs it, and closes. POSIX requires this after any
// create/rename/unlink to make directory entries durable.
func SyncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open dir %s: %w", dir, err)
	}
	defer func() { _ = f.Close() }()
	st, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat dir %s: %w", dir, err)
	}
	if !st.IsDir() {
		return fmt.Errorf("%s is not a directory", dir)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("fsync dir %s: %w", dir, err)
	}
	return nil
}

// SyncDirChain fsyncs base and every intermediate directory down to base/rel.
// Use after `os.MkdirAll(filepath.Join(base, rel))` to make each newly-created
// directory entry durable: when MkdirAll creates both `<sh1>` and `<sh1>/<sh2>`
// in one call, fsync'ing only `base` makes `<sh1>` durable but leaves the
// `<sh2>` directory entry in `<sh1>` un-synced — a crash between the two
// can lose the inner shard dir even though MkdirAll appeared to succeed.
//
// Stops and returns the first error. Idempotent on already-synced dirs.
func SyncDirChain(base, rel string) error {
	for _, p := range dirChainPaths(base, rel) {
		if err := SyncDir(p); err != nil {
			return err
		}
	}
	return nil
}

// dirChainPaths returns base, base/<first>, base/<first>/<second>, ... down
// to base/rel. Empty/`.` rel returns just base. Split out as a pure helper
// so call-path enumeration is unit-testable without filesystem state.
func dirChainPaths(base, rel string) []string {
	out := []string{base}
	clean := filepath.Clean(rel)
	if clean == "" || clean == "." {
		return out
	}
	cur := base
	for _, part := range strings.Split(clean, string(filepath.Separator)) {
		if part == "" {
			continue
		}
		cur = filepath.Join(cur, part)
		out = append(out, cur)
	}
	return out
}
