package fsutil

import (
	"fmt"
	"path/filepath"
	"strings"
)

// ContainedPath verifies that EvalSymlinks(child) remains inside
// EvalSymlinks(parent), returning the canonical resolved child path.
// Used for mission_path containment and any other path-traversal
// defense. Returns error if either eval fails or child escapes.
//
// Note: this is defense-in-depth, not a security boundary against adversarial
// missions running under the same UID. It catches accidental
// path traversal and symlinks pointing outside mission_dir.
func ContainedPath(parent, child string) (string, error) {
	rp, err := filepath.EvalSymlinks(parent)
	if err != nil {
		return "", fmt.Errorf("eval parent %s: %w", parent, err)
	}
	rc, err := filepath.EvalSymlinks(child)
	if err != nil {
		return "", fmt.Errorf("eval child %s: %w", child, err)
	}
	rp = filepath.Clean(rp)
	rc = filepath.Clean(rc)
	if rc != rp && !strings.HasPrefix(rc, rp+string(filepath.Separator)) {
		return "", fmt.Errorf("path %s escapes parent %s (resolved=%s)", child, parent, rc)
	}
	return rc, nil
}
