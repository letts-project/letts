package lettsconfig

import (
	"fmt"
	"os"
	"runtime"
)

// PermissionsError signals that letts.yaml carries plain-text tokens but
// is world-readable.
type PermissionsError struct {
	Path string
	Mode os.FileMode
}

func (e *PermissionsError) Error() string {
	return fmt.Sprintf("letts.yaml %s has unsafe permissions %o (contains plain-text tokens; require 0600 or 0400)", e.Path, e.Mode.Perm())
}

// CheckPermissions enforces 0600/0400 only when c contains a plain-text
// (non-${VAR}) token in Auth or any Dugdale. Returns nil otherwise.
// Windows: no-op (no posix mode).
func CheckPermissions(path string, c *Config) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	if !hasPlainToken(c) {
		return nil
	}
	st, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat %s: %w", path, err)
	}
	perm := st.Mode().Perm()
	if perm != 0o600 && perm != 0o400 {
		return &PermissionsError{Path: path, Mode: st.Mode()}
	}
	return nil
}

func hasPlainToken(c *Config) bool {
	if IsPlainToken(c.Auth.Token) || IsPlainToken(c.Auth.AdminToken) || IsPlainToken(c.Auth.ExecToken) {
		return true
	}
	for _, d := range c.Dugdales {
		if IsPlainToken(d.Token) || IsPlainToken(d.AdminToken) || IsPlainToken(d.ExecToken) {
			return true
		}
	}
	for _, t := range c.Templates {
		if IsPlainToken(t.Token) || IsPlainToken(t.AdminToken) || IsPlainToken(t.ExecToken) {
			return true
		}
	}
	return false
}
