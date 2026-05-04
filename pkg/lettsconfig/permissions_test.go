package lettsconfig

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestPermissionsAllEnvTokensSkipsCheck(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("posix permissions only")
	}
	tmp := t.TempDir()
	p := filepath.Join(tmp, "letts.yaml")
	src := `
auth:
  token: "${LETTS_DISP}"
dugdales:
  - id: s1
    host: h
`
	if err := os.WriteFile(p, []byte(src), 0644); err != nil {
		t.Fatal(err)
	}
	c, err := Load([]byte(src))
	if err != nil {
		t.Fatal(err)
	}
	if err := CheckPermissions(p, c); err != nil {
		t.Errorf("env-only tokens: unexpected check failure: %v", err)
	}
}

func TestPermissionsPlainTokenLooseModeRejected(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("posix permissions only")
	}
	tmp := t.TempDir()
	p := filepath.Join(tmp, "letts.yaml")
	src := `
auth:
  token: "plaintext-token"
dugdales:
  - id: s1
    host: h
`
	if err := os.WriteFile(p, []byte(src), 0644); err != nil {
		t.Fatal(err)
	}
	c, _ := Load([]byte(src))
	err := CheckPermissions(p, c)
	var pe *PermissionsError
	if !errors.As(err, &pe) {
		t.Fatalf("expected PermissionsError, got %v", err)
	}
}

func TestPermissionsPlainTokenTightModeOK(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("posix permissions only")
	}
	tmp := t.TempDir()
	p := filepath.Join(tmp, "letts.yaml")
	src := `
auth:
  token: "plaintext-token"
dugdales:
  - id: s1
    host: h
`
	// CheckPermissions accepts both 0600 (rw) and 0400 (read-only).
	for _, mode := range []os.FileMode{0o600, 0o400} {
		t.Run(mode.String(), func(t *testing.T) {
			if err := os.WriteFile(p, []byte(src), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(p, mode); err != nil {
				t.Fatal(err)
			}
			c, _ := Load([]byte(src))
			if err := CheckPermissions(p, c); err != nil {
				t.Errorf("%o with plain token: unexpected check failure: %v", mode, err)
			}
		})
	}
}
