package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCheckConfigPermissionsStrict(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "dugdale.yaml")
	if err := os.WriteFile(p, []byte("listen: 127.0.0.1:7180\ndata_dir: /tmp\nauth:\n  tokens: [t]\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	c := &DugdaleConfig{Auth: AuthConfig{Tokens: []string{"t"}}}
	err := CheckConfigPermissions(p, c, false)
	if err == nil {
		t.Errorf("expected error for 0644 perms when secrets present")
	}
	if err := os.Chmod(p, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := CheckConfigPermissions(p, c, false); err != nil {
		t.Errorf("0600 with secrets should pass: %v", err)
	}
}

func TestCheckConfigPermissionsNoSecretsSkips(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "dugdale.yaml")
	if err := os.WriteFile(p, []byte("listen: 127.0.0.1:7180\ndata_dir: /tmp\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	c := &DugdaleConfig{}
	if err := CheckConfigPermissions(p, c, false); err != nil {
		t.Errorf("no secrets → 0644 should pass: %v", err)
	}
}

func TestCheckConfigPermissionsInsecureFlag(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "dugdale.yaml")
	os.WriteFile(p, []byte("listen: x\nauth:\n  tokens: [t]\n"), 0o644) //nolint:errcheck
	c := &DugdaleConfig{Auth: AuthConfig{Tokens: []string{"t"}}}
	if err := CheckConfigPermissions(p, c, true); err != nil {
		t.Errorf("insecure flag should bypass: %v", err)
	}
}
