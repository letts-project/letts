package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDiscoverDugdaleExplicitFlag(t *testing.T) {
	dir := t.TempDir()
	want := filepath.Join(dir, "x.yaml")
	os.WriteFile(want, []byte("listen: x\ndata_dir: /tmp\n"), 0o600) //nolint:errcheck
	got, err := DiscoverDugdale(want, "", []string{})
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func TestDiscoverDugdaleEnvVar(t *testing.T) {
	dir := t.TempDir()
	want := filepath.Join(dir, "dugdale.yaml")
	os.WriteFile(want, []byte{}, 0o600) //nolint:errcheck
	got, err := DiscoverDugdale("", want, []string{})
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("got %s", got)
	}
}

func TestDiscoverDugdaleCascade(t *testing.T) {
	dir := t.TempDir()
	cwd := filepath.Join(dir, "cwd-dugdale.yaml")
	system := filepath.Join(dir, "system-dugdale.yaml")
	os.WriteFile(system, []byte{}, 0o600) //nolint:errcheck
	got, err := DiscoverDugdale("", "", []string{cwd, system})
	if err != nil {
		t.Fatal(err)
	}
	if got != system {
		t.Errorf("got %s, want %s (cwd missing → system)", got, system)
	}
}

func TestDiscoverDugdaleNotFound(t *testing.T) {
	_, err := DiscoverDugdale("", "", []string{"/nonexistent/path/dugdale.yaml"})
	if err == nil {
		t.Error("expected ErrNoDugdaleConfig")
	}
}
