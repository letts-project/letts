package lettsconfig

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDiscoverExplicitFlag(t *testing.T) {
	tmp := t.TempDir()
	p := filepath.Join(tmp, "x.yaml")
	if err := os.WriteFile(p, []byte("dugdales: []\n"), 0644); err != nil {
		t.Fatal(err)
	}
	got, err := Discover(DiscoverOpts{Flag: p, Getenv: envFromMap(nil)})
	if err != nil {
		t.Fatal(err)
	}
	if got != p {
		t.Errorf("got %q want %q", got, p)
	}
}

func TestDiscoverEnv(t *testing.T) {
	tmp := t.TempDir()
	p := filepath.Join(tmp, "via-env.yaml")
	if err := os.WriteFile(p, []byte("dugdales: []\n"), 0644); err != nil {
		t.Fatal(err)
	}
	got, err := Discover(DiscoverOpts{
		Getenv: envFromMap(map[string]string{"LETTS_CONFIG": p}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got != p {
		t.Errorf("got %q want %q", got, p)
	}
}

func TestDiscoverCwd(t *testing.T) {
	tmp := t.TempDir()
	p := filepath.Join(tmp, "letts.yaml")
	if err := os.WriteFile(p, []byte("dugdales: []\n"), 0644); err != nil {
		t.Fatal(err)
	}
	got, err := Discover(DiscoverOpts{
		Cwd:    tmp,
		Getenv: envFromMap(nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got != p {
		t.Errorf("got %q want %q", got, p)
	}
}

func TestDiscoverNotFound(t *testing.T) {
	tmp := t.TempDir()
	_, err := Discover(DiscoverOpts{
		Cwd:              tmp,
		Getenv:           envFromMap(nil),
		HomeDir:          tmp,
		XDGConfigHome:    tmp,
		EtcLettsYamlPath: filepath.Join(tmp, "etc-letts.yaml"),
	})
	if err == nil {
		t.Fatal("expected not-found error")
	}
}
