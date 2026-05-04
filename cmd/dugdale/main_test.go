package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"letts/internal/version"
)

func TestRunVersion(t *testing.T) {
	origVersion, origCommit, origBuiltAt := version.Version, version.Commit, version.BuiltAt
	t.Cleanup(func() {
		version.Version, version.Commit, version.BuiltAt = origVersion, origCommit, origBuiltAt
	})
	version.Version = "1.2.3"
	version.Commit = "abc"
	version.BuiltAt = "2026-05-04"

	var stdout bytes.Buffer
	code := run([]string{"dugdale", "--version"}, &stdout)
	if code != 0 {
		t.Fatalf("--version exit=%d, want 0", code)
	}
	got := stdout.String()
	for _, want := range []string{"1.2.3", "abc", "2026-05-04"} {
		if !strings.Contains(got, want) {
			t.Errorf("--version output missing %q: %s", want, got)
		}
	}
}

func TestRunHelp(t *testing.T) {
	var stdout bytes.Buffer
	code := run([]string{"dugdale", "--help"}, &stdout)
	if code != 0 {
		t.Fatalf("--help exit=%d, want 0", code)
	}
	if !strings.Contains(stdout.String(), "Usage") {
		t.Errorf("--help missing Usage line")
	}
}

// minimalCfg writes a valid dugdale.yaml under a tmp dir and returns the
// config path and the data_dir it points at. permissionsCheck is bypassed via
// --insecure-config-permissions so the test doesn't depend on UID/GID.
func minimalCfg(t *testing.T) (cfgPath, dataDir string) {
	t.Helper()
	dir := t.TempDir()
	dataDir = filepath.Join(dir, "data")
	cfgPath = filepath.Join(dir, "dugdale.yaml")
	body := "listen: 127.0.0.1:0\n" +
		"data_dir: " + dataDir + "\n" +
		"auth:\n  tokens:\n    - disp-token\n" +
		"admin:\n  tokens:\n    - admin-token\n"
	if err := os.WriteFile(cfgPath, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return
}

func TestRunCheckConfigOK(t *testing.T) {
	cfgPath, _ := minimalCfg(t)
	var stdout bytes.Buffer
	code := run([]string{"dugdale", "--config", cfgPath, "--insecure-config-permissions", "--check-config"}, &stdout)
	if code != 0 {
		t.Fatalf("--check-config exit=%d body=%s", code, stdout.String())
	}
	if !strings.Contains(stdout.String(), "ok") {
		t.Errorf("--check-config missing 'ok': %q", stdout.String())
	}
}

func TestRunCheckConfigBadFile(t *testing.T) {
	dir := t.TempDir()
	bad := filepath.Join(dir, "broken.yaml")
	_ = os.WriteFile(bad, []byte("listen: ::: not yaml"), 0o644)
	var stdout bytes.Buffer
	code := run([]string{"dugdale", "--config", bad, "--insecure-config-permissions", "--check-config"}, &stdout)
	if code != 3 {
		t.Errorf("--check-config bad exit=%d, want 3 (body=%s)", code, stdout.String())
	}
}

func TestRunMigrateOnlyAppliesAndExits(t *testing.T) {
	cfgPath, dataDir := minimalCfg(t)
	var stdout bytes.Buffer
	code := run([]string{"dugdale", "--config", cfgPath, "--insecure-config-permissions", "--migrate-only"}, &stdout)
	if code != 0 {
		t.Fatalf("--migrate-only exit=%d body=%s", code, stdout.String())
	}
	if !strings.Contains(stdout.String(), "migrations applied") {
		t.Errorf("--migrate-only missing 'migrations applied': %q", stdout.String())
	}
	if _, err := os.Stat(filepath.Join(dataDir, "state.db")); err != nil {
		t.Errorf("state.db not created: %v", err)
	}
}

func TestRunInvalidFlagReturns2(t *testing.T) {
	var stdout bytes.Buffer
	code := run([]string{"dugdale", "--no-such-flag"}, &stdout)
	if code != 2 {
		t.Errorf("invalid flag exit=%d, want 2", code)
	}
}

func TestRunMissingConfigReturns3(t *testing.T) {
	var stdout bytes.Buffer
	code := run([]string{"dugdale", "--config", "/no/such/path/dugdale.yaml", "--check-config"}, &stdout)
	if code != 3 {
		t.Errorf("missing config exit=%d, want 3", code)
	}
}

// TestMainSetsBuildInfoMetric verifies the letts_dugdale_info gauge gets
// populated with non-empty version+commit labels via publishBuildInfo. This
// is the one-shot startup wiring.
func TestMainSetsBuildInfoMetric(t *testing.T) {
	origVersion, origCommit := version.Version, version.Commit
	t.Cleanup(func() {
		version.Version, version.Commit = origVersion, origCommit
	})
	version.Version = "9.9.9-test"
	version.Commit = "testcommit"

	publishBuildInfo()

	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "letts_dugdale_info" {
			continue
		}
		for _, m := range mf.GetMetric() {
			lbl := map[string]string{}
			for _, p := range m.GetLabel() {
				lbl[p.GetName()] = p.GetValue()
			}
			if lbl["version"] == "9.9.9-test" && lbl["commit"] == "testcommit" {
				return
			}
		}
	}
	t.Fatalf("expected letts_dugdale_info{version=9.9.9-test,commit=testcommit} populated")
}
