package main

import (
	"strings"
	"testing"

	"letts/pkg/lettsconfig"
)

func TestResolveExecTargetsRejectsNone(t *testing.T) {
	cfg := buildExecTestConfig(t)
	_, err := resolveExecTargets(cfg, execTargetFlags{lane: "light"}, nil)
	if err == nil || !strings.Contains(err.Error(), "exactly one") {
		t.Errorf("err=%v, want 'exactly one' badusage", err)
	}
}

func TestResolveExecTargetsRejectsTwoTargets(t *testing.T) {
	cfg := buildExecTestConfig(t)
	_, err := resolveExecTargets(cfg, execTargetFlags{
		lane: "light", host: "s1", all: true,
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "exactly one") {
		t.Errorf("err=%v, want 'exactly one' badusage", err)
	}
}

func TestResolveExecTargetsRejectsAllThree(t *testing.T) {
	cfg := buildExecTestConfig(t)
	_, err := resolveExecTargets(cfg, execTargetFlags{
		lane: "light", host: "s1", match: []string{"prod"}, all: true,
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "exactly one") {
		t.Errorf("err=%v, want 'exactly one' badusage", err)
	}
}

func TestResolveExecTargetsRejectsNoLane(t *testing.T) {
	cfg := buildExecTestConfig(t)
	_, err := resolveExecTargets(cfg, execTargetFlags{all: true}, nil)
	if err == nil || !strings.Contains(err.Error(), "--lane") {
		t.Errorf("err=%v, want --lane badusage", err)
	}
}

func TestResolveExecTargetsHostSingle(t *testing.T) {
	cfg := buildExecTestConfig(t)
	hosts, err := resolveExecTargets(cfg, execTargetFlags{lane: "light", host: "s1"}, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(hosts) != 1 || hosts[0] != "s1" {
		t.Errorf("hosts=%v, want [s1]", hosts)
	}
}

func TestResolveExecTargetsHostList(t *testing.T) {
	cfg := buildExecTestConfig(t)
	hosts, err := resolveExecTargets(cfg, execTargetFlags{lane: "light", host: "s1,s2"}, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(hosts) != 2 {
		t.Errorf("hosts=%v, want 2", hosts)
	}
}

func TestResolveExecTargetsMatchLabels(t *testing.T) {
	cfg := buildExecTestConfig(t)
	hosts, err := resolveExecTargets(cfg, execTargetFlags{lane: "light", match: []string{"prod"}}, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(hosts) == 0 {
		t.Errorf("hosts empty, want at least 1 prod-labeled dugdale from buildExecTestConfig")
	}
}

func TestResolveExecTargetsAll(t *testing.T) {
	cfg := buildExecTestConfig(t)
	hosts, err := resolveExecTargets(cfg, execTargetFlags{lane: "light", all: true}, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(hosts) < 2 {
		t.Errorf("--all returned %d, want >=2 from buildExecTestConfig", len(hosts))
	}
}

func TestResolveExecTargetsMatchEmptyResult(t *testing.T) {
	cfg := buildExecTestConfig(t)
	_, err := resolveExecTargets(cfg, execTargetFlags{
		lane: "light", match: []string{"nonexistent-label"},
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "no dugdales matched") {
		t.Errorf("err=%v, want 'no dugdales matched' badusage", err)
	}
}

// buildExecTestConfig builds a minimal lettsconfig.Config with 3 dugdales:
//
//	s1 [prod, web], s2 [prod, third-party], s3 [dev]
//
// All have a 'light' lane. Field types match the canonical shape used by
// existing tests (apply_test.go): Dugdales is a value slice, LaneCfg (not
// LaneSpec) is the lane map value type.
func buildExecTestConfig(t *testing.T) *lettsconfig.Config {
	t.Helper()
	return &lettsconfig.Config{
		Dugdales: []lettsconfig.Dugdale{
			{ID: "s1", Host: "127.0.0.1", Port: 8001, Labels: []string{"prod", "web"},
				Lanes: map[string]lettsconfig.LaneCfg{"light": {Concurrency: 1}}},
			{ID: "s2", Host: "127.0.0.1", Port: 8002, Labels: []string{"prod", "third-party"},
				Lanes: map[string]lettsconfig.LaneCfg{"light": {Concurrency: 1}}},
			{ID: "s3", Host: "127.0.0.1", Port: 8003, Labels: []string{"dev"},
				Lanes: map[string]lettsconfig.LaneCfg{"light": {Concurrency: 1}}},
		},
	}
}
