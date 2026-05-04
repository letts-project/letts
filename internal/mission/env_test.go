package mission

import (
	"fmt"
	"strings"
	"testing"

	"letts/internal/config"
)

// envSliceToMap converts a KEY=VALUE slice to a map for easy lookup in tests.
func envSliceToMap(env []string) map[string]string {
	m := make(map[string]string, len(env))
	for _, e := range env {
		idx := strings.IndexByte(e, '=')
		if idx < 0 {
			m[e] = ""
		} else {
			m[e[:idx]] = e[idx+1:]
		}
	}
	return m
}

func TestBuildEnvBase(t *testing.T) {
	env, err := BuildEnv("/home/dugdale", config.MissionEnvConfig{}, nil, BaseVars{
		MissionID: "mission-123",
		Kind:      "mission",
		Lane:      "normal",
		Workdir:   "/data/work/mission-123",
	}, func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatalf("BuildEnv error: %v", err)
	}

	m := envSliceToMap(env)

	checks := map[string]string{
		"PATH":             "/usr/local/bin:/usr/bin:/bin",
		"HOME":             "/home/dugdale",
		"TZ":               "UTC",
		"LETTS_MISSION_ID": "mission-123",
		"LETTS_KIND":       "mission",
		"LETTS_LANE":       "normal",
		"LETTS_WORKDIR":    "/data/work/mission-123",
		"LETTS_TMPDIR":     "/data/work/mission-123/tmp",
	}
	for k, want := range checks {
		if got, ok := m[k]; !ok {
			t.Errorf("missing %s", k)
		} else if got != want {
			t.Errorf("%s: got %q, want %q", k, got, want)
		}
	}
}

func TestBuildEnvInherit(t *testing.T) {
	lookup := func(name string) (string, bool) {
		switch name {
		case "LANG":
			return "en_US.UTF-8", true
		case "TERM":
			return "xterm", true
		default:
			return "", false
		}
	}

	cfg := config.MissionEnvConfig{
		Inherit: []string{"LANG"}, // only LANG in whitelist
	}

	env, err := BuildEnv("/home/dugdale", cfg, nil, BaseVars{
		MissionID: "m1",
		Kind:      "mission",
		Lane:      "normal",
		Workdir:   "/data/work/m1",
	}, lookup)
	if err != nil {
		t.Fatal(err)
	}

	m := envSliceToMap(env)
	if _, ok := m["LANG"]; !ok {
		t.Error("LANG should be inherited")
	}
	if m["LANG"] != "en_US.UTF-8" {
		t.Errorf("LANG: got %q", m["LANG"])
	}
	if _, ok := m["TERM"]; ok {
		t.Error("TERM should not be inherited (not in whitelist)")
	}
}

func TestBuildEnvInheritSkipsLETTS(t *testing.T) {
	lookup := func(name string) (string, bool) {
		return "some_value", true
	}

	cfg := config.MissionEnvConfig{
		Inherit: []string{"LETTS_SOMETHING", "LANG"},
	}

	env, err := BuildEnv("/home/dugdale", cfg, nil, BaseVars{
		MissionID: "m2",
		Kind:      "mission",
		Lane:      "normal",
		Workdir:   "/data/work/m2",
	}, lookup)
	if err != nil {
		t.Fatal(err)
	}

	m := envSliceToMap(env)
	// LETTS_SOMETHING in inherit should be silently skipped.
	// But the LETTS_* vars we set ourselves will still be there.
	// Check that the user-supplied LETTS_SOMETHING didn't get in via Inherit.
	// We can do this by counting LETTS_SOMETHING entries from the lookup.
	// The base env sets LETTS_MISSION_ID etc. but not LETTS_SOMETHING.
	if val, ok := m["LETTS_SOMETHING"]; ok && val == "some_value" {
		t.Error("LETTS_SOMETHING from inherit should have been skipped")
	}
	if _, ok := m["LANG"]; !ok {
		t.Error("LANG should be inherited")
	}
}

func TestBuildEnvSet(t *testing.T) {
	lookup := func(name string) (string, bool) {
		if name == "MY_TOKEN" {
			return "tok123", true
		}
		return "", false
	}

	cfg := config.MissionEnvConfig{
		Set: map[string]string{
			"MYAPP_ENV":  "production",
			"MYAPP_AUTH": "${MY_TOKEN}",
		},
	}

	env, err := BuildEnv("/home/dugdale", cfg, nil, BaseVars{
		MissionID: "m3",
		Kind:      "mission",
		Lane:      "normal",
		Workdir:   "/data/work/m3",
	}, lookup)
	if err != nil {
		t.Fatalf("BuildEnv error: %v", err)
	}

	m := envSliceToMap(env)
	if m["MYAPP_ENV"] != "production" {
		t.Errorf("MYAPP_ENV: got %q", m["MYAPP_ENV"])
	}
	if m["MYAPP_AUTH"] != "tok123" {
		t.Errorf("MYAPP_AUTH: got %q (should be expanded)", m["MYAPP_AUTH"])
	}
}

func TestBuildEnvSetLETTSRejected(t *testing.T) {
	cfg := config.MissionEnvConfig{
		Set: map[string]string{
			"LETTS_FOO": "override",
		},
	}

	_, err := BuildEnv("/home/dugdale", cfg, nil, BaseVars{
		MissionID: "m4",
		Kind:      "mission",
		Lane:      "normal",
		Workdir:   "/data/work/m4",
	}, func(string) (string, bool) { return "", false })

	if err == nil {
		t.Fatal("expected error for LETTS_FOO in set, got nil")
	}
	if !strings.Contains(err.Error(), "LETTS_FOO") {
		t.Errorf("error should mention LETTS_FOO: %v", err)
	}
}

func TestBuildEnvInputs(t *testing.T) {
	inputs := []EnvInputs{
		{
			Role:   "input_data",
			Path:   "/data/work/m5/in/input_data",
			Sha256: "deadbeef",
			Size:   1024,
		},
	}

	env, err := BuildEnv("/home/dugdale", config.MissionEnvConfig{}, inputs, BaseVars{
		MissionID: "m5",
		Kind:      "mission",
		Lane:      "normal",
		Workdir:   "/data/work/m5",
	}, func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatal(err)
	}

	m := envSliceToMap(env)
	if m["LETTS_IN_input_data"] != "/data/work/m5/in/input_data" {
		t.Errorf("LETTS_IN_input_data: %q", m["LETTS_IN_input_data"])
	}
	if m["LETTS_IN_input_data__SHA256"] != "deadbeef" {
		t.Errorf("LETTS_IN_input_data__SHA256: %q", m["LETTS_IN_input_data__SHA256"])
	}
	if m["LETTS_IN_input_data__SIZE"] != fmt.Sprintf("%d", 1024) {
		t.Errorf("LETTS_IN_input_data__SIZE: %q", m["LETTS_IN_input_data__SIZE"])
	}
}
