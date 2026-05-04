package e2e_test

import (
	"strings"
	"testing"
)

// TestExecResolvesAliasFromLettsYAML drives `letts exec --host=<alias>`
// against a real dugdale where the alias resolves to the dugdale's actual
// id. resolveExecTargets must return the resolved id (not the user's
// alias string verbatim) and thread getenv, or any ${VAR} in the alias
// chain fails. Same flag works for `letts run`.
func TestExecResolvesAliasFromLettsYAML(t *testing.T) {
	d := startDaemon(t, daemonOpts{
		ExecEnabled: true,
		ExecToken:   "exec-tok-e2e",
	})
	d.Apply(t, map[string]any{
		"lanes": map[string]any{
			"normal": map[string]any{"concurrency": 1},
		},
	})

	_, env := d.WriteLettsYAML(t, lettsYAMLOpts{
		DugdaleID: "s7",
		Lane:      "normal",
		Aliases:   map[string]string{"local": "s7"},
	})

	stdout, stderr, code := d.RunLetts(t,
		[]string{"exec", "--host=local", "--lane=normal", "--", "echo", "hello-from-alias"},
		env)

	if code != 0 {
		t.Fatalf("exec via alias failed: exit=%d\nstdout=%s\nstderr=%s", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "hello-from-alias") {
		t.Errorf("stdout missing expected text: %q", stdout)
	}
}

// TestExecResolvesAliasWithEnvSubstitution covers the second half:
// aliases whose value comes from ${VAR}. The previous nil-getenv
// argument made every env-driven alias error at validate-time.
func TestExecResolvesAliasWithEnvSubstitution(t *testing.T) {
	d := startDaemon(t, daemonOpts{
		ExecEnabled: true,
		ExecToken:   "exec-tok-e2e",
	})
	d.Apply(t, map[string]any{
		"lanes": map[string]any{
			"normal": map[string]any{"concurrency": 1},
		},
	})

	_, env := d.WriteLettsYAML(t, lettsYAMLOpts{
		DugdaleID: "s7",
		Lane:      "normal",
		Aliases:   map[string]string{"local": "${LETTS_LOCAL_DUGDALE}"},
		ExtraEnv:  map[string]string{"LETTS_LOCAL_DUGDALE": "s7"},
	})

	stdout, stderr, code := d.RunLetts(t,
		[]string{"exec", "--host=local", "--lane=normal", "--", "echo", "env-alias-ok"},
		env)

	if code != 0 {
		t.Fatalf("exec via env-alias failed: exit=%d\nstdout=%s\nstderr=%s", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "env-alias-ok") {
		t.Errorf("stdout missing expected text: %q", stdout)
	}
}
