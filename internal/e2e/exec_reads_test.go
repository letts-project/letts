package e2e_test

import (
	"encoding/json"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type outputFrame struct {
	T        int64  `json:"t" yaml:"t"`
	Stream   string `json:"stream" yaml:"stream"`
	Data     string `json:"data" yaml:"data"`
	Encoding string `json:"encoding" yaml:"encoding"`
}

// TestExecMissionReads_RealDaemon reads a finished exec with a letts.yaml
// holding dispatch, admin and exec tokens: its combined capture through
// `letts logs` in every --output format, and its record and events through
// the commands that pick the token by mission kind.
func TestExecMissionReads_RealDaemon(t *testing.T) {
	d := startDaemon(t, daemonOpts{ExecEnabled: true, ExecToken: "exec-tok-e2e"})
	d.Apply(t, map[string]any{
		"lanes": map[string]any{"normal": map[string]any{"concurrency": 1}},
	})
	_, env := d.WriteLettsYAML(t, lettsYAMLOpts{DugdaleID: "s7", Lane: "normal"})

	const id = "01900000-0000-7000-8000-00000000a001"
	_, stderr, code := d.RunLetts(t, []string{
		"exec", "--host=s7", "--lane=normal", "--allow-shell", "--mission-id=" + id, "--",
		"sh", "-c", "echo out-line; sleep 0.2; echo 'err -> line' >&2",
	}, env)
	if code != 0 {
		t.Fatalf("exec failed: exit=%d stderr=%s", code, stderr)
	}

	logs := func(args ...string) string {
		t.Helper()
		stdout, stderr, code := d.RunLetts(t, append([]string{"logs", id, "--host=s7"}, args...), env)
		if code != 0 {
			t.Fatalf("logs %v: exit=%d stderr=%s", args, code, stderr)
		}
		return stdout
	}

	if got := logs(); got != "out-line\nerr -> line\n" {
		t.Errorf("text: got %q", got)
	}
	if got := logs("--stream=stderr"); got != "err -> line\n" {
		t.Errorf("text stderr: got %q", got)
	}

	nd := strings.Split(strings.TrimSpace(logs("--output=ndjson")), "\n")
	if len(nd) != 2 {
		t.Fatalf("ndjson: want 2 lines, got %q", nd)
	}
	var first outputFrame
	if err := json.Unmarshal([]byte(nd[0]), &first); err != nil || first.Stream != "stdout" || first.Data != "out-line\n" || first.T == 0 {
		t.Errorf("ndjson[0] = %+v (err %v)", first, err)
	}

	var arr []outputFrame
	if err := json.Unmarshal([]byte(logs("--output=json")), &arr); err != nil {
		t.Fatalf("json: %v", err)
	}
	if len(arr) != 2 || arr[1].Stream != "stderr" || arr[1].Data != "err -> line\n" {
		t.Errorf("json frames = %+v", arr)
	}

	arr = nil
	if err := yaml.Unmarshal([]byte(logs("--output=yaml", "--stream=stdout")), &arr); err != nil {
		t.Fatalf("yaml: %v", err)
	}
	if len(arr) != 1 || arr[0].Stream != "stdout" || arr[0].Data != "out-line\n" || arr[0].T != 0 {
		t.Errorf("yaml stdout frames = %+v", arr)
	}

	for _, args := range [][]string{
		{"ctl", "exec", "output", id, "--host=s7"},
		{"ctl", "exec", "output", id},
	} {
		stdout, stderr, code := d.RunLetts(t, args, env)
		if code != 0 || stdout != "out-line\nerr -> line\n" {
			t.Errorf("%v: exit=%d stdout=%q stderr=%s", args, code, stdout, stderr)
		}
	}

	for _, args := range [][]string{
		{"events", id, "--host=s7"},
		{"events", id, "--host=s7", "--follow"},
	} {
		stdout, stderr, code := d.RunLetts(t, args, env)
		if code != 0 || !strings.Contains(stdout, `"event":"done"`) {
			t.Errorf("%v: exit=%d stdout=%q stderr=%s", args, code, stdout, stderr)
		}
	}

	for _, args := range [][]string{
		{"ctl", "missions", "show", id, "--host=s7"},
		{"ctl", "missions", "show", id},
		{"ctl", "exec", "show", id, "--host=s7"},
		{"--output=yaml", "ctl", "exec", "show", id},
	} {
		stdout, stderr, code := d.RunLetts(t, args, env)
		if code != 0 || !strings.Contains(stdout, id) || !strings.Contains(stdout, "exec") {
			t.Errorf("%v: exit=%d stdout=%q stderr=%s", args, code, stdout, stderr)
		}
	}

	stdout, stderr, code := d.RunLetts(t, []string{"--output=json", "ctl", "lanes", "list", "--host=s7"}, env)
	if code != 0 || !strings.Contains(stdout, `"normal"`) {
		t.Errorf("lanes list: exit=%d stdout=%q stderr=%s", code, stdout, stderr)
	}
}

// TestExecKeepsUnterminatedLastLine_RealDaemon: prefix and ndjson modes must
// print a final output line that has no trailing newline.
func TestExecKeepsUnterminatedLastLine_RealDaemon(t *testing.T) {
	d := startDaemon(t, daemonOpts{ExecEnabled: true, ExecToken: "exec-tok-e2e"})
	d.Apply(t, map[string]any{
		"lanes": map[string]any{"normal": map[string]any{"concurrency": 1}},
	})
	_, env := d.WriteLettsYAML(t, lettsYAMLOpts{DugdaleID: "s7", Lane: "normal"})

	stdout, stderr, code := d.RunLetts(t, []string{"exec", "--host=s7", "--lane=normal", "--output=prefix", "--", "printf", "a\\nno-newline"}, env)
	if code != 0 || stdout != "[s7] a\n[s7] no-newline\n" {
		t.Errorf("prefix: exit=%d stdout=%q stderr=%s", code, stdout, stderr)
	}

	stdout, stderr, code = d.RunLetts(t, []string{"exec", "--host=s7", "--lane=normal", "--output=ndjson", "--", "printf", "no-newline"}, env)
	if code != 0 || !strings.Contains(stdout, `"line":"no-newline"`) {
		t.Errorf("ndjson: exit=%d stdout=%q stderr=%s", code, stdout, stderr)
	}
}
