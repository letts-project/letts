package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadDugdaleMinimal(t *testing.T) {
	c, err := LoadDugdaleFile("../../testdata/dugdale-minimal.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if c.Listen != "127.0.0.1:7180" {
		t.Errorf("listen = %s", c.Listen)
	}
	if c.DataDir != "/var/lib/letts" {
		t.Errorf("data_dir = %s", c.DataDir)
	}
	// Defaults
	if c.Cleanup.SuccessTTL != 24*time.Hour {
		t.Errorf("cleanup.success_ttl default = %v, want 24h", c.Cleanup.SuccessTTL)
	}
	if c.Limits.MaxOutputBuffer != 16*1024*1024 {
		t.Errorf("limits.max_output_buffer default = %d", c.Limits.MaxOutputBuffer)
	}
	if c.Limits.DefaultKillGrace != 5*time.Second {
		t.Errorf("limits.default_kill_grace default = %v", c.Limits.DefaultKillGrace)
	}
	if c.Network.BehindTLSProxy {
		t.Errorf("network.behind_tls_proxy default should be false")
	}
}

func TestLoadDugdaleFull(t *testing.T) {
	c, err := LoadDugdaleFile("../../testdata/dugdale-full.yaml")
	if err != nil {
		t.Fatal(err)
	}
	// The minimal sample has no exec section; full sample has enabled=false.
	if c.Exec.Enabled {
		t.Errorf("full sample exec.enabled = true, want false")
	}
	if c.Limits.MaxEventLineSize != 1024*1024 {
		t.Errorf("limits.max_event_line_size = %d, want 1MiB", c.Limits.MaxEventLineSize)
	}
}

// TestLoadDugdaleRejectMissionEnvLETTSOverride enforces that the reserved
// LETTS_* namespace cannot be overridden via mission_env.set (validated at
// daemon startup) — and the symmetric
// guarantee for mission_env.inherit. Without load-time validation,
// LETTS_* keys in `set` would only surface at mission-spawn time
// (`spawn_failed`), and LETTS_* entries in `inherit` would be silently
// skipped — no feedback that the config is a hidden bomb.
func TestLoadDugdaleRejectMissionEnvLETTSOverride(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "set contains LETTS_*",
			yaml: `
listen: 127.0.0.1:7180
data_dir: /tmp/x
mission_env:
  set:
    LETTS_SECRET: bad
`,
			want: "LETTS_",
		},
		{
			name: "inherit contains LETTS_*",
			yaml: `
listen: 127.0.0.1:7180
data_dir: /tmp/x
mission_env:
  inherit: [LETTS_TOKEN]
`,
			want: "LETTS_",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadDugdaleBytes([]byte(tc.yaml))
			if err == nil {
				t.Fatalf("expected load error, got nil")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q missing %q", err.Error(), tc.want)
			}
		})
	}
}

// TestLoadDugdaleRejectExecEnabledWithoutAccess enforces: when
// exec is opt-in (enabled=true) but the operator left exec.tokens empty
// AND there is no other route to call it (no admin.tokens), the
// endpoint is mounted but every request 401s. The
// only useful effect of `enabled=true` is to reserve memory for the
// handler — a clear misconfiguration. Refuse to start so operator gets
// loud feedback instead of debugging "why does my exec always 401?".
func TestLoadDugdaleRejectExecEnabledWithoutAccess(t *testing.T) {
	yamlSrc := `
listen: 127.0.0.1:7180
data_dir: /tmp/x
exec:
  enabled: true
  tokens: []
`
	_, err := LoadDugdaleBytes([]byte(yamlSrc))
	if err == nil {
		t.Fatal("expected error for exec.enabled=true with no usable auth path")
	}
	if !strings.Contains(err.Error(), "exec") {
		t.Errorf("error %q should mention exec", err.Error())
	}
}

// TestLoadDugdaleAcceptsExecEnabledWithAdminTokens verifies the opt-out:
// admin tokens give admin scope the right to call /v1/exec/dispatch
// (superset), so a config with exec.enabled=true, exec.tokens=[], and
// admin.tokens=[...] is intentional ("only admins may exec") and must
// load without error.
func TestLoadDugdaleAcceptsExecEnabledWithAdminTokens(t *testing.T) {
	yamlSrc := `
listen: 127.0.0.1:7180
data_dir: /tmp/x
admin:
  tokens: [admin-tok]
exec:
  enabled: true
  tokens: []
`
	if _, err := LoadDugdaleBytes([]byte(yamlSrc)); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestLoadDugdaleRejectMissingRequired(t *testing.T) {
	_, err := LoadDugdaleBytes([]byte("auth:\n  tokens: [t]\n"))
	if err == nil {
		t.Errorf("missing listen should error")
	}
}

// TestLoadDugdaleRejectsNegativeNumericConfigs: parseBytesOr
// / parseDurationOr accepted negative values silently. That fed
// `time.NewTicker(-5m)` (startup panic) for cleanup.sweep_interval and
// turned `max_data_dir_size: -1GiB` into "every dispatch returns 503".
func TestLoadDugdaleRejectsNegativeNumericConfigs(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "negative sweep_interval",
			yaml: `
listen: 127.0.0.1:7180
data_dir: /tmp/x
auth: {tokens: [t]}
cleanup:
  sweep_interval: -5m
`,
			want: "non-negative",
		},
		{
			name: "negative max_data_dir_size",
			yaml: `
listen: 127.0.0.1:7180
data_dir: /tmp/x
auth: {tokens: [t]}
limits:
  max_data_dir_size: -1GiB
`,
			want: "non-negative",
		},
		{
			name: "negative duration in days",
			yaml: `
listen: 127.0.0.1:7180
data_dir: /tmp/x
auth: {tokens: [t]}
cleanup:
  failed_ttl: -2d
`,
			want: "non-negative",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := LoadDugdaleBytes([]byte(c.yaml))
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("err=%v, want substring %q", err, c.want)
			}
		})
	}
}

// TestExecConfigDefaults verifies that ExecConfig fields get their default values
// when the config sets enabled:true but omits the size/duration knobs.
func TestExecConfigDefaults(t *testing.T) {
	yamlSrc := `
listen: 127.0.0.1:7180
data_dir: /tmp/x
auth:
  tokens: [t]
exec:
  enabled: true
  tokens: [e]
`
	c, err := LoadDugdaleBytes([]byte(yamlSrc))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !c.Exec.Enabled {
		t.Errorf("Exec.Enabled = false, want true")
	}
	if c.Exec.AllowShell {
		t.Errorf("Exec.AllowShell default = true, want false")
	}
	if c.Exec.MaxScriptSize != 256*1024 {
		t.Errorf("Exec.MaxScriptSize = %d, want 262144 (256KiB)", c.Exec.MaxScriptSize)
	}
	if c.Exec.MaxInputsPerExec != 32 {
		t.Errorf("Exec.MaxInputsPerExec = %d, want 32", c.Exec.MaxInputsPerExec)
	}
	if c.Exec.MaxOutputsPerExec != 32 {
		t.Errorf("Exec.MaxOutputsPerExec = %d, want 32", c.Exec.MaxOutputsPerExec)
	}
	if c.Exec.ExecSuccessTTL != time.Hour {
		t.Errorf("Exec.ExecSuccessTTL = %v, want 1h", c.Exec.ExecSuccessTTL)
	}
	if c.Exec.ExecFailedTTL != 24*time.Hour {
		t.Errorf("Exec.ExecFailedTTL = %v, want 24h", c.Exec.ExecFailedTTL)
	}
}

// TestExplicitZeroIntCapsPreserved: applyDefaults used to
// clobber explicit YAML `0` with the default for max_progress_rate,
// max_output_files_per_mission, max_inputs_per_exec, max_outputs_per_exec.
// Every consumer of these gates on `> 0`, so 0 is the spelling of "no
// cap" — silently rewriting it changes behaviour.
func TestExplicitZeroIntCapsPreserved(t *testing.T) {
	yamlSrc := `
listen: 127.0.0.1:7180
data_dir: /tmp/x
auth:
  tokens: [t]
limits:
  max_progress_rate: 0
  max_output_files_per_mission: 0
exec:
  enabled: true
  tokens: [e]
  max_inputs_per_exec: 0
  max_outputs_per_exec: 0
`
	c, err := LoadDugdaleBytes([]byte(yamlSrc))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if c.Limits.MaxProgressRate != 0 {
		t.Errorf("MaxProgressRate=%d want 0 (explicit no-cap)", c.Limits.MaxProgressRate)
	}
	if c.Limits.MaxOutputFilesPerMsn != 0 {
		t.Errorf("MaxOutputFilesPerMsn=%d want 0", c.Limits.MaxOutputFilesPerMsn)
	}
	if c.Exec.MaxInputsPerExec != 0 {
		t.Errorf("Exec.MaxInputsPerExec=%d want 0", c.Exec.MaxInputsPerExec)
	}
	if c.Exec.MaxOutputsPerExec != 0 {
		t.Errorf("Exec.MaxOutputsPerExec=%d want 0", c.Exec.MaxOutputsPerExec)
	}
}

// TestOmittedIntCapsFallBackToDefault: when a field is absent from the
// YAML, applyDefaults supplies the standard default. Counterpart to
// TestExplicitZeroIntCapsPreserved — both must hold after the
// pointer-sentinel change.
func TestOmittedIntCapsFallBackToDefault(t *testing.T) {
	yamlSrc := `
listen: 127.0.0.1:7180
data_dir: /tmp/x
auth:
  tokens: [t]
exec:
  enabled: true
  tokens: [e]
`
	c, err := LoadDugdaleBytes([]byte(yamlSrc))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if c.Limits.MaxProgressRate != 50 {
		t.Errorf("MaxProgressRate=%d want 50 (default)", c.Limits.MaxProgressRate)
	}
	if c.Limits.MaxOutputFilesPerMsn != 32 {
		t.Errorf("MaxOutputFilesPerMsn=%d want 32 (default)", c.Limits.MaxOutputFilesPerMsn)
	}
	if c.Exec.MaxInputsPerExec != 32 {
		t.Errorf("Exec.MaxInputsPerExec=%d want 32 (default)", c.Exec.MaxInputsPerExec)
	}
	if c.Exec.MaxOutputsPerExec != 32 {
		t.Errorf("Exec.MaxOutputsPerExec=%d want 32 (default)", c.Exec.MaxOutputsPerExec)
	}
}

// TestTLSGuardRejectsExecTokensOnNonLoopback verifies: dugdale must
// refuse to start when exec.enabled=true, exec.tokens is non-empty, listen
// is non-loopback, no behind_tls_proxy assertion, and no insecure_plain_http
// escape.
func TestTLSGuardRejectsExecTokensOnNonLoopback(t *testing.T) {
	yamlSrc := `
listen: 0.0.0.0:7180
data_dir: /tmp/x
exec:
  enabled: true
  tokens: [e]
network:
  behind_tls_proxy: false
`
	_, err := LoadDugdaleBytes([]byte(yamlSrc))
	if err == nil {
		t.Fatal("expected error: exec tokens on non-loopback without TLS proxy must refuse")
	}
	if !strings.Contains(err.Error(), "exec") {
		t.Errorf("error should mention exec: %q", err.Error())
	}
}

// TestTLSGuardAcceptsExecTokensWithProxy verifies the guard passes when the
// operator asserts behind_tls_proxy=true.
func TestTLSGuardAcceptsExecTokensWithProxy(t *testing.T) {
	yamlSrc := `
listen: 0.0.0.0:7180
data_dir: /tmp/x
exec:
  enabled: true
  tokens: [e]
network:
  behind_tls_proxy: true
`
	if _, err := LoadDugdaleBytes([]byte(yamlSrc)); err != nil {
		t.Fatalf("behind_tls_proxy=true should allow exec tokens on non-loopback; got %v", err)
	}
}

// TestTLSGuardAcceptsLoopbackListen verifies that loopback listen doesn't
// trip the guard regardless of TLS proxy assertion.
func TestTLSGuardAcceptsLoopbackListen(t *testing.T) {
	yamlSrc := `
listen: 127.0.0.1:7180
data_dir: /tmp/x
auth:
  tokens: [t]
admin:
  tokens: [a]
exec:
  enabled: true
  tokens: [e]
`
	if _, err := LoadDugdaleBytes([]byte(yamlSrc)); err != nil {
		t.Fatalf("loopback listen should always pass guard; got %v", err)
	}
}

// TestTLSGuardAcceptsInsecurePlainHTTP verifies the dev/lab escape hatch
// allows startup even on non-loopback without TLS proxy.
func TestTLSGuardAcceptsInsecurePlainHTTP(t *testing.T) {
	yamlSrc := `
listen: 0.0.0.0:7180
data_dir: /tmp/x
exec:
  enabled: true
  tokens: [e]
network:
  insecure_plain_http: true
`
	if _, err := LoadDugdaleBytes([]byte(yamlSrc)); err != nil {
		t.Fatalf("insecure_plain_http=true should override guard; got %v", err)
	}
}

// TestTLSGuardRejectsAuthTokensOnNonLoopback ensures the pre-existing auth-
// token rule trips too (regression net for the broader invariant).
func TestTLSGuardRejectsAuthTokensOnNonLoopback(t *testing.T) {
	yamlSrc := `
listen: 0.0.0.0:7180
data_dir: /tmp/x
auth:
  tokens: [t]
`
	_, err := LoadDugdaleBytes([]byte(yamlSrc))
	if err == nil {
		t.Fatal("expected error: auth tokens on non-loopback without TLS proxy must refuse")
	}
}

// TestExecDisabledIgnoresTokensInGuard verifies that exec.enabled=false short-
// circuits the exec branch of the guard even if exec.tokens is non-empty
// (operator leaving stale list around with feature off).
func TestExecDisabledIgnoresTokensInGuard(t *testing.T) {
	yamlSrc := `
listen: 0.0.0.0:7180
data_dir: /tmp/x
exec:
  enabled: false
  tokens: [e]
network:
  behind_tls_proxy: true
`
	if _, err := LoadDugdaleBytes([]byte(yamlSrc)); err != nil {
		t.Fatalf("exec.enabled=false should not trip guard on its own; got %v", err)
	}
}
