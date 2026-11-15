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

func TestLoadDugdaleRejectsUnknownKeys(t *testing.T) {
	for name, extra := range map[string]string{
		"top level": "bogus: 1\n",
		"nested":    "limits:\n  max_output_bufer: 1MiB\n",
	} {
		yamlSrc := "listen: 127.0.0.1:7180\ndata_dir: /tmp/x\n" + extra
		if _, err := LoadDugdaleBytes([]byte(yamlSrc)); err == nil {
			t.Errorf("%s: expected an error for an unknown key", name)
		}
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

// TestExplicitZeroIntCapsPreserved: applyDefaults used to
// clobber explicit YAML `0` with the default for max_progress_rate and
// max_output_files_per_mission.
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
admin:
  tokens: [a]
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
