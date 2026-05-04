// Package config parses dugdale.yaml and letts.yaml configuration files.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// DugdaleConfig matches dugdale.yaml schema.
//
// DiskUsage is a runtime callback wired by cmd/dugdale once the disk-usage
// monitor goroutine is running. Anything that needs to gate work on the
// max_data_dir_size soft cap consults this — the dispatch/exec/staging
// handlers and output collection during mission Finalize.
// Tests can leave it nil; production wiring sets it to the monitor's
// cached Size().
type DugdaleConfig struct {
	Listen     string           `yaml:"listen"`
	DataDir    string           `yaml:"data_dir"`
	Network    NetworkConfig    `yaml:"network"`
	Auth       AuthConfig       `yaml:"auth"`
	Admin      AdminConfig      `yaml:"admin"`
	Log        LogConfig        `yaml:"log"`
	Cleanup    CleanupConfig    `yaml:"cleanup"`
	Limits     LimitsConfig     `yaml:"limits"`
	MissionEnv MissionEnvConfig `yaml:"mission_env"`
	Exec       ExecConfig       `yaml:"exec"`

	DiskUsage func() int64 `yaml:"-"`
}

// NetworkConfig holds network-layer options.
type NetworkConfig struct {
	BehindTLSProxy    bool     `yaml:"behind_tls_proxy"`
	InsecurePlainHTTP bool     `yaml:"insecure_plain_http"`
	TrustedProxies    []string `yaml:"trusted_proxies"`
	UseXForwardedFor  bool     `yaml:"use_x_forwarded_for"`
}

// AuthConfig holds dispatcher authentication tokens.
type AuthConfig struct {
	Tokens []string `yaml:"tokens"`
}

// AdminConfig holds admin tokens.
type AdminConfig struct {
	Tokens []string `yaml:"tokens"`
}

// LogConfig holds logging settings.
type LogConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
	Output string `yaml:"output"`
}

// CleanupConfig holds TTL and sweep settings.
// Durations are parsed from Raw string fields via applyDefaults.
type CleanupConfig struct {
	SuccessTTL       time.Duration `yaml:"-"`
	FailedTTL        time.Duration `yaml:"-"`
	StagingTTL       time.Duration `yaml:"-"`
	DownloadedGrace  time.Duration `yaml:"-"`
	LostCleanupGrace time.Duration `yaml:"-"`
	SweepInterval    time.Duration `yaml:"-"`

	// Raw YAML strings for parsing.
	SuccessTTLRaw       string `yaml:"success_ttl"`
	FailedTTLRaw        string `yaml:"failed_ttl"`
	StagingTTLRaw       string `yaml:"staging_ttl"`
	DownloadedGraceRaw  string `yaml:"downloaded_grace"`
	LostCleanupGraceRaw string `yaml:"lost_cleanup_grace"`
	SweepIntervalRaw    string `yaml:"sweep_interval"`
}

// LimitsConfig holds all resource-limit settings.
// Size fields are parsed from Raw string fields via applyDefaults.
type LimitsConfig struct {
	MaxOutputBuffer     int64 `yaml:"-"`
	MaxEventsBuffer     int64 `yaml:"-"`
	MaxEventLineSize    int64 `yaml:"-"`
	MaxReturnValueSize  int64 `yaml:"-"`
	MaxFailMessageSize  int64 `yaml:"-"`
	MaxFailDetailsSize  int64 `yaml:"-"`
	MaxDispatchBodySize int64 `yaml:"-"`
	MaxMissionInputSize int64 `yaml:"-"`
	MaxExecBodySize     int64 `yaml:"-"`
	MaxApplyBodySize    int64 `yaml:"-"`
	// MaxProgressRate / MaxOutputFilesPerMsn use *int raw fields below
	// so an explicit `0` in YAML survives applyDefaults as
	// "no cap" rather than being clobbered by the default.
	MaxProgressRate      int           `yaml:"-"`
	ProgressBufferSize   int64         `yaml:"-"`
	MaxOutputFilesPerMsn int           `yaml:"-"`
	MaxOutputFileSize    int64         `yaml:"-"`
	MaxStagingUploadSize int64         `yaml:"-"`
	UploadIdleTimeout    time.Duration `yaml:"-"`
	MaxIncompleteUploads int           `yaml:"-"`
	MaxIncompleteBytes   int64         `yaml:"-"`
	MaxQueuePerLane      int           `yaml:"max_queue_per_lane"`
	MaxQueueTotal        int           `yaml:"max_queue_total"`
	MaxDataDirSize       int64         `yaml:"-"`
	DefaultKillGrace     time.Duration `yaml:"-"`
	ReaderPostExitGrace  time.Duration `yaml:"-"`
	CacheSize            int           `yaml:"cache_size"`

	// Raw fields for sizes/durations parsed from human strings or numbers.
	MaxOutputBufferRaw    string `yaml:"max_output_buffer"`
	MaxEventsBufferRaw    string `yaml:"max_events_buffer"`
	MaxEventLineSizeRaw   string `yaml:"max_event_line_size"`
	MaxReturnValueSizeRaw string `yaml:"max_return_value_size"`
	MaxFailMessageSizeRaw string `yaml:"max_fail_message_size"`
	MaxFailDetailsSizeRaw string `yaml:"max_fail_details_size"`
	MaxDispatchBodyRaw    string `yaml:"max_dispatch_body_size"`
	MaxMissionInputRaw    string `yaml:"max_mission_input_size"`
	MaxExecBodyRaw        string `yaml:"max_exec_body_size"`
	MaxApplyBodyRaw       string `yaml:"max_apply_body_size"`
	ProgressBufferRaw     string `yaml:"progress_buffer_size"`
	MaxOutputFileSizeRaw  string `yaml:"max_output_file_size"`
	MaxStagingUploadRaw   string `yaml:"max_staging_upload_size"`
	UploadIdleTimeoutRaw  string `yaml:"upload_idle_timeout"`
	MaxIncompleteBytesRaw string `yaml:"max_incomplete_staging_bytes"`
	// MaxIncompleteUploadsRaw uses *int so we can distinguish "field absent"
	// (→ default 128) from "explicit 0" (→ unlimited).
	MaxIncompleteUploadsRaw *int `yaml:"max_incomplete_staging_uploads"`
	// Same *int sentinel for the cap fields below — explicit
	// 0 means "no cap" (consumers all gate on `> 0`), but a plain int
	// field can't tell that apart from "field missing", so applyDefaults
	// used to clobber operator-supplied zeros with the default.
	MaxProgressRateRaw      *int   `yaml:"max_progress_rate"`
	MaxOutputFilesPerMsnRaw *int   `yaml:"max_output_files_per_mission"`
	MaxDataDirRaw           string `yaml:"max_data_dir_size"`
	DefaultKillGraceRaw     string `yaml:"default_kill_grace"`
	ReaderPostExitGraceRaw  string `yaml:"reader_post_exit_grace"`
}

// MissionEnvConfig controls environment variable inheritance for missions.
type MissionEnvConfig struct {
	Inherit []string          `yaml:"inherit"`
	Set     map[string]string `yaml:"set"`
}

// ExecConfig holds exec feature settings (opt-in).
type ExecConfig struct {
	Enabled    bool     `yaml:"enabled"`
	Tokens     []string `yaml:"tokens"`
	AllowShell bool     `yaml:"allow_shell"`
	// MaxInputsPerExec / MaxOutputsPerExec use *int raw fields
	// so an explicit YAML `0` survives applyDefaults as "no cap".
	MaxScriptSize     int64         `yaml:"-"`
	MaxInputsPerExec  int           `yaml:"-"`
	MaxOutputsPerExec int           `yaml:"-"`
	ExecSuccessTTL    time.Duration `yaml:"-"`
	ExecFailedTTL     time.Duration `yaml:"-"`

	MaxScriptSizeRaw     string `yaml:"max_script_size"`
	MaxInputsPerExecRaw  *int   `yaml:"max_inputs_per_exec"`
	MaxOutputsPerExecRaw *int   `yaml:"max_outputs_per_exec"`
	ExecSuccessTTLRaw    string `yaml:"exec_success_ttl"`
	ExecFailedTTLRaw     string `yaml:"exec_failed_ttl"`
}

// LoadDugdaleFile reads and parses dugdale.yaml from a file path.
func LoadDugdaleFile(path string) (*DugdaleConfig, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read dugdale config %s: %w", path, err)
	}
	return LoadDugdaleBytes(raw)
}

// LoadDugdaleBytes parses dugdale.yaml from bytes; primarily for tests.
func LoadDugdaleBytes(raw []byte) (*DugdaleConfig, error) {
	var c DugdaleConfig
	// Strict mode: unknown fields are an error. Mirrors lettsconfig.Load —
	// without strict parsing a typo in dugdale.yaml (e.g. `max_output_bufer`)
	// would silently use the default value.
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("parse dugdale config: %w", err)
	}
	if err := applyDefaults(&c); err != nil {
		return nil, err
	}
	if err := validateDugdale(&c); err != nil {
		return nil, err
	}
	return &c, nil
}

func validateDugdale(c *DugdaleConfig) error {
	if c.Listen == "" {
		return errors.New("listen is required")
	}
	if c.DataDir == "" {
		return errors.New("data_dir is required")
	}
	if err := validateTLSGuard(c); err != nil {
		return err
	}
	if err := validateMissionEnv(&c.MissionEnv); err != nil {
		return err
	}
	if err := validateExecAuthReachable(c); err != nil {
		return err
	}
	if err := validateLogConfig(c); err != nil {
		return err
	}
	if err := validateEventLineHeadroom(c); err != nil {
		return err
	}
	return nil
}

// validateExecAuthReachable enforces: when exec is opt-in
// (Exec.Enabled=true) the operator must provide some way to authenticate
// against the endpoint. With no exec.tokens AND no admin.tokens, every request
// hits the auth middleware and 401s — the only effect of `enabled: true` is to
// reserve memory for handlers. Refuse to start so the misconfiguration is loud.
//
// We accept (a) non-empty exec.tokens, or (b) non-empty admin.tokens (admin
// scope supersets exec via AuthEither).
func validateExecAuthReachable(c *DugdaleConfig) error {
	if !c.Exec.Enabled {
		return nil
	}
	if len(c.Exec.Tokens) > 0 {
		return nil
	}
	if len(c.Admin.Tokens) > 0 {
		return nil
	}
	return errors.New("refuse to start: exec.enabled=true but no usable auth path " +
		"(set at least one of: exec.tokens, admin.tokens)")
}

// validateLogConfig rejects invalid log.level / log.format enums at config
// load so `dugdale --check-config` — which returns before the
// logger is constructed — catches them, instead of the daemon failing only at
// real startup. applyDefaults fills empty values; this rejects bad non-empty.
func validateLogConfig(c *DugdaleConfig) error {
	switch c.Log.Level {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("invalid log.level %q (want debug|info|warn|error)", c.Log.Level)
	}
	switch c.Log.Format {
	case "json", "text":
	default:
		return fmt.Errorf("invalid log.format %q (want json|text)", c.Log.Format)
	}
	return nil
}

// validateEventLineHeadroom enforces: the
// per-field caps must leave room for the done-event envelope under
// max_event_line_size, otherwise a finalize could build a terminal `done`
// event that exceeds the per-line limit, Append rejects it, and the mission
// sticks forever in mission_finalize_intents (and /readyz stays 200). A
// generous fixed envelope budget covers seq/event/time/outcome plus the
// outputs map (≤ max_output_files_per_mission small entries).
func validateEventLineHeadroom(c *DugdaleConfig) error {
	const envelope int64 = 64 * 1024
	lim := c.Limits.MaxEventLineSize
	if lim <= 0 {
		return nil // 0 = unlimited line; no constraint
	}
	if c.Limits.MaxReturnValueSize+envelope > lim {
		return fmt.Errorf("refuse to start: max_return_value_size (%d) + done-event envelope (%d) exceeds max_event_line_size (%d); a success done event could not be written",
			c.Limits.MaxReturnValueSize, envelope, lim)
	}
	if c.Limits.MaxFailMessageSize+c.Limits.MaxFailDetailsSize+envelope > lim {
		return fmt.Errorf("refuse to start: max_fail_message_size (%d) + max_fail_details_size (%d) + done-event envelope (%d) exceeds max_event_line_size (%d); a failed done event could not be written",
			c.Limits.MaxFailMessageSize, c.Limits.MaxFailDetailsSize, envelope, lim)
	}
	return nil
}

// validateMissionEnv enforces: LETTS_* is a reserved namespace
// for the daemon's own variables (LETTS_MISSION_ID, LETTS_KIND,
// LETTS_LANE, LETTS_WORKDIR, LETTS_IN_<role>, ...). Operators must not
// shadow these via mission_env.set, and must not silently sneak host
// LETTS_* values through mission_env.inherit. Detection at config load
// gives clear feedback; the previous "set" check happened at every
// mission spawn.
func validateMissionEnv(m *MissionEnvConfig) error {
	for k := range m.Set {
		if strings.HasPrefix(k, "LETTS_") {
			return fmt.Errorf("mission_env.set key %q uses reserved LETTS_* namespace", k)
		}
	}
	for _, name := range m.Inherit {
		if strings.HasPrefix(name, "LETTS_") {
			return fmt.Errorf("mission_env.inherit name %q uses reserved LETTS_* namespace", name)
		}
	}
	return nil
}

// validateTLSGuard implements the refuse-to-start rule. On a
// non-loopback listen, any bearer-token scope being active means tokens
// would travel in plain text on the wire. The operator must either:
//   - assert behind_tls_proxy=true (their guarantee that a TLS-terminating
//     proxy fronts the listener), or
//   - set insecure_plain_http=true (loud dev/lab escape hatch).
//
// Otherwise we refuse to start. exec-scope tokens are particularly dangerous
// (RCE capability) so they trip the guard the same way as auth/admin.
func validateTLSGuard(c *DugdaleConfig) error {
	if isLoopbackListen(c.Listen) {
		return nil
	}
	if c.Network.BehindTLSProxy {
		return nil
	}
	if c.Network.InsecurePlainHTTP {
		// dev/lab escape hatch; loud-warn is logged at startup elsewhere.
		return nil
	}
	var active []string
	if len(c.Auth.Tokens) > 0 {
		active = append(active, "auth")
	}
	if len(c.Admin.Tokens) > 0 {
		active = append(active, "admin")
	}
	if c.Exec.Enabled && len(c.Exec.Tokens) > 0 {
		active = append(active, "exec")
	}
	if len(active) == 0 {
		return nil
	}
	return fmt.Errorf(
		"refuse to start: listen %q is non-loopback and %s tokens are configured, "+
			"but network.behind_tls_proxy is not set; bearer tokens would travel in plain HTTP "+
			"(set behind_tls_proxy=true if a TLS proxy fronts this listener, or insecure_plain_http=true for dev/lab)",
		c.Listen, strings.Join(active, "/"),
	)
}

// isLoopbackListen returns true when the listen address is bound to a
// loopback interface (127.0.0.0/8, ::1, or the textual "localhost"). Empty
// host (e.g. ":7180") binds all interfaces, so treated as non-loopback.
func isLoopbackListen(listen string) bool {
	host, _, err := net.SplitHostPort(listen)
	if err != nil {
		// Fallback: treat as the full string (e.g. someone passed just an IP).
		host = listen
	}
	if host == "" {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	return ip.IsLoopback()
}

// applyDefaults sets unset duration/size fields to their defaults.
// All raw string fields are parsed into typed durations/bytes here.
func applyDefaults(c *DugdaleConfig) error {
	type strToDur struct {
		raw string
		dst *time.Duration
		def time.Duration
	}
	durFields := []strToDur{
		{c.Cleanup.SuccessTTLRaw, &c.Cleanup.SuccessTTL, 24 * time.Hour},
		{c.Cleanup.FailedTTLRaw, &c.Cleanup.FailedTTL, 7 * 24 * time.Hour},
		{c.Cleanup.StagingTTLRaw, &c.Cleanup.StagingTTL, 1 * time.Hour},
		{c.Cleanup.DownloadedGraceRaw, &c.Cleanup.DownloadedGrace, 1 * time.Hour},
		{c.Cleanup.LostCleanupGraceRaw, &c.Cleanup.LostCleanupGrace, 10 * time.Minute},
		{c.Cleanup.SweepIntervalRaw, &c.Cleanup.SweepInterval, 5 * time.Minute},
		{c.Limits.UploadIdleTimeoutRaw, &c.Limits.UploadIdleTimeout, 30 * time.Second},
		{c.Limits.DefaultKillGraceRaw, &c.Limits.DefaultKillGrace, 5 * time.Second},
		{c.Limits.ReaderPostExitGraceRaw, &c.Limits.ReaderPostExitGrace, 5 * time.Second},
		{c.Exec.ExecSuccessTTLRaw, &c.Exec.ExecSuccessTTL, 1 * time.Hour},
		{c.Exec.ExecFailedTTLRaw, &c.Exec.ExecFailedTTL, 24 * time.Hour},
	}
	for _, f := range durFields {
		v, err := parseDurationOr(f.raw, f.def)
		if err != nil {
			return err
		}
		*f.dst = v
	}

	type strToBytes struct {
		raw string
		dst *int64
		def int64
	}
	byteFields := []strToBytes{
		{c.Limits.MaxOutputBufferRaw, &c.Limits.MaxOutputBuffer, 16 * 1024 * 1024},
		{c.Limits.MaxEventsBufferRaw, &c.Limits.MaxEventsBuffer, 1024 * 1024},
		{c.Limits.MaxEventLineSizeRaw, &c.Limits.MaxEventLineSize, 1024 * 1024},
		{c.Limits.MaxReturnValueSizeRaw, &c.Limits.MaxReturnValueSize, 768 * 1024},
		{c.Limits.MaxFailMessageSizeRaw, &c.Limits.MaxFailMessageSize, 64 * 1024},
		{c.Limits.MaxFailDetailsSizeRaw, &c.Limits.MaxFailDetailsSize, 256 * 1024},
		{c.Limits.MaxDispatchBodyRaw, &c.Limits.MaxDispatchBodySize, 2 * 1024 * 1024},
		{c.Limits.MaxMissionInputRaw, &c.Limits.MaxMissionInputSize, 1024 * 1024},
		{c.Limits.MaxExecBodyRaw, &c.Limits.MaxExecBodySize, 1024 * 1024},
		{c.Limits.MaxApplyBodyRaw, &c.Limits.MaxApplyBodySize, 1024 * 1024},
		{c.Limits.ProgressBufferRaw, &c.Limits.ProgressBufferSize, 256 * 1024},
		{c.Limits.MaxOutputFileSizeRaw, &c.Limits.MaxOutputFileSize, 0},
		{c.Limits.MaxStagingUploadRaw, &c.Limits.MaxStagingUploadSize, 0},
		{c.Limits.MaxIncompleteBytesRaw, &c.Limits.MaxIncompleteBytes, 0},
		{c.Limits.MaxDataDirRaw, &c.Limits.MaxDataDirSize, 0},
		{c.Exec.MaxScriptSizeRaw, &c.Exec.MaxScriptSize, 256 * 1024},
	}
	for _, f := range byteFields {
		v, err := parseBytesOr(f.raw, f.def)
		if err != nil {
			return err
		}
		*f.dst = v
	}

	// Pointer raw → preserves an explicit YAML `0` as "no cap"
	// instead of clobbering with the default.
	if c.Limits.MaxProgressRateRaw == nil {
		c.Limits.MaxProgressRate = 50
	} else {
		c.Limits.MaxProgressRate = *c.Limits.MaxProgressRateRaw
	}
	if c.Limits.MaxOutputFilesPerMsnRaw == nil {
		c.Limits.MaxOutputFilesPerMsn = 32
	} else {
		c.Limits.MaxOutputFilesPerMsn = *c.Limits.MaxOutputFilesPerMsnRaw
	}
	// max_incomplete_staging_uploads: 0 (explicit) = unlimited;
	// only fill in the default when the field is absent from the YAML.
	if c.Limits.MaxIncompleteUploadsRaw == nil {
		c.Limits.MaxIncompleteUploads = 128
	} else {
		c.Limits.MaxIncompleteUploads = *c.Limits.MaxIncompleteUploadsRaw
	}
	if c.Limits.CacheSize == 0 {
		c.Limits.CacheSize = -16000
	}
	if c.Exec.MaxInputsPerExecRaw == nil {
		c.Exec.MaxInputsPerExec = 32
	} else {
		c.Exec.MaxInputsPerExec = *c.Exec.MaxInputsPerExecRaw
	}
	if c.Exec.MaxOutputsPerExecRaw == nil {
		c.Exec.MaxOutputsPerExec = 32
	} else {
		c.Exec.MaxOutputsPerExec = *c.Exec.MaxOutputsPerExecRaw
	}

	if c.Log.Level == "" {
		c.Log.Level = "info"
	}
	if c.Log.Format == "" {
		c.Log.Format = "json"
	}
	if c.Log.Output == "" {
		c.Log.Output = "stderr"
	}

	return nil
}

func parseDurationOr(raw string, def time.Duration) (time.Duration, error) {
	if raw == "" {
		return def, nil
	}
	// Support "Nd" for N days since time.ParseDuration doesn't handle "d".
	if strings.HasSuffix(raw, "d") {
		n, err := strconv.ParseInt(strings.TrimSuffix(raw, "d"), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid duration %q: %w", raw, err)
		}
		if n < 0 {
			return 0, fmt.Errorf("duration %q must be non-negative", raw)
		}
		return time.Duration(n) * 24 * time.Hour, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid duration %q: %w", raw, err)
	}
	if d < 0 {
		return 0, fmt.Errorf("duration %q must be non-negative", raw)
	}
	return d, nil
}

func parseBytesOr(raw string, def int64) (int64, error) {
	if raw == "" {
		return def, nil
	}
	s := strings.TrimSpace(raw)
	mult := int64(1)
	switch {
	case strings.HasSuffix(s, "GiB"):
		mult = 1024 * 1024 * 1024
		s = strings.TrimSuffix(s, "GiB")
	case strings.HasSuffix(s, "MiB"):
		mult = 1024 * 1024
		s = strings.TrimSuffix(s, "MiB")
	case strings.HasSuffix(s, "KiB"):
		mult = 1024
		s = strings.TrimSuffix(s, "KiB")
	}
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid byte size %q: %w", raw, err)
	}
	if n < 0 {
		return 0, fmt.Errorf("byte size %q must be non-negative", raw)
	}
	return n * mult, nil
}
