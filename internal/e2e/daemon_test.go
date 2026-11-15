// Package e2e_test runs each test against a real dugdale binary as a
// subprocess on a random loopback port with its own data_dir and config.
// Use this when behavior depends on real process boundaries: signal
// handling, lock files, OS-level shutdown sequencing, exit codes.
//
// For pure in-process invariants prefer internal/integration which is
// faster (no fork+build).
package e2e_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

const (
	defaultStartupTimeout = 20 * time.Second
	defaultShutdownGrace  = 15 * time.Second
)

// buildBinaries locates the project root and builds the `dugdale` binary
// once per test run, returning the directory that holds it.
var (
	binOnce sync.Once
	binDir  string
	binErr  error
)

func buildBinaries(t *testing.T) string {
	t.Helper()
	binOnce.Do(func() {
		repo, err := repoRoot()
		if err != nil {
			binErr = err
			return
		}
		dir, err := os.MkdirTemp("", "letts-e2e-bin-")
		if err != nil {
			binErr = err
			return
		}
		for _, target := range []string{"dugdale"} {
			out := filepath.Join(dir, target)
			cmd := exec.Command("go", "build", "-o", out, "./cmd/"+target)
			cmd.Dir = repo
			cmd.Env = append(os.Environ(), "GOFLAGS=-mod=mod")
			if buf, err := cmd.CombinedOutput(); err != nil {
				binErr = fmt.Errorf("build %s: %v\n%s", target, err, buf)
				return
			}
		}
		binDir = dir
	})
	if binErr != nil {
		t.Fatalf("build binaries: %v", binErr)
	}
	return binDir
}

func repoRoot() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	dir := wd
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("go.mod not found from " + wd)
		}
		dir = parent
	}
}

// pickPort claims a random loopback port and immediately releases it.
// Race with another process binding the same port before dugdale starts
// is possible but vanishingly rare in test envs.
func pickPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pick port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
}

// daemon is a running dugdale subprocess.
type daemon struct {
	t            *testing.T
	cmd          *exec.Cmd
	dataDir      string
	configPath   string
	URL          string
	AdminTok     string
	logBuf       *threadSafeBuffer
	dugdaleBin   string
	stopOnce     sync.Once
	exitCode     int
	exitErr      error
	exitObserved chan struct{}
}

type threadSafeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *threadSafeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *threadSafeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// daemonOpts configures the dugdale config file written for a test.
type daemonOpts struct {
	DispatchToken string
	AdminToken    string
	SuccessTTL    string // e.g. "72h"; empty → 24h default
	FailedTTL     string
	StagingTTL    string
	ExtraYAML     string // appended verbatim under top-level
}

// startDaemon builds binaries (cached), writes a fresh config and data_dir,
// starts dugdale, polls /healthz, returns the daemon handle. Test cleanup
// stops the daemon.
func startDaemon(t *testing.T, opts daemonOpts) *daemon {
	t.Helper()
	dir := buildBinaries(t)
	dataDir := t.TempDir()
	port := pickPort(t)

	if opts.DispatchToken == "" {
		opts.DispatchToken = "disp-token-e2e"
	}
	if opts.AdminToken == "" {
		opts.AdminToken = "admin-token-e2e"
	}
	if opts.SuccessTTL == "" {
		opts.SuccessTTL = "24h"
	}
	if opts.FailedTTL == "" {
		opts.FailedTTL = "7d"
	}
	if opts.StagingTTL == "" {
		opts.StagingTTL = "1h"
	}

	cfgYAML := fmt.Sprintf(`listen: 127.0.0.1:%d
data_dir: %s
auth:
  tokens: ["%s"]
admin:
  tokens: ["%s"]
log:
  level: warn
  format: text
  output: stderr
cleanup:
  success_ttl: %s
  failed_ttl: %s
  staging_ttl: %s
  downloaded_grace: 1h
  lost_cleanup_grace: 1m
  sweep_interval: 1s
limits:
  max_dispatch_body_size: 1MiB
  max_apply_body_size: 1MiB
  max_mission_input_size: 256KiB
  max_output_buffer: 4MiB
  max_events_buffer: 1MiB
  max_event_line_size: 1MiB
  max_return_value_size: 256KiB
  max_fail_message_size: 64KiB
  max_fail_details_size: 64KiB
  default_kill_grace: 1s
  reader_post_exit_grace: 500ms
%s`, port, dataDir, opts.DispatchToken, opts.AdminToken, opts.SuccessTTL, opts.FailedTTL, opts.StagingTTL, opts.ExtraYAML)

	cfgPath := filepath.Join(dataDir, "dugdale.yaml")
	if err := os.WriteFile(cfgPath, []byte(cfgYAML), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	dugBin := filepath.Join(dir, "dugdale")
	logBuf := &threadSafeBuffer{}

	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, dugBin, "--config", cfgPath)
	cmd.Stdout = logBuf
	cmd.Stderr = logBuf
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatalf("start dugdale: %v", err)
	}

	d := &daemon{
		t:            t,
		cmd:          cmd,
		dataDir:      dataDir,
		configPath:   cfgPath,
		URL:          fmt.Sprintf("http://127.0.0.1:%d", port),
		AdminTok:     opts.AdminToken,
		logBuf:       logBuf,
		dugdaleBin:   dugBin,
		exitObserved: make(chan struct{}),
	}

	go func() {
		err := cmd.Wait()
		d.exitErr = err
		if ee, ok := err.(*exec.ExitError); ok {
			d.exitCode = ee.ExitCode()
		}
		close(d.exitObserved)
		cancel()
	}()

	t.Cleanup(func() { d.Stop() })

	if err := d.waitHealthy(defaultStartupTimeout); err != nil {
		t.Fatalf("dugdale failed to become healthy: %v\nlogs:\n%s", err, d.Logs())
	}
	return d
}

func (d *daemon) waitHealthy(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 1 * time.Second}
	for {
		resp, err := client.Get(d.URL + "/v1/healthz")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == 200 {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("not healthy after %s", timeout)
		}
		select {
		case <-d.exitObserved:
			return fmt.Errorf("daemon exited before becoming healthy (exit=%d)", d.exitCode)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// Stop sends SIGTERM and waits up to defaultShutdownGrace for clean exit;
// then SIGKILL if still alive. Safe to call multiple times.
func (d *daemon) Stop() {
	d.stopOnce.Do(func() {
		if d.cmd.Process == nil {
			return
		}
		_ = d.cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-d.exitObserved:
		case <-time.After(defaultShutdownGrace):
			_ = d.cmd.Process.Kill()
			<-d.exitObserved
		}
	})
}

// Logs returns the combined stdout+stderr captured so far.
func (d *daemon) Logs() string { return d.logBuf.String() }

// ExitCode returns the OS exit code (0 if not yet observed or clean exit).
func (d *daemon) ExitCode() int {
	select {
	case <-d.exitObserved:
		return d.exitCode
	default:
		return -1
	}
}

// WaitExit blocks until the daemon exits or timeout elapses.
func (d *daemon) WaitExit(timeout time.Duration) (int, error) {
	select {
	case <-d.exitObserved:
		return d.exitCode, d.exitErr
	case <-time.After(timeout):
		return -1, fmt.Errorf("daemon did not exit within %s", timeout)
	}
}

// DataDir is the daemon's data directory (sqlite, output, staging).
func (d *daemon) DataDir() string { return d.dataDir }

// Do executes an HTTP request with Authorization: Bearer <tok>. Caller
// closes Body. Method/path/body shape is the same as net/http.
func (d *daemon) Do(method, path, token string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest(method, d.URL+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return http.DefaultClient.Do(req)
}

// DoJSON wraps Do and unmarshals 2xx response bodies into out (if non-nil).
// Non-2xx returns the response status+body in an error.
func (d *daemon) DoJSON(method, path, token string, reqBody, out any) error {
	var rdr io.Reader
	if reqBody != nil {
		buf, err := jsonMarshal(reqBody)
		if err != nil {
			return err
		}
		rdr = bytes.NewReader(buf)
	}
	resp, err := d.Do(method, path, token, rdr)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("%s %s → %d: %s", method, path, resp.StatusCode, body)
	}
	if out != nil {
		return jsonUnmarshal(body, out)
	}
	return nil
}

// Apply posts /v1/admin/apply with the given desired-state JSON.
func (d *daemon) Apply(t *testing.T, desired any) {
	t.Helper()
	if err := d.DoJSON("POST", "/v1/admin/apply", d.AdminTok, desired, nil); err != nil {
		t.Fatalf("apply: %v", err)
	}
}

// AssertLogContains fails the test if the captured log doesn't contain s.
func (d *daemon) AssertLogContains(t *testing.T, s string) {
	t.Helper()
	if !strings.Contains(d.Logs(), s) {
		t.Errorf("expected log to contain %q\nlogs:\n%s", s, d.Logs())
	}
}
