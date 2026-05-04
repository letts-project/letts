package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/spf13/cobra"

	"letts/internal/ids"
	"letts/pkg/lettsclient"
	"letts/pkg/lettsconfig"
)

// execFlags is the full `letts exec` flag surface.
type execFlags struct {
	// Target (exactly one of host/match/all)
	host  string
	match []string
	all   bool
	lane  string

	// Content delivery (--script, key=path inputs/outputs, stdin)
	script string
	in     []string // key=path
	out    []string // key=path
	stdin  string   // "" → resolved at runtime per TTY and host count

	// Timeouts
	timeout     string
	waitTimeout string // "" → auto: timeout + 30s if --timeout set, else infinite

	// Output
	detach       bool
	outputFmt    string // "" → auto: raw if N=1, prefix if N>1
	outputBuffer int    // bytes; default 64KiB for json mode

	// Hygiene and identity
	allowShell bool
	missionID  string
	groupID    string

	// Positional argv after `--`
	argv []string

	// Resolved at runtime (post-target-resolution), NOT bound to a cobra
	// flag. runExec sets these before routing to runExecOne / runExecFanOut
	// so per-host code can upload bytes once per dugdale and stash the
	// resulting staging_id into the dispatch payload.
	//   stdinMode  — "none"|"single"|"broadcast" (resolveStdinMode output)
	//   stdinBytes — full stdin payload, read once at top of runExec.
	//                Empty when stdinMode == "none".
	stdinMode  string
	stdinBytes []byte
}

// newExecCmd builds the `letts exec` cobra command.
func newExecCmd() *cobra.Command {
	ef := &execFlags{}
	c := &cobra.Command{
		Use:   "exec [flags] [-- command args...]",
		Short: "Run an ad-hoc command on one or more dugdales",
		Long: `Dispatch a one-off command (not a named mission). Requires exec.enabled
on the target dugdale and an exec-scope token.

Exactly one of --host, --match, --all must be specified. --lane is required.`,
		DisableFlagParsing: false,
		RunE: func(cmd *cobra.Command, args []string) error {
			ef.argv = args
			ac, format, err := setupAppCtx(cmd)
			if err != nil {
				return err
			}
			defer ac.Close()
			return runExec(cmd, ac, ef, format)
		},
	}
	c.Flags().StringVar(&ef.host, "host", "", "dugdale id or comma list (s1,s2,s7)")
	c.Flags().StringSliceVar(&ef.match, "match", nil, "label filter (AND); repeat or comma-list (e.g. prod,web)")
	c.Flags().BoolVar(&ef.all, "all", false, "all dugdales with the lane configured")
	c.Flags().StringVar(&ef.lane, "lane", "", "lane name (required)")
	c.Flags().StringVar(&ef.script, "script", "", "local script path; uploaded once per dugdale via content-addressed staging")
	c.Flags().StringSliceVar(&ef.in, "in", nil, "input file role=path (repeatable); uploaded per dugdale; staged at $LETTS_IN_<role>")
	c.Flags().StringSliceVar(&ef.out, "out", nil, "output file role=path (repeatable); downloaded after success; existing files refused")
	c.Flags().StringVar(&ef.stdin, "stdin", "", "stdin mode: none|single|broadcast (default: none if TTY, single if piped to 1 host, error otherwise)")
	c.Flags().StringVar(&ef.timeout, "timeout", "", "server-side exec timeout, e.g. 5m")
	c.Flags().StringVar(&ef.waitTimeout, "wait-timeout", "", "client wait deadline; default = --timeout + 30s")
	c.Flags().BoolVar(&ef.detach, "detach", false, "print exec_id (or group_id) and exit")
	c.Flags().StringVar(&ef.outputFmt, "output", "", "raw|prefix|json|ndjson (default raw if N=1, prefix if N>1)")
	c.Flags().IntVar(&ef.outputBuffer, "output-buffer", 64*1024, "per-host buffer cap for json mode (bytes)")
	c.Flags().BoolVar(&ef.allowShell, "allow-shell", false, "permit shell-form argv (server still gates by exec.allow_shell)")
	c.Flags().StringVar(&ef.missionID, "mission-id", "", "override exec_id (UUIDv7; single-host only)")
	c.Flags().StringVar(&ef.groupID, "group-id", "", "override group_id (string; multi-host)")
	return c
}

// runExec is the top-level dispatcher. Validates flags, resolves targets,
// then routes to runExecOne (single host) or runExecFanOut.
func runExec(cmd *cobra.Command, ac *appCtx, ef *execFlags, f Format) error {
	// Pre-validate --out keys before any HTTP/staging work (mirrors --in
	// below). parseExecKV enforces the key regex, reserved __ prefix, and
	// duplicates; running it once up-front turns a typo into a single
	// BadUsage (exit 2) before dispatch fans out to N hosts. The per-host
	// download itself re-parses to get pairs because the request payload
	// only needs keys.
	if len(ef.out) > 0 {
		if _, err := parseExecKV(ef.out, "--out"); err != nil {
			return err
		}
	}

	// Pre-validate --script file existence before any HTTP/staging work so a
	// typo fails fast as BadUsage instead of bubbling through wrapExecTransport
	// (→ 255) after we've already opened a client. Each dugdale uploads
	// independently, so detecting "file missing" once up front also avoids
	// printing N "open file" errors in fan-out mode.
	if ef.script != "" {
		if _, err := os.Stat(ef.script); err != nil {
			return NewBadUsageError("--script: " + err.Error())
		}
	}

	// Pre-validate --in keys before any HTTP/staging work. parseExecKV
	// enforces the key regex, reserved __ prefix, and duplicates; running it
	// once up-front turns a typo into a single BadUsage (exit 2) before we
	// open clients or upload bytes to N hosts. The per-host upload itself
	// still happens in runExecOne / dispatchExecToHost (path may be valid
	// syntactically but unreadable on disk).
	if len(ef.in) > 0 {
		if _, err := parseExecKV(ef.in, "--in"); err != nil {
			return err
		}
	}

	hosts, err := resolveExecTargets(ac.Config, execTargetFlags{
		lane:  ef.lane,
		host:  ef.host,
		match: ef.match,
		all:   ef.all,
	}, ac.Getenv)
	if err != nil {
		return err
	}

	// Resolve --stdin mode and read bytes once. Auto rules short-circuit
	// explicit error reporting: piping into N>1 hosts without
	// --stdin=broadcast is a BadUsage so the operator opts into the per-host
	// fan-out cost. The bytes are slurped at the top of runExec so neither
	// runExecOne nor each per-host goroutine in runExecFanOut has to
	// coordinate around fd0. Per-host uploads (single = 1 host; broadcast =
	// N hosts) happen later via uploadStdinToHost; staging dedupes on
	// sha256+size so siblings with prior copies skip the wire transfer.
	stdinMode, err := resolveStdinMode(ef.stdin, len(hosts), isTerminalFD(os.Stdin.Fd()))
	if err != nil {
		return err
	}
	ef.stdinMode = stdinMode
	if stdinMode != "none" {
		data, rerr := readStdinAll()
		if rerr != nil {
			return wrapExecTransport(rerr)
		}
		ef.stdinBytes = data
		// Auto-detected non-TTY with 0 bytes piped is identical in behaviour
		// to "no stdin requested": semantically the child sees an empty
		// stdin either way. Skip the staging upload entirely — the
		// /v1/staging/by-content lookup rejects size=0 (positive integer
		// only), so attempting to dedupe and upload here would surface a
		// confusing "size must be a positive integer" error to users
		// running letts in a CI/cron/non-TTY shell without an explicit
		// stdin pipe. Only downgrade for the auto-resolved "single" mode;
		// explicit --stdin=single|broadcast respects user intent.
		if ef.stdin == "" && len(data) == 0 {
			ef.stdinMode = "none"
		}
	}

	// Single-host runs with --output=json|ndjson|prefix route through the
	// fan-out machinery so the {results:[...]} shape is identical to N>1
	// cases. Raw (default for N=1) stays on the direct runExecOne path
	// because there's no aggregate envelope to construct.
	if len(hosts) > 1 || (ef.outputFmt != "" && ef.outputFmt != "raw") {
		return runExecFanOut(cmd, ac, ef, hosts, f)
	}
	if ef.outputFmt == "" {
		ef.outputFmt = "raw"
	}
	return runExecOne(cmd, ac, ef, hosts[0], f)
}

// computeWaitDeadline applies the --wait-timeout default rule:
//   - explicit "0"     → infinite (zero time.Time)
//   - explicit value   → now + value
//   - unset, no -timeout → infinite
//   - unset, --timeout → now + timeout + 30s grace
//
// Returns zero time.Time if no deadline. On parse error returns a
// BadUsageError.
func computeWaitDeadline(waitTimeoutFlag, timeoutFlag string, now time.Time) (time.Time, error) {
	if waitTimeoutFlag == "0" {
		return time.Time{}, nil // explicit infinite
	}
	if waitTimeoutFlag != "" {
		d, err := time.ParseDuration(waitTimeoutFlag)
		if err != nil {
			return time.Time{}, NewBadUsageError("--wait-timeout: " + err.Error())
		}
		return now.Add(d), nil
	}
	if timeoutFlag == "" {
		return time.Time{}, nil // both unset → infinite
	}
	d, err := time.ParseDuration(timeoutFlag)
	if err != nil {
		return time.Time{}, NewBadUsageError("--timeout: " + err.Error())
	}
	return now.Add(d + 30*time.Second), nil
}

// runExecOne is the single-host exec pipeline. Pre-terminal errors
// (transport, auth, config, staging, exec dispatch, event stream before
// done) are wrapped via wrapExecTransport so mapErrorToExit produces 255.
// A terminal done event returns an ExecOutcomeError so the same mapper
// applies the outcome → exit-code rules.
func runExecOne(cmd *cobra.Command, ac *appCtx, ef *execFlags, host string, f Format) error {
	// Allow-shell hygiene check, identical to server-side.
	if !ef.allowShell && isShellForm(ef.argv) {
		return NewBadUsageError("shell-form argv requires --allow-shell (see --help)")
	}
	if len(ef.argv) == 0 && ef.script == "" {
		return NewBadUsageError("either --script or '-- command' is required")
	}

	c, err := ac.ClientForHost(host, lettsconfig.ScopeExec)
	if err != nil {
		return wrapExecTransport(err)
	}

	// Dedupe is per-dugdale: uploadOrReuse hashes the local file, asks this
	// host's daemon if it already has the bytes, and on miss uploads them.
	// Single-host path only walks the local pipeline once.
	var scriptStagingID string
	if ef.script != "" {
		id, err := uploadOrReuse(c, ef.script)
		if err != nil {
			return wrapExecTransport(err)
		}
		scriptStagingID = id
	}

	// --in role=path: validation already ran in runExec; here we just upload
	// each file via uploadOrReuse and collect the resulting staging_ids into
	// ExecFileRef rows for the dispatch payload. Dedupe is per-dugdale so
	// this loop runs once for the single-host path.
	var inRefs []lettsclient.ExecFileRef
	if len(ef.in) > 0 {
		pairs, err := parseExecKV(ef.in, "--in")
		if err != nil {
			return err
		}
		for _, p := range pairs {
			id, err := uploadOrReuse(c, p.Path)
			if err != nil {
				return wrapExecTransport(err)
			}
			inRefs = append(inRefs, lettsclient.ExecFileRef{Key: p.Key, StagingID: id})
		}
	}

	// --out role=path: only the keys travel in the dispatch payload (so the
	// server can pre-allocate output roles). The path is held on the client
	// side for the post-done download below. Validation already ran in
	// runExec; re-parse here to extract pairs.
	var outKeys []string
	var outPairs []execKV
	if len(ef.out) > 0 {
		pairs, err := parseExecKV(ef.out, "--out")
		if err != nil {
			return err
		}
		outPairs = pairs
		outKeys = make([]string, 0, len(pairs))
		for _, p := range pairs {
			outKeys = append(outKeys, p.Key)
		}
	}

	// --stdin per-host upload. runExec slurped the bytes once and resolved
	// the mode; here we hash and upload via uploadStdinToHost which dedupes
	// against the host's staging when sha256 and size match. None mode leaves
	// stdinSID empty so buildExecRequest omits the request fields.
	var stdinSID string
	if ef.stdinMode != "" && ef.stdinMode != "none" {
		id, err := uploadStdinToHost(c, ef.stdinBytes)
		if err != nil {
			return wrapExecTransport(err)
		}
		stdinSID = id
	}

	missionID := ef.missionID
	if missionID == "" {
		missionID = ids.NewUUIDv7()
	}

	req := buildExecRequest(execPayloadInputs{
		missionID:       missionID,
		lane:            ef.lane,
		argv:            ef.argv,
		timeout:         ef.timeout,
		hostsCount:      1,
		scriptStagingID: scriptStagingID,
		scriptPath:      ef.script,
		in:              inRefs,
		out:             outKeys,
		stdinMode:       ef.stdinMode,
		stdinStagingID:  stdinSID,
	})

	resp, err := lettsclient.Exec(c, req)
	if err != nil {
		return wrapExecTransport(err)
	}

	if ef.detach {
		_, _ = fmt.Fprintln(cmd.OutOrStdout(), resp.ExecID)
		return nil
	}

	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}
	deadline, err := computeWaitDeadline(ef.waitTimeout, ef.timeout, time.Now())
	if err != nil {
		return err
	}
	if !deadline.IsZero() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}

	// Output tail goroutines (raw mode = stdout→stdout, stderr→stderr).
	var tailWG sync.WaitGroup
	tailCtx, cancelTail := context.WithCancel(ctx)
	defer cancelTail()

	stdoutTailErr := make(chan error, 1)
	stderrTailErr := make(chan error, 1)
	tailWG.Add(2)
	go func() {
		defer tailWG.Done()
		stdoutTailErr <- tailExecStream(tailCtx, c, resp.ExecID, "stdout", cmd.OutOrStdout())
	}()
	go func() {
		defer tailWG.Done()
		stderrTailErr <- tailExecStream(tailCtx, c, resp.ExecID, "stderr", cmd.ErrOrStderr())
	}()

	// StreamEvents until done or error.
	var doneEv *lettsclient.Event
	streamErr := lettsclient.StreamEvents(ctx, c, resp.ExecID,
		lettsclient.StreamOpts{Follow: true},
		func(ev lettsclient.Event) error {
			if ev.Event == "done" {
				doneEv = &ev
			}
			return nil
		})

	// Wait-timeout check first (before transport classification).
	if errors.Is(streamErr, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return NewWaitTimeoutError()
	}
	if streamErr != nil && doneEv == nil {
		return wrapExecTransport(streamErr)
	}

	// Drain output tail (1s cap; mirrors run.go). The cancel+<-drained
	// dance ensures fan-out callers don't race the deferred cancelTail()
	// against per-host buffer snapshots — wait until the goroutines exit.
	drained := make(chan struct{})
	go func() { tailWG.Wait(); close(drained) }()
	select {
	case <-drained:
	case <-time.After(1 * time.Second):
		cancelTail()
		<-drained
	}
	// Drain BOTH tail-error channels unconditionally — even on the happy
	// `doneEv != nil` path. On `doneEv != nil` a non-context-cancel tail
	// error means we returned "success" but lost some output bytes; log
	// to stderr so the user can investigate, but don't override the
	// authoritative done outcome — the done event is the source of truth.
	// Fan-out amplifies this N× per host, so silent drops would be
	// especially bad there.
	soErr := readTailErr(stdoutTailErr)
	seErr := readTailErr(stderrTailErr)
	if doneEv == nil {
		if soErr != nil {
			return wrapExecTransport(soErr)
		}
		if seErr != nil {
			return wrapExecTransport(seErr)
		}
		return wrapExecTransport(errors.New("event stream ended without a done event"))
	}
	if soErr != nil {
		_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "letts: warning: stdout tail interrupted: %v\n", soErr)
	}
	if seErr != nil {
		_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "letts: warning: stderr tail interrupted: %v\n", seErr)
	}

	// On a successful done event, download each declared --out role from
	// staging into its target path. Failed/killed runs leave outputs on the
	// dugdale (the done event may still list them, but they're only
	// promised on success). A missing role in the map after success is a
	// server-side contract violation → transport class.
	//
	// Use the same all-or-none coordinator the multi-host path
	// uses (downloadAllAtomic). Pre-checks no existing finals, downloads
	// to sidecar tmps, then promotes via rename — on any failure the
	// FS state rolls back so no half-written set of outputs is left.
	if doneEv.Outcome == "success" && len(outPairs) > 0 {
		downloads := make([]atomicDownload, 0, len(outPairs))
		for _, p := range outPairs {
			eo, ok := doneEv.Outputs[p.Key]
			if !ok || eo.StagingID == "" {
				return wrapExecTransport(fmt.Errorf("server reported success but output role %q missing staging_id", p.Key))
			}
			downloads = append(downloads, atomicDownload{
				Client: c, StagingID: eo.StagingID, FinalPath: p.Path,
			})
		}
		if err := downloadAllAtomic(downloads); err != nil {
			// BadUsageError (output_exists) keeps its exit-2 class; any
			// other error is transport (network / fs / etc).
			var bue *BadUsageError
			if errors.As(err, &bue) {
				return err
			}
			return wrapExecTransport(err)
		}
	}

	exitCode := 0
	if doneEv.ExitCode != nil {
		exitCode = *doneEv.ExitCode
	}
	return &ExecOutcomeError{
		Outcome:  doneEv.Outcome,
		ExitCode: exitCode,
	}
}

// readTailErr non-blockingly drains a 1-cap error channel, returning
// nil if empty or the value is context.Canceled (clean shutdown).
func readTailErr(ch chan error) error {
	select {
	case e := <-ch:
		if e == nil || errors.Is(e, context.Canceled) {
			return nil
		}
		return e
	default:
		return nil
	}
}

// tailExecStream is the exec analogue of run.go's tailMissionStream but
// writes verbatim bytes to dst (no [stdout]/[stderr] prefix — raw mode
// passes the remote stream through as-is). For fan-out (prefix/json/ndjson),
// the per-host goroutine wraps dst in a format-specific adapter.
func tailExecStream(ctx context.Context, c *lettsclient.Client, missionID, streamName string, dst io.Writer) error {
	rc, err := lettsclient.OpenOutput(ctx, c, missionID, lettsclient.OutputOpts{
		Stream: streamName,
		Follow: true,
	})
	if err != nil {
		// Polling for the file to appear is handled inside OpenOutput.
		return err
	}
	defer func() { _ = rc.Close() }()
	_, err = io.Copy(dst, rc)
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil
	}
	return err
}
