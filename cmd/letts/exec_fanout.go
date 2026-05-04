package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/spf13/cobra"

	"letts/internal/ids"
	"letts/pkg/lettsclient"
	"letts/pkg/lettsconfig"
)

// execFanOutResult captures the per-host outcome of an exec fan-out.
// Sum-type semantics: exactly one of (DoneEv) or (Err) carries the
// meaningful result.
//   - HasErr=true  → Err set, DoneEv nil (transport, auth, config, wait
//     timeout, bad usage — anything that prevented a terminal done).
//   - HasErr=false → DoneEv set (terminal done event reached). Stdout/Stderr
//     hold buffered bytes for json/ndjson aggregation; for prefix mode the
//     live prefixedSink writes directly and these slices stay nil.
type execFanOutResult struct {
	Host            string
	ExecID          string
	DoneEv          *lettsclient.Event
	Stdout          []byte
	Stderr          []byte
	StdoutTruncated bool
	StderrTruncated bool

	Err    error
	HasErr bool
}

// runExecFanOut dispatches an exec to N>1 hosts in parallel. Raw format
// is rejected here (single-host only — no per-host disambiguation in a
// flat byte stream). Reuses run.go's per-host goroutine pattern (see
// runFanOut).
//
// Output flow:
//   - --output=raw                    → BadUsage
//   - --output=prefix (default)       → live prefixedSink interleaves
//     [host] lines across stdout/stderr as bytes arrive. Post-aggregation
//     emits only [FAIL] lines for hosts that errored.
//   - --output=json                   → buffered capBuffer per host;
//     post-aggregation emits {group_id, results:[...]} on stdout.
//   - --output=ndjson                 → live ndjsonSink emits per-host
//     output and lifecycle envelopes during the run; post-aggregation
//     appends one {host, event:"error", ...} per failed host.
func runExecFanOut(cmd *cobra.Command, ac *appCtx, ef *execFlags, hosts []string, f Format) error {
	if ef.outputFmt == "raw" {
		return NewBadUsageError("--output=raw requires a single host")
	}
	if ef.missionID != "" {
		return NewBadUsageError("--mission-id not allowed with multiple hosts (would collide ids)")
	}

	// Default group_id when not set.
	groupID := ef.groupID
	if groupID == "" {
		groupID = ids.NewUUIDv7()
	}

	// Default for N>1 is prefix mode.
	outputFmt := ef.outputFmt
	if outputFmt == "" {
		outputFmt = "prefix"
	}

	// Shell-form hygiene check applies to fan-out too (mirrors runExecOne).
	// Done here so each per-host goroutine doesn't repeat it.
	if !ef.allowShell && isShellForm(ef.argv) {
		return NewBadUsageError("shell-form argv requires --allow-shell (see --help)")
	}
	if len(ef.argv) == 0 && ef.script == "" {
		return NewBadUsageError("either --script or '-- command' is required")
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

	// Live writers for streaming formats. Prefix mode installs a single
	// shared sink per stream; per-host writers serialise through its mutex
	// so concurrent lines never tear. ndjson mode installs one shared
	// ndjsonSink writing to stdout — both stdout/stderr lines and
	// lifecycle events fan into it. json mode keeps the buffered capBuffer
	// path (no live writers).
	var stdoutSink, stderrSink *prefixedSink
	var ndjsonOut *ndjsonSink
	switch outputFmt {
	case "prefix":
		stdoutSink = newPrefixedSink(cmd.OutOrStdout())
		stderrSink = newPrefixedSink(cmd.ErrOrStderr())
	case "ndjson":
		ndjsonOut = newNDJSONSink(cmd.OutOrStdout())
	}

	results := make([]execFanOutResult, len(hosts))

	var wg sync.WaitGroup
	for i, h := range hosts {
		i, h := i, h
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = dispatchExecToHost(ctx, ac, ef, h, groupID, len(hosts), outputFmt, stdoutSink, stderrSink, ndjsonOut)
		}()
	}
	wg.Wait()

	// Detach short-circuit: skip output aggregation and exit-code translation
	// — the only deliverables are the recovery handle on stdout (group_id
	// for N>1, exec_id for a lone-host fan-out routing case under
	// --output=prefix|json|ndjson) and per-failure [FAIL] lines on stderr.
	// If any host dispatched OK we still print the handle so the caller can
	// recover the partial group; any setup failure flips exit to 255 via
	// ExecTransportError. Skips writeExecFanOutResults entirely so detach
	// never emits format-specific tails (no JSON envelope, no ndjson error
	// records — caller asked to detach, not to summarise).
	if ef.detach {
		anyOK := false
		anyFail := false
		for _, r := range results {
			if r.HasErr {
				anyFail = true
				_, _ = cmd.ErrOrStderr().Write([]byte("[FAIL] " + r.Host + " — " + r.Err.Error() + "\n"))
			} else {
				anyOK = true
			}
		}
		if anyOK {
			if len(hosts) > 1 {
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), groupID)
			} else {
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), results[0].ExecID)
			}
		}
		if anyFail {
			return &ExecTransportError{Inner: errors.New("partial dispatch failure")}
		}
		return nil
	}

	// All-or-none --out coordinator. Runs BEFORE post-aggregation so a
	// failed download flips the run to a transport error and rolls back any
	// sidecar/promoted files — operators never see a partial set of per-host
	// outputs on disk after a coordinator failure. No-op when --out is
	// empty. Clients are looked up lazily inside the coordinator via
	// ac.ClientForHost (idempotent and cached).
	if len(ef.out) > 0 {
		if err := downloadFanOutOutputs(ac, results, ef.out); err != nil {
			return wrapExecTransport(err)
		}
	}

	// Format-specific post-aggregation. For prefix and ndjson modes the
	// live writes already happened during the run; this only emits the
	// tails ([FAIL] lines for prefix, {event:"error",...} envelopes for
	// ndjson). Pass the already-opened ndjson sink through so we don't
	// open a second one mid-stream.
	if err := writeExecFanOutResults(cmd, outputFmt, ef.outputBuffer, groupID, results, ndjsonOut); err != nil {
		return err
	}
	return computeFanOutExitCode(results)
}

// dispatchExecToHost runs the single-host exec lifecycle and captures the
// result for fan-out aggregation. Each call is independent — no shared
// state with siblings except for the group_id and (for prefix/ndjson modes)
// the shared output sinks. Sinks are nil for json mode; the lifecycle falls
// back to buffered capBuffer in that case.
func dispatchExecToHost(ctx context.Context, ac *appCtx, ef *execFlags, host, groupID string, hostsCount int, outputFmt string, stdoutSink, stderrSink *prefixedSink, ndjsonOut *ndjsonSink) execFanOutResult {
	res := execFanOutResult{Host: host}

	c, err := ac.ClientForHost(host, lettsconfig.ScopeExec)
	if err != nil {
		res.Err = wrapExecTransport(err)
		res.HasErr = true
		return res
	}

	// Dedupe is per-dugdale, NOT global: each host's goroutine runs
	// uploadOrReuse against ITS OWN daemon. Two hosts that already have the
	// bytes both hit dedupe (no upload); two fresh hosts both upload. The
	// sha256 lookup makes this cheap on the wire (one HEAD per dugdale).
	var scriptStagingID string
	if ef.script != "" {
		id, err := uploadOrReuse(c, ef.script)
		if err != nil {
			res.Err = wrapExecTransport(err)
			res.HasErr = true
			return res
		}
		scriptStagingID = id
	}

	// --in role=path per host: identical per-dugdale dedupe semantics as
	// --script. Validation already ran in runExec; here we just upload and
	// collect the refs. Errors bubble out as per-host transport failures so
	// one bad path doesn't fail the whole fan-out.
	var inRefs []lettsclient.ExecFileRef
	if len(ef.in) > 0 {
		pairs, err := parseExecKV(ef.in, "--in")
		if err != nil {
			res.Err = err
			res.HasErr = true
			return res
		}
		for _, p := range pairs {
			id, err := uploadOrReuse(c, p.Path)
			if err != nil {
				res.Err = wrapExecTransport(err)
				res.HasErr = true
				return res
			}
			inRefs = append(inRefs, lettsclient.ExecFileRef{Key: p.Key, StagingID: id})
		}
	}

	// --out role=path: keys only travel in the dispatch payload (server
	// pre-allocates output roles). The download itself is driven by the
	// all-or-none coordinator post-wg.Wait. Validation already ran in
	// runExec; re-parse to extract keys.
	var outKeys []string
	if len(ef.out) > 0 {
		pairs, err := parseExecKV(ef.out, "--out")
		if err != nil {
			res.Err = err
			res.HasErr = true
			return res
		}
		outKeys = make([]string, 0, len(pairs))
		for _, p := range pairs {
			outKeys = append(outKeys, p.Key)
		}
	}

	// --stdin per-host upload. The fan-out coordinator (runExec) slurped the
	// bytes once and resolved the mode; here each per-host goroutine uploads
	// independently to its OWN dugdale via uploadStdinToHost. Same dedupe
	// semantics as --script / --in: by-content lookup short-circuits the
	// wire transfer when the host already holds the bytes. broadcast = N
	// hosts upload N times (best effort dedupe); single = N must be 1
	// (validated upstream by resolveStdinMode) so this branch fires exactly
	// once.
	var stdinSID string
	if ef.stdinMode != "" && ef.stdinMode != "none" {
		id, err := uploadStdinToHost(c, ef.stdinBytes)
		if err != nil {
			res.Err = wrapExecTransport(err)
			res.HasErr = true
			return res
		}
		stdinSID = id
	}

	missionID := ids.NewUUIDv7()
	req := buildExecRequest(execPayloadInputs{
		missionID:       missionID,
		lane:            ef.lane,
		argv:            ef.argv,
		timeout:         ef.timeout,
		hostsCount:      hostsCount,
		groupID:         groupID,
		scriptStagingID: scriptStagingID,
		scriptPath:      ef.script,
		in:              inRefs,
		out:             outKeys,
		stdinMode:       ef.stdinMode,
		stdinStagingID:  stdinSID,
	})
	resp, err := lettsclient.Exec(c, req)
	if err != nil {
		res.Err = wrapExecTransport(err)
		res.HasErr = true
		return res
	}
	res.ExecID = resp.ExecID

	if ef.detach {
		return res
	}

	// Tail and StreamEvents. Prefix mode writes live through stdoutSink /
	// stderrSink; ndjson mode writes events and output envelopes through
	// ndjsonOut; json mode falls back to capBuffer when all sinks are nil.
	doneEv, stdoutBuf, stderrBuf, stdoutTrunc, stderrTrunc, lifecycleErr := runExecHostLifecycle(ctx, c, ef, resp.ExecID, outputFmt, host, stdoutSink, stderrSink, ndjsonOut)
	res.DoneEv = doneEv
	res.Stdout = stdoutBuf
	res.Stderr = stderrBuf
	res.StdoutTruncated = stdoutTrunc
	res.StderrTruncated = stderrTrunc
	if lifecycleErr != nil && doneEv == nil {
		res.Err = wrapExecTransport(lifecycleErr)
		res.HasErr = true
		return res
	}
	return res
}

// runExecHostLifecycle is the per-host equivalent of runExecOne's body
// minus the cobra wiring and exit-code translation. Returns the done
// event (if reached), captured stdout/stderr buffers and truncation flags,
// and a pre-terminal error (or nil if a done event was seen).
//
// Writer selection:
//   - prefix mode with sinks != nil → live writers via sink.WriterFor(host, …).
//     Returned []byte stdout/stderr stay nil and truncation flags stay false:
//     bytes streamed out during the run, no post-aggregation render needed.
//   - ndjson mode with ndjsonOut != nil → live writers wrap bytes into
//     {host, stream, line} envelopes; the event callback also emits
//     {host, event, ...} lines through the same sink. Returned slices stay nil.
//   - otherwise (json, or prefix/ndjson with nil sinks for tests)
//     → buffered capBuffer; the returned slices feed the aggregator.
func runExecHostLifecycle(ctx context.Context, c *lettsclient.Client, ef *execFlags, execID, outputFmt, host string, stdoutSink, stderrSink *prefixedSink, ndjsonOut *ndjsonSink) (*lettsclient.Event, []byte, []byte, bool, bool, error) {
	bufCap := ef.outputBuffer
	if bufCap <= 0 {
		bufCap = 64 * 1024
	}

	var stdoutW, stderrW io.Writer
	var stdoutCap, stderrCap *capBuffer
	switch {
	case outputFmt == "prefix" && stdoutSink != nil && stderrSink != nil:
		stdoutW = stdoutSink.WriterFor(host, "")
		stderrW = stderrSink.WriterFor(host, "[stderr]")
	case outputFmt == "ndjson" && ndjsonOut != nil:
		stdoutW = &ndjsonOutputWriter{sink: ndjsonOut, host: host, stream: "stdout"}
		stderrW = &ndjsonOutputWriter{sink: ndjsonOut, host: host, stream: "stderr"}
	default:
		stdoutCap = &capBuffer{cap: bufCap}
		stderrCap = &capBuffer{cap: bufCap}
		stdoutW = stdoutCap
		stderrW = stderrCap
	}

	tailCtx, cancelTail := context.WithCancel(ctx)
	defer cancelTail()

	var tailWG sync.WaitGroup
	tailWG.Add(2)
	go func() { defer tailWG.Done(); _ = tailExecStream(tailCtx, c, execID, "stdout", stdoutW) }()
	go func() { defer tailWG.Done(); _ = tailExecStream(tailCtx, c, execID, "stderr", stderrW) }()

	var doneEv *lettsclient.Event
	streamErr := lettsclient.StreamEvents(ctx, c, execID,
		lettsclient.StreamOpts{Follow: true},
		func(ev lettsclient.Event) error {
			if ndjsonOut != nil {
				envelope := map[string]any{
					"host":  host,
					"event": ev.Event,
					"seq":   ev.Seq,
				}
				if ev.Outcome != "" {
					envelope["outcome"] = ev.Outcome
				}
				if ev.ExitCode != nil {
					envelope["exit_code"] = *ev.ExitCode
				}
				if ev.Signal != "" {
					envelope["signal"] = ev.Signal
				}
				_ = ndjsonOut.Emit(envelope)
			}
			if ev.Event == "done" {
				e := ev
				doneEv = &e
			}
			return nil
		})

	// Drain output tail (1s cap; same pattern as runExecOne). The
	// cancel+<-drained dance prevents racing the deferred cancelTail()
	// against per-host buffer snapshots — wait until the goroutines exit.
	drained := make(chan struct{})
	go func() { tailWG.Wait(); close(drained) }()
	select {
	case <-drained:
	case <-time.After(1 * time.Second):
		cancelTail()
		<-drained
	}

	// bufBytes returns either the capBuffer's contents or nil for the live
	// path (where bytes already streamed out through the sink).
	bufBytes := func() (out, err []byte, outT, errT bool) {
		if stdoutCap == nil {
			return nil, nil, false, false
		}
		return stdoutCap.Bytes(), stderrCap.Bytes(), stdoutCap.Truncated, stderrCap.Truncated
	}

	if errors.Is(streamErr, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		so, se, soT, seT := bufBytes()
		return doneEv, so, se, soT, seT, NewWaitTimeoutError()
	}
	if streamErr != nil && doneEv == nil {
		so, se, soT, seT := bufBytes()
		return nil, so, se, soT, seT, streamErr
	}

	so, se, soT, seT := bufBytes()
	return doneEv, so, se, soT, seT, nil
}

// capBuffer is a bytes-bounded write sink with a truncation marker. Writes
// past cap are silently dropped (returning len(p) so callers see "success");
// the Truncated flag is read by the JSON aggregator to set
// stdout_truncated/stderr_truncated booleans on the per-host result.
type capBuffer struct {
	buf       []byte
	cap       int
	Truncated bool
}

func (b *capBuffer) Write(p []byte) (int, error) {
	if len(b.buf) >= b.cap {
		b.Truncated = true
		return len(p), nil // pretend success — drop silently
	}
	room := b.cap - len(b.buf)
	if len(p) > room {
		b.buf = append(b.buf, p[:room]...)
		b.buf = append(b.buf, []byte("\n...[truncated, more bytes lost (client-side)]")...)
		b.Truncated = true
		return len(p), nil
	}
	b.buf = append(b.buf, p...)
	return len(p), nil
}

func (b *capBuffer) Bytes() []byte { return b.buf }

// computeFanOutExitCode returns a sentinel typed error encoding the
// worst-of exit code across all hosts. mapErrorToExit (exitcode.go) decodes
// the returned error back into the integer exit code.
//
// Worst-of precedence: 0 < N < 124 < 125 < 2 < 255 conceptually, but the
// encoding compresses to: worst==0 → nil; worst==255 → ExecTransportError
// wrapper; worst==124 → WaitTimeoutError; worst==2 → BadUsageError; any
// other positive worst → ExecOutcomeError{Outcome:"failed", ExitCode:worst}
// which mapExecOutcomeExitCode preserves verbatim (worst>0 here so the
// "failed+0→1" remap can't trip us).
func computeFanOutExitCode(results []execFanOutResult) error {
	worst := 0
	for _, r := range results {
		var code int
		switch {
		case r.HasErr:
			var bu *BadUsageError
			var wt *WaitTimeoutError
			switch {
			case errors.As(r.Err, &bu):
				code = exitBadUsage
			case errors.As(r.Err, &wt):
				code = exitWaitTimeout
			default:
				code = 255
			}
		case r.DoneEv != nil:
			ec := 0
			if r.DoneEv.ExitCode != nil {
				ec = *r.DoneEv.ExitCode
			}
			code = mapExecOutcomeExitCode(r.DoneEv.Outcome, ec)
		default:
			code = 255 // shouldn't happen; defensive
		}
		if code > worst {
			worst = code
		}
	}
	if worst == 0 {
		return nil
	}
	if worst == 255 {
		return &ExecTransportError{Inner: errors.New("one or more hosts failed")}
	}
	if worst == exitWaitTimeout {
		return NewWaitTimeoutError()
	}
	if worst == exitBadUsage {
		return NewBadUsageError("one or more hosts: bad usage")
	}
	// Encode worst back via outcome=failed exit=<worst> so the mapper
	// returns it unchanged. worst>0 here so the failed+0→1 remap can't
	// collide.
	return &ExecOutcomeError{Outcome: "failed", ExitCode: worst}
}

// writeExecFanOutResults dispatches to the per-format writer. For prefix
// and ndjson modes the live writes already happened via the sinks during
// the run; this only emits the per-host tails:
//   - prefix → "[host][FAIL] err" on stderr per failed host
//   - ndjson → {host, event:"error", error:<class>, message:...} on stdout
//     per host that failed before reaching a done event
//
// The ndjsonOut sink is passed through from runExecFanOut so the live
// stream and the tail envelopes share one mutex-protected writer; passing
// nil falls back to constructing a fresh sink against cmd.OutOrStdout()
// (preserved for json mode and future callers).
func writeExecFanOutResults(cmd *cobra.Command, outputFmt string, bufCap int, groupID string, results []execFanOutResult, ndjsonOut *ndjsonSink) error {
	switch outputFmt {
	case "prefix":
		ew := cmd.ErrOrStderr()
		for _, r := range results {
			if r.HasErr {
				_, _ = ew.Write([]byte("[" + r.Host + "][FAIL] " + r.Err.Error() + "\n"))
			}
		}
		return nil
	case "json":
		return writeExecFanOutJSON(cmd.OutOrStdout(), groupID, results)
	case "ndjson":
		sink := ndjsonOut
		if sink == nil {
			sink = newNDJSONSink(cmd.OutOrStdout())
		}
		for _, r := range results {
			if r.HasErr && r.DoneEv == nil {
				envelope := map[string]any{
					"host":    r.Host,
					"event":   "error",
					"message": r.Err.Error(),
				}
				classifyError(r.Err, envelope)
				_ = sink.Emit(envelope)
			}
		}
		return nil
	default:
		return NewBadUsageError("--output=" + outputFmt + ": unsupported")
	}
}
