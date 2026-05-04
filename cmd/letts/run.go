package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"

	"letts/internal/ids"
	"letts/pkg/lettsclient"
	"letts/pkg/lettsconfig"
)

// runFlags holds parsed flags for `letts run`.
//
// The shape is a superset of dispatchFlags: it adds follow/timeout/output-file
// knobs that don't apply to fire-and-forget `letts dispatch`. The `host` field
// accepts a comma-separated list (`s1,s2,s7`), which
// runCore unpacks into a parallel fan-out.
type runFlags struct {
	route       string
	host        string
	match       []string
	lane        string
	mission     string
	input       string
	inputFile   string
	files       []string // role=path (input staging)
	outputFiles []string // role=path (download from done.outputs)
	timeout     string   // server-side mission timeout
	waitTimeout string   // client-side wait deadline
	noProgress  bool     // --no-progress: hide progress events
	quiet       bool     // from global --quiet: suppress event/log tailing
	logs        bool     // --logs: show stderr logs even in --output=json
	missionID   string
}

func newRunCmd() *cobra.Command {
	rf := &runFlags{}
	c := &cobra.Command{
		Use:   "run",
		Short: "Dispatch a mission and follow it until terminal",
		RunE: func(cmd *cobra.Command, args []string) error {
			ac, format, err := setupAppCtx(cmd)
			if err != nil {
				return err
			}
			defer ac.Close()
			// The global --quiet/-q drives run's event/log tailing.
			rf.quiet = ac.Quiet
			return runCore(cmd, ac, rf, format)
		},
	}
	c.Flags().StringVar(&rf.route, "route", "", "symbolic route name")
	c.Flags().StringVar(&rf.host, "host", "", "dugdale id")
	c.Flags().StringSliceVar(&rf.match, "match", nil, "label filter for auto-select")
	c.Flags().StringVar(&rf.lane, "lane", "", "lane name")
	c.Flags().StringVar(&rf.mission, "mission", "", "mission name (required)")
	c.Flags().StringVar(&rf.input, "input", "", "input JSON literal")
	c.Flags().StringVar(&rf.inputFile, "input-file", "", "input JSON file (- for stdin)")
	c.Flags().StringSliceVar(&rf.files, "file", nil, "input file role=path (repeatable)")
	c.Flags().StringSliceVar(&rf.outputFiles, "output-file", nil, "download output role=path (repeatable)")
	c.Flags().StringVar(&rf.timeout, "timeout", "", "mission timeout, e.g. 5m")
	c.Flags().StringVar(&rf.waitTimeout, "wait-timeout", "", "client wait timeout, e.g. 1m")
	c.Flags().BoolVar(&rf.noProgress, "no-progress", false, "do not show progress events")
	c.Flags().BoolVar(&rf.logs, "logs", false, "show stderr logs even in --output=json")
	c.Flags().StringVar(&rf.missionID, "mission-id", "", "override mission id (UUID v7)")
	return c
}

// runCore is the dispatcher behind `letts run`. It accepts the comma-separated
// --host syntax (`s1,s2,s7`):
//
//   - 0 or 1 host → singlehost path via runOne with the existing semantics;
//   - 2+ hosts   → parallel fan-out via runFanOut; aggregate exit code = worst
//     outcome wins (success < failed < abnormal).
func runCore(cmd *cobra.Command, ac *appCtx, rf *runFlags, f Format) error {
	if rf.mission == "" {
		return NewBadUsageError("--mission is required")
	}
	hosts := splitHosts(rf.host)
	if len(hosts) <= 1 {
		var override string
		if len(hosts) == 1 {
			override = hosts[0]
		}
		return runOne(cmd, ac, rf, override, nil, f, cmd.OutOrStdout(), cmd.ErrOrStderr())
	}
	if len(rf.outputFiles) > 0 {
		return NewBadUsageError("--output-file not supported with multiple --host (specify a single host)")
	}
	if rf.missionID != "" {
		return NewBadUsageError("--mission-id not supported with multiple --host (would reuse the same id across hosts)")
	}
	return runFanOut(cmd, ac, rf, hosts, f)
}

// splitHosts splits the `--host` flag value on commas, trimming whitespace,
// dropping empty pieces, and de-duplicating repeats while preserving first
// occurrence order. "" → nil; "s1" → ["s1"]; "s1,s2,s7" → ["s1","s2","s7"];
// "s1,s1,s2" → ["s1","s2"]. Dedup is silent because `--host=s1,s1` clearly
// means "run on s1 once", and surfacing it as BadUsage would only annoy
// users who built the host list programmatically (e.g. shell `$HOSTS`).
func splitHosts(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	seen := make(map[string]bool, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" || seen[p] {
			continue
		}
		seen[p] = true
		out = append(out, p)
	}
	return out
}

// runOne is the singlehost composite behind `letts run`. Steps:
//  1. validate flags and resolve (host, lane);
//  2. load input JSON (or accept pre-loaded bytes from a fan-out parent);
//  3. stage --file uploads;
//  4. POST /v1/dispatch with the chosen idempotency key;
//  5. stream /v1/missions/{id}/events?follow=true until the done event;
//  6. surface return and map outcome to exit-code semantics via typed errors.
//
// hostOverride lets runFanOut inject the per-goroutine host while reusing all
// other rf fields. Empty string preserves the original singlehost path
// (resolveTarget honours rf.route/rf.host/rf.lane/rf.match).
//
// inputOverride lets runFanOut pre-load --input-file=- once (so N goroutines
// don't race on stdin) and pass the resolved bytes. nil means "load from rf".
//
// stdout/stderr are explicit so runFanOut can route per-host output into a
// shared, formatter-specific sink without racing on cmd.SetOut/SetErr.
func runOne(cmd *cobra.Command, ac *appCtx, rf *runFlags, hostOverride string, inputOverride json.RawMessage, f Format, stdout, stderr io.Writer) error {
	hostArg := rf.host
	if hostOverride != "" {
		hostArg = hostOverride
	}
	host, lane, err := resolveTarget(ac, rf.route, hostArg, rf.lane, rf.match)
	if err != nil {
		return err
	}
	input := inputOverride
	if input == nil {
		input, err = loadInput(cmd.InOrStdin(), rf.input, rf.inputFile)
		if err != nil {
			return err
		}
	}

	c, err := ac.ClientForHost(host, lettsconfig.ScopeDispatch)
	if err != nil {
		return err
	}

	files, err := stageFiles(c, rf.files)
	if err != nil {
		return err
	}

	missionID := rf.missionID
	if missionID == "" {
		missionID = ids.NewUUIDv7()
	}
	dispatchResp, err := lettsclient.Dispatch(c, lettsclient.DispatchRequest{
		MissionID: missionID,
		Mission:   rf.mission,
		Lane:      lane,
		Input:     input,
		Files:     files,
		Timeout:   rf.timeout,
	})
	if err != nil {
		return err
	}

	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}
	// Use the same wait-deadline rule as `letts exec` instead
	// of only honoring an explicit --wait-timeout. Unset with --timeout set →
	// mission timeout + 30s grace; --wait-timeout=0 → infinite (was firing
	// immediately); both unset → infinite.
	deadline, derr := computeWaitDeadline(rf.waitTimeout, rf.timeout, time.Now())
	if derr != nil {
		return derr
	}
	if !deadline.IsZero() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}

	// Output-tail goroutines: stream /v1/missions/{id}/output?stream=stdout|
	// stderr&follow=true and emit each line on stderr with [stdout]/[stderr]
	// prefixes.
	//   - text mode (default)  → prefixedStderr to console, no capture
	//   - json with --logs     → prefixedStderr to console and capture
	//   - json (default)       → capture only (surfaced via logs: {} field)
	//   - ndjson               → no tail (every event already on stdout)
	//   - --quiet              → no tail at all
	var tailWG sync.WaitGroup
	var pw *prefixedStderr
	var captureOut, captureErr *prefixedStderr
	var captureOutBuf, captureErrBuf *captureBuf
	var cancelTail context.CancelFunc = func() {} // no-op when tail not active
	wantConsole := f == FormatText || (f == FormatJSON && rf.logs)
	wantCapture := f == FormatJSON
	if !rf.quiet && (wantConsole || wantCapture) && f != FormatNDJSON {
		if wantConsole {
			pw = &prefixedStderr{w: stderr}
		}
		if wantCapture {
			captureOutBuf = newCaptureBuf(64 * 1024)
			captureErrBuf = newCaptureBuf(64 * 1024)
			captureOut = &prefixedStderr{w: captureOutBuf}
			captureErr = &prefixedStderr{w: captureErrBuf}
		}
		var tailCtx context.Context
		tailCtx, cancelTail = context.WithCancel(ctx)
		defer cancelTail()
		tailWG.Add(2)
		go func() {
			defer tailWG.Done()
			tailMissionStream(tailCtx, c, dispatchResp.MissionID, "stdout", "stdout", multiPrefixed(pw, captureOut))
		}()
		go func() {
			defer tailWG.Done()
			tailMissionStream(tailCtx, c, dispatchResp.MissionID, "stderr", "stderr", multiPrefixed(pw, captureErr))
		}()
	}

	var doneEv lettsclient.Event
	streamErr := lettsclient.StreamEvents(ctx, c, dispatchResp.MissionID,
		lettsclient.StreamOpts{Follow: true},
		func(ev lettsclient.Event) error {
			if f == FormatNDJSON {
				// Emit every event verbatim to stdout (preserves omitempty
				// fields like progress value=0.0).
				if err := writeRawEventLine(stdout, ev); err != nil {
					return err
				}
			} else if !rf.quiet {
				if pw != nil {
					logEventToStderrPrefixed(pw, ev, f, rf.noProgress)
				} else {
					logEventToStderr(stderr, ev, f, rf.noProgress)
				}
			}
			if ev.Event == "done" {
				doneEv = ev
			}
			return nil
		})

	// Deadline check first: a hung server returns ctx.DeadlineExceeded via
	// StreamEvents, but we surface it as our typed WaitTimeoutError so
	// mapErrorToExit produces exit 124.
	if errors.Is(streamErr, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return NewWaitTimeoutError()
	}
	if streamErr != nil {
		return streamErr
	}

	// Drain-on-done: give the output-tail goroutines up to 1s to flush their
	// in-flight bytes. This ensures any final [stdout]/[stderr] line lands
	// in stderr BEFORE the result JSON appears on stdout (clean visual). On
	// a slow stream we cap rather than block forever — the mission is done,
	// nobody is waiting for further output.
	if pw != nil {
		drained := make(chan struct{})
		go func() { tailWG.Wait(); close(drained) }()
		select {
		case <-drained:
			// Both tail goroutines exited cleanly within the drain window.
		case <-time.After(1 * time.Second):
			// Drain window exceeded — force-cancel and BLOCK until the
			// goroutines actually exit. Without this wait, fan-out callers
			// (runFanOut) would race the deferred cancelTail() against their
			// own deferred errBuf snapshot — a real concurrent write on
			// bytes.Buffer when output streams are still producing lines as
			// runOne returns. Cancellation propagates through OpenOutput's
			// http.Client Read in ~milliseconds; the wait is bounded by ctx
			// cancellation semantics, not by another timer.
			cancelTail()
			<-drained
		}
	}

	// Download any --output-file pairs before printing the result so a download
	// failure short-circuits the success path. We only attempt this on a
	// successful outcome — a failed/abnormal mission has no outputs to fetch.
	if doneEv.Outcome == "success" && len(rf.outputFiles) > 0 {
		if err := downloadOutputs(c, dispatchResp.MissionID, doneEv.Outputs, rf.outputFiles); err != nil {
			return err
		}
	}

	var capturedStdout, capturedStderr []byte
	if captureOutBuf != nil {
		capturedStdout = captureOutBuf.Bytes()
	}
	if captureErrBuf != nil {
		capturedStderr = captureErrBuf.Bytes()
	}
	if err := printRunResultWithLogs(stdout, doneEv, f, capturedStdout, capturedStderr); err != nil {
		return err
	}

	switch doneEv.Outcome {
	case "success":
		return nil
	case "failed":
		msg := doneEv.FailMessage
		if msg == "" {
			msg = "(no message)"
		}
		return fmt.Errorf("mission failed: %s", msg)
	default:
		// killed | timeout | oom | crashed | lost | "" (no done at all)
		return NewMissionAbnormalError(doneEv.Outcome)
	}
}

// fanOutResult carries one goroutine's worth of run output back to the main
// goroutine in runFanOut. For text/json modes we keep the captured stdout/
// stderr buffers and the typed err to aggregate after the wait. NDJSON skips
// the buffers — events stream through hostPrefixWriter directly during run.
type fanOutResult struct {
	Host   string
	Out    []byte // captured stdout (text/json); empty in ndjson
	Errb   []byte // captured stderr (text only)
	Err    error
	HasErr bool
}

// runFanOut implements `letts run --host=s1,s2,...`. For each host it spins up
// a goroutine that invokes runOne with a host override, then aggregates the
// per-host results into a single CLI response.
//
// Output by format:
//   - text:   per-host blocks separated by "== <host> ==" headers; the existing
//     text body (return JSON on stdout, [event] lines on stderr) goes
//     under the block header on the same channel.
//   - json:   one `{results: [{host, ok, outcome, return, exit_code, ...}, ...]}`
//     object on stdout.
//   - ndjson: each per-host event is rewritten with a top-level "host" key and
//     written to stdout as it arrives. Per-host ordering is preserved;
//     cross-host order is non-deterministic.
//
// Aggregate exit code: worst result wins (success < failed < abnormal). A
// MissionAbnormalError on any host beats a plain "mission failed" on others;
// a BadUsageError/NetworkError propagates as-is.
func runFanOut(cmd *cobra.Command, ac *appCtx, rf *runFlags, hosts []string, f Format) error {
	// Pre-load the input once so N goroutines don't race on os.Stdin for
	// --input-file=-. For non-stdin sources this is just an early validation
	// that surfaces bad input before any HTTP call.
	preLoadedInput, err := loadInput(cmd.InOrStdin(), rf.input, rf.inputFile)
	if err != nil {
		return err
	}

	results := make([]fanOutResult, len(hosts))

	// For ndjson we share stdout with mutex-protected per-event writes so that
	// per-host event order is preserved while goroutines interleave naturally.
	var ndjsonMu sync.Mutex

	var wg sync.WaitGroup
	for i, h := range hosts {
		i, h := i, h
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i].Host = h

			var stdout, stderr io.Writer
			switch f {
			case FormatNDJSON:
				stdout = newHostPrefixWriter(h, cmd.OutOrStdout(), &ndjsonMu)
				stderr = io.Discard
			default:
				var outBuf, errBuf bytes.Buffer
				stdout = &outBuf
				stderr = &errBuf
				defer func() {
					results[i].Out = outBuf.Bytes()
					results[i].Errb = errBuf.Bytes()
				}()
			}

			err := runOne(cmd, ac, rf, h, preLoadedInput, f, stdout, stderr)
			if err != nil {
				results[i].Err = err
				results[i].HasErr = true
			}
		}()
	}
	wg.Wait()

	// Aggregate output per format.
	switch f {
	case FormatJSON:
		if err := writeFanOutJSON(cmd.OutOrStdout(), results); err != nil {
			return err
		}
	case FormatNDJSON:
		// Events already streamed; nothing more to emit on stdout.
	default:
		writeFanOutText(cmd.OutOrStdout(), cmd.ErrOrStderr(), results)
	}

	return fanOutAggregateError(results)
}

// fanOutAggregateError returns the worst-outcome error across all per-host
// results. Precedence (highest wins):
//  1. typed errors that are NOT MissionAbnormal nor "mission failed" (config /
//     network / bad-usage etc.) — propagated as the first such error to avoid
//     swallowing setup failures behind mission outcomes.
//  2. MissionAbnormalError on any host → MissionAbnormalError listing all
//     abnormal hosts (outcome of the first one for code mapping).
//  3. "mission failed" on any host → plain error naming the failing hosts.
//  4. all success → nil.
func fanOutAggregateError(results []fanOutResult) error {
	var setupErrs []fanOutResult
	var abnormal []fanOutResult
	var failed []fanOutResult
	for _, r := range results {
		if !r.HasErr {
			continue
		}
		var ma *MissionAbnormalError
		if errors.As(r.Err, &ma) {
			abnormal = append(abnormal, r)
			continue
		}
		// "mission failed" is a plain fmt.Errorf in runOne — detect by string.
		if strings.HasPrefix(r.Err.Error(), "mission failed") {
			failed = append(failed, r)
			continue
		}
		setupErrs = append(setupErrs, r)
	}
	if len(setupErrs) > 0 {
		return setupErrs[0].Err
	}
	if len(abnormal) > 0 {
		var ma *MissionAbnormalError
		_ = errors.As(abnormal[0].Err, &ma)
		hosts := make([]string, 0, len(abnormal))
		for _, r := range abnormal {
			hosts = append(hosts, r.Host)
		}
		return NewMissionAbnormalError(ma.Outcome + " on " + strings.Join(hosts, ","))
	}
	if len(failed) > 0 {
		hosts := make([]string, 0, len(failed))
		for _, r := range failed {
			hosts = append(hosts, r.Host)
		}
		return fmt.Errorf("mission failed on %s", strings.Join(hosts, ","))
	}
	return nil
}

// writeFanOutText writes per-host blocks. Each block is prefixed with
// "== <host> ==" on stdout; the captured per-host stdout/stderr buffers are
// written immediately after on their respective channels. A trailing
// "[FAIL] <host> — <err>" line is appended to stderr for hosts that errored.
func writeFanOutText(stdout, stderr io.Writer, results []fanOutResult) {
	for _, r := range results {
		_, _ = fmt.Fprintf(stdout, "== %s ==\n", r.Host)
		if len(r.Out) > 0 {
			_, _ = stdout.Write(r.Out)
		}
		if len(r.Errb) > 0 {
			_, _ = fmt.Fprintf(stderr, "== %s ==\n", r.Host)
			_, _ = stderr.Write(r.Errb)
		}
		if r.HasErr {
			_, _ = fmt.Fprintf(stderr, "[FAIL] %s — %v\n", r.Host, r.Err)
		}
	}
}

// writeFanOutJSON emits {"results":[{host,ok,error?,...done fields...},...]}.
// The per-host done payload (outcome, return, exit_code, ...) is recovered by
// re-parsing the captured stdout buffer that runOne produced in json mode.
func writeFanOutJSON(w io.Writer, results []fanOutResult) error {
	type row struct {
		Host    string          `json:"host"`
		OK      bool            `json:"ok"`
		Error   string          `json:"error,omitempty"`
		Outcome string          `json:"outcome,omitempty"`
		Return  json.RawMessage `json:"return,omitempty"`
		ExitVal *int            `json:"exit_code,omitempty"`
		Signal  string          `json:"signal,omitempty"`
		Reason  string          `json:"fail_reason,omitempty"`
		Message string          `json:"fail_message,omitempty"`
		Dur     int64           `json:"duration_ms,omitempty"`
	}
	rows := make([]row, 0, len(results))
	for _, r := range results {
		row := row{Host: r.Host}
		if r.HasErr {
			row.Error = r.Err.Error()
		} else {
			row.OK = true
		}
		// Parse the captured single-host json body when present.
		if len(r.Out) > 0 {
			var parsed struct {
				Outcome     string          `json:"outcome"`
				Return      json.RawMessage `json:"return"`
				ExitCode    *int            `json:"exit_code"`
				Signal      string          `json:"signal"`
				FailReason  string          `json:"fail_reason"`
				FailMessage string          `json:"fail_message"`
				DurationMs  int64           `json:"duration_ms"`
			}
			if err := json.Unmarshal(r.Out, &parsed); err == nil {
				row.Outcome = parsed.Outcome
				row.Return = parsed.Return
				row.ExitVal = parsed.ExitCode
				row.Signal = parsed.Signal
				row.Reason = parsed.FailReason
				row.Message = parsed.FailMessage
				row.Dur = parsed.DurationMs
			}
		}
		rows = append(rows, row)
	}
	return PrintJSON(w, map[string]any{"results": rows})
}

// hostPrefixWriter buffers NDJSON bytes written by runOne, splits on newline,
// rewrites each complete line as `{"host":"<id>",...original fields...}`, and
// writes the rewritten line to the shared target under a mutex so per-host
// event order is preserved while goroutines interleave.
type hostPrefixWriter struct {
	host string
	dst  io.Writer
	mu   *sync.Mutex
	buf  []byte
}

func newHostPrefixWriter(host string, dst io.Writer, mu *sync.Mutex) *hostPrefixWriter {
	return &hostPrefixWriter{host: host, dst: dst, mu: mu}
}

func (w *hostPrefixWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	for {
		idx := bytes.IndexByte(w.buf, '\n')
		if idx < 0 {
			break
		}
		line := w.buf[:idx]
		w.buf = w.buf[idx+1:]
		if len(line) == 0 {
			continue
		}
		if err := w.emitLine(line); err != nil {
			return len(p), err
		}
	}
	return len(p), nil
}

// emitLine wraps one raw NDJSON line with the host field and writes to dst
// under the shared mutex. Non-JSON lines (shouldn't happen from runOne, but
// fall back gracefully) are written verbatim with a host comment prefix.
func (w *hostPrefixWriter) emitLine(line []byte) error {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(line, &m); err != nil {
		// Defensive: forward verbatim so we don't silently drop output.
		// Copy the line then append \n so we don't overwrite w.buf's tail.
		raw := make([]byte, 0, len(line)+1)
		raw = append(raw, line...)
		raw = append(raw, '\n')
		w.mu.Lock()
		defer w.mu.Unlock()
		_, err := w.dst.Write(raw)
		return err
	}
	// Build a deterministic encoding with host first.
	var out bytes.Buffer
	out.WriteByte('{')
	out.WriteString(`"host":`)
	hostJSON, _ := json.Marshal(w.host)
	out.Write(hostJSON)
	for k, v := range m {
		out.WriteByte(',')
		kJSON, _ := json.Marshal(k)
		out.Write(kJSON)
		out.WriteByte(':')
		out.Write(v)
	}
	out.WriteByte('}')
	out.WriteByte('\n')
	w.mu.Lock()
	defer w.mu.Unlock()
	_, err := w.dst.Write(out.Bytes())
	return err
}

// logEventToStderr renders a streamed event as a human-readable line
// (text mode). For JSON/NDJSON modes stderr is suppressed by default,
// so this is a no-op; the --logs flag (which would surface stderr in JSON
// mode) is honored at the caller level (printRunResultWithLogs routes
// return to stdout regardless).
//
// Kept for the fan-out / non-text path that bypasses prefixedStderr (text
// mode with output-tail goroutines uses logEventToStderrPrefixed instead).
func logEventToStderr(w io.Writer, ev lettsclient.Event, f Format, noProgress bool) {
	if f == FormatJSON || f == FormatNDJSON {
		// json/ndjson modes suppress stderr by default; --logs handled in printRunResultWithLogs.
		return
	}
	switch ev.Event {
	case "progress":
		if !noProgress {
			_, _ = fmt.Fprintf(w, "[progress] %.2f %s\n", ev.Value, ev.Message)
		}
	case "running":
		_, _ = fmt.Fprintf(w, "[event] running pid=%d\n", ev.Pid)
	case "queued":
		_, _ = fmt.Fprintf(w, "[event] queued\n")
	case "done":
		_, _ = fmt.Fprintf(w, "[event] done outcome=%s\n", ev.Outcome)
	}
}

// prefixedStderr is a thread-safe stderr writer that emits "[prefix] line\n"
// per call. The mutex guarantees lines from different sources (events,
// stdout tail, stderr tail) never interleave mid-line — without it a
// [stdout] write could land in the middle of a [progress] write.
type prefixedStderr struct {
	mu sync.Mutex
	w  io.Writer
}

func (p *prefixedStderr) line(prefix, msg string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, _ = fmt.Fprintf(p.w, "[%s] %s\n", prefix, msg)
}

// multiPrefixed bundles 1-2 prefixedStderr sinks into a single one whose
// line() method fans out to both. Nil arguments are skipped, so callers
// can pass (consoleSink, nil) for text mode or (nil, captureSink) for
// JSON-without---logs without writing branchy wiring at the call site.
// Returns nil when both inputs are nil so tailMissionStream's nil check
// keeps working.
func multiPrefixed(a, b *prefixedStderr) *prefixedStderr {
	if a == nil && b == nil {
		return nil
	}
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	return &prefixedStderr{w: &teeWriter{a: a.w, b: b.w}}
}

// teeWriter writes every byte to both underlying writers. Errors from the
// secondary write are swallowed — the capture buffer should never fail
// (in-memory), and propagating a console-stderr failure into the JSON
// pipeline would mask the mission outcome.
type teeWriter struct{ a, b io.Writer }

func (t *teeWriter) Write(p []byte) (int, error) {
	n, err := t.a.Write(p)
	_, _ = t.b.Write(p)
	return n, err
}

// captureBuf is an in-memory, mutex-protected, byte-capped buffer used to
// collect mission stdout/stderr for the JSON `logs` field. Writes beyond
// cap are silently dropped (returning len(p) so callers see "success");
// the truncation is invisible in the rendered JSON — readers wanting full
// fidelity should fetch /v1/missions/{id}/output directly.
type captureBuf struct {
	mu  sync.Mutex
	buf []byte
	cap int
}

func newCaptureBuf(cap int) *captureBuf { return &captureBuf{cap: cap} }

func (b *captureBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	room := b.cap - len(b.buf)
	if room <= 0 {
		return len(p), nil
	}
	if len(p) > room {
		b.buf = append(b.buf, p[:room]...)
		return len(p), nil
	}
	b.buf = append(b.buf, p...)
	return len(p), nil
}

func (b *captureBuf) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]byte, len(b.buf))
	copy(out, b.buf)
	return out
}

// tailMissionStream opens /output?stream=which&follow=true and writes each
// complete line through p.line(prefix, …). Returns silently on EOF, context
// cancel, transient error, or 404 (the latter is normal when a mission
// produced no output on that stream). Mission progress doesn't depend on
// the tail, so all errors here are non-fatal.
func tailMissionStream(ctx context.Context, c *lettsclient.Client, missionID, which, prefix string, p *prefixedStderr) {
	rc, err := lettsclient.OpenOutput(ctx, c, missionID, lettsclient.OutputOpts{Stream: which, Follow: true})
	if err != nil {
		// 404 is the normal "mission produced no output on
		// this stream" case and stays silent. Surfacing other errors
		// to stderr lets operators notice when a server-side failure
		// (disk read, transient 5xx, premature 410) silently dropped
		// tail output that they probably wanted to see.
		if he, ok := err.(*lettsclient.HTTPError); ok && he.Status == 404 {
			return
		}
		if ctx.Err() == nil {
			p.line("warning", fmt.Sprintf("%s tail interrupted: %v", which, err))
		}
		return
	}
	defer func() { _ = rc.Close() }()
	scanner := bufio.NewScanner(rc)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		p.line(prefix, scanner.Text())
	}
	if err := scanner.Err(); err != nil && ctx.Err() == nil {
		p.line("warning", fmt.Sprintf("%s tail interrupted: %v", which, err))
	}
}

// logEventToStderrPrefixed mirrors logEventToStderr but writes through a
// prefixedStderr so it serializes with output-tail goroutines.
func logEventToStderrPrefixed(p *prefixedStderr, ev lettsclient.Event, f Format, noProgress bool) {
	if f == FormatJSON || f == FormatNDJSON {
		return
	}
	switch ev.Event {
	case "progress":
		if !noProgress {
			p.line("progress", fmt.Sprintf("%.2f %s", ev.Value, ev.Message))
		}
	case "running":
		p.line("event", fmt.Sprintf("running pid=%d", ev.Pid))
	case "queued":
		p.line("event", "queued")
	case "done":
		p.line("event", fmt.Sprintf("done outcome=%s", ev.Outcome))
	}
}

// printRunResultWithLogs writes the terminal result of a mission to stdout,
// embedding stdout/stderr under the JSON result's `logs` key.
// Layout depends on Format:
//
//   - text: pretty-printed `return` JSON only;
//   - json: single object {outcome, return, exit_code, signal, fail_*, duration_ms, logs:{stdout, stderr}};
//   - ndjson: no-op (events were emitted to stdout during the stream loop).
//
// JSON mode always emits the logs field, with empty strings when capture
// wasn't enabled (--quiet or --output=json without --logs in earlier
// builds — keeping the field-presence contract stable for consumers).
func printRunResultWithLogs(w io.Writer, doneEv lettsclient.Event, f Format, stdoutBytes, stderrBytes []byte) error {
	switch f {
	case FormatJSON:
		obj := map[string]any{
			"outcome":      doneEv.Outcome,
			"return":       doneEv.Return,
			"exit_code":    doneEv.ExitCode,
			"signal":       doneEv.Signal,
			"fail_reason":  doneEv.FailReason,
			"fail_message": doneEv.FailMessage,
			"duration_ms":  doneEv.DurationMs,
			"logs": map[string]string{
				"stdout": string(stdoutBytes),
				"stderr": string(stderrBytes),
			},
		}
		return PrintJSON(w, obj)
	case FormatNDJSON:
		// In ndjson mode every event was already emitted to stdout during
		// the stream loop; printRunResult is a no-op.
		return nil
	default:
		// text — pretty-print return JSON.
		if len(doneEv.Return) > 0 {
			var v any
			if err := json.Unmarshal(doneEv.Return, &v); err == nil {
				return PrintJSON(w, v)
			}
			_, _ = w.Write(doneEv.Return)
			_, _ = io.WriteString(w, "\n")
		}
		return nil
	}
}

// downloadOutputs realises --output-file=role=path for a finished mission.
//
// The done event carries outputs as a
// map[role]{staging_id, sha256, size}, so we can pull bytes via
// GET /v1/staging/{id} directly without a follow-up GET /v1/missions/{id}.
// Each role is streamed straight to disk; the first failure aborts and
// returns the wrapped error (later pairs are not attempted).
func downloadOutputs(c *lettsclient.Client, missionID string, outs map[string]lettsclient.EventOutput, pairs []string) error {
	if len(pairs) == 0 {
		return nil
	}
	for _, p := range pairs {
		idx := strings.Index(p, "=")
		if idx < 1 {
			return NewBadUsageError("--output-file expects role=path, got " + p)
		}
		role, dst := p[:idx], p[idx+1:]
		out, ok := outs[role]
		if !ok {
			return fmt.Errorf("no output with role %q in mission %s", role, missionID)
		}
		if err := downloadOneOutput(c, role, dst, out); err != nil {
			return err
		}
	}
	return nil
}

// downloadOneOutput streams a single output role to dst atomically: bytes go to
// a sidecar tmp in the destination directory, are verified against the done
// event's sha256/size, fsync'd, then promoted via os.Rename (atomic; overwrites
// an existing dst as os.Create did). Any failure removes the tmp and leaves dst
// untouched, so a network error or corrupt transfer never yields a
// truncated/wrong file at the final path.
func downloadOneOutput(c *lettsclient.Client, role, dst string, out lettsclient.EventOutput) error {
	rc, _, err := lettsclient.GetStaging(c, out.StagingID, "")
	if err != nil {
		return fmt.Errorf("download role=%s: %w", role, err)
	}
	defer func() { _ = rc.Close() }()

	tmp, err := os.CreateTemp(filepath.Dir(dst), filepath.Base(dst)+".tmp.*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }

	hasher := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(tmp, hasher), rc)
	if copyErr != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("download role=%s: %w", role, copyErr)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return err
	}

	// Integrity checks against the done event's declared sha256/size before
	// promoting. A mismatch means a corrupt/short transfer — fail without
	// leaving the bad bytes at dst.
	if out.Size > 0 && n != out.Size {
		cleanup()
		return fmt.Errorf("download role=%s: size mismatch (got %d, want %d)", role, n, out.Size)
	}
	if out.SHA256 != "" {
		if got := hex.EncodeToString(hasher.Sum(nil)); got != out.SHA256 {
			cleanup()
			return fmt.Errorf("download role=%s: sha256 mismatch (got %s, want %s)", role, got, out.SHA256)
		}
	}

	if err := os.Rename(tmpName, dst); err != nil {
		cleanup()
		return fmt.Errorf("promote role=%s: %w", role, err)
	}
	return nil
}
