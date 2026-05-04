package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"

	"letts/pkg/lettsclient"
)

// prefixedSink writes per-host lines to a single underlying writer,
// serialised via a mutex. Each per-host WriterFor returns an io.Writer
// that buffers partial lines until a newline arrives, then emits
// "[host][label] line\n" atomically through the sink mutex. The two-mutex
// design (per-writer and sink) lets each host accumulate its own partial
// buffer without contending with siblings, while still serialising the
// final byte write to the shared sink.
type prefixedSink struct {
	mu sync.Mutex
	w  io.Writer
}

// newPrefixedSink wraps an io.Writer (typically cmd.OutOrStdout() or
// cmd.ErrOrStderr()) in a sink suitable for fan-out prefix mode.
func newPrefixedSink(w io.Writer) *prefixedSink { return &prefixedSink{w: w} }

// WriterFor returns a per-host io.Writer. host appears as "[host] ";
// label (e.g. "[stderr]") is interpolated as "[host]<label> " when set.
func (s *prefixedSink) WriterFor(host, label string) io.Writer {
	return &prefixedHostWriter{sink: s, host: host, label: label}
}

// prefixedHostWriter accumulates bytes for one host until a newline
// arrives, then drains complete lines through the sink mutex with the
// host prefix. Multiple goroutines may write concurrently on the same
// per-host writer (defensive: not expected in fan-out, but the inner mu
// keeps Write reentrant-safe).
type prefixedHostWriter struct {
	sink    *prefixedSink
	host    string
	label   string
	mu      sync.Mutex
	partial bytes.Buffer
}

// Write appends p to the per-host partial buffer, then drains every
// complete line ("[host][label] line\n") atomically through the sink
// mutex. Returns len(p) and nil — partial writes are never reported,
// so callers see "success" once bytes are accepted into the buffer.
func (w *prefixedHostWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, _ = w.partial.Write(p)
	for {
		b := w.partial.Bytes()
		nl := bytes.IndexByte(b, '\n')
		if nl < 0 {
			break
		}
		line := b[:nl]
		// Build "[host][label] line\n" once, write atomically under sink mu.
		var sb strings.Builder
		sb.WriteString("[")
		sb.WriteString(w.host)
		sb.WriteString("]")
		if w.label != "" {
			sb.WriteString(w.label)
		}
		sb.WriteString(" ")
		sb.Write(line)
		sb.WriteByte('\n')
		w.sink.mu.Lock()
		_, _ = w.sink.w.Write([]byte(sb.String()))
		w.sink.mu.Unlock()
		// Advance partial buffer past this line + newline byte.
		w.partial.Next(nl + 1)
	}
	return len(p), nil
}

// ndjsonSink wraps a mutex-protected writer; one JSON line per Emit call
// is the contract. The sink holds the only mutex that touches the
// underlying writer, so live event taps and per-host output wrappers can
// race-safely share one cmd.OutOrStdout() destination.
type ndjsonSink struct {
	mu sync.Mutex
	w  io.Writer
}

// newNDJSONSink wraps an io.Writer (typically cmd.OutOrStdout()) into a
// race-safe NDJSON emitter for fan-out ndjson mode.
func newNDJSONSink(w io.Writer) *ndjsonSink { return &ndjsonSink{w: w} }

// Emit marshals obj as a single JSON line (terminated by '\n') and writes
// it atomically to the underlying writer under the sink mutex. Returns the
// first error from json.Marshal or the underlying Write.
func (s *ndjsonSink) Emit(obj any) error {
	b, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	s.mu.Lock()
	_, err = s.w.Write(b)
	s.mu.Unlock()
	return err
}

// ndjsonOutputWriter wraps remote stdout/stderr bytes into {host, stream,
// line} NDJSON envelopes. Bytes are buffered until a newline arrives,
// then drained one envelope per complete line through the sink mutex.
// Mirrors prefixedHostWriter's partial-line buffering but emits structured
// JSON instead of "[host] line".
type ndjsonOutputWriter struct {
	sink    *ndjsonSink
	host    string
	stream  string // "stdout" | "stderr"
	mu      sync.Mutex
	partial bytes.Buffer
}

// Write appends p to the per-stream partial buffer, then emits one
// {host, stream, line} envelope per complete line. Returns len(p) and nil
// — partial writes are never reported, mirroring prefixedHostWriter so
// callers see "success" once bytes are accepted into the buffer.
func (w *ndjsonOutputWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, _ = w.partial.Write(p)
	for {
		b := w.partial.Bytes()
		nl := bytes.IndexByte(b, '\n')
		if nl < 0 {
			break
		}
		_ = w.sink.Emit(map[string]any{
			"host":   w.host,
			"stream": w.stream,
			"line":   string(b[:nl]),
		})
		w.partial.Next(nl + 1)
	}
	return len(p), nil
}

// execJSONResultSuccess and execJSONResultError are the sum-type variants
// emitted in the aggregate JSON envelope. Encoder emits only set fields
// via omitempty; the "error" / "outcome" fields are mutually exclusive — a
// row is either one or the other, never both. Consumers must dispatch on
// the presence of "error" to decide which schema applies.
type execJSONResultSuccess struct {
	Host            string `json:"host"`
	ExecID          string `json:"exec_id"`
	Outcome         string `json:"outcome"`
	ExitCode        int    `json:"exit_code"`
	Signal          string `json:"signal,omitempty"`
	DurationMs      int64  `json:"duration_ms,omitempty"`
	Stdout          string `json:"stdout"`
	Stderr          string `json:"stderr"`
	StdoutTruncated bool   `json:"stdout_truncated,omitempty"`
	StderrTruncated bool   `json:"stderr_truncated,omitempty"`
}

// execJSONResultError is the error variant of the sum type. Error tag is
// the classification name (transport | unauthorized | bad_request | config
// | wait_timeout); HTTPStatus and ErrorCode are populated when the source
// of the error is an HTTP non-2xx response.
type execJSONResultError struct {
	Host       string `json:"host"`
	Error      string `json:"error"`
	Message    string `json:"message"`
	HTTPStatus int    `json:"http_status,omitempty"`
	ErrorCode  string `json:"error_code,omitempty"`
}

// writeExecFanOutJSON emits the aggregate {group_id, results: [...]} object
// after all per-host goroutines have completed. Each row is one of
// execJSONResultSuccess (DoneEv reached) or execJSONResultError
// (pre-terminal failure). group_id is encoded as explicit JSON null when
// empty so consumers can distinguish "no group" from "missing field".
func writeExecFanOutJSON(w io.Writer, groupID string, results []execFanOutResult) error {
	rows := make([]any, len(results))
	for i, r := range results {
		if r.HasErr {
			rows[i] = errorRowFromResult(r)
		} else {
			rows[i] = successRowFromResult(r)
		}
	}
	body := map[string]any{
		"group_id": nullIfEmpty(groupID),
		"results":  rows,
	}
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	return enc.Encode(body)
}

// nullIfEmpty returns nil (which encodes as JSON null) when s is "", else s.
// Used so the aggregate body emits explicit null for absent group_id rather
// than omitting the field or emitting an empty string.
func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// classifyExecErr maps any error produced by the exec pipeline into the
// fan-out error taxonomy. Returned values are: errKind (one of
// "bad_request" | "wait_timeout" | "unauthorized" | "transport" | "config"),
// httpStatus (0 if no HTTP layer was involved), errorCode (the server's
// machine code, "" if absent).
//
// Order matters: BadUsage / WaitTimeout come first because wrapExecTransport
// (exec_exit.go) explicitly skips wrapping those types — they reach here
// un-wrapped. Everything else may be wrapped in *ExecTransportError, so we
// unwrap to *lettsclient.HTTPError to enrich with status and code, then map
// 401 to "unauthorized". Legacy typed errors (AuthError / NetworkError /
// ConfigError) kept as final fallback for forward compatibility.
//
// Shared between errorRowFromResult (JSON aggregate) and classifyError
// (NDJSON envelope) so the two code paths can't drift on classification.
func classifyExecErr(err error) (errKind string, httpStatus int, errorCode string) {
	var bu *BadUsageError
	var wt *WaitTimeoutError
	switch {
	case errors.As(err, &bu):
		return "bad_request", 0, ""
	case errors.As(err, &wt):
		return "wait_timeout", 0, ""
	}
	var he *lettsclient.HTTPError
	if errors.As(err, &he) {
		switch {
		case he.Status == 401 || he.Status == 403:
			// Groups 401 and 403 under AuthException; 403
			// (kind isolation / exec disabled) was mislabelled bad_request.
			return "unauthorized", he.Status, he.Code
		case he.Status >= 400 && he.Status < 500:
			return "bad_request", he.Status, he.Code
		default:
			return "transport", he.Status, he.Code
		}
	}
	var auth *AuthError
	var nerr *NetworkError
	var cerr *ConfigError
	switch {
	case errors.As(err, &auth):
		return "unauthorized", auth.Status, ""
	case errors.As(err, &nerr):
		return "transport", 0, ""
	case errors.As(err, &cerr):
		return "config", 0, ""
	}
	return "transport", 0, ""
}

// errorRowFromResult classifies r.Err into the fan-out error taxonomy via
// the shared classifyExecErr helper. See classifyExecErr for the
// classification rules.
func errorRowFromResult(r execFanOutResult) execJSONResultError {
	kind, status, code := classifyExecErr(r.Err)
	return execJSONResultError{
		Host:       r.Host,
		Error:      kind,
		Message:    r.Err.Error(),
		HTTPStatus: status,
		ErrorCode:  code,
	}
}

// classifyError populates the given NDJSON envelope map with error
// classification fields. Uses the shared classifyExecErr helper so the
// JSON aggregate and NDJSON live stream can never disagree on what e.g. a
// 401 HTTPError means.
//
// The envelope receives:
//   - "error": the classification name (transport | unauthorized | bad_request
//     | wait_timeout | config)
//   - "http_status": the inner HTTP status if any (omitted when 0)
//   - "error_code": the server-supplied machine code (omitted when "")
func classifyError(err error, m map[string]any) {
	kind, status, code := classifyExecErr(err)
	m["error"] = kind
	if status != 0 {
		m["http_status"] = status
	}
	if code != "" {
		m["error_code"] = code
	}
}

// successRowFromResult assembles a success row from the captured done event
// plus the buffered stdout/stderr. Dereferences r.DoneEv.ExitCode (*int)
// defensively — a missing exit_code on a done event is treated as 0.
// r.DoneEv must be non-nil; callers route HasErr=true rows to
// errorRowFromResult before reaching here.
func successRowFromResult(r execFanOutResult) execJSONResultSuccess {
	if r.DoneEv == nil {
		return execJSONResultSuccess{Host: r.Host, ExecID: r.ExecID}
	}
	exitCode := 0
	if r.DoneEv.ExitCode != nil {
		exitCode = *r.DoneEv.ExitCode
	}
	return execJSONResultSuccess{
		Host:            r.Host,
		ExecID:          r.ExecID,
		Outcome:         r.DoneEv.Outcome,
		ExitCode:        exitCode,
		Signal:          r.DoneEv.Signal,
		DurationMs:      r.DoneEv.DurationMs,
		Stdout:          string(r.Stdout),
		Stderr:          string(r.Stderr),
		StdoutTruncated: r.StdoutTruncated,
		StderrTruncated: r.StderrTruncated,
	}
}
