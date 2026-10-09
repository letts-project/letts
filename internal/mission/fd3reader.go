package mission

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	"letts/internal/config"
)

// Fd3State accumulates everything the finalizer needs from the fd3 channel
// after the reader returns. All fields are guarded by mu while the reader is
// running; once ReadFd3 returns, callers can read without locking.
type Fd3State struct {
	mu sync.Mutex

	// Final is the terminal success/fail event, set at most once. A second
	// success/fail line records "duplicate_final_event" instead of overwriting.
	Final *Fd3Final
	// OutputFiles collects declared output_file keys (set semantics).
	OutputFiles map[string]struct{}
	// Violations is the (ordered) list of protocol violations observed,
	// capped at maxRecordedViolations — outcome classification only ever
	// consults the earliest entries.
	Violations []Fd3Violation
	// ProgressDrops counts progress events dropped because progressCh was full.
	ProgressDrops int64

	// finalLine is the fd3 line number Final was read from.
	finalLine int64
}

// Fd3Violation is one fd3 protocol violation. Reason is the fail_reason it
// classifies as; Message and Details become fail_message and fail_details.
type Fd3Violation struct {
	Reason  string
	Message string
	Details json.RawMessage
}

// Fd3Final is the terminal event from the mission process.
type Fd3Final struct {
	Kind     string          // "success" | "fail"
	Return   json.RawMessage // for success
	Message  string          // for fail
	Reason   string          // for fail
	Details  json.RawMessage // for fail
	ExitHint int             // for fail (default 1 if absent)
}

// Fd3Limits carries config-derived bounds applied by the reader.
type Fd3Limits struct {
	MaxEventLineSize     int64 // 0 = no cap
	MaxOutputFilesPerMsn int   // 0 = no cap
	MaxProgressRate      int   // applied by the writer, not here
	ProgressBufferSize   int64 // applied by the writer, not here
}

// ProgressEvent flows from the reader goroutine to the writer goroutine.
type ProgressEvent struct {
	Value   *float64
	Message string
}

// ReadFd3 runs the reader loop, returning when r reaches EOF or ctx is done.
// progressCh is closed before return; state is fully populated.
func ReadFd3(ctx context.Context, r io.Reader, limits Fd3Limits, progressCh chan<- ProgressEvent, state *Fd3State) {
	defer close(progressCh)

	br := bufio.NewReaderSize(r, 64*1024)
	var lineNo int64
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		lineNo++
		l := readLineBounded(br, limits.MaxEventLineSize)
		if l.oversized {
			state.recordViolation(lineTooLargeViolation(l, lineNo, limits.MaxEventLineSize))
		} else if len(l.data) > 0 {
			handleLine(l.data, lineNo, limits, progressCh, state)
		}
		if l.eof {
			return
		}
	}
}

// oversizedHeadBytes is how much of an oversized line readLineBounded keeps.
const oversizedHeadBytes = 512

// boundedLine is one fd3 line as returned by readLineBounded.
type boundedLine struct {
	data      []byte // line content without the newline; nil when oversized
	size      int64  // full line length in bytes, without the newline
	oversized bool
	head      []byte // copy of the first oversizedHeadBytes of an oversized line
	eof       bool
}

// readLineBounded reads one logical line from br, capping accumulated size at
// maxSize. If maxSize>0 and the line would exceed it, the function still
// drains all chunks up to the next newline, returning oversized=true, a nil
// data and the line's head. Returns eof=true once the reader is exhausted.
func readLineBounded(br *bufio.Reader, maxSize int64) (l boundedLine) {
	for {
		chunk, isPrefix, err := br.ReadLine()
		l.size += int64(len(chunk))
		if !l.oversized && len(chunk) > 0 {
			if maxSize > 0 && int64(len(l.data))+int64(len(chunk)) > maxSize {
				l.oversized = true
				l.head = make([]byte, 0, oversizedHeadBytes)
				l.head = append(l.head, l.data[:min(len(l.data), oversizedHeadBytes)]...)
				l.head = append(l.head, chunk[:min(len(chunk), oversizedHeadBytes-len(l.head))]...)
				l.data = nil
			} else {
				l.data = append(l.data, chunk...)
			}
		}
		if !isPrefix {
			if err != nil {
				l.eof = true
			}
			return
		}
		if err != nil {
			l.eof = true
			return
		}
	}
}

// sniffEventKind returns the top-level "event" value of a JSON object prefix
// that may be truncated, when it is a known event kind; otherwise "".
func sniffEventKind(head []byte) string {
	dec := json.NewDecoder(bytes.NewReader(head))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return ""
	}
	for dec.More() {
		key, err := dec.Token()
		if err != nil {
			return ""
		}
		if key != "event" {
			var skip json.RawMessage
			if err := dec.Decode(&skip); err != nil {
				return ""
			}
			continue
		}
		val, err := dec.Token()
		if err != nil {
			return ""
		}
		switch val {
		case "progress", "output_file", "success", "fail":
			return val.(string)
		}
		return ""
	}
	return ""
}

func handleLine(line []byte, lineNo int64, limits Fd3Limits, progressCh chan<- ProgressEvent, state *Fd3State) {
	var head struct {
		Event string `json:"event"`
	}
	if err := json.Unmarshal(line, &head); err != nil {
		state.recordViolation(undecodableLineViolation(line, lineNo, err))
		return
	}
	switch head.Event {
	case "progress":
		var ev struct {
			Value   *float64 `json:"value"`
			Message string   `json:"message"`
		}
		if err := json.Unmarshal(line, &ev); err != nil {
			state.recordViolation(schemaViolation("progress", lineNo, err))
			return
		}
		select {
		case progressCh <- ProgressEvent{Value: ev.Value, Message: ev.Message}:
		default:
			state.mu.Lock()
			state.ProgressDrops++
			state.mu.Unlock()
		}
	case "output_file":
		var ev struct {
			Key string `json:"key"`
		}
		if err := json.Unmarshal(line, &ev); err != nil {
			state.recordViolation(schemaViolation("output_file", lineNo, err))
			return
		}
		// Key regex ^[A-Za-z_][A-Za-z0-9_]{0,63}$ plus
		// reserved __ prefix. Without this guard a key like
		// "a/b" leaks past the v1 no-subdirectory contract because
		// openat(O_NOFOLLOW) only protects the final path component;
		// the slash also propagates into LETTS_IN_<role>=... env on the
		// next mission that refs this output.
		if err := config.ValidateRoleKey(ev.Key); err != nil {
			state.recordViolation(protocolViolation(
				fmt.Sprintf("%s: %s", fd3Subject("output_file", lineNo), clipUTF8(err.Error(), maxErrorTextBytes)),
				fd3ViolationDetails{Fd3Line: lineNo, Event: "output_file", Key: clipUTF8(ev.Key, maxEchoBytes)}))
			return
		}
		state.mu.Lock()
		if state.OutputFiles == nil {
			state.OutputFiles = map[string]struct{}{}
		}
		if _, exists := state.OutputFiles[ev.Key]; !exists {
			if limits.MaxOutputFilesPerMsn > 0 && len(state.OutputFiles) >= limits.MaxOutputFilesPerMsn {
				state.recordViolationLocked(Fd3Violation{
					Reason: "too_many_output_files",
					Message: fmt.Sprintf("%s declares key %q beyond max_output_files_per_mission (%d)",
						fd3Subject("output_file", lineNo), ev.Key, limits.MaxOutputFilesPerMsn),
					Details: fd3ViolationDetails{Fd3Line: lineNo, Event: "output_file", Key: ev.Key,
						MaxOutputFiles: limits.MaxOutputFilesPerMsn}.raw(),
				})
				state.mu.Unlock()
				return
			}
			state.OutputFiles[ev.Key] = struct{}{}
		}
		state.mu.Unlock()
	case "success":
		var ev struct {
			Return json.RawMessage `json:"return"`
		}
		// A type mismatch on any typed field is a schema violation, same as
		// unparseable JSON — a malformed final must never half-populate a
		// Final. (The RawMessage capture itself can't fail, but the check
		// keeps this branch symmetric with fail's typed fields.)
		if err := json.Unmarshal(line, &ev); err != nil {
			state.recordViolation(schemaViolation("success", lineNo, err))
			return
		}
		// Return must be object or null — array/scalar
		// is a protocol violation. Inspect the first non-whitespace byte to
		// classify; missing/empty return is treated as null (acceptable).
		if !isObjectOrNull(ev.Return) {
			state.recordViolation(notObjectViolation("success", "return", ev.Return, lineNo))
			return
		}
		state.setFinal(&Fd3Final{Kind: "success", Return: ev.Return}, lineNo)
	case "fail":
		var ev struct {
			Message  string          `json:"message"`
			Reason   string          `json:"reason"`
			Details  json.RawMessage `json:"details"`
			ExitCode *int            `json:"exit_code"`
		}
		// A fail line that doesn't match the schema (e.g. exit_code as a
		// string) classifies exactly like other schema violations — taking
		// the partially-filled struct instead would commit a half-populated
		// final to the mission row.
		if err := json.Unmarshal(line, &ev); err != nil {
			state.recordViolation(schemaViolation("fail", lineNo, err))
			return
		}
		// details, like success.return, must be a JSON object or null:
		// downstream consumers type fail_details as a nullable map, and a
		// scalar/array here would propagate verbatim into the public done
		// event and the DB, breaking typed clients.
		if !isObjectOrNull(ev.Details) {
			state.recordViolation(notObjectViolation("fail", "details", ev.Details, lineNo))
			return
		}
		f := &Fd3Final{Kind: "fail", Message: ev.Message, Reason: ev.Reason, Details: ev.Details, ExitHint: 1}
		if ev.ExitCode != nil {
			f.ExitHint = *ev.ExitCode
		}
		state.setFinal(f, lineNo)
	default:
		state.recordViolation(unknownEventViolation(head.Event, lineNo))
	}
}

// isObjectOrNull reports whether b is empty, JSON null, or a JSON object.
// Enforces the final-event field schema: success.return and fail.details
// must both be object or null — array/string/number/bool are protocol
// violations. Whitespace is tolerated before the first significant byte.
func isObjectOrNull(b []byte) bool {
	for i, c := range b {
		switch c {
		case ' ', '\t', '\n', '\r':
			continue
		case '{':
			return true
		case 'n':
			// "null" — accept the bare token without re-parsing.
			rest := b[i:]
			return len(rest) >= 4 && string(rest[:4]) == "null"
		default:
			return false
		}
	}
	// Empty / whitespace-only counts as omitted → treat as null.
	return true
}

// maxRecordedViolations caps Fd3State.Violations. The cap is safe because
// outcome classification (Compute) only ever consults the earliest relevant
// entries — later duplicates add no information. Without it, the reader's
// deliberate drain-everything stance becomes a memory hazard: a mission that
// accidentally pipes a data stream into fd 3 appends one violation per newline
// for its whole lifetime. The reader must stay O(1) memory regardless of how
// much garbage fd3 delivers, so once full, further records are dropped.
const maxRecordedViolations = 32

func (s *Fd3State) recordViolation(v Fd3Violation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recordViolationLocked(v)
}

// recordViolationLocked is the single append path for violations; the caller
// must hold s.mu. Every violation source routes through here so any future
// one is bounded by construction.
func (s *Fd3State) recordViolationLocked(v Fd3Violation) {
	if len(s.Violations) >= maxRecordedViolations {
		return
	}
	s.Violations = append(s.Violations, v)
}

func (s *Fd3State) setFinal(f *Fd3Final, lineNo int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Final != nil {
		s.recordViolationLocked(Fd3Violation{
			Reason: "duplicate_final_event",
			Message: fmt.Sprintf("%s is a second final event after the %s event on line %d",
				fd3Subject(f.Kind, lineNo), s.Final.Kind, s.finalLine),
			Details: fd3ViolationDetails{Fd3Line: lineNo, Event: f.Kind,
				FirstEvent: s.Final.Kind, FirstFd3Line: s.finalLine}.raw(),
		})
		return
	}
	s.Final = f
	s.finalLine = lineNo
}

// Mission-supplied strings echoed in violation messages and details are
// clipped to maxEchoBytes and error texts to maxErrorTextBytes, so a violation
// stays small whatever the offending line holds.
const (
	maxEchoBytes      = 128
	maxErrorTextBytes = 256
)

// fd3ViolationDetails is the fail_details object of an fd3 violation.
type fd3ViolationDetails struct {
	Fd3Line          int64  `json:"fd3_line"`
	LineBytes        int64  `json:"line_bytes,omitempty"`
	MaxEventLineSize int64  `json:"max_event_line_size,omitempty"`
	Event            string `json:"event,omitempty"`
	Key              string `json:"key,omitempty"`
	MaxOutputFiles   int    `json:"max_output_files_per_mission,omitempty"`
	FirstEvent       string `json:"first_event,omitempty"`
	FirstFd3Line     int64  `json:"first_fd3_line,omitempty"`
}

func (d fd3ViolationDetails) raw() json.RawMessage {
	b, _ := json.Marshal(d)
	return b
}

// fd3Subject names an fd3 line in violation messages: "fd3 line N", or
// "fd3 <kind> event (line N)" when the event kind is known.
func fd3Subject(kind string, lineNo int64) string {
	if kind == "" {
		return fmt.Sprintf("fd3 line %d", lineNo)
	}
	return fmt.Sprintf("fd3 %s event (line %d)", kind, lineNo)
}

func protocolViolation(msg string, d fd3ViolationDetails) Fd3Violation {
	return Fd3Violation{Reason: "event_protocol_error", Message: msg, Details: d.raw()}
}

func lineTooLargeViolation(l boundedLine, lineNo, maxSize int64) Fd3Violation {
	kind := sniffEventKind(l.head)
	msg := fmt.Sprintf("%s is %d bytes, exceeds max_event_line_size (%d bytes)", fd3Subject(kind, lineNo), l.size, maxSize)
	if kind == "success" {
		msg += "; return less data or write it to an output file"
	}
	return Fd3Violation{
		Reason:  "event_line_too_large",
		Message: msg,
		Details: fd3ViolationDetails{Fd3Line: lineNo, LineBytes: l.size, MaxEventLineSize: maxSize, Event: kind}.raw(),
	}
}

func undecodableLineViolation(line []byte, lineNo int64, err error) Fd3Violation {
	var syntaxErr *json.SyntaxError
	if errors.As(err, &syntaxErr) {
		return protocolViolation(fmt.Sprintf("%s is not valid JSON: %s; line starts with %q",
			fd3Subject("", lineNo), clipUTF8(err.Error(), maxErrorTextBytes), clipUTF8(line, maxEchoBytes)),
			fd3ViolationDetails{Fd3Line: lineNo})
	}
	return protocolViolation(fmt.Sprintf("%s does not match the event schema: %s", fd3Subject("", lineNo), describeDecodeError(err)),
		fd3ViolationDetails{Fd3Line: lineNo})
}

func schemaViolation(kind string, lineNo int64, err error) Fd3Violation {
	return protocolViolation(fmt.Sprintf("%s does not match the schema: %s", fd3Subject(kind, lineNo), describeDecodeError(err)),
		fd3ViolationDetails{Fd3Line: lineNo, Event: kind})
}

func notObjectViolation(kind, field string, value []byte, lineNo int64) Fd3Violation {
	return protocolViolation(fmt.Sprintf("%s: %q must be a JSON object or null, got %s", fd3Subject(kind, lineNo), field, jsonValueKind(value)),
		fd3ViolationDetails{Fd3Line: lineNo, Event: kind})
}

func unknownEventViolation(name string, lineNo int64) Fd3Violation {
	msg := fmt.Sprintf(`%s has a missing or empty "event" field`, fd3Subject("", lineNo))
	if name != "" {
		msg = fmt.Sprintf("%s has unknown event %q (want progress, output_file, success or fail)", fd3Subject("", lineNo), clipUTF8(name, maxEchoBytes))
	}
	return protocolViolation(msg, fd3ViolationDetails{Fd3Line: lineNo})
}

// describeDecodeError renders a json.Unmarshal error of an fd3 line as a
// short single-line text.
func describeDecodeError(err error) string {
	var typeErr *json.UnmarshalTypeError
	if errors.As(err, &typeErr) {
		if typeErr.Field == "" {
			return "expected a JSON object, got " + clipUTF8(typeErr.Value, maxEchoBytes)
		}
		return fmt.Sprintf("field %q must be %s, got %s", typeErr.Field, typeErr.Type, clipUTF8(typeErr.Value, maxEchoBytes))
	}
	return clipUTF8(err.Error(), maxErrorTextBytes)
}

// jsonValueKind names the JSON type of a valid JSON value that is not an
// object or null.
func jsonValueKind(b []byte) string {
	b = bytes.TrimLeft(b, " \t\r\n")
	if len(b) == 0 {
		return "empty"
	}
	switch b[0] {
	case '[':
		return "array"
	case '"':
		return "string"
	case 't', 'f':
		return "boolean"
	}
	return "number"
}
