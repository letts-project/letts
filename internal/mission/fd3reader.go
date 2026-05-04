package mission

import (
	"bufio"
	"context"
	"encoding/json"
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
	Violations []string
	// ProgressDrops counts progress events dropped because progressCh was full.
	ProgressDrops int64
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
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		line, oversized, eof := readLineBounded(br, limits.MaxEventLineSize)
		if oversized {
			state.recordViolation("event_line_too_large")
		} else if len(line) > 0 {
			handleLine(line, limits, progressCh, state)
		}
		if eof {
			return
		}
	}
}

// readLineBounded reads one logical line from br, capping accumulated size at
// maxSize. If maxSize>0 and the line would exceed it, the function still
// drains all chunks up to the next newline, returning oversized=true and
// line=nil. Returns eof=true once the reader is exhausted.
func readLineBounded(br *bufio.Reader, maxSize int64) (line []byte, oversized, eof bool) {
	for {
		chunk, isPrefix, err := br.ReadLine()
		if !oversized && len(chunk) > 0 {
			if maxSize > 0 && int64(len(line))+int64(len(chunk)) > maxSize {
				oversized = true
				line = nil
			} else {
				line = append(line, chunk...)
			}
		}
		if !isPrefix {
			if err != nil {
				eof = true
			}
			return
		}
		if err != nil {
			eof = true
			return
		}
	}
}

func handleLine(line []byte, limits Fd3Limits, progressCh chan<- ProgressEvent, state *Fd3State) {
	var head struct {
		Event string `json:"event"`
	}
	if err := json.Unmarshal(line, &head); err != nil {
		state.recordViolation("event_protocol_error")
		return
	}
	switch head.Event {
	case "progress":
		var ev struct {
			Value   *float64 `json:"value"`
			Message string   `json:"message"`
		}
		if err := json.Unmarshal(line, &ev); err != nil {
			state.recordViolation("event_protocol_error")
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
			state.recordViolation("event_protocol_error")
			return
		}
		// Key regex ^[A-Za-z_][A-Za-z0-9_]{0,63}$ plus
		// reserved __ prefix. Without this guard a key like
		// "a/b" leaks past the v1 no-subdirectory contract because
		// openat(O_NOFOLLOW) only protects the final path component;
		// the slash also propagates into LETTS_IN_<role>=... env on the
		// next mission that refs this output.
		if err := config.ValidateRoleKey(ev.Key); err != nil {
			state.recordViolation("event_protocol_error")
			return
		}
		state.mu.Lock()
		if state.OutputFiles == nil {
			state.OutputFiles = map[string]struct{}{}
		}
		if _, exists := state.OutputFiles[ev.Key]; !exists {
			if limits.MaxOutputFilesPerMsn > 0 && len(state.OutputFiles) >= limits.MaxOutputFilesPerMsn {
				state.recordViolationLocked("too_many_output_files")
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
			state.recordViolation("event_protocol_error")
			return
		}
		// Return must be object or null — array/scalar
		// is a protocol violation. Inspect the first non-whitespace byte to
		// classify; missing/empty return is treated as null (acceptable).
		if !isObjectOrNull(ev.Return) {
			state.recordViolation("event_protocol_error")
			return
		}
		state.setFinal(&Fd3Final{Kind: "success", Return: ev.Return})
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
			state.recordViolation("event_protocol_error")
			return
		}
		// details, like success.return, must be a JSON object or null:
		// downstream consumers type fail_details as a nullable map, and a
		// scalar/array here would propagate verbatim into the public done
		// event and the DB, breaking typed clients.
		if !isObjectOrNull(ev.Details) {
			state.recordViolation("event_protocol_error")
			return
		}
		f := &Fd3Final{Kind: "fail", Message: ev.Message, Reason: ev.Reason, Details: ev.Details, ExitHint: 1}
		if ev.ExitCode != nil {
			f.ExitHint = *ev.ExitCode
		}
		state.setFinal(f)
	default:
		state.recordViolation("event_protocol_error")
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
// accidentally pipes a data stream into fd 3 appends one string per newline
// for its whole lifetime. The reader must stay O(1) memory regardless of how
// much garbage fd3 delivers, so once full, further records are dropped.
const maxRecordedViolations = 32

func (s *Fd3State) recordViolation(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recordViolationLocked(name)
}

// recordViolationLocked is the single append path for violations; the caller
// must hold s.mu. Every violation source routes through here so any future
// one is bounded by construction.
func (s *Fd3State) recordViolationLocked(name string) {
	if len(s.Violations) >= maxRecordedViolations {
		return
	}
	s.Violations = append(s.Violations, name)
}

func (s *Fd3State) setFinal(f *Fd3Final) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Final != nil {
		s.recordViolationLocked("duplicate_final_event")
		return
	}
	s.Final = f
}
