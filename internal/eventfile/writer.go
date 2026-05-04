// Package eventfile implements per-mission NDJSON event file writing and reading.
package eventfile

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"

	"letts/internal/fsutil"
)

// EventKind identifies the type of an event.
type EventKind string

const (
	KindQueued   EventKind = "queued"
	KindRunning  EventKind = "running"
	KindProgress EventKind = "progress"
	KindDone     EventKind = "done"
)

// isLifecycle reports whether the kind bypasses the accumulated-buffer cap.
func isLifecycle(k EventKind) bool {
	return k == KindQueued || k == KindRunning || k == KindDone
}

// Limits controls size caps for the events file.
type Limits struct {
	// MaxEventsBuffer is the total accumulated size cap for progress events.
	// Once the buffer bytes written for progress events exceed this value, further
	// progress events are dropped silently.  Lifecycle/done events bypass this.
	// Zero means no cap.
	MaxEventsBuffer int64

	// MaxEventLineSize caps the encoded size (bytes) of each individual event
	// line (including the trailing newline).  Oversize progress events are
	// dropped; oversize lifecycle/done events return an error.
	// Zero means no cap.
	MaxEventLineSize int64
}

// Writer appends NDJSON events to a per-mission file.
type Writer struct {
	path      string
	parentDir string
	f         *os.File
	seq       int64
	limits    Limits

	// progressBytes tracks accumulated bytes written for progress events.
	progressBytes int64
	// progressDrops counts how many progress events were dropped.
	progressDrops atomic.Int64

	// pendingNewline records that the previous Append's Write failed partway
	// (e.g. ENOSPC), so the file may end mid-line. The next Append first
	// terminates that torn line with a bare '\n' — otherwise a later
	// successful append (typically the terminal done after cleanup freed
	// disk space) would glue onto the fragment and become unparseable.
	pendingNewline bool
}

// eventFilePath returns the file path for a mission's events file.
func eventFilePath(parentDir, missionID string) string {
	return filepath.Join(parentDir, missionID+"-events")
}

// Create creates a new events file. Fails with os.ErrExist if the file exists.
//
// Also fsyncs the parent directory so the new entry is durable
// against a crash between Create and the first Append+SyncParentDir.
// In normal flow dispatch.go does that quickly anyway, but a crash in
// between would leave the file in an indeterminate "dir entry exists or
// doesn't" state on next boot, fighting with orphan cleanup. The fsync
// is best-effort: a failure is logged-and-counted by the caller via
// metrics.ObserveSyncDir, but Create itself doesn't error out — the
// file is open and the caller can still write to it.
func Create(parentDir, missionID string) (*Writer, error) {
	path := eventFilePath(parentDir, missionID)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	// Best-effort parent dir fsync. Ignore errors here — the caller's
	// post-Append SyncParentDir will surface persistent failures.
	_ = fsutil.SyncDir(parentDir)
	return &Writer{
		path:      path,
		parentDir: parentDir,
		f:         f,
	}, nil
}

// Open opens an existing events file for appending. Scans the file to
// determine the last seq.
//
// A file ending mid-line (torn tail from ENOSPC or power loss — progress
// appends are deliberately not fsynced) is repaired first: the torn line is
// terminated with a bare '\n' so every subsequent append starts on a fresh
// line and stays parseable. The fragment itself remains in the file as a
// junk line; readers skip lines that don't parse as JSON.
func Open(parentDir, missionID string) (*Writer, error) {
	path := eventFilePath(parentDir, missionID)
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	w := &Writer{
		path:      path,
		parentDir: parentDir,
		f:         f,
	}
	// Terminate a torn tail before scanning so the scan sees a well-formed
	// newline-terminated file.
	if err := w.terminateTornTail(); err != nil {
		_ = f.Close()
		return nil, err
	}
	// Determine last seq by scanning.
	if err := w.scanLastSeq(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return w, nil
}

// terminateTornTail writes a single '\n' through the append handle when the
// file is non-empty and its last byte is not a newline. Without this, the
// next append — including the terminal done event — would glue onto the
// torn line and become invisible to every line-oriented consumer (scans,
// the HTTP stream, done-detection).
func (w *Writer) terminateTornTail() error {
	rf, err := os.Open(w.path)
	if err != nil {
		return err
	}
	defer func() { _ = rf.Close() }()
	st, err := rf.Stat()
	if err != nil {
		return err
	}
	if st.Size() == 0 {
		return nil
	}
	var last [1]byte
	if _, err := rf.ReadAt(last[:], st.Size()-1); err != nil {
		return err
	}
	if last[0] == '\n' {
		return nil
	}
	_, err = w.f.Write([]byte("\n"))
	return err
}

// maxScanLineSize is the hard ceiling for any single event-file line during
// scan. The default for max_event_line_size is 1 MiB; operators may tune it
// upward via dugdale.yaml limits.max_event_line_size. We pick a generous
// 16 MiB ceiling to accommodate raised limits while still bounding memory.
// A truly malformed/adversarial file exceeding this will return ErrTooLong
// so the caller can surface the inconsistency rather than silently truncate.
const maxScanLineSize = 16 << 20

// scanLastSeq reads the events file from the beginning to find the highest seq.
func (w *Writer) scanLastSeq() error {
	rf, err := os.Open(w.path)
	if err != nil {
		return err
	}
	defer func() { _ = rf.Close() }()
	scanner := bufio.NewScanner(rf)
	// Grow buffer up to maxScanLineSize so legitimate large progress events
	// (up to max_event_line_size) don't break the scan. Default 64 KiB
	// would error on any progress line above that threshold.
	scanner.Buffer(make([]byte, 64*1024), maxScanLineSize)
	for scanner.Scan() {
		line := scanner.Bytes()
		var ev struct {
			Seq int64 `json:"seq"`
		}
		if json.Unmarshal(line, &ev) == nil && ev.Seq > w.seq {
			w.seq = ev.Seq
		}
	}
	return scanner.Err()
}

// SetLimits sets size limits. Must be called before Append.
func (w *Writer) SetLimits(l Limits) { w.limits = l }

// Path returns the full path to the events file.
func (w *Writer) Path() string { return w.path }

// LastSeq returns the highest seq written so far.
func (w *Writer) LastSeq() int64 { return w.seq }

// ProgressDrops returns the count of dropped progress events.
func (w *Writer) ProgressDrops() int64 { return w.progressDrops.Load() }

// Append encodes and appends one event. It increments seq automatically.
// If fsync is true, the file is fsynced after the write.
// Progress events past MaxEventsBuffer are dropped (returns seq=0, err=nil).
// Oversize lines return an error for lifecycle/done, are dropped for progress.
func (w *Writer) Append(kind EventKind, fields map[string]any, fsync bool) (int64, error) {
	// A previous Append's Write failed partway, so the file ends mid-line.
	// Terminate the torn line first; only clear the flag once the newline
	// actually lands — if it fails too, the event must not be written, or
	// it would glue onto the fragment and become unparseable.
	if w.pendingNewline {
		if _, err := w.f.Write([]byte("\n")); err != nil {
			return 0, fmt.Errorf("terminate torn line: %w", err)
		}
		w.pendingNewline = false
	}

	// Build event map.
	ev := make(map[string]any, len(fields)+2)
	for k, v := range fields {
		ev[k] = v
	}
	nextSeq := w.seq + 1
	ev["seq"] = nextSeq
	ev["event"] = string(kind)

	line, err := json.Marshal(ev)
	if err != nil {
		return 0, fmt.Errorf("marshal event: %w", err)
	}
	line = append(line, '\n')

	lineSize := int64(len(line))

	// Check per-line size limit.
	if w.limits.MaxEventLineSize > 0 && lineSize > w.limits.MaxEventLineSize {
		if isLifecycle(kind) {
			return 0, fmt.Errorf("event line size %d exceeds MaxEventLineSize %d", lineSize, w.limits.MaxEventLineSize)
		}
		// Progress: drop silently.
		w.progressDrops.Add(1)
		return 0, nil
	}

	// Check accumulated progress buffer cap.
	if !isLifecycle(kind) && w.limits.MaxEventsBuffer > 0 {
		if w.progressBytes+lineSize > w.limits.MaxEventsBuffer {
			w.progressDrops.Add(1)
			return 0, nil
		}
	}

	// Write the line. On error the file may now end mid-line (a partial
	// write, e.g. ENOSPC): n>0 means some bytes landed, so remember to
	// terminate the torn line before the next event. n==0 means nothing
	// reached the file and no repair is needed.
	if n, err := w.f.Write(line); err != nil {
		if n > 0 {
			w.pendingNewline = true
			// Burn the attempted seq too: the partial write can stop exactly
			// at the line-minus-newline boundary, in which case the healing
			// '\n' turns the fragment into a parseable event that already
			// carries nextSeq. Reusing nextSeq for the next append would emit
			// two parseable lines with equal seq — advance past it instead,
			// mirroring what scanLastSeq naturally does on the reopen path
			// (it would count such a line and resume after it).
			w.seq = nextSeq
		}
		return 0, fmt.Errorf("write event: %w", err)
	}

	w.seq = nextSeq

	// Update accumulated progress bytes.
	if !isLifecycle(kind) {
		w.progressBytes += lineSize
	}

	if fsync {
		if err := w.f.Sync(); err != nil {
			return w.seq, fmt.Errorf("fsync: %w", err)
		}
	}

	return w.seq, nil
}

// ErrTerminalEventConflict is returned by AppendDoneIdempotent when the
// events file already contains a `done` event whose content does
// not match the intent's desired done event. The
// public stream may already have been consumed by a client, so the
// daemon must NOT overwrite it with a different outcome; finalization
// blocks and readyz/startup repair must surface this for manual repair.
var ErrTerminalEventConflict = errors.New("eventfile: existing terminal event conflicts with intent")

// AppendDoneIdempotent appends a `done` event if none exists, OR
// idempotently no-ops if the existing `done` carries the same fields.
// If the existing done's fields differ, returns ErrTerminalEventConflict
// so the caller (commitFinalize / repair) can block the final SQL update
// rather than producing a DB-vs-stream inconsistency.
//
// Idempotency is fields-only: the existing done is compared against the
// proposed `fields` with the writer-managed `seq` key excluded on both
// sides (`event` must still be "done", and the field sets must be equal
// in both directions). The intent's fields are the authoritative outcome;
// a seq divergence can only come from junk lines that failed to parse
// during an earlier scan (torn tails terminated on Open), and the public
// stream may already have been consumed — so equal-content acceptance is
// the only safe idempotency. Treating a seq-only divergence as a conflict
// would permanently block finalization over a difference no consumer can
// observe as an outcome change.
//
// When no done exists, the appended done carries expectedSeq — the
// durably-committed done_seq from the finalize intent — unless the scanned
// last seq has already reached it, in which case scannedLastSeq+1 preserves
// per-file seq monotonicity.
func (w *Writer) AppendDoneIdempotent(fields map[string]any, expectedSeq int64) error {
	// Scan file to check for existing done, capture its content, find last seq.
	rf, err := os.Open(w.path)
	if err != nil {
		return err
	}
	var lastSeq int64
	var existingDone map[string]any
	scanner := bufio.NewScanner(rf)
	// Same buffer expansion as scanLastSeq — legitimate large progress lines
	// must not break done-idempotency repair.
	scanner.Buffer(make([]byte, 64*1024), maxScanLineSize)
	for scanner.Scan() {
		line := scanner.Bytes()
		var ev map[string]any
		if err := json.Unmarshal(line, &ev); err != nil {
			continue
		}
		if s, ok := ev["seq"].(float64); ok && int64(s) > lastSeq {
			lastSeq = int64(s)
		}
		if evt, _ := ev["event"].(string); evt == string(KindDone) {
			existingDone = ev
		}
	}
	_ = rf.Close()
	if err := scanner.Err(); err != nil {
		return err
	}

	if existingDone != nil {
		if doneEventsMatch(existingDone, fields) {
			return nil // true idempotent
		}
		return ErrTerminalEventConflict
	}

	// Honour the intent's durable done_seq: clients resume from the last
	// seq they observed, and the intent committed exactly where the done
	// belongs. Unparseable junk lines (terminated torn tails) may hide
	// events between the last scanned seq and expectedSeq, so the scan can
	// run behind the intent — never skip the done back to scannedLastSeq+1
	// in that case. Only when the file has already moved past expectedSeq
	// does monotonicity win.
	nextSeq := expectedSeq
	if lastSeq >= expectedSeq {
		nextSeq = lastSeq + 1
	}
	w.seq = nextSeq - 1

	_, err = w.Append(KindDone, fields, true)
	return err
}

// doneEventsMatch compares an existing done event from the file against
// the proposed fields about to be appended, with the writer-managed `seq`
// key excluded on both sides. Returns true iff the existing event is a
// `done` and every remaining key in `fields` maps to the same value in
// `existing` (and existing has no extra non-control keys). JSON numeric
// values come back as float64 from json.Unmarshal — we normalise both
// sides through json round-trip for type-agnostic comparison.
func doneEventsMatch(existing, fields map[string]any) bool {
	if evt, _ := existing["event"].(string); evt != string(KindDone) {
		return false
	}
	// Build the expected event the way Append would emit it minus the seq,
	// marshal both through json so types normalise (int → float64, nested
	// struct → map[string]any), then compare via re-decode.
	expected := make(map[string]any, len(fields)+1)
	for k, v := range fields {
		expected[k] = v
	}
	delete(expected, "seq") // defensive: callers strip it already
	expected["event"] = string(KindDone)
	existingNoSeq := make(map[string]any, len(existing))
	for k, v := range existing {
		if k == "seq" {
			continue
		}
		existingNoSeq[k] = v
	}
	expectedNorm, err := normaliseEvent(expected)
	if err != nil {
		return false
	}
	existingNorm, err := normaliseEvent(existingNoSeq)
	if err != nil {
		return false
	}
	return jsonValuesEqual(expectedNorm, existingNorm)
}

// normaliseEvent round-trips through json.Marshal/Unmarshal so both sides
// are compared in the same shape (numbers as float64, nested structs as
// map[string]any). Cheap and correctness-bounded — done events are small.
func normaliseEvent(m map[string]any) (map[string]any, error) {
	buf, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal(buf, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// jsonValuesEqual checks equality of two values both produced by
// json.Unmarshal (so types are limited to nil, bool, float64, string,
// []any, map[string]any). The explicit walk over that closed type set
// avoids pulling in reflect.DeepEqual and keeps comparison behaviour
// explicit for the events use-case.
func jsonValuesEqual(a, b any) bool {
	switch av := a.(type) {
	case nil:
		return b == nil
	case bool:
		bv, ok := b.(bool)
		return ok && av == bv
	case float64:
		bv, ok := b.(float64)
		return ok && av == bv
	case string:
		bv, ok := b.(string)
		return ok && av == bv
	case []any:
		bv, ok := b.([]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for i := range av {
			if !jsonValuesEqual(av[i], bv[i]) {
				return false
			}
		}
		return true
	case map[string]any:
		bv, ok := b.(map[string]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for k, v := range av {
			if !jsonValuesEqual(v, bv[k]) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// Close closes the underlying file.
func (w *Writer) Close() error {
	if w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	return err
}

// SyncParentDir calls fsutil.SyncDir on the parent directory.
func (w *Writer) SyncParentDir() error {
	return fsutil.SyncDir(w.parentDir)
}

// ErrFileExists wraps os.ErrExist for package-level checking.
var ErrFileExists = errors.New("eventfile: file already exists")
