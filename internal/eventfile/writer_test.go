package eventfile_test

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"letts/internal/eventfile"
)

// readAllEvents reads and parses all events from the events file.
// Uses an expanded scanner buffer (16 MiB) to match the production reader
// behaviour — otherwise tests with progress events above 64 KiB silently
// truncate and give misleading assertions.
func readAllEvents(t *testing.T, w *eventfile.Writer) []map[string]any {
	t.Helper()
	f, err := os.Open(w.Path())
	if err != nil {
		t.Fatalf("open events file: %v", err)
	}
	defer func() { _ = f.Close() }()
	var events []map[string]any
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 16<<20)
	for scanner.Scan() {
		var ev map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &ev); err != nil {
			t.Fatalf("parse event: %v", err)
		}
		events = append(events, ev)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}
	return events
}

// TestCreateAppendQueued verifies Create followed by Append writes one event with seq=1.
func TestCreateAppendQueued(t *testing.T) {
	dir := t.TempDir()
	w, err := eventfile.Create(dir, "mission-001")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer func() { _ = w.Close() }()

	seq, err := w.Append(eventfile.KindQueued, map[string]any{"lane": "fast"}, false)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if seq != 1 {
		t.Errorf("seq: got %d, want 1", seq)
	}

	events := readAllEvents(t, w)
	if len(events) != 1 {
		t.Fatalf("events: want 1, got %d", len(events))
	}
	if events[0]["event"] != "queued" {
		t.Errorf("event: got %v, want queued", events[0]["event"])
	}
	if events[0]["seq"].(float64) != 1 {
		t.Errorf("seq in file: got %v, want 1", events[0]["seq"])
	}
	if events[0]["lane"] != "fast" {
		t.Errorf("lane: got %v, want fast", events[0]["lane"])
	}
}

// TestCreateTwiceSameMission verifies that creating the same file twice returns os.ErrExist.
func TestCreateTwiceSameMission(t *testing.T) {
	dir := t.TempDir()
	w1, err := eventfile.Create(dir, "m1")
	if err != nil {
		t.Fatalf("first create: %v", err)
	}
	defer func() { _ = w1.Close() }()

	_, err = eventfile.Create(dir, "m1")
	if !errors.Is(err, os.ErrExist) {
		t.Errorf("expected os.ErrExist, got %v", err)
	}
}

// Create must make the new dir entry visible to subsequent
// readers. The fsync of the parent dir inside Create isn't directly
// observable from Go user-space, but the regression guard is: after
// Create returns, an os.ReadDir on parentDir lists the file. (Without
// the fsync this is still typically true on Linux/Darwin because the
// dentry cache is updated; the fsync just makes it durable.)
func TestCreateFileFindableInParentReadDir(t *testing.T) {
	dir := t.TempDir()
	w, err := eventfile.Create(dir, "m9-fsync")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer func() { _ = w.Close() }()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read parent dir: %v", err)
	}
	found := false
	for _, e := range entries {
		if e.Name() == "m9-fsync-events" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("events file missing from parent dir readdir, entries=%v", entries)
	}
}

// TestAppendMultipleSeqIncrements verifies seq increases 1,2,3,...
func TestAppendMultipleSeqIncrements(t *testing.T) {
	dir := t.TempDir()
	w, err := eventfile.Create(dir, "m2")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer func() { _ = w.Close() }()

	for i := 1; i <= 5; i++ {
		seq, err := w.Append(eventfile.KindProgress, map[string]any{"i": i}, false)
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		if seq != int64(i) {
			t.Errorf("step %d: seq=%d, want %d", i, seq, i)
		}
	}
	if w.LastSeq() != 5 {
		t.Errorf("LastSeq: want 5, got %d", w.LastSeq())
	}
}

// TestLifecycleBypassesBufferCap verifies that lifecycle events bypass MaxEventsBuffer.
func TestLifecycleBypassesBufferCap(t *testing.T) {
	dir := t.TempDir()
	w, err := eventfile.Create(dir, "m3")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer func() { _ = w.Close() }()

	// Set a tiny buffer cap.
	w.SetLimits(eventfile.Limits{MaxEventsBuffer: 1})

	// Lifecycle events must succeed even when buffer is "full".
	if _, err := w.Append(eventfile.KindQueued, map[string]any{}, false); err != nil {
		t.Errorf("queued lifecycle bypass failed: %v", err)
	}
	if _, err := w.Append(eventfile.KindRunning, map[string]any{}, false); err != nil {
		t.Errorf("running lifecycle bypass failed: %v", err)
	}
	if _, err := w.Append(eventfile.KindDone, map[string]any{}, false); err != nil {
		t.Errorf("done lifecycle bypass failed: %v", err)
	}
}

// TestProgressDroppedPastBufferCap verifies progress events past MaxEventsBuffer are dropped.
func TestProgressDroppedPastBufferCap(t *testing.T) {
	dir := t.TempDir()
	w, err := eventfile.Create(dir, "m4")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer func() { _ = w.Close() }()

	// Allow one event then cap.
	w.SetLimits(eventfile.Limits{MaxEventsBuffer: 50})

	// First progress — may fit or not depending on size; use a tiny payload.
	seq1, err := w.Append(eventfile.KindProgress, map[string]any{"x": "y"}, false)
	if err != nil {
		t.Fatalf("first progress: %v", err)
	}

	// Now flood with progress events well past the cap.
	dropped := 0
	for i := 0; i < 20; i++ {
		seq, err := w.Append(eventfile.KindProgress, map[string]any{"data": "longerstring"}, false)
		if err != nil {
			t.Fatalf("progress %d: %v", i, err)
		}
		if seq == 0 {
			dropped++
		}
	}

	if dropped == 0 {
		t.Errorf("expected some progress events to be dropped")
	}
	if w.ProgressDrops() != int64(dropped) {
		t.Errorf("ProgressDrops: want %d, got %d", dropped, w.ProgressDrops())
	}
	// The first event was either written or dropped, seq1 is either 1 or 0.
	_ = seq1
}

// TestOversizeProgressDropped verifies that an oversize progress event is dropped, not errored.
func TestOversizeProgressDropped(t *testing.T) {
	dir := t.TempDir()
	w, err := eventfile.Create(dir, "m5")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer func() { _ = w.Close() }()

	// Very small line size cap.
	w.SetLimits(eventfile.Limits{MaxEventLineSize: 10})

	seq, err := w.Append(eventfile.KindProgress, map[string]any{"data": "this is a long string"}, false)
	if err != nil {
		t.Errorf("oversize progress should not return error, got %v", err)
	}
	if seq != 0 {
		t.Errorf("expected seq=0 for dropped event, got %d", seq)
	}
	if w.ProgressDrops() != 1 {
		t.Errorf("ProgressDrops: want 1, got %d", w.ProgressDrops())
	}
}

// TestOversizeDoneReturnsError verifies that an oversize done event returns an error.
func TestOversizeDoneReturnsError(t *testing.T) {
	dir := t.TempDir()
	w, err := eventfile.Create(dir, "m6")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer func() { _ = w.Close() }()

	w.SetLimits(eventfile.Limits{MaxEventLineSize: 10})

	_, err = w.Append(eventfile.KindDone, map[string]any{"data": "long payload"}, false)
	if err == nil {
		t.Error("expected error for oversize done event")
	}
}

// TestAppendDoneIdempotentDetectsContentConflict enforces: a terminal event
// whose content conflicts with the intent is an unrecoverable consistency
// error — the public stream may already have been read by a client, so
// dugdale must NOT write a different DB outcome over it; finalization
// blocks.
//
// An append that silently returns nil whenever a done event already
// exists — even one with different outcome/fail_reason — would let the
// caller UPDATE missions with the intent's outcome, leaving the events
// stream and DB row contradicting each other.
func TestAppendDoneIdempotentDetectsContentConflict(t *testing.T) {
	dir := t.TempDir()
	w, err := eventfile.Create(dir, "m-conflict")
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Lifecycle events and initial done(success).
	_, _ = w.Append(eventfile.KindQueued, map[string]any{}, false)
	_, _ = w.Append(eventfile.KindRunning, map[string]any{}, false)
	if err := w.AppendDoneIdempotent(map[string]any{
		"outcome": "success", "exit_code": int64(0),
	}, 3); err != nil {
		t.Fatalf("initial done: %v", err)
	}
	_ = w.Close()

	// Re-open and try to append a *different* done event (outcome=failed).
	// Must surface ErrTerminalEventConflict, not silently return nil.
	w2, err := eventfile.Open(dir, "m-conflict")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = w2.Close() }()
	err = w2.AppendDoneIdempotent(map[string]any{
		"outcome": "failed", "exit_code": int64(1), "fail_reason": "uncaught_exception",
	}, 3)
	if !errors.Is(err, eventfile.ErrTerminalEventConflict) {
		t.Fatalf("got %v, want ErrTerminalEventConflict", err)
	}
}

// TestAppendDoneIdempotentSameContent verifies the true idempotent case
// (same outcome → nil) still works after the conflict detection.
func TestAppendDoneIdempotentSameContent(t *testing.T) {
	dir := t.TempDir()
	w, err := eventfile.Create(dir, "m-same")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer func() { _ = w.Close() }()

	_, _ = w.Append(eventfile.KindQueued, map[string]any{}, false)
	_, _ = w.Append(eventfile.KindRunning, map[string]any{}, false)
	fields := map[string]any{"outcome": "success", "exit_code": int64(0)}
	if err := w.AppendDoneIdempotent(fields, 3); err != nil {
		t.Fatalf("first done: %v", err)
	}
	if err := w.AppendDoneIdempotent(fields, 3); err != nil {
		t.Fatalf("second done (true idempotent): %v", err)
	}
}

// TestAppendDoneIdempotent verifies that calling AppendDoneIdempotent twice writes only one done event.
func TestAppendDoneIdempotent(t *testing.T) {
	dir := t.TempDir()
	w, err := eventfile.Create(dir, "m7")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer func() { _ = w.Close() }()

	_, _ = w.Append(eventfile.KindQueued, map[string]any{}, false)
	_, _ = w.Append(eventfile.KindRunning, map[string]any{}, false)

	if err := w.AppendDoneIdempotent(map[string]any{"outcome": "success"}, 2); err != nil {
		t.Fatalf("first done: %v", err)
	}
	if err := w.AppendDoneIdempotent(map[string]any{"outcome": "success"}, 3); err != nil {
		t.Fatalf("second done (idempotent): %v", err)
	}

	events := readAllEvents(t, w)
	doneCount := 0
	for _, ev := range events {
		if ev["event"] == "done" {
			doneCount++
		}
	}
	if doneCount != 1 {
		t.Errorf("expected 1 done event, got %d", doneCount)
	}
}

// TestOpenHandlesLargeProgressLine reproduces the bufio.Scanner default
// 64 KiB buffer bug: a legitimate progress event of size 200 KiB
// (well under the default MaxEventLineSize=1 MiB) must not break Open's
// scanLastSeq scanner. Same applies to AppendDoneIdempotent.
func TestOpenHandlesLargeProgressLine(t *testing.T) {
	dir := t.TempDir()
	w, err := eventfile.Create(dir, "m-large")
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Allow large lines (1 MiB) and large buffer.
	w.SetLimits(eventfile.Limits{
		MaxEventLineSize: 1 << 20,
		MaxEventsBuffer:  4 << 20,
	})

	// Write a queued, a running, and ONE big progress event (~200 KiB).
	if _, err := w.Append(eventfile.KindQueued, map[string]any{}, false); err != nil {
		t.Fatalf("queued: %v", err)
	}
	if _, err := w.Append(eventfile.KindRunning, map[string]any{}, false); err != nil {
		t.Fatalf("running: %v", err)
	}
	bigMessage := make([]byte, 200*1024)
	for i := range bigMessage {
		bigMessage[i] = 'x'
	}
	if _, err := w.Append(eventfile.KindProgress, map[string]any{"message": string(bigMessage)}, false); err != nil {
		t.Fatalf("big progress: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Open must succeed and report lastSeq=3.
	w2, err := eventfile.Open(dir, "m-large")
	if err != nil {
		t.Fatalf("open after big progress: %v", err)
	}
	defer func() { _ = w2.Close() }()

	if got := w2.LastSeq(); got != 3 {
		t.Errorf("LastSeq after reopen with big line: want 3, got %d", got)
	}
}

// TestAppendDoneIdempotentHandlesLargeProgressLine verifies the same buffer fix
// applies to AppendDoneIdempotent's internal scan.
func TestAppendDoneIdempotentHandlesLargeProgressLine(t *testing.T) {
	dir := t.TempDir()
	w, err := eventfile.Create(dir, "m-large-done")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer func() { _ = w.Close() }()

	w.SetLimits(eventfile.Limits{
		MaxEventLineSize: 1 << 20,
		MaxEventsBuffer:  4 << 20,
	})

	_, _ = w.Append(eventfile.KindQueued, map[string]any{}, false)
	bigMessage := make([]byte, 200*1024)
	for i := range bigMessage {
		bigMessage[i] = 'y'
	}
	if _, err := w.Append(eventfile.KindProgress, map[string]any{"message": string(bigMessage)}, false); err != nil {
		t.Fatalf("big progress: %v", err)
	}

	// AppendDoneIdempotent must succeed despite the 200 KiB line in file.
	if err := w.AppendDoneIdempotent(map[string]any{"outcome": "success"}, 2); err != nil {
		t.Fatalf("done after big progress: %v", err)
	}

	events := readAllEvents(t, w)
	if len(events) < 3 {
		t.Fatalf("want >=3 events, got %d", len(events))
	}
	last := events[len(events)-1]
	if last["event"] != "done" {
		t.Errorf("last event: want done, got %v", last["event"])
	}
}

// readParseableEvents reads all lines of the events file and returns the
// ones that parse as JSON, skipping junk (terminated torn-tail fragments) —
// the same tolerance every production consumer applies.
func readParseableEvents(t *testing.T, w *eventfile.Writer) []map[string]any {
	t.Helper()
	raw, err := os.ReadFile(w.Path())
	if err != nil {
		t.Fatalf("read events file: %v", err)
	}
	var events []map[string]any
	for _, line := range splitLines(raw) {
		var ev map[string]any
		if json.Unmarshal(line, &ev) != nil {
			continue
		}
		events = append(events, ev)
	}
	return events
}

// appendRaw appends raw bytes to a mission's events file, bypassing the
// Writer. Used to fabricate torn tails (partial lines without a trailing
// newline) the way an ENOSPC mid-write or power loss would leave them.
func appendRaw(t *testing.T, dir, missionID, raw string) {
	t.Helper()
	f, err := os.OpenFile(filepath.Join(dir, missionID+"-events"), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("open raw append: %v", err)
	}
	if _, err := f.WriteString(raw); err != nil {
		t.Fatalf("raw append: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close raw append: %v", err)
	}
}

// TestOpenTerminatesTornTail: progress appends are not fsynced, so a crash or
// ENOSPC can leave the file ending mid-line. Open must terminate that torn
// tail with a newline so every subsequent append starts on a fresh line and
// stays parseable — in particular the terminal done event.
func TestOpenTerminatesTornTail(t *testing.T) {
	dir := t.TempDir()
	w, err := eventfile.Create(dir, "m-torn")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, _ = w.Append(eventfile.KindQueued, map[string]any{}, false)
	_, _ = w.Append(eventfile.KindRunning, map[string]any{}, false)
	_ = w.Close()
	appendRaw(t, dir, "m-torn", `{"seq":3,"event":"progress","msg":"tor`)

	w2, err := eventfile.Open(dir, "m-torn")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = w2.Close() }()

	// The torn line is unparseable junk, so the last parseable seq is 2.
	if got := w2.LastSeq(); got != 2 {
		t.Errorf("LastSeq: want 2, got %d", got)
	}

	seq, err := w2.Append(eventfile.KindDone, map[string]any{"outcome": "lost"}, true)
	if err != nil {
		t.Fatalf("append done after torn tail: %v", err)
	}
	if seq != 3 {
		t.Errorf("done seq: want 3, got %d", seq)
	}

	// The file must be fully newline-terminated and the done event must sit
	// on its own parseable line (not glued onto the torn fragment).
	raw, err := os.ReadFile(w2.Path())
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if len(raw) == 0 || raw[len(raw)-1] != '\n' {
		t.Fatalf("file does not end with newline: %q", raw)
	}
	var doneSeen bool
	for _, line := range splitLines(raw) {
		var ev map[string]any
		if json.Unmarshal(line, &ev) != nil {
			continue // the terminated torn fragment — expected junk
		}
		if ev["event"] == "done" {
			doneSeen = true
			if ev["seq"].(float64) != 3 {
				t.Errorf("done seq in file: got %v, want 3", ev["seq"])
			}
		}
	}
	if !doneSeen {
		t.Error("no parseable done line after torn-tail repair")
	}
}

// TestOpenLeavesWellFormedFileUnchanged: the torn-tail repair in Open must be
// a no-op on a healthy newline-terminated file (no blank lines injected).
func TestOpenLeavesWellFormedFileUnchanged(t *testing.T) {
	dir := t.TempDir()
	w, err := eventfile.Create(dir, "m-clean")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, _ = w.Append(eventfile.KindQueued, map[string]any{}, false)
	_, _ = w.Append(eventfile.KindRunning, map[string]any{}, false)
	_ = w.Close()
	before, err := os.ReadFile(filepath.Join(dir, "m-clean-events"))
	if err != nil {
		t.Fatal(err)
	}

	w2, err := eventfile.Open(dir, "m-clean")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	_ = w2.Close()

	after, err := os.ReadFile(filepath.Join(dir, "m-clean-events"))
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Errorf("Open mutated a well-formed file:\nbefore=%q\nafter=%q", before, after)
	}
}

// TestOpenEmptyFile: Open on a freshly-created (zero-byte) file must not
// write anything and must report LastSeq=0.
func TestOpenEmptyFile(t *testing.T) {
	dir := t.TempDir()
	w, err := eventfile.Create(dir, "m-empty")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_ = w.Close()

	w2, err := eventfile.Open(dir, "m-empty")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = w2.Close() }()
	if got := w2.LastSeq(); got != 0 {
		t.Errorf("LastSeq: want 0, got %d", got)
	}
	st, err := os.Stat(w2.Path())
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() != 0 {
		t.Errorf("empty file grew to %d bytes after Open", st.Size())
	}
}

// splitLines splits a raw events file into newline-terminated lines (without
// the terminator). A trailing fragment without '\n' is returned as-is.
func splitLines(raw []byte) [][]byte {
	var out [][]byte
	for len(raw) > 0 {
		i := -1
		for j, b := range raw {
			if b == '\n' {
				i = j
				break
			}
		}
		if i < 0 {
			out = append(out, raw)
			break
		}
		out = append(out, raw[:i])
		raw = raw[i+1:]
	}
	return out
}

// TestAppendDoneIdempotentUsesIntentSeqAfterTornTail: when the file's tail
// was torn (later events lost their parseable form), the appended done must
// carry the intent's durably-committed done_seq — not scannedLastSeq+1 —
// so clients that already observed seqs up to done_seq-1 see the done at
// the expected position.
func TestAppendDoneIdempotentUsesIntentSeqAfterTornTail(t *testing.T) {
	dir := t.TempDir()
	w, err := eventfile.Create(dir, "m-intent-seq")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, _ = w.Append(eventfile.KindQueued, map[string]any{}, false) // seq 1
	_ = w.Close()
	// Torn progress line: seq 2..4 were written unfsynced and lost their
	// trailing bytes — only a fragment survives.
	appendRaw(t, dir, "m-intent-seq", `{"seq":4,"event":"progress","msg":"tor`)

	w2, err := eventfile.Open(dir, "m-intent-seq")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = w2.Close() }()

	fields := map[string]any{"outcome": "success", "exit_code": int64(0)}
	if err := w2.AppendDoneIdempotent(fields, 5); err != nil {
		t.Fatalf("append done: %v", err)
	}

	events := readParseableEvents(t, w2)
	last := events[len(events)-1]
	if last["event"] != "done" {
		t.Fatalf("last event: want done, got %v", last)
	}
	if last["seq"].(float64) != 5 {
		t.Errorf("done seq: want 5 (intent seq), got %v", last["seq"])
	}
}

// TestAppendDoneIdempotentMonotonicSeqWhenFileAhead: when the scanned last
// seq has already reached or passed the intent's done_seq, per-file seq
// monotonicity wins — the done must land at scannedLastSeq+1, never at a seq
// the file has already moved past.
func TestAppendDoneIdempotentMonotonicSeqWhenFileAhead(t *testing.T) {
	dir := t.TempDir()
	w, err := eventfile.Create(dir, "m-mono-seq")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer func() { _ = w.Close() }()
	// Five events: seqs 1..5.
	_, _ = w.Append(eventfile.KindQueued, map[string]any{}, false)
	_, _ = w.Append(eventfile.KindRunning, map[string]any{}, false)
	for i := 0; i < 3; i++ {
		if _, err := w.Append(eventfile.KindProgress, map[string]any{"i": i}, false); err != nil {
			t.Fatalf("progress %d: %v", i, err)
		}
	}

	// The intent's done_seq (3) lags the file's last seq (5).
	if err := w.AppendDoneIdempotent(map[string]any{"outcome": "success"}, 3); err != nil {
		t.Fatalf("append done: %v", err)
	}

	events := readAllEvents(t, w)
	var dones []map[string]any
	for _, ev := range events {
		if ev["event"] == "done" {
			dones = append(dones, ev)
		}
	}
	if len(dones) != 1 {
		t.Fatalf("want exactly 1 done line, got %d", len(dones))
	}
	if dones[0]["seq"].(float64) != 6 {
		t.Errorf("done seq: want 6 (last seq 5 + 1), got %v", dones[0]["seq"])
	}
}

// TestAppendDoneIdempotentAppendsFreshDoneAfterGluedLine: a done event whose
// write was glued onto a torn progress line is unparseable, so it does not
// count as an existing terminal event. The repair path must append a fresh
// done at the intent's seq.
func TestAppendDoneIdempotentAppendsFreshDoneAfterGluedLine(t *testing.T) {
	dir := t.TempDir()
	w, err := eventfile.Create(dir, "m-glued")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, _ = w.Append(eventfile.KindQueued, map[string]any{}, false) // seq 1
	_ = w.Close()
	// A torn progress line with a done append glued onto it — one junk line.
	appendRaw(t, dir, "m-glued",
		`{"seq":2,"event":"progress","msg":"to{"seq":3,"event":"done","outcome":"success","exit_code":0}`+"\n")

	w2, err := eventfile.Open(dir, "m-glued")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = w2.Close() }()

	fields := map[string]any{"outcome": "success", "exit_code": int64(0)}
	if err := w2.AppendDoneIdempotent(fields, 3); err != nil {
		t.Fatalf("append done: %v", err)
	}

	events := readParseableEvents(t, w2)
	last := events[len(events)-1]
	if last["event"] != "done" || last["seq"].(float64) != 3 {
		t.Errorf("want parseable done at seq 3, got %v", last)
	}
}

// TestAppendDoneIdempotentSameFieldsDifferentSeq: an existing done whose
// fields equal the intent's but whose seq diverges (possible when junk lines
// shifted the scanned last seq in an earlier repair) is the same outcome —
// it must be accepted as idempotent, not flagged as a conflict that blocks
// finalization forever.
func TestAppendDoneIdempotentSameFieldsDifferentSeq(t *testing.T) {
	dir := t.TempDir()
	w, err := eventfile.Create(dir, "m-seq-div")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer func() { _ = w.Close() }()
	_, _ = w.Append(eventfile.KindQueued, map[string]any{}, false)  // seq 1
	_, _ = w.Append(eventfile.KindRunning, map[string]any{}, false) // seq 2
	fields := map[string]any{"outcome": "success", "exit_code": int64(0)}
	// Done lands at seq 9 — far from the intent's done_seq below.
	appendRaw(t, dir, "m-seq-div", `{"seq":9,"event":"done","outcome":"success","exit_code":0}`+"\n")

	if err := w.AppendDoneIdempotent(fields, 3); err != nil {
		t.Fatalf("want idempotent nil for same fields, got %v", err)
	}

	events := readAllEvents(t, w)
	doneCount := 0
	for _, ev := range events {
		if ev["event"] == "done" {
			doneCount++
		}
	}
	if doneCount != 1 {
		t.Errorf("expected exactly 1 done line, got %d", doneCount)
	}
}

// TestOpenExistingFile verifies Open correctly reads the last seq.
func TestOpenExistingFile(t *testing.T) {
	dir := t.TempDir()
	w, err := eventfile.Create(dir, "m8")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, _ = w.Append(eventfile.KindQueued, map[string]any{}, false)
	_, _ = w.Append(eventfile.KindRunning, map[string]any{}, false)
	_ = w.Close()

	// Reopen for append.
	w2, err := eventfile.Open(dir, "m8")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = w2.Close() }()

	if w2.LastSeq() != 2 {
		t.Errorf("LastSeq after reopen: want 2, got %d", w2.LastSeq())
	}

	// Append continues from seq=3.
	seq, err := w2.Append(eventfile.KindProgress, map[string]any{}, false)
	if err != nil {
		t.Fatalf("append after open: %v", err)
	}
	if seq != 3 {
		t.Errorf("seq after reopen: want 3, got %d", seq)
	}
}
