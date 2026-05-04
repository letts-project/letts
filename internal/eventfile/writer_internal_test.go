package eventfile

import (
	"bufio"
	"encoding/json"
	"os"
	"testing"
)

// TestAppendTerminatesTornLineBeforeNextEvent: when a Write fails partway
// (e.g. ENOSPC), the file ends mid-line and the writer records that via
// pendingNewline. The next Append must first terminate the torn line so the
// new event — typically the terminal done after the cleanup goroutine freed
// disk space — starts on a fresh line instead of gluing onto the fragment.
// The partial-write state itself can't be provoked deterministically with a
// real fd, so the test plants the torn bytes and the flag directly.
func TestAppendTerminatesTornLineBeforeNextEvent(t *testing.T) {
	dir := t.TempDir()
	w, err := Create(dir, "m-pending")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer func() { _ = w.Close() }()

	if _, err := w.Append(KindRunning, map[string]any{}, false); err != nil {
		t.Fatalf("running: %v", err)
	}
	// Simulate a partial write: a fragment landed, then the write errored.
	if _, err := w.f.WriteString(`{"seq":2,"event":"prog`); err != nil {
		t.Fatalf("plant torn fragment: %v", err)
	}
	w.pendingNewline = true

	seq, err := w.Append(KindDone, map[string]any{"outcome": "success"}, true)
	if err != nil {
		t.Fatalf("append after torn write: %v", err)
	}
	if seq != 2 {
		t.Errorf("done seq: want 2, got %d", seq)
	}
	if w.pendingNewline {
		t.Error("pendingNewline not cleared after successful termination")
	}

	// File must now hold: running, terminated junk fragment, done — and the
	// done must be parseable on its own line.
	f, err := os.Open(w.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	scanner := bufio.NewScanner(f)
	var lines [][]byte
	for scanner.Scan() {
		lines = append(lines, append([]byte(nil), scanner.Bytes()...))
	}
	if len(lines) != 3 {
		t.Fatalf("want 3 lines (running, junk, done), got %d: %q", len(lines), lines)
	}
	var done map[string]any
	if err := json.Unmarshal(lines[2], &done); err != nil {
		t.Fatalf("done line glued onto torn fragment: %v (%q)", err, lines[2])
	}
	if done["event"] != "done" || done["seq"].(float64) != 2 {
		t.Errorf("done line: %v", done)
	}
}

// TestAppendKeepsPendingNewlineWhenTerminationFails: if the healing newline
// itself cannot be written, the event must not be written either and the
// flag must stay set — otherwise a later append would glue onto the torn
// line after all.
func TestAppendKeepsPendingNewlineWhenTerminationFails(t *testing.T) {
	dir := t.TempDir()
	w, err := Create(dir, "m-pending-fail")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	w.pendingNewline = true
	// Close the fd underneath the writer so every write fails, standing in
	// for a persistently full disk.
	if err := w.f.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := w.Append(KindDone, map[string]any{"outcome": "success"}, false); err == nil {
		t.Fatal("want error when newline termination fails")
	}
	if !w.pendingNewline {
		t.Error("pendingNewline cleared despite failed termination write")
	}
}
