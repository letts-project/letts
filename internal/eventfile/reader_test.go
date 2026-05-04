package eventfile_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"letts/internal/eventfile"
)

// collectEvents runs Stream and collects all emitted lines as parsed maps.
func collectEvents(t *testing.T, parentDir, missionID string, opts eventfile.ReadOptions) []map[string]any {
	t.Helper()
	var mu sync.Mutex
	var events []map[string]any
	ctx := context.Background()
	err := eventfile.Stream(ctx, parentDir, missionID, opts, func(line []byte) error {
		var ev map[string]any
		if err := json.Unmarshal(line, &ev); err != nil {
			return err
		}
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	return events
}

// TestStreamNoFollow reads 4 events (queued, running, 2×progress, done) without follow.
func TestStreamNoFollow(t *testing.T) {
	dir := t.TempDir()
	w, err := eventfile.Create(dir, "r1")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, _ = w.Append(eventfile.KindQueued, map[string]any{}, false)
	_, _ = w.Append(eventfile.KindRunning, map[string]any{}, false)
	_, _ = w.Append(eventfile.KindProgress, map[string]any{"msg": "p1"}, false)
	_ = w.AppendDoneIdempotent(map[string]any{"outcome": "success"}, 3)
	_ = w.Close()

	events := collectEvents(t, dir, "r1", eventfile.ReadOptions{})
	if len(events) != 4 {
		t.Errorf("want 4 events, got %d: %v", len(events), events)
	}
	if events[0]["event"] != "queued" {
		t.Errorf("first event: got %v, want queued", events[0]["event"])
	}
	if events[3]["event"] != "done" {
		t.Errorf("last event: got %v, want done", events[3]["event"])
	}
}

// TestStreamFromFilter verifies that events with seq <= From are skipped.
func TestStreamFromFilter(t *testing.T) {
	dir := t.TempDir()
	w, err := eventfile.Create(dir, "r2")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	// seq 1,2,3,4
	_, _ = w.Append(eventfile.KindQueued, map[string]any{}, false)
	_, _ = w.Append(eventfile.KindRunning, map[string]any{}, false)
	_, _ = w.Append(eventfile.KindProgress, map[string]any{"x": 1}, false)
	_ = w.AppendDoneIdempotent(map[string]any{}, 3)
	_ = w.Close()

	events := collectEvents(t, dir, "r2", eventfile.ReadOptions{From: 2})
	if len(events) != 2 {
		t.Errorf("want 2 events (seq 3,4), got %d: %v", len(events), events)
	}
	for _, ev := range events {
		seq := int64(ev["seq"].(float64))
		if seq <= 2 {
			t.Errorf("event with seq %d should have been filtered out", seq)
		}
	}
}

// TestStreamFollowSeesNewEvents verifies that a follower picks up events appended
// after the initial EOF.
func TestStreamFollowSeesNewEvents(t *testing.T) {
	dir := t.TempDir()
	w, err := eventfile.Create(dir, "r3")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	// Write initial events.
	_, _ = w.Append(eventfile.KindQueued, map[string]any{}, false)
	_, _ = w.Append(eventfile.KindRunning, map[string]any{}, false)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var mu sync.Mutex
	var collected []map[string]any
	streamDone := make(chan error, 1)

	go func() {
		err := eventfile.Stream(ctx, dir, "r3", eventfile.ReadOptions{
			Follow:    true,
			PollEvery: 20 * time.Millisecond,
		}, func(line []byte) error {
			var ev map[string]any
			if err := json.Unmarshal(line, &ev); err != nil {
				return err
			}
			mu.Lock()
			collected = append(collected, ev)
			mu.Unlock()
			return nil
		})
		streamDone <- err
	}()

	// Give the goroutine a moment to consume initial events and reach EOF.
	time.Sleep(100 * time.Millisecond)

	// Append progress and done.
	_, _ = w.Append(eventfile.KindProgress, map[string]any{"msg": "step1"}, false)
	_ = w.AppendDoneIdempotent(map[string]any{"outcome": "success"}, 3)
	_ = w.Close()

	// Stream should complete once it sees the done event.
	select {
	case err := <-streamDone:
		if err != nil {
			t.Fatalf("stream error: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for stream to finish")
	}

	mu.Lock()
	n := len(collected)
	mu.Unlock()

	if n != 4 {
		mu.Lock()
		t.Errorf("want 4 events, got %d: %v", n, collected)
		mu.Unlock()
	}
}

// TestStreamContextCancellation verifies that cancelling the context ends a follower.
func TestStreamContextCancellation(t *testing.T) {
	dir := t.TempDir()
	w, err := eventfile.Create(dir, "r4")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, _ = w.Append(eventfile.KindQueued, map[string]any{}, false)
	// No done event — stream will follow forever.

	ctx, cancel := context.WithCancel(context.Background())
	streamDone := make(chan error, 1)
	go func() {
		err := eventfile.Stream(ctx, dir, "r4", eventfile.ReadOptions{
			Follow:    true,
			PollEvery: 20 * time.Millisecond,
		}, func(line []byte) error { return nil })
		streamDone <- err
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()
	_ = w.Close()

	select {
	case err := <-streamDone:
		if err != context.Canceled {
			t.Errorf("want context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("timed out waiting for stream to cancel")
	}
}

// rawAppend appends raw bytes to the mission's events file. Used to simulate
// in-flight appends (partial lines) and torn-tail junk the way concurrent
// writers and crashed writers produce them.
func rawAppend(t *testing.T, dir, missionID, raw string) {
	t.Helper()
	f, err := os.OpenFile(filepath.Join(dir, missionID+"-events"), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("open raw append: %v", err)
	}
	if _, err := f.WriteString(raw); err != nil {
		t.Fatalf("raw append: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// TestStreamHoldsPartialLineAcrossPolls: a single write() of an event line is
// not atomic versus a concurrent read, so a follower can observe half a line
// at EOF. The stream must buffer the partial bytes across poll iterations and
// emit exactly one complete line once the terminating newline arrives — never
// two invalid-JSON fragments.
func TestStreamHoldsPartialLineAcrossPolls(t *testing.T) {
	dir := t.TempDir()
	w, err := eventfile.Create(dir, "rp")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, _ = w.Append(eventfile.KindQueued, map[string]any{}, false) // seq 1
	_ = w.Close()
	// An append caught mid-write: the line's tail (and newline) not yet visible.
	rawAppend(t, dir, "rp", `{"seq":2,"event":"progress","msg":"hi"`)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var mu sync.Mutex
	var collected []map[string]any
	streamDone := make(chan error, 1)
	go func() {
		streamDone <- eventfile.Stream(ctx, dir, "rp", eventfile.ReadOptions{
			Follow:    true,
			PollEvery: 20 * time.Millisecond,
		}, func(line []byte) error {
			var ev map[string]any
			if err := json.Unmarshal(line, &ev); err != nil {
				return err
			}
			mu.Lock()
			collected = append(collected, ev)
			mu.Unlock()
			return nil
		})
	}()

	// Let the follower reach the partial tail and poll on it a few times.
	time.Sleep(150 * time.Millisecond)
	rawAppend(t, dir, "rp", "}\n"+`{"seq":3,"event":"done","outcome":"success"}`+"\n")

	select {
	case err := <-streamDone:
		if err != nil {
			t.Fatalf("stream error (fragment emitted as a line?): %v", err)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for stream to finish")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(collected) != 3 {
		t.Fatalf("want 3 events (queued, progress, done), got %d: %v", len(collected), collected)
	}
	if collected[1]["event"] != "progress" || collected[1]["msg"] != "hi" {
		t.Errorf("split line not reassembled: %v", collected[1])
	}
	if collected[2]["event"] != "done" {
		t.Errorf("last event: %v", collected[2])
	}
}

// TestStreamDropsTrailingPartialWithoutFollow: in non-follow mode a
// non-newline-terminated tail at EOF is an unacknowledged torn tail — it must
// be dropped, not handed to the client as if it were an event.
func TestStreamDropsTrailingPartialWithoutFollow(t *testing.T) {
	dir := t.TempDir()
	w, err := eventfile.Create(dir, "rt")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, _ = w.Append(eventfile.KindQueued, map[string]any{}, false)
	_ = w.Close()
	rawAppend(t, dir, "rt", `{"seq":2,"event":"progress","msg":"tor`)

	events := collectEvents(t, dir, "rt", eventfile.ReadOptions{})
	if len(events) != 1 || events[0]["event"] != "queued" {
		t.Errorf("want only the queued event, got %v", events)
	}
}

// TestStreamSkipsCorruptLineAndTerminatesOnDone: the writer's torn-tail
// repair terminates junk fragments with a newline, so files can permanently
// contain a newline-terminated non-JSON line. The stream must skip it (one
// corrupt line must not poison every consumer forever) and still detect the
// done event that follows.
func TestStreamSkipsCorruptLineAndTerminatesOnDone(t *testing.T) {
	dir := t.TempDir()
	w, err := eventfile.Create(dir, "rc")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, _ = w.Append(eventfile.KindQueued, map[string]any{}, false) // seq 1
	_ = w.Close()
	rawAppend(t, dir, "rc",
		`{"seq":2,"event":"progress","msg":"tor`+"\n"+ // repaired torn tail
			`{"seq":3,"event":"done","outcome":"lost","exit_code":0}`+"\n")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var mu sync.Mutex
	var collected []map[string]any
	err = eventfile.Stream(ctx, dir, "rc", eventfile.ReadOptions{
		Follow:    true,
		PollEvery: 20 * time.Millisecond,
	}, func(line []byte) error {
		var ev map[string]any
		if err := json.Unmarshal(line, &ev); err != nil {
			return err
		}
		mu.Lock()
		collected = append(collected, ev)
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(collected) != 2 {
		t.Fatalf("want 2 events (queued, done), got %d: %v", len(collected), collected)
	}
	if collected[1]["event"] != "done" {
		t.Errorf("last event: %v", collected[1])
	}
}

// TestStreamFollowFromAtDoneSeqTerminates: the From filter skips emission,
// but done-detection must still see the skipped line. A follower resuming
// with from >= the done's seq would otherwise poll forever on a finished
// mission.
func TestStreamFollowFromAtDoneSeqTerminates(t *testing.T) {
	dir := t.TempDir()
	w, err := eventfile.Create(dir, "rf")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, _ = w.Append(eventfile.KindQueued, map[string]any{}, false)  // seq 1
	_, _ = w.Append(eventfile.KindRunning, map[string]any{}, false) // seq 2
	_ = w.AppendDoneIdempotent(map[string]any{"outcome": "success"}, 3)
	_ = w.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var emitted int
	err = eventfile.Stream(ctx, dir, "rf", eventfile.ReadOptions{
		From:      3,
		Follow:    true,
		PollEvery: 20 * time.Millisecond,
	}, func(line []byte) error {
		emitted++
		return nil
	})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	if emitted != 0 {
		t.Errorf("want 0 emitted events (all at/below from), got %d", emitted)
	}
}

// TestStreamRejectsOversizeLine: Stream's bufio.Reader.ReadBytes
// grew its buffer without bound. A corrupt events file (partial flush
// joining two lines, manual edit) could OOM the daemon when a follow=true
// HTTP client opened a stream. Now bounded at 16 MiB with a typed error.
func TestStreamRejectsOversizeLine(t *testing.T) {
	dir := t.TempDir()
	w, err := eventfile.Create(dir, "oversize")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_ = w.Close()

	// Append a single line that exceeds the 16 MiB cap.
	path := filepath.Join(dir, "oversize-events")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("open append: %v", err)
	}
	big := strings.Repeat("x", (16<<20)+1024)
	if _, err := f.WriteString(big + "\n"); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = f.Close()

	err = eventfile.Stream(context.Background(), dir, "oversize",
		eventfile.ReadOptions{},
		func([]byte) error { return nil })
	if !errors.Is(err, eventfile.ErrEventLineTooLarge) {
		t.Errorf("err=%v, want ErrEventLineTooLarge", err)
	}
}
