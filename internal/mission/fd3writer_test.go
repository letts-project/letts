package mission

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"letts/internal/eventfile"
	"letts/internal/ids"
)

func newEventWriter(t *testing.T) (*eventfile.Writer, string) {
	t.Helper()
	dir := t.TempDir()
	id := ids.NewUUIDv7()
	w, err := eventfile.Create(dir, id)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w, filepath.Join(dir, id+"-events")
}

func countLines(t *testing.T, path string) int {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	n := 0
	for _, c := range b {
		if c == '\n' {
			n++
		}
	}
	return n
}

func TestRunFd3WriterRateLimitDropsBurst(t *testing.T) {
	w, path := newEventWriter(t)
	progressCh := make(chan ProgressEvent, 1000)
	for i := 0; i < 1000; i++ {
		v := float64(i) / 1000
		progressCh <- ProgressEvent{Value: &v}
	}
	close(progressCh)

	var dropped int64
	done := make(chan struct{})
	go RunFd3Writer(context.Background(), progressCh, w, 50, &dropped, done)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("writer didn't return")
	}

	written := int64(countLines(t, path))
	// First bucket of 50 is consumed; refill happens at 1s. The drain
	// completes in <100ms typically, so we expect ~50 written and ~950 dropped.
	// Allow generous slack for slow CI.
	if written < 30 || written > 200 {
		t.Errorf("written=%d, want roughly 50 (range 30-200)", written)
	}
	if dropped < 700 {
		t.Errorf("dropped=%d, want >= 700", dropped)
	}
	if written+dropped != 1000 {
		t.Errorf("written+dropped = %d, want 1000", written+dropped)
	}
}

func TestRunFd3WriterUnlimitedRateWritesAll(t *testing.T) {
	w, path := newEventWriter(t)
	progressCh := make(chan ProgressEvent, 100)
	for i := 0; i < 100; i++ {
		v := float64(i)
		progressCh <- ProgressEvent{Value: &v, Message: "step"}
	}
	close(progressCh)

	var dropped int64
	done := make(chan struct{})
	go RunFd3Writer(context.Background(), progressCh, w, 0, &dropped, done)
	<-done

	if got := countLines(t, path); got != 100 {
		t.Errorf("written=%d, want 100", got)
	}
	if dropped != 0 {
		t.Errorf("dropped=%d, want 0", dropped)
	}
}

func TestRunFd3WriterContextCancel(t *testing.T) {
	w, _ := newEventWriter(t)
	progressCh := make(chan ProgressEvent, 1)
	ctx, cancel := context.WithCancel(context.Background())
	var dropped int64
	done := make(chan struct{})
	go RunFd3Writer(ctx, progressCh, w, 0, &dropped, done)

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("writer didn't return after ctx cancel")
	}
}

// TestRunFd3WriterContextCancelCountsBufferedDrops is the fix: when
// ctx is cancelled while events are still buffered in progressCh, the
// writer drains the remainder non-blockingly and counts each event as a
// drop. Pre-fix the ctx.Done() branch returned immediately and silently
// lost the buffered events from progress_dropped.
func TestRunFd3WriterContextCancelCountsBufferedDrops(t *testing.T) {
	w, _ := newEventWriter(t)
	progressCh := make(chan ProgressEvent, 16)
	for i := 0; i < 12; i++ {
		v := float64(i)
		progressCh <- ProgressEvent{Value: &v}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel BEFORE the writer starts so it sees ctx.Done() first.

	var dropped int64
	done := make(chan struct{})
	go RunFd3Writer(ctx, progressCh, w, 0, &dropped, done)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("writer didn't return after ctx cancel")
	}

	if dropped != 12 {
		t.Errorf("dropped=%d want 12 (buffered events must count as drops)", dropped)
	}
}

func TestRunFd3WriterChannelClosedReturns(t *testing.T) {
	w, _ := newEventWriter(t)
	progressCh := make(chan ProgressEvent)
	close(progressCh)
	var dropped int64
	done := make(chan struct{})
	go RunFd3Writer(context.Background(), progressCh, w, 50, &dropped, done)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("writer didn't return on closed empty channel")
	}
}
