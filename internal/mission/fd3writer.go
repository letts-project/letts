package mission

import (
	"context"
	"time"

	"letts/internal/eventfile"
)

// RunFd3Writer drains progressCh, rate-limits to maxRatePerSec via a one-second
// token bucket, and appends progress events to ew. The function returns when
// progressCh is closed or ctx is cancelled, then closes done.
//
// dropped, if non-nil, is incremented when an event is dropped due to the rate
// limit or when eventfile.Append returns an error. The events file's own
// per-line and per-buffer caps increment its internal ProgressDrops counter
// independently — this counter only sees the writer's own drops.
func RunFd3Writer(ctx context.Context, progressCh <-chan ProgressEvent, ew *eventfile.Writer, maxRatePerSec int, dropped *int64, done chan<- struct{}) {
	defer close(done)

	tokens := maxRatePerSec
	var refill <-chan time.Time
	if maxRatePerSec > 0 {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		refill = ticker.C
	}

	for {
		// Prioritise cancellation. When maxRatePerSec==0 there is no refill
		// ticker, so the main select below has only ctx.Done() and progressCh
		// ready; Go picks a ready case at random, which would non-
		// deterministically emit some buffered events instead of counting them
		// as drops. Once cancelled we must stop
		// emitting and drain the remainder as drops so progress_dropped is
		// deterministic and truthful.
		select {
		case <-ctx.Done():
			drainProgressOnCancel(progressCh, dropped)
			return
		default:
		}

		select {
		case <-ctx.Done():
			// Flush-or-drop every queued event on
			// cancellation. We non-blockingly drain progressCh and count each
			// remaining event as a drop — they're too late to emit (the events
			// file is about to close) but the counter must reflect the truth so
			// consumers see "we lost N progress events".
			drainProgressOnCancel(progressCh, dropped)
			return
		case <-refill:
			tokens = maxRatePerSec
		case ev, ok := <-progressCh:
			if !ok {
				return
			}
			if maxRatePerSec > 0 && tokens <= 0 {
				if dropped != nil {
					*dropped++
				}
				continue
			}
			fields := map[string]any{
				"time": time.Now().UnixMilli(),
			}
			if ev.Value != nil {
				fields["value"] = *ev.Value
			}
			if ev.Message != "" {
				fields["message"] = ev.Message
			}
			if _, err := ew.Append(eventfile.KindProgress, fields, false); err != nil {
				if dropped != nil {
					*dropped++
				}
			}
			if maxRatePerSec > 0 {
				tokens--
			}
		}
	}
}

// drainProgressOnCancel reads everything currently in progressCh without
// blocking and increments dropped per event. Returns as soon as the
// channel has nothing left to read OR is closed.
func drainProgressOnCancel(progressCh <-chan ProgressEvent, dropped *int64) {
	for {
		select {
		case _, ok := <-progressCh:
			if !ok {
				return
			}
			if dropped != nil {
				*dropped++
			}
		default:
			return
		}
	}
}
