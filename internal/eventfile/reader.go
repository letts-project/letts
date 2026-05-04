package eventfile

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"time"
)

// ErrEventLineTooLarge is returned by Stream when a single line on disk
// exceeds maxScanLineSize. Caused by a corrupt events file (partial
// write that joined two lines, manual edit, etc.). Without the cap a
// follow=true client could OOM the daemon.
var ErrEventLineTooLarge = errors.New("eventfile: line exceeds 16 MiB cap")

// ReadOptions controls streaming behaviour.
type ReadOptions struct {
	// From: skip events with seq <= From.
	From int64

	// Follow: if true, poll for new events after reaching EOF until a done
	// event is seen or the context is cancelled.
	Follow bool

	// PollEvery is the polling interval used when Follow=true.
	// Defaults to 100ms if zero.
	PollEvery time.Duration
}

// Stream reads events from the events file, calling emit for each complete
// line whose seq > opts.From.
//
// Only newline-terminated data counts as a line. A partial tail at EOF — an
// append in flight (a single write() of a line is not atomic versus a
// concurrent read), or a torn tail left by a crashed writer — is buffered
// across poll iterations and emitted once its terminating '\n' arrives. In
// non-follow mode a trailing partial at EOF is dropped: it is an
// unacknowledged torn tail, not a complete event.
//
// Lines that are empty or not valid JSON are skipped. The writer's torn-tail
// repair (Open / post-error Append) terminates junk fragments with a bare
// '\n', so a file can permanently contain a corrupt line; emitting it
// verbatim would poison every consumer of the stream forever, while the
// fragment was never acknowledged as an event in the first place.
//
// Without follow, Stream returns nil when EOF is reached.
// With follow, Stream polls for new data after EOF until a "done" event is
// seen or ctx is cancelled. Done-detection runs on every valid line —
// including lines skipped by the From filter — so a resume with
// from >= the done's seq terminates instead of following forever.
// Returns ctx.Err() on cancellation.
func Stream(ctx context.Context, parentDir, missionID string, opts ReadOptions, emit func(line []byte) error) error {
	path := eventFilePath(parentDir, missionID)

	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	pollEvery := opts.PollEvery
	if pollEvery == 0 {
		pollEvery = 100 * time.Millisecond
	}

	reader := bufio.NewReader(f)

	// partial accumulates the bytes of a line whose terminating '\n' has not
	// been read yet. bufio.Reader has already consumed them from the file,
	// so they must be carried here across poll iterations.
	var partial []byte

	for {
		// Check for cancellation before each read attempt.
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// The line-size cap applies to the accumulated line, so the bound
		// shrinks by what is already buffered.
		chunk, err := readBoundedLine(reader, maxScanLineSize-len(partial))
		if errors.Is(err, ErrEventLineTooLarge) {
			return err
		}
		partial = append(partial, chunk...)

		if n := len(partial); n > 0 && partial[n-1] == '\n' {
			// Complete line. Emit without the trailing newline — the caller
			// re-adds it as needed.
			line := partial[:n-1]
			partial = nil

			// Skip newline-terminated junk (empty or invalid JSON).
			if len(line) > 0 && json.Valid(line) {
				var meta struct {
					Seq   int64  `json:"seq"`
					Event string `json:"event"`
				}
				parsed := json.Unmarshal(line, &meta) == nil

				// From filter: emit only events with seq > From. Lines whose
				// seq cannot be parsed are emitted (matching scan behaviour:
				// unknown seq is not silently dropped).
				if !(parsed && opts.From > 0 && meta.Seq <= opts.From) {
					if emitErr := emit(line); emitErr != nil {
						return emitErr
					}
				}

				// Done-detection must also see lines the From filter
				// skipped — a follower resuming at/after the done's seq
				// would otherwise poll a finished mission forever.
				if parsed && meta.Event == string(KindDone) {
					return nil
				}
			}
		}

		if err == io.EOF {
			if !opts.Follow {
				// Any trailing partial is dropped — see doc comment.
				return nil
			}
			// Follow: wait and retry.
			if werr := waitForData(ctx, pollEvery); werr != nil {
				return werr
			}
			continue
		}
		if err != nil {
			return err
		}
	}
}

// readBoundedLine reads bytes from r up to and including the next '\n',
// capped at max bytes. Returns ErrEventLineTooLarge if a line exceeds
// the cap (rather than growing the buffer without bound like
// bufio.Reader.ReadBytes does).
func readBoundedLine(r *bufio.Reader, max int) ([]byte, error) {
	var out []byte
	for {
		chunk, err := r.ReadSlice('\n')
		if len(out)+len(chunk) > max {
			return nil, ErrEventLineTooLarge
		}
		out = append(out, chunk...)
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		return out, err
	}
}

// waitForData blocks until the poll duration elapses or ctx is cancelled.
func waitForData(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}
