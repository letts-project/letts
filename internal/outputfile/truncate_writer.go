// Package outputfile provides shared truncate writers for mission stdout,
// stderr, and combined NDJSON files.
package outputfile

import (
	"fmt"
	"io"
	"os"
	"sync"
)

const truncMarker = "\n[--- truncated, exceeded %d bytes ---]\n"

// TruncateWriter is the shared backing store for stdout+stderr+combined.
// All three derive Writer instances from it.
type TruncateWriter struct {
	mu       sync.Mutex
	limit    int64
	current  int64
	truncStd bool
	truncErr bool

	stdout *os.File
	stderr *os.File
	comb   *os.File
}

// New creates the writer; caller passes pre-opened files (one each).
func New(limit int64, stdout, stderr, combined *os.File) *TruncateWriter {
	return &TruncateWriter{limit: limit, stdout: stdout, stderr: stderr, comb: combined}
}

// Stdout returns an io.Writer for stdout that respects shared limits.
func (t *TruncateWriter) Stdout() io.Writer { return &writerSide{tw: t, isStderr: false} }

// Stderr returns an io.Writer for stderr that respects shared limits.
func (t *TruncateWriter) Stderr() io.Writer { return &writerSide{tw: t, isStderr: true} }

// Truncated returns whether stdout/stderr were truncated (read after Wait).
func (t *TruncateWriter) Truncated() (bool, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.truncStd, t.truncErr
}

type writerSide struct {
	tw       *TruncateWriter
	isStderr bool
}

func (s *writerSide) Write(p []byte) (int, error) {
	s.tw.mu.Lock()
	defer s.tw.mu.Unlock()

	// Already truncated: discard silently.
	if (s.isStderr && s.tw.truncErr) || (!s.isStderr && s.tw.truncStd) {
		return len(p), nil
	}

	remaining := s.tw.limit - s.tw.current

	dst := s.tw.stdout
	if s.isStderr {
		dst = s.tw.stderr
	}

	if remaining <= 0 {
		// Budget just ran out — write marker once.
		marker := []byte(fmt.Sprintf(truncMarker, s.tw.limit))
		dst.Write(marker) //nolint:errcheck
		if s.isStderr {
			s.tw.truncErr = true
		} else {
			s.tw.truncStd = true
		}
		return len(p), nil
	}

	// Write as much as the budget allows.
	chunk := p
	if int64(len(chunk)) > remaining {
		chunk = chunk[:remaining]
	}

	if _, err := dst.Write(chunk); err != nil {
		return 0, err
	}
	if s.tw.comb != nil {
		appendCombined(s.tw.comb, chunk, s.isStderr)
	}
	s.tw.current += int64(len(chunk))

	// If we had to clip the write, append the truncate marker now.
	if int64(len(p)) > remaining {
		marker := []byte(fmt.Sprintf(truncMarker, s.tw.limit))
		dst.Write(marker) //nolint:errcheck
		if s.isStderr {
			s.tw.truncErr = true
		} else {
			s.tw.truncStd = true
		}
	}

	return len(p), nil
}
