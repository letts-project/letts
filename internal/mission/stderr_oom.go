package mission

import (
	"bytes"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"unicode/utf8"
)

const phpOOMMarker = "Allowed memory size of"

const (
	// oomLineMax caps the captured marker line returned by Line.
	oomLineMax = 512
	// oomLinePrefixMax is how much of the line before the marker is kept.
	oomLinePrefixMax = 128
	// oomWindow bounds the retained tail of the current line while searching.
	oomWindow = oomLinePrefixMax + len(phpOOMMarker)
)

// OOMDetector wraps a downstream writer and atomically sets flag the first
// time the PHP OOM marker is observed in the byte stream. It also captures
// the stderr line holding the first marker (see Line). The marker and the
// line are detected across chunk boundaries.
type OOMDetector struct {
	dst  io.Writer
	flag *atomic.Bool

	mu       sync.Mutex
	cur      []byte // tail of the current line, at most oomWindow bytes between writes
	line     []byte // marker line from up to oomLinePrefixMax bytes before the marker
	lineDone bool
}

// NewOOMDetector returns a writer that mirrors p to dst and probes for the
// marker as a side effect.
func NewOOMDetector(dst io.Writer, flag *atomic.Bool) *OOMDetector {
	return &OOMDetector{dst: dst, flag: flag}
}

func (o *OOMDetector) Write(p []byte) (int, error) {
	n, err := o.dst.Write(p)
	if n <= 0 {
		return n, err
	}
	o.mu.Lock()
	o.scan(p[:n])
	o.mu.Unlock()
	return n, err
}

// lineCaptureMax leaves room to cut the captured line at a rune boundary.
const lineCaptureMax = oomLineMax + utf8.UTFMax

func (o *OOMDetector) scan(p []byte) {
	for len(p) > 0 && !o.lineDone {
		seg, rest, newline := bytes.Cut(p, []byte{'\n'})
		p = rest
		if o.line != nil {
			o.line = append(o.line, seg[:min(len(seg), lineCaptureMax-len(o.line))]...)
			o.lineDone = newline || len(o.line) >= lineCaptureMax
			continue
		}
		o.cur = append(o.cur, seg...)
		if i := bytes.Index(o.cur, []byte(phpOOMMarker)); i >= 0 {
			o.flag.Store(true)
			start := max(0, i-oomLinePrefixMax)
			o.line = append([]byte(nil), o.cur[start:min(len(o.cur), start+lineCaptureMax)]...)
			o.lineDone = newline || len(o.line) >= lineCaptureMax
			o.cur = nil
			continue
		}
		if newline {
			o.cur = o.cur[:0]
		} else if len(o.cur) > oomWindow {
			o.cur = append(o.cur[:0], o.cur[len(o.cur)-oomWindow:]...)
		}
	}
}

// Line returns the stderr line holding the first PHP OOM marker, trimmed,
// with invalid UTF-8 replaced and clipped to oomLineMax bytes at a rune
// boundary, or "" if the marker was not seen. A longer line is cut to the
// marker's surroundings.
func (o *OOMDetector) Line() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	b := o.line
	for len(b) > 0 && !utf8.RuneStart(b[0]) {
		b = b[1:]
	}
	return clipUTF8(strings.ToValidUTF8(strings.TrimSpace(string(b)), "\uFFFD"), oomLineMax)
}
