package mission

import (
	"bytes"
	"io"
	"sync/atomic"
)

const phpOOMMarker = "Allowed memory size of"

// OOMDetector wraps a downstream writer and atomically sets flag the first
// time the PHP OOM marker is observed in the byte stream. It retains the last
// len(marker)-1 bytes between Write calls so the marker is detected across
// chunk boundaries.
type OOMDetector struct {
	dst  io.Writer
	flag *atomic.Bool
	tail []byte
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
	if o.flag.Load() {
		// Already triggered — still keep tail current so caller invariants
		// hold, but skip the search.
		o.updateTail(p[:n])
		return n, err
	}
	// Concatenate retained tail with the bytes we just forwarded.
	buf := make([]byte, 0, len(o.tail)+n)
	buf = append(buf, o.tail...)
	buf = append(buf, p[:n]...)
	if bytes.Contains(buf, []byte(phpOOMMarker)) {
		o.flag.Store(true)
	}
	o.tail = updatedTail(buf)
	return n, err
}

func (o *OOMDetector) updateTail(p []byte) {
	if len(o.tail) == 0 && len(p) >= len(phpOOMMarker)-1 {
		o.tail = append(o.tail[:0], p[len(p)-(len(phpOOMMarker)-1):]...)
		return
	}
	buf := make([]byte, 0, len(o.tail)+len(p))
	buf = append(buf, o.tail...)
	buf = append(buf, p...)
	o.tail = updatedTail(buf)
}

func updatedTail(buf []byte) []byte {
	retain := len(phpOOMMarker) - 1
	if retain > len(buf) {
		retain = len(buf)
	}
	out := make([]byte, retain)
	copy(out, buf[len(buf)-retain:])
	return out
}
