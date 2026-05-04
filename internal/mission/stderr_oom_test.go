package mission

import (
	"bytes"
	"sync/atomic"
	"testing"
)

func TestOOMDetectorMatchesContiguous(t *testing.T) {
	var buf bytes.Buffer
	var flag atomic.Bool
	d := NewOOMDetector(&buf, &flag)
	if _, err := d.Write([]byte("PHP Fatal error:  Allowed memory size of 16777216 bytes exhausted\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if !flag.Load() {
		t.Fatal("flag should be true after marker")
	}
	if !bytes.Contains(buf.Bytes(), []byte("Allowed memory size of")) {
		t.Errorf("downstream missing marker: %q", buf.String())
	}
}

func TestOOMDetectorNoMatch(t *testing.T) {
	var buf bytes.Buffer
	var flag atomic.Bool
	d := NewOOMDetector(&buf, &flag)
	if _, err := d.Write([]byte("normal stderr line\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if flag.Load() {
		t.Fatal("flag should not be set without marker")
	}
}

func TestOOMDetectorMatchesAcrossWrites(t *testing.T) {
	var buf bytes.Buffer
	var flag atomic.Bool
	d := NewOOMDetector(&buf, &flag)
	// Split the marker across three writes so the matcher must use its tail.
	chunks := []string{
		"PHP Fatal error:  Allowed mem",
		"ory size of 16777216 bytes",
		" exhausted\n",
	}
	for _, c := range chunks {
		if _, err := d.Write([]byte(c)); err != nil {
			t.Fatalf("write %q: %v", c, err)
		}
	}
	if !flag.Load() {
		t.Fatal("flag should be true after split-write marker")
	}
}

func TestOOMDetectorTailRetainsAcrossManyTinyWrites(t *testing.T) {
	var buf bytes.Buffer
	var flag atomic.Bool
	d := NewOOMDetector(&buf, &flag)
	full := "Allowed memory size of"
	for i := 0; i < len(full); i++ {
		if _, err := d.Write([]byte{full[i]}); err != nil {
			t.Fatalf("write[%d]: %v", i, err)
		}
	}
	if !flag.Load() {
		t.Fatal("flag should be true after byte-by-byte marker")
	}
}

func TestOOMDetectorPassesBytesThrough(t *testing.T) {
	var buf bytes.Buffer
	var flag atomic.Bool
	d := NewOOMDetector(&buf, &flag)
	payload := []byte("some random stderr output without OOM\n")
	n, err := d.Write(payload)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if n != len(payload) {
		t.Errorf("wrote %d bytes, want %d", n, len(payload))
	}
	if !bytes.Equal(buf.Bytes(), payload) {
		t.Errorf("downstream got %q, want %q", buf.String(), payload)
	}
}

func TestOOMDetectorRepeatedFlagOnceTriggered(t *testing.T) {
	var buf bytes.Buffer
	var flag atomic.Bool
	d := NewOOMDetector(&buf, &flag)
	_, _ = d.Write([]byte("Allowed memory size of\n"))
	if !flag.Load() {
		t.Fatal("flag should be true")
	}
	_, _ = d.Write([]byte("more stuff\n"))
	if !flag.Load() {
		t.Fatal("flag should remain true after subsequent writes")
	}
}
