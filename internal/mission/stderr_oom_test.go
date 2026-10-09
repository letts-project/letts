package mission

import (
	"bytes"
	"strings"
	"sync/atomic"
	"testing"
	"unicode/utf8"
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

func writeChunks(t *testing.T, d *OOMDetector, chunks ...string) {
	t.Helper()
	for _, c := range chunks {
		if _, err := d.Write([]byte(c)); err != nil {
			t.Fatalf("write %q: %v", c, err)
		}
	}
}

func TestOOMDetectorLine(t *testing.T) {
	const fatal = "PHP Fatal error:  Allowed memory size of 134217728 bytes exhausted (tried to allocate 20480 bytes) in /srv/app/Mission.php on line 42"
	cases := []struct {
		name   string
		chunks []string
		want   string
	}{
		{"single write", []string{fatal + "\n"}, fatal},
		{"surrounding lines", []string{"PHP Warning:  foo\nnotice\n" + fatal + "\nPHP Stack trace:\n"}, fatal},
		{"marker and line end split across writes", []string{"warn\nPHP Fatal error:  Allowed mem", "ory size of 16 bytes", " exhausted (tried)", "\nnext line\n"},
			"PHP Fatal error:  Allowed memory size of 16 bytes exhausted (tried)"},
		{"crlf", []string{fatal + "\r\n"}, fatal},
		{"no trailing newline", []string{fatal}, fatal},
		{"first marker line wins", []string{fatal + "\nPHP Fatal error:  Allowed memory size of 1 bytes exhausted\n"}, fatal},
		{"no marker", []string{"PHP Warning:  foo\nall good\n"}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			var flag atomic.Bool
			d := NewOOMDetector(&buf, &flag)
			writeChunks(t, d, tc.chunks...)
			if got := d.Line(); got != tc.want {
				t.Errorf("Line()=%q\nwant    %q", got, tc.want)
			}
			if flag.Load() != (tc.want != "") {
				t.Errorf("flag=%v, want %v", flag.Load(), tc.want != "")
			}
		})
	}
}

func TestOOMDetectorLineByteByByte(t *testing.T) {
	const line = "PHP Fatal error:  Allowed memory size of 16 bytes exhausted"
	var buf bytes.Buffer
	var flag atomic.Bool
	d := NewOOMDetector(&buf, &flag)
	in := "noise\n" + line + "\nmore\n"
	for i := 0; i < len(in); i++ {
		writeChunks(t, d, in[i:i+1])
	}
	if got := d.Line(); got != line {
		t.Errorf("Line()=%q, want %q", got, line)
	}
}

func TestOOMDetectorLineBoundedAndRuneSafe(t *testing.T) {
	const marker = "Allowed memory size of 16 bytes exhausted"
	longHead := strings.Repeat("ш", 10*1024)
	longTail := strings.Repeat("щ", 10*1024)
	cases := []struct {
		name   string
		chunks []string
	}{
		{"long text before the marker", []string{longHead[:7001], longHead[7001:] + marker + "\n"}},
		{"long text after the marker", []string{"PHP Fatal error:  " + marker + " in " + longTail[:3], longTail[3:] + "\n"}},
		{"long on both sides", []string{longHead + marker + longTail + "\n"}},
		{"invalid utf-8 around the marker", []string{"\xff\xfePHP Fatal error:  " + marker + " \xc3\n"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			var flag atomic.Bool
			d := NewOOMDetector(&buf, &flag)
			writeChunks(t, d, tc.chunks...)
			got := d.Line()
			if !strings.Contains(got, marker) {
				t.Errorf("Line() lost the marker: %q", got)
			}
			if len(got) > 512 || !utf8.ValidString(got) || strings.ContainsAny(got, "\r\n") {
				t.Errorf("Line() not a bounded valid single line (%d bytes): %q", len(got), got)
			}
			if buf.String() != strings.Join(tc.chunks, "") {
				t.Error("downstream bytes differ from input")
			}
		})
	}
}
