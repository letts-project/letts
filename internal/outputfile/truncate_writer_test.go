package outputfile

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
)

// openTmpFile creates a temp file and returns it; caller should defer close+remove.
func openTmpFile(t *testing.T) *os.File {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "out-*")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	return f
}

// readFile reads entire content of f (seeks to beginning first).
func readFile(t *testing.T, f *os.File) []byte {
	t.Helper()
	if _, err := f.Seek(0, 0); err != nil {
		t.Fatalf("seek: %v", err)
	}
	data, err := os.ReadFile(f.Name())
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return data
}

// TestStdoutStderrSeparate verifies writes go to separate files.
func TestStdoutStderrSeparate(t *testing.T) {
	stdout := openTmpFile(t)
	stderr := openTmpFile(t)
	comb := openTmpFile(t)

	tw := New(1000, stdout, stderr, comb)

	if _, err := fmt.Fprintf(tw.Stdout(), "hello stdout"); err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintf(tw.Stderr(), "hello stderr"); err != nil {
		t.Fatal(err)
	}

	so := readFile(t, stdout)
	se := readFile(t, stderr)

	if string(so) != "hello stdout" {
		t.Errorf("stdout file: got %q, want %q", so, "hello stdout")
	}
	if string(se) != "hello stderr" {
		t.Errorf("stderr file: got %q, want %q", se, "hello stderr")
	}
}

// TestCombinedNDJSON verifies both streams appear in the combined file as NDJSON.
func TestCombinedNDJSON(t *testing.T) {
	stdout := openTmpFile(t)
	stderr := openTmpFile(t)
	comb := openTmpFile(t)

	tw := New(10000, stdout, stderr, comb)

	_, _ = fmt.Fprintf(tw.Stdout(), "out-data")
	_, _ = fmt.Fprintf(tw.Stderr(), "err-data")

	data := readFile(t, comb)
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 combined lines, got %d: %s", len(lines), data)
	}

	for i, line := range lines {
		var obj map[string]any
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			t.Fatalf("line %d not valid JSON: %v", i, err)
		}
		if _, ok := obj["t"]; !ok {
			t.Errorf("line %d missing 't'", i)
		}
		if _, ok := obj["stream"]; !ok {
			t.Errorf("line %d missing 'stream'", i)
		}
		if _, ok := obj["data"]; !ok {
			t.Errorf("line %d missing 'data'", i)
		}
	}

	// First line is stdout, second is stderr.
	var first map[string]any
	_ = json.Unmarshal([]byte(lines[0]), &first)
	if first["stream"] != "stdout" {
		t.Errorf("first line stream: want stdout, got %v", first["stream"])
	}
	var second map[string]any
	_ = json.Unmarshal([]byte(lines[1]), &second)
	if second["stream"] != "stderr" {
		t.Errorf("second line stream: want stderr, got %v", second["stream"])
	}
}

// TestTruncateSharedLimit verifies that after total exceeds limit, a single marker
// is written per stream and further writes are silently discarded.
func TestTruncateSharedLimit(t *testing.T) {
	stdout := openTmpFile(t)
	stderr := openTmpFile(t)
	comb := openTmpFile(t)

	// limit = 100 bytes shared
	tw := New(100, stdout, stderr, comb)

	// Write 60 bytes to stdout — OK
	data60 := bytes.Repeat([]byte("A"), 60)
	n, err := tw.Stdout().Write(data60)
	if err != nil || n != 60 {
		t.Fatalf("stdout first write: n=%d err=%v", n, err)
	}

	// Write 60 bytes to stderr — overflows (sum=120 > 100)
	n, err = tw.Stderr().Write(data60)
	if err != nil || n != 60 {
		t.Fatalf("stderr overflow write: n=%d err=%v (should still return len(p), nil)", n, err)
	}

	// Stderr should be truncated; stdout should NOT be.
	stdTrunc, errTrunc := tw.Truncated()
	if stdTrunc {
		t.Error("stdout should not be truncated")
	}
	if !errTrunc {
		t.Error("stderr should be truncated")
	}

	// Further write to stderr must silently succeed (return n, nil) and be discarded.
	n, err = tw.Stderr().Write([]byte("should be dropped"))
	if err != nil || n != len("should be dropped") {
		t.Fatalf("post-truncate stderr write: n=%d err=%v", n, err)
	}

	// Stderr file should contain the truncate marker.
	se := string(readFile(t, stderr))
	if !strings.Contains(se, "truncated") {
		t.Errorf("stderr missing truncate marker: %q", se)
	}

	// Stdout file should not contain truncate marker.
	so := string(readFile(t, stdout))
	if strings.Contains(so, "truncated") {
		t.Errorf("stdout has unexpected truncate marker: %q", so)
	}
}

// TestTruncateMarkerOnce verifies the marker is written only once per stream.
func TestTruncateMarkerOnce(t *testing.T) {
	stdout := openTmpFile(t)
	stderr := openTmpFile(t)
	comb := openTmpFile(t)

	tw := New(10, stdout, stderr, comb)

	// Force truncation on stdout by writing 20 bytes.
	_, _ = tw.Stdout().Write(bytes.Repeat([]byte("B"), 20))
	// Write again — should silently discard.
	_, _ = tw.Stdout().Write(bytes.Repeat([]byte("C"), 20))

	so := string(readFile(t, stdout))
	// Count occurrences of "truncated" — must be exactly 1.
	count := strings.Count(so, "truncated")
	if count != 1 {
		t.Errorf("expected exactly 1 truncate marker, got %d in: %q", count, so)
	}
}

// TestTruncatedFlags verifies both flags when both streams exceed limit.
func TestTruncatedFlags(t *testing.T) {
	stdout := openTmpFile(t)
	stderr := openTmpFile(t)
	comb := openTmpFile(t)

	tw := New(5, stdout, stderr, comb)

	_, _ = tw.Stdout().Write([]byte("123456")) // overflows
	_, _ = tw.Stderr().Write([]byte("abcdef")) // also overflows

	stdTrunc, errTrunc := tw.Truncated()
	if !stdTrunc {
		t.Error("stdout should be truncated")
	}
	if !errTrunc {
		t.Error("stderr should be truncated")
	}
}

// TestConcurrentWrites verifies race-safety (run with -race).
func TestConcurrentWrites(t *testing.T) {
	stdout := openTmpFile(t)
	stderr := openTmpFile(t)
	comb := openTmpFile(t)

	tw := New(100000, stdout, stderr, comb)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _ = tw.Stdout().Write([]byte("stdout chunk"))
		}()
		go func() {
			defer wg.Done()
			_, _ = tw.Stderr().Write([]byte("stderr chunk"))
		}()
	}
	wg.Wait()
}

// TestNonUTF8InCombined verifies non-UTF-8 bytes produce base64 encoding in combined.
func TestNonUTF8InCombined(t *testing.T) {
	stdout := openTmpFile(t)
	stderr := openTmpFile(t)
	comb := openTmpFile(t)

	tw := New(10000, stdout, stderr, comb)

	// Write non-UTF-8 byte sequence.
	_, _ = tw.Stdout().Write([]byte{0xFF, 0xFE, 0x00})

	data := readFile(t, comb)
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if len(lines) < 1 {
		t.Fatal("no combined lines")
	}

	var obj map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &obj); err != nil {
		t.Fatalf("parse combined line: %v", err)
	}
	enc, _ := obj["encoding"].(string)
	if enc != "base64" {
		t.Errorf("expected encoding=base64 for non-UTF-8 data, got %q (obj=%v)", enc, obj)
	}
}
