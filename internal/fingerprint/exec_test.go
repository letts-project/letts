package fingerprint

import (
	"encoding/hex"
	"testing"
)

// TestExecFingerprintStableAcrossInKeyOrder verifies in[] entries are
// sorted before canonicalization — caller-supplied order must not change
// the hash.
func TestExecFingerprintStableAcrossInKeyOrder(t *testing.T) {
	base := ExecInput{
		Lane:    "normal",
		Command: []string{"convert", "{in:pdf}", "{out:png}"},
		In: []ExecFileRef{
			{Key: "pdf", StagingID: "0192a8b3-d2c1-7abc-bad0-1234567890ab", Sha256: "abc", Size: 100},
			{Key: "extra", StagingID: "0192a8b3-d2c1-7abc-bad0-1234567890ac", Sha256: "def", Size: 200},
		},
		Out: []ExecOutKey{{Key: "png"}},
	}
	reordered := base
	reordered.In = []ExecFileRef{base.In[1], base.In[0]}

	a, err := Exec(base)
	if err != nil {
		t.Fatalf("Exec(base): %v", err)
	}
	b, err := Exec(reordered)
	if err != nil {
		t.Fatalf("Exec(reordered): %v", err)
	}
	if a != b {
		t.Errorf("in[] key order changed fingerprint: %s vs %s", a, b)
	}
	if _, err := hex.DecodeString(a); err != nil {
		t.Errorf("fingerprint not hex: %s", a)
	}
	if len(a) != 64 {
		t.Errorf("fingerprint len = %d, want 64", len(a))
	}
}

// TestExecFingerprintStableAcrossOutKeyOrder verifies out[] is also sorted.
func TestExecFingerprintStableAcrossOutKeyOrder(t *testing.T) {
	base := ExecInput{
		Lane:    "normal",
		Command: []string{"x"},
		Out:     []ExecOutKey{{Key: "a"}, {Key: "b"}, {Key: "c"}},
	}
	reordered := base
	reordered.Out = []ExecOutKey{{Key: "c"}, {Key: "a"}, {Key: "b"}}

	a, _ := Exec(base)
	b, _ := Exec(reordered)
	if a != b {
		t.Errorf("out[] order changed fingerprint: %s vs %s", a, b)
	}
}

// TestExecFingerprintExcludesGroupIDAndDisplayName verifies metadata fields
// are not part of the canonical payload — they may change across retries
// without affecting idempotency.
func TestExecFingerprintExcludesGroupIDAndDisplayName(t *testing.T) {
	base := ExecInput{Lane: "normal", Command: []string{"uptime"}}
	a, _ := Exec(base)

	withGroup := base
	withGroup.GroupID = "render-batch-2026-05"
	if fp, _ := Exec(withGroup); fp != a {
		t.Errorf("GroupID should be excluded; got %s vs base %s", fp, a)
	}

	withName := base
	withName.DisplayName = "ImageMagick conversion"
	if fp, _ := Exec(withName); fp != a {
		t.Errorf("DisplayName should be excluded; got %s vs base %s", fp, a)
	}
}

// TestExecFingerprintChangesWithLane verifies a different lane yields a
// different fingerprint.
func TestExecFingerprintChangesWithLane(t *testing.T) {
	a := ExecInput{Lane: "normal", Command: []string{"uptime"}}
	b := a
	b.Lane = "heavy"
	ha, _ := Exec(a)
	hb, _ := Exec(b)
	if ha == hb {
		t.Errorf("lane change should alter fingerprint")
	}
}

// TestExecFingerprintChangesWithTimeout verifies timeout_ms is part of the
// canonical payload.
func TestExecFingerprintChangesWithTimeout(t *testing.T) {
	a := ExecInput{Lane: "normal", Command: []string{"uptime"}}
	b := a
	t1 := int64(30_000)
	b.TimeoutMs = &t1
	ha, _ := Exec(a)
	hb, _ := Exec(b)
	if ha == hb {
		t.Errorf("timeout change should alter fingerprint")
	}
}

// TestExecFingerprintEmptyStdinEqualsNone verifies the "" → "none" stdin
// normalization makes both forms produce the same fingerprint.
func TestExecFingerprintEmptyStdinEqualsNone(t *testing.T) {
	a := ExecInput{Lane: "normal", Command: []string{"uptime"}, Stdin: ""}
	b := ExecInput{Lane: "normal", Command: []string{"uptime"}, Stdin: "none"}
	ha, _ := Exec(a)
	hb, _ := Exec(b)
	if ha != hb {
		t.Errorf("empty stdin should equal 'none'; got %s vs %s", ha, hb)
	}
}

// TestExecFingerprintScriptIncluded verifies script presence affects the
// fingerprint.
func TestExecFingerprintScriptIncluded(t *testing.T) {
	a := ExecInput{Lane: "normal", Command: []string{"uptime"}}
	b := a
	b.Script = &ExecScriptRef{StagingID: "0192a8b3-d2c1-7abc-bad0-1234567890ab", Sha256: "abcdef"}
	ha, _ := Exec(a)
	hb, _ := Exec(b)
	if ha == hb {
		t.Errorf("script presence should alter fingerprint")
	}
}
