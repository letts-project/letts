package fingerprint

import (
	"encoding/hex"
	"encoding/json"
	"testing"
)

// TestCanonicalizeInputRejectsDuplicateKeys: RFC 8785 section 3.1
// (I-JSON): duplicate object keys are forbidden. Go's decoder silently keeps
// the last value, so {"a":1,"a":2} and {"a":2} would otherwise hash to the same
// idempotency fingerprint.
func TestCanonicalizeInputRejectsDuplicateKeys(t *testing.T) {
	bad := []string{`{"a":1,"a":2}`, `{"x":{"b":1,"b":2}}`, `[{"k":1,"k":2}]`}
	for _, s := range bad {
		if _, err := CanonicalizeInput(json.RawMessage(s)); err == nil {
			t.Errorf("expected error for duplicate key in %s", s)
		}
	}
	good := []string{`{"a":1,"b":2}`, `{"x":{"a":1},"y":{"a":2}}`, `[{"k":1},{"k":2}]`, `42`, `null`}
	for _, s := range good {
		if _, err := CanonicalizeInput(json.RawMessage(s)); err != nil {
			t.Errorf("valid input %s rejected: %v", s, err)
		}
	}
}

func TestMissionFingerprintStableAcrossKeyOrder(t *testing.T) {
	a := MissionInput{
		Lane:           "normal",
		Mission:        "BookCalc",
		InputCanonical: []byte(`{"a":1,"b":2}`),
		TimeoutMs:      ptrInt64(300000),
		Files: []FileRef{
			{Role: "cover", StagingID: "0192a8b3-d2c1-7abc-bad0-1234567890ab", Sha256: "abc", Size: 100},
			{Role: "extra", StagingID: "0192a8b3-d2c1-7abc-bad0-1234567890ac", Sha256: "def", Size: 200},
		},
	}
	b := a
	// Reverse files; canonicalization must sort by role.
	b.Files = []FileRef{a.Files[1], a.Files[0]}

	hashA, err := Mission(a)
	if err != nil {
		t.Fatal(err)
	}
	hashB, err := Mission(b)
	if err != nil {
		t.Fatal(err)
	}
	if hashA != hashB {
		t.Errorf("file order changed fingerprint: %s vs %s", hashA, hashB)
	}
	if _, err := hex.DecodeString(hashA); err != nil {
		t.Errorf("fingerprint not hex: %s", hashA)
	}
	if len(hashA) != 64 {
		t.Errorf("fingerprint len = %d, want 64", len(hashA))
	}
}

func TestMissionFingerprintChangesWithTimeout(t *testing.T) {
	a := MissionInput{Lane: "normal", Mission: "X", InputCanonical: []byte(`{}`)}
	b := a
	b.TimeoutMs = ptrInt64(100)
	ha, _ := Mission(a)
	hb, _ := Mission(b)
	if ha == hb {
		t.Errorf("timeout change should alter fingerprint")
	}
}

func ptrInt64(v int64) *int64 { return &v }

func TestCanonicalizeInputPreservesInt64BeyondFloat64Precision(t *testing.T) {
	cases := []struct {
		name string
		a, b string
	}{
		{
			name: "size differs by 2 near 2^53",
			a:    `{"size":9007199254740993}`,
			b:    `{"size":9007199254740995}`,
		},
		{
			name: "MaxInt64 vs MaxInt64-1",
			a:    `{"v":9223372036854775807}`,
			b:    `{"v":9223372036854775806}`,
		},
		{
			name: "negative beyond -2^53",
			a:    `{"v":-9007199254740993}`,
			b:    `{"v":-9007199254740995}`,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ca, err := CanonicalizeInput([]byte(c.a))
			if err != nil {
				t.Fatal(err)
			}
			cb, err := CanonicalizeInput([]byte(c.b))
			if err != nil {
				t.Fatal(err)
			}
			if string(ca) == string(cb) {
				t.Fatalf("distinct inputs produce identical canonical bytes:\n  a=%s\n  b=%s\n  both → %s",
					c.a, c.b, ca)
			}
		})
	}
}

func TestCanonicalizeInputRoundTripsLargeInteger(t *testing.T) {
	src := `{"size":9223372036854775807}`
	got, err := CanonicalizeInput([]byte(src))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"size":9223372036854775807}`
	if string(got) != want {
		t.Errorf("canonical = %q, want %q", got, want)
	}
}

func TestMissionFingerprintDistinctForInt64BeyondFloat64Precision(t *testing.T) {
	ca, _ := CanonicalizeInput([]byte(`{"size":9007199254740993}`))
	cb, _ := CanonicalizeInput([]byte(`{"size":9007199254740995}`))
	a := MissionInput{Lane: "normal", Mission: "X", InputCanonical: ca}
	b := MissionInput{Lane: "normal", Mission: "X", InputCanonical: cb}
	ha, _ := Mission(a)
	hb, _ := Mission(b)
	if ha == hb {
		t.Fatalf("distinct large integers collapsed to same fingerprint: %s", ha)
	}
}
