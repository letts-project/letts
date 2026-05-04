package jcs

import (
	"math"
	"strings"
	"testing"
)

// TestCanonicalizeSortsKeysByUTF16: RFC 8785 sorts object
// keys by UTF-16 code units, NOT UTF-8 bytes / code points. U+10000
// (supplementary plane) has first UTF-16 code unit 0xD800, which sorts BEFORE
// the BMP char U+FF61 — but its UTF-8 bytes (f0 90 80 80) sort AFTER U+FF61's
// (ef bd a1). sort.Strings (byte order) gets this wrong.
func TestCanonicalizeSortsKeysByUTF16(t *testing.T) {
	supp := "\U00010000"
	bmp := "｡"
	got, err := Canonicalize(map[string]any{supp: 1, bmp: 2})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"` + supp + `":1,"` + bmp + `":2}`
	if string(got) != want {
		t.Errorf("UTF-16 key sort:\n got %q\nwant %q", string(got), want)
	}
}

func TestCanonicalizeObjects(t *testing.T) {
	in := map[string]any{
		"b": 1,
		"a": map[string]any{"y": 2, "x": 1},
	}
	out, err := Canonicalize(in)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"a":{"x":1,"y":2},"b":1}`
	if string(out) != want {
		t.Errorf("got %s want %s", out, want)
	}
}

func TestCanonicalizeArrayPreservesOrder(t *testing.T) {
	in := []any{3, 1, 2}
	out, _ := Canonicalize(in)
	if string(out) != "[3,1,2]" {
		t.Errorf("got %s", out)
	}
}

func TestCanonicalizeNumbers(t *testing.T) {
	cases := []struct {
		in   any
		want string
	}{
		{0, "0"},
		{math.Copysign(0, -1), "0"},
		{1.0, "1"},
		{1.5, "1.5"},
		{1e21, "1e+21"},
		{1e-7, "1e-7"},
		{0.0001, "0.0001"},
		{0.00001, "0.00001"},
		{0.000001, "0.000001"},
		{0.0000001, "1e-7"},
	}
	for _, c := range cases {
		got, err := Canonicalize(c.in)
		if err != nil {
			t.Fatalf("Canonicalize(%v): %v", c.in, err)
		}
		if string(got) != c.want {
			t.Errorf("Canonicalize(%v) = %s, want %s", c.in, got, c.want)
		}
	}
}

// int64 values with magnitude > 2^53 lose precision when round-
// tripped through float64. Canonicalize is used to build fingerprints, so
// a precision loss means two semantically-distinct inputs could collide.
func TestCanonicalizeIntegersBeyond2Pow53(t *testing.T) {
	cases := []struct {
		in   any
		want string
	}{
		// At the safe-integer boundary: 2^53 = 9007199254740992. JS Number can
		// represent it exactly; 2^53+1 cannot. We MUST emit the exact integer.
		{int64(1<<53 - 1), "9007199254740991"},
		{int64(1 << 53), "9007199254740992"},
		{int64(1<<53 + 1), "9007199254740993"},
		{int64(1<<53 + 2), "9007199254740994"},
		{int64(math.MaxInt64), "9223372036854775807"},
		{int64(-(1<<53 + 1)), "-9007199254740993"},
		{int64(math.MinInt64), "-9223372036854775808"},
		{uint64(1<<53 + 1), "9007199254740993"},
		{uint64(math.MaxUint64), "18446744073709551615"},
		// Confirm small ints still emit integer form (no decimal point).
		{int(1), "1"},
		{int8(-7), "-7"},
		{uint16(42), "42"},
	}
	for _, c := range cases {
		got, err := Canonicalize(c.in)
		if err != nil {
			t.Fatalf("Canonicalize(%v): %v", c.in, err)
		}
		if string(got) != c.want {
			t.Errorf("Canonicalize(%T %v) = %s, want %s", c.in, c.in, got, c.want)
		}
	}
}

// Investigated the encodeNumber exponent-strip logic for RFC 8785
// corner cases. The current `len(expPart) > 1 && expPart[0] == '0'` loop
// is correct for every value Go's strconv.FormatFloat 'g'
// can emit: 'g' always produces a two-digit exponent like "1e+09" (gets
// stripped to "1e+9"), never a single-zero exponent like "1e+0" (which
// would need a different branch, but doesn't arise because 'g' with
// precision -1 picks the shorter form — "1" beats "1e+0" — for values
// at the exponent boundary). Locking in the boundary and RFC vectors as
// regression guard: a future strconv behavior change would surface here.
func TestCanonicalizeNumbersRFC8785Vectors(t *testing.T) {
	cases := []struct {
		in   float64
		want string
		note string
	}{
		{1e10, "10000000000", "1e10 in-range fixed (no exponent)"},
		{1e15, "1000000000000000", "1e15 in-range fixed"},
		{1e20, "100000000000000000000", "1e20 in-range fixed (max in-range power)"},
		{1e21, "1e+21", "exact 1e21 boundary: exponent path"},
		{9.999999999999997e22, "9.999999999999997e+22", "RFC vector"},
		{5e-7, "5e-7", "below 1e-6 boundary: exponent path"},
		{1.7976931348623157e308, "1.7976931348623157e+308", "MaxFloat64"},
		{5e-324, "5e-324", "smallest denormalized"},
		{math.Copysign(0, -1), "0", "negative zero collapses"},
		{1.000000000000007e15, "1000000000000007", "RFC lossy mantissa stays fixed"},
		{1e-9, "1e-9", "two-digit exponent must strip leading zero"},
		{5e-100, "5e-100", "three-digit exponent untouched"},
		{-1e-9, "-1e-9", "negative value, leading-zero strip"},
	}
	for _, c := range cases {
		got, err := Canonicalize(c.in)
		if err != nil {
			t.Fatalf("Canonicalize(%g): %v", c.in, err)
		}
		if string(got) != c.want {
			t.Errorf("[%s] Canonicalize(%g) = %s, want %s", c.note, c.in, got, c.want)
		}
	}
}

func TestCanonicalizeRejectsNaNInf(t *testing.T) {
	for _, v := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		if _, err := Canonicalize(v); err == nil {
			t.Errorf("Canonicalize(%v) expected error", v)
		}
	}
}

func TestCanonicalizeUnicodeStrings(t *testing.T) {
	// JCS uses ECMA-262 string serialization: escapes only the required
	// set: ", \, and U+0000-U+001F. Other code points pass through as UTF-8.
	in := "héllo\nworld"
	out, _ := Canonicalize(in)
	if !strings.Contains(string(out), "héllo") {
		t.Errorf("expected héllo passthrough; got %s", out)
	}
	if !strings.Contains(string(out), `\n`) {
		t.Errorf("expected \\n escape; got %s", out)
	}
}

func TestCanonicalizeNestedFromJSON(t *testing.T) {
	// Validate stable output across two semantically-equal inputs.
	a := map[string]any{"k": []any{map[string]any{"b": 2, "a": 1}}}
	b := map[string]any{"k": []any{map[string]any{"a": 1, "b": 2}}}
	outA, _ := Canonicalize(a)
	outB, _ := Canonicalize(b)
	if string(outA) != string(outB) {
		t.Errorf("non-deterministic: %s vs %s", outA, outB)
	}
}
