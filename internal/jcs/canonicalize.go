// Package jcs implements RFC 8785 (JSON Canonicalization Scheme) for the
// subset of JSON that letts uses in idempotency fingerprints.
package jcs

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

// lessUTF16 reports whether a sorts before b by UTF-16 code-unit sequence, as
// RFC 8785 mandates for object property names. This differs
// from byte/code-point order only for supplementary-plane characters
// (U+10000..U+10FFFF), whose lead surrogate (0xD800..0xDBFF) sorts below BMP
// chars in U+E000..U+FFFF.
func lessUTF16(a, b string) bool {
	ua := utf16.Encode([]rune(a))
	ub := utf16.Encode([]rune(b))
	for k := 0; k < len(ua) && k < len(ub); k++ {
		if ua[k] != ub[k] {
			return ua[k] < ub[k]
		}
	}
	return len(ua) < len(ub)
}

// ErrUnsupportedNumber is returned for NaN/+Inf/-Inf which are not valid JSON.
var ErrUnsupportedNumber = errors.New("jcs: NaN/Inf is not representable as JSON")

// Canonicalize returns the RFC 8785 canonical JSON serialization of v.
// Supported types: nil, bool, string, all int/uint widths, float32/float64,
// []any, map[string]any. JSON-decoded objects (json.Unmarshal into any)
// produce these types directly.
func Canonicalize(v any) ([]byte, error) {
	var buf bytes.Buffer
	if err := encode(&buf, v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func encode(buf *bytes.Buffer, v any) error {
	switch x := v.(type) {
	case nil:
		buf.WriteString("null")
	case bool:
		if x {
			buf.WriteString("true")
		} else {
			buf.WriteString("false")
		}
	case string:
		return encodeString(buf, x)
	case json.Number:
		return encodeJSONNumber(buf, x)
	case float64:
		return encodeNumber(buf, x)
	case float32:
		return encodeNumber(buf, float64(x))
	// Integer types format as decimal directly. Routing int64
	// through float64 loses precision for |x| > 2^53 (so e.g. MaxInt64
	// becomes 9223372036854776000). Fingerprints over staging file sizes
	// or other int64 fields must preserve exact bytes — collide-safe.
	// ECMA-262 Number.prototype.toString agrees with FormatInt for the
	// safe-integer range; outside it, JCS still mandates a single
	// canonical form and the precise integer is the only sensible
	// choice when the input language (Go) carries the precision.
	case int:
		buf.WriteString(strconv.FormatInt(int64(x), 10))
	case int8:
		buf.WriteString(strconv.FormatInt(int64(x), 10))
	case int16:
		buf.WriteString(strconv.FormatInt(int64(x), 10))
	case int32:
		buf.WriteString(strconv.FormatInt(int64(x), 10))
	case int64:
		buf.WriteString(strconv.FormatInt(x, 10))
	case uint:
		buf.WriteString(strconv.FormatUint(uint64(x), 10))
	case uint8:
		buf.WriteString(strconv.FormatUint(uint64(x), 10))
	case uint16:
		buf.WriteString(strconv.FormatUint(uint64(x), 10))
	case uint32:
		buf.WriteString(strconv.FormatUint(uint64(x), 10))
	case uint64:
		buf.WriteString(strconv.FormatUint(x, 10))
	case []any:
		buf.WriteByte('[')
		for i, item := range x {
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := encode(buf, item); err != nil {
				return err
			}
		}
		buf.WriteByte(']')
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		// RFC 8785: sort object property names by UTF-16 code units.
		// Equals byte order for the BMP, but differs for
		// supplementary-plane keys, so we can't use sort.Strings.
		sort.Slice(keys, func(i, j int) bool { return lessUTF16(keys[i], keys[j]) })
		buf.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := encodeString(buf, k); err != nil {
				return err
			}
			buf.WriteByte(':')
			if err := encode(buf, x[k]); err != nil {
				return err
			}
		}
		buf.WriteByte('}')
	default:
		return fmt.Errorf("jcs: unsupported type %T", v)
	}
	return nil
}

// encodeNumber formats f according to RFC 8785, which mandates
// ECMAScript Number.prototype.toString output.
//
// ECMA-262 uses fixed-point notation when |x| >= 1e-6 and |x| < 1e21.
// Go's strconv.FormatFloat with 'g' switches to exponential earlier (at 1e-5
// for some values like 1e-6), so we use 'f' for the [1e-6, 1e21) range and
// strip trailing zeros. For values outside that range we use 'g' with
// exponent normalization (strip leading zeros, ensure explicit sign).
func encodeNumber(buf *bytes.Buffer, f float64) error {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return ErrUnsupportedNumber
	}
	if f == 0 {
		buf.WriteByte('0')
		return nil
	}

	absF := math.Abs(f)
	var s string
	if absF >= 1e-6 && absF < 1e21 {
		// ECMA-262: fixed-point notation in [1e-6, 1e21).
		// 'f' with -1 precision produces the shortest exact decimal.
		s = strconv.FormatFloat(f, 'f', -1, 64)
	} else {
		// Exponential notation; normalize the exponent to match ECMA-262.
		s = strconv.FormatFloat(f, 'g', -1, 64)
		// Ensure the exponent has an explicit sign (Go sometimes omits '+').
		for i := 0; i < len(s); i++ {
			if s[i] == 'e' && i+1 < len(s) && s[i+1] != '+' && s[i+1] != '-' {
				s = s[:i+1] + "+" + s[i+1:]
				break
			}
		}
		// Strip leading zeros from the exponent (e.g., "1e+07" → "1e+7").
		if idx := indexExp(s); idx >= 0 {
			mantissa := s[:idx]
			expPart := s[idx+1:]
			sign := byte('+')
			if expPart[0] == '+' || expPart[0] == '-' {
				sign = expPart[0]
				expPart = expPart[1:]
			}
			for len(expPart) > 1 && expPart[0] == '0' {
				expPart = expPart[1:]
			}
			s = mantissa + "e" + string(sign) + expPart
		}
	}
	buf.WriteString(s)
	return nil
}

// encodeJSONNumber emits a json.Number losslessly when it parses as an
// integer (preserving precision beyond float64's 2^53 mantissa), and falls
// back to encodeNumber's ECMA-262 float formatting otherwise.
func encodeJSONNumber(buf *bytes.Buffer, n json.Number) error {
	s := string(n)
	if !strings.ContainsAny(s, ".eE") {
		if i, err := n.Int64(); err == nil {
			buf.WriteString(strconv.FormatInt(i, 10))
			return nil
		}
		if !strings.HasPrefix(s, "-") {
			if u, err := strconv.ParseUint(s, 10, 64); err == nil {
				buf.WriteString(strconv.FormatUint(u, 10))
				return nil
			}
		}
	}
	f, err := n.Float64()
	if err != nil {
		return fmt.Errorf("jcs: %w", err)
	}
	return encodeNumber(buf, f)
}

func indexExp(s string) int {
	for i := 0; i < len(s); i++ {
		if s[i] == 'e' || s[i] == 'E' {
			return i
		}
	}
	return -1
}

func encodeString(buf *bytes.Buffer, s string) error {
	if !utf8.ValidString(s) {
		return errors.New("jcs: string is not valid UTF-8")
	}
	buf.WriteByte('"')
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		switch r {
		case '\\':
			buf.WriteString(`\\`)
		case '"':
			buf.WriteString(`\"`)
		case '\b':
			buf.WriteString(`\b`)
		case '\f':
			buf.WriteString(`\f`)
		case '\n':
			buf.WriteString(`\n`)
		case '\r':
			buf.WriteString(`\r`)
		case '\t':
			buf.WriteString(`\t`)
		default:
			if r < 0x20 {
				fmt.Fprintf(buf, `\u%04x`, r)
			} else {
				buf.WriteRune(r)
			}
		}
		i += size
	}
	buf.WriteByte('"')
	return nil
}
