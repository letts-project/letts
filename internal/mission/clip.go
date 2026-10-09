package mission

import "unicode/utf8"

// clipUTF8 returns the longest prefix of s that is at most n bytes and does
// not end inside a UTF-8 sequence. Input that is not valid UTF-8 near the cut
// is cut at exactly n bytes.
func clipUTF8[T string | []byte](s T, n int) string {
	if n <= 0 {
		return ""
	}
	if len(s) <= n {
		return string(s)
	}
	for i := n; i >= 0 && i > n-utf8.UTFMax; i-- {
		if utf8.RuneStart(s[i]) {
			return string(s[:i])
		}
	}
	return string(s[:n])
}
