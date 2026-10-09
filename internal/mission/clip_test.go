package mission

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestClipUTF8(t *testing.T) {
	cases := []struct {
		s    string
		n    int
		want string
	}{
		{"abc", 5, "abc"},
		{"abc", 3, "abc"},
		{"abc", 2, "ab"},
		{"abc", 0, ""},
		{"жжж", 3, "ж"},
		{"жжж", 4, "жж"},
		{"жжж", 1, ""},
		{"a😀b", 4, "a"},
		{"a😀b", 5, "a😀"},
		{"\x80\x80\x80\x80\x80\x80", 4, "\x80\x80\x80\x80"},
	}
	for _, tc := range cases {
		if got := clipUTF8(tc.s, tc.n); got != tc.want {
			t.Errorf("clipUTF8(%q, %d)=%q, want %q", tc.s, tc.n, got, tc.want)
		}
		if got := clipUTF8([]byte(tc.s), tc.n); got != tc.want {
			t.Errorf("clipUTF8([]byte(%q), %d)=%q, want %q", tc.s, tc.n, got, tc.want)
		}
	}

	s := strings.Repeat("я😀z", 50)
	for n := 0; n <= len(s); n++ {
		got := clipUTF8(s, n)
		if len(got) > n || !utf8.ValidString(got) || !strings.HasPrefix(s, got) {
			t.Fatalf("clipUTF8(_, %d)=%q: not a valid prefix within the limit", n, got)
		}
		if n-len(got) >= utf8.UTFMax {
			t.Fatalf("clipUTF8(_, %d) dropped %d bytes", n, n-len(got))
		}
	}
}
