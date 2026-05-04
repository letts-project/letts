package main

import (
	"testing"
	"unicode/utf8"
)

func TestBuildDisplayNameArgvOnly(t *testing.T) {
	got := buildDisplayName([]string{"uptime"}, "", 1)
	if got != "uptime" {
		t.Errorf("got %q, want %q", got, "uptime")
	}
}

func TestBuildDisplayNameArgvWithSpaces(t *testing.T) {
	got := buildDisplayName([]string{"df", "-h", "/var"}, "", 1)
	if got != "df -h /var" {
		t.Errorf("got %q", got)
	}
}

func TestBuildDisplayNameScriptOnly(t *testing.T) {
	got := buildDisplayName(nil, "/tmp/convert.sh", 1)
	if got != "convert.sh" {
		t.Errorf("got %q", got)
	}
}

func TestBuildDisplayNameScriptAndArgv(t *testing.T) {
	got := buildDisplayName([]string{"bash", "$LETTS_SCRIPT"}, "/tmp/convert.sh", 1)
	// $LETTS_SCRIPT is shell-quoted because '$' is a shell metachar.
	want := `bash '$LETTS_SCRIPT' (script=convert.sh)`
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestBuildDisplayNameMultiHostSuffix(t *testing.T) {
	got := buildDisplayName([]string{"uptime"}, "", 3)
	if got != "uptime [+2 hosts]" {
		t.Errorf("got %q", got)
	}
}

func TestBuildDisplayNameTruncated(t *testing.T) {
	longArg := "very-long-command-that-exceeds-the-sixty-char-display-name-limit-by-a-lot"
	got := buildDisplayName([]string{longArg}, "", 1)
	// truncateEllipsis caps at 60 RUNES, not bytes (U+2026 is 3 bytes / 1 rune).
	if n := utf8.RuneCountInString(got); n > 60 {
		t.Errorf("got rune count=%d (%q), want <=60", n, got)
	}
	// Truncation should end with U+2026 (...)
	if !endsWithEllipsis(got) {
		t.Errorf("got %q, expected ellipsis suffix", got)
	}
}

func TestBuildDisplayNameMultiHostKeepsSuffix(t *testing.T) {
	got := buildDisplayName([]string{"very-long-argv-1234567890-abcdefghij"}, "", 5)
	// Must contain the host count suffix
	if !contains(got, "[+4 hosts]") {
		t.Errorf("got %q, missing host suffix", got)
	}
	if n := utf8.RuneCountInString(got); n > 60 {
		t.Errorf("got rune count=%d, want <=60", n)
	}
}

func TestBuildDisplayNameQuotesShellMetachars(t *testing.T) {
	got := buildDisplayName([]string{"echo", "hello world"}, "", 1)
	// shellQuote on "hello world" → 'hello world'
	if got != `echo 'hello world'` {
		t.Errorf("got %q", got)
	}
}

func endsWithEllipsis(s string) bool {
	return len(s) >= 3 && s[len(s)-3:] == "…"
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
