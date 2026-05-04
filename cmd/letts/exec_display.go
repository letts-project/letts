package main

import (
	"fmt"
	"path/filepath"
	"strings"
)

// buildDisplayName composes a 1-line human label for the exec invocation.
// Truncates to 60 chars to keep missions list / UI columns sane.
// hostsCount=1 → no suffix; hostsCount>1 → " [+N hosts]" appended.
func buildDisplayName(argv []string, scriptPath string, hostsCount int) string {
	var base string
	switch {
	case len(argv) > 0 && scriptPath != "":
		base = shortArgv(argv) + " (script=" + filepath.Base(scriptPath) + ")"
	case len(argv) > 0:
		base = shortArgv(argv)
	case scriptPath != "":
		base = filepath.Base(scriptPath)
	default:
		base = "exec"
	}
	if hostsCount > 1 {
		suffix := fmt.Sprintf(" [+%d hosts]", hostsCount-1)
		return truncateEllipsis(base, 60-len(suffix)) + suffix
	}
	return truncateEllipsis(base, 60)
}

// shortArgv joins argv with spaces, shell-quoting tokens that contain
// whitespace or shell metachars. Display-only — never re-parsed.
func shortArgv(argv []string) string {
	parts := make([]string, len(argv))
	for i, a := range argv {
		parts[i] = shellQuote(a)
	}
	return strings.Join(parts, " ")
}

func shellQuote(s string) string {
	if s == "" {
		return "''"
	}
	if !strings.ContainsAny(s, " \t\n\"'$`\\|&;<>(){}[]*?#~") {
		return s
	}
	// Single-quote, escaping internal single quotes via the close-quote
	// /backslash-quote/open-quote idiom.
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// truncateEllipsis trims `s` to at most n chars (counting rune length not
// byte length), replacing the trailing 3 chars with U+2026 if truncation
// occurred. If n <= 3, returns up to n leading chars without ellipsis.
func truncateEllipsis(s string, n int) string {
	if n <= 0 {
		return ""
	}
	runes := []rune(s)
	if len(runes) <= n {
		return s
	}
	if n <= 3 {
		return string(runes[:n])
	}
	return string(runes[:n-1]) + "…"
}
