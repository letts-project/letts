package main

import "letts/internal/fingerprint"

// isShellForm wraps fingerprint.IsShellForm so the CLI use site looks
// local without an extra import at every call site. Must use the shared
// helper — never reimplement, so client and server stay byte-identical.
func isShellForm(argv []string) bool {
	return fingerprint.IsShellForm(argv)
}
