package fingerprint

import (
	"path/filepath"
	"strings"
)

// shellBasenames is the canonical shell binary set checked by IsShellForm.
var shellBasenames = map[string]bool{
	"sh": true, "bash": true, "zsh": true,
	"dash": true, "ksh": true, "ash": true,
}

// ShellFormCase is one row of the canonical shell-form detection table.
type ShellFormCase struct {
	Name  string
	Argv  []string
	Shell bool
}

// ShellFormCases is the canonical shell-form detection table, exported so
// CLI and server tests both run identical cases.
var ShellFormCases = []ShellFormCase{
	{"plain argv", []string{"uptime"}, false},
	{"bash -c", []string{"bash", "-c", "uptime"}, true},
	{"bash -lc", []string{"bash", "-lc", "echo hi"}, true},
	{"bash --command", []string{"bash", "--command", "echo hi"}, true},
	{"bash script (no -c)", []string{"bash", "script.sh"}, false},
	{"env bash -c", []string{"env", "bash", "-c", "x"}, false},
	{"abs bash -c", []string{"/usr/bin/bash", "-c", "x"}, true},
	{"zsh -ec", []string{"zsh", "-ec", "x"}, true},
	{"sh -c", []string{"sh", "-c", "x"}, true},
	{"dash -c", []string{"dash", "-c", "x"}, true},
	{"ksh -c", []string{"ksh", "-c", "x"}, true},
	{"ash -c", []string{"ash", "-c", "x"}, true},
	{"bash -i (no c)", []string{"bash", "-i"}, false},
	{"empty argv", []string{}, false},
}

// IsShellForm reports whether argv invokes a shell with an inline command
// (the case blocked when allow_shell=false). Exact rule:
//
//	basename(argv[0]) ∈ {sh,bash,zsh,dash,ksh,ash}
//	AND (
//	    any arg equals "--command"
//	  OR any arg is a short-option (starts with "-" but not "--") AND contains 'c'
//	)
//
// Hygiene only — not a security boundary (script and RCE-from-token still
// possible).
func IsShellForm(argv []string) bool {
	if len(argv) == 0 {
		return false
	}
	if !shellBasenames[filepath.Base(argv[0])] {
		return false
	}
	for _, a := range argv[1:] {
		if a == "--command" {
			return true
		}
		if strings.HasPrefix(a, "-") && !strings.HasPrefix(a, "--") && strings.ContainsRune(a, 'c') {
			return true
		}
	}
	return false
}
