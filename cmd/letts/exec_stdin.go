package main

import (
	"io"
	"os"

	"golang.org/x/term"

	"letts/pkg/lettsclient"
)

// stdinReader is the io.Reader from which readStdinAll consumes bytes.
// Defaults to os.Stdin in production; tests override the package var to
// inject deterministic input without touching the real fd0.
var stdinReader io.Reader = os.Stdin

// isTerminalFD wraps term.IsTerminal so tests can stub the auto-detect
// branch via package var. Production passes os.Stdin.Fd(); tests replace
// the function with a constant true/false to drive resolveStdinMode.
var isTerminalFD = func(fd uintptr) bool { return term.IsTerminal(int(fd)) }

// resolveStdinMode applies the auto-detect rules for --stdin:
//
//	no --stdin + TTY stdin            → none
//	no --stdin + non-TTY + 1 host     → single
//	no --stdin + non-TTY + N>1 hosts  → BadUsage (require explicit --stdin=broadcast)
//	--stdin=single with N>1 hosts     → BadUsage
//	--stdin=single + TTY stdin        → BadUsage
//	--stdin=broadcast + TTY stdin     → BadUsage
//
// The empty-flag auto path biases toward least surprise: a piped invocation
// against a single host implicitly enables single (matches the natural
// `cat foo | letts exec --host s1 -- bash` UX); piping to multiple hosts
// without --stdin=broadcast is rejected so the operator opts into the
// per-host fan-out cost explicitly.
func resolveStdinMode(flag string, hostsCount int, stdinIsTerminal bool) (string, error) {
	if flag == "" {
		if stdinIsTerminal {
			return "none", nil
		}
		if hostsCount == 1 {
			return "single", nil
		}
		return "", NewBadUsageError("multi-host with non-TTY stdin requires --stdin=broadcast")
	}
	switch flag {
	case "none":
		return "none", nil
	case "single":
		if hostsCount != 1 {
			return "", NewBadUsageError("--stdin=single requires exactly one --host")
		}
		if stdinIsTerminal {
			return "", NewBadUsageError("--stdin=single requires piped stdin (TTY detected)")
		}
		return "single", nil
	case "broadcast":
		if stdinIsTerminal {
			return "", NewBadUsageError("--stdin=broadcast requires piped stdin (TTY detected)")
		}
		return "broadcast", nil
	}
	return "", NewBadUsageError("--stdin: must be none|single|broadcast")
}

// readStdinAll reads stdinReader to EOF. For multi-host broadcast we pay
// the buffering cost once and reuse the bytes across per-dugdale uploads
// (content-addressed dedupe still wins when sibling hosts already hold
// the same bytes from a prior run).
func readStdinAll() ([]byte, error) {
	return io.ReadAll(stdinReader)
}

// uploadStdinToHost is a thin wrapper over uploadOrReuseBytes so callers
// that already have stdin bytes in memory don't need to know about the
// staging-side dedupe path. Returns the staging_id to populate in
// ExecRequest.StdinStagingID for this host.
func uploadStdinToHost(c *lettsclient.Client, data []byte) (string, error) {
	return uploadOrReuseBytes(c, data)
}
