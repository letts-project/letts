package main

import (
	"errors"
	"fmt"
	"io"
	"net/url"
	"testing"

	"letts/pkg/lettsclient"
)

func TestMapErrorToExitClassification(t *testing.T) {
	cases := []struct {
		err  error
		want int
	}{
		{nil, exitOK},
		{NewBadUsageError("missing --mission"), exitBadUsage},
		{NewConfigError("token unresolved"), exitConfigError},
		{NewNetworkError(errors.New("connection refused")), exitNetworkError},
		{NewAuthError(401, "bad token"), exitAuthError},
		{NewWaitTimeoutError(), exitWaitTimeout},
		{NewMissionAbnormalError("killed"), exitMissionAbnormal},
		{fmt.Errorf("anything else"), exitFailure},
		// Raw client errors must classify too (production never
		// wrapped them into the typed errors above).
		{&lettsclient.HTTPError{Status: 401, Code: "unauthorized"}, exitAuthError},
		{&lettsclient.HTTPError{Status: 403, Code: "forbidden"}, exitAuthError},
		{&url.Error{Op: "Post", URL: "http://h/v1/dispatch", Err: errors.New("connection refused")}, exitNetworkError},
		// Plain cobra/pflag usage errors are bad usage, not generic failure.
		// "required flag(s)" has no typed interception point at all (cobra
		// emits it inside Execute before RunE), so the prefix matcher is the
		// only mechanism that can classify it.
		{errors.New(`unknown command "bogus" for "letts"`), exitBadUsage},
		{errors.New(`required flag(s) "host" not set`), exitBadUsage},
		// Prefix anchoring: usage-looking text in the middle of a message
		// must NOT classify as usage (daemon errors embed arbitrary text).
		{errors.New(`daemon said: unknown flag: --weird`), exitFailure},
	}
	for _, tc := range cases {
		got := mapErrorToExit(tc.err)
		if got != tc.want {
			t.Errorf("mapErrorToExit(%v) = %d, want %d", tc.err, got, tc.want)
		}
	}
}

// TestRootCommandUsageErrorsExitBadUsage drives the real assembled root
// command with malformed invocations and asserts each maps to exit 2. All
// cases fail during cobra's parse/validate phase, before any RunE runs, so
// no config file or network stub is needed.
func TestRootCommandUsageErrorsExitBadUsage(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{"unknown flag", []string{"version", "--no-such-flag"}},
		{"unknown shorthand flag", []string{"version", "-Z"}},
		{"bad value for typed flag", []string{"ctl", "missions", "list", "--limit=NaN"}},
		{"missing positional arg", []string{"ctl", "missions", "show"}},
		{"extra positional arg", []string{"ctl", "missions", "show", "id-1", "id-2"}},
		{"extra positional arg (max-1 command)", []string{"ctl", "missions", "restart", "id-1", "id-2"}},
		{"unknown subcommand", []string{"no-such-command"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := newRootCmd()
			root.SetOut(io.Discard)
			root.SetErr(io.Discard)
			root.SetArgs(tc.args)
			err := root.Execute()
			if err == nil {
				t.Fatalf("letts %v: expected a usage error, got nil", tc.args)
			}
			if got := mapErrorToExit(err); got != exitBadUsage {
				t.Errorf("letts %v: exit=%d want %d (err=%v)", tc.args, got, exitBadUsage, err)
			}
		})
	}
}
