package main

import (
	"errors"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/spf13/cobra"

	"letts/pkg/lettsclient"
)

// Typed errors so RunE handlers can return them and main() maps to exit codes.

type BadUsageError struct{ Msg string }

func (e *BadUsageError) Error() string  { return e.Msg }
func NewBadUsageError(msg string) error { return &BadUsageError{Msg: msg} }

// badUsageArgs wraps a cobra positional-args validator so its failure maps
// to exit 2. Cobra returns plain errors from Args validators ("accepts 1
// arg(s), received 0"), which would otherwise fall through mapErrorToExit's
// default and exit 1 — violating the documented bad-usage contract.
func badUsageArgs(v cobra.PositionalArgs) cobra.PositionalArgs {
	return func(cmd *cobra.Command, args []string) error {
		if err := v(cmd, args); err != nil {
			// Usage output is silenced, so the one-line error must say
			// which command was misused. main() already prefixes "letts: ",
			// so strip the root name from the command path to avoid
			// "letts: letts ctl ..." doubling.
			path := strings.TrimPrefix(cmd.CommandPath(), cmd.Root().Name()+" ")
			return NewBadUsageError(path + ": " + err.Error())
		}
		return nil
	}
}

// exactArgs / maximumNArgs are the BadUsage-mapping replacements for the
// bare cobra validators. Every command binding must use these (not
// cobra.ExactArgs / cobra.MaximumNArgs directly) so positional-arity
// mistakes exit 2.
func exactArgs(n int) cobra.PositionalArgs    { return badUsageArgs(cobra.ExactArgs(n)) }
func maximumNArgs(n int) cobra.PositionalArgs { return badUsageArgs(cobra.MaximumNArgs(n)) }

type ConfigError struct{ Msg string }

func (e *ConfigError) Error() string  { return "config: " + e.Msg }
func NewConfigError(msg string) error { return &ConfigError{Msg: msg} }

type NetworkError struct{ Inner error }

func (e *NetworkError) Error() string { return "network: " + e.Inner.Error() }
func (e *NetworkError) Unwrap() error { return e.Inner }
func NewNetworkError(err error) error { return &NetworkError{Inner: err} }

type AuthError struct {
	Status int
	Msg    string
}

func (e *AuthError) Error() string { return e.Msg }
func NewAuthError(status int, msg string) error {
	return &AuthError{Status: status, Msg: msg}
}

type WaitTimeoutError struct{}

func (e *WaitTimeoutError) Error() string { return "client wait timeout" }
func NewWaitTimeoutError() error          { return &WaitTimeoutError{} }

type MissionAbnormalError struct{ Outcome string }

func (e *MissionAbnormalError) Error() string {
	return "mission did not exit normally: " + e.Outcome
}
func NewMissionAbnormalError(outcome string) error {
	return &MissionAbnormalError{Outcome: outcome}
}

func mapErrorToExit(err error) int {
	if err == nil {
		return exitOK
	}
	// Exec markers come FIRST so that wrapping (e.g. *ExecTransportError
	// containing a *ConfigError) overrides the inner-error classification.
	// All pre-terminal errors in the exec path must map to CLI 255 —
	// without this ordering errors.As walks the Unwrap chain and matches
	// the inner *ConfigError → exit 3, the wrong class.
	var ete *ExecTransportError
	if errors.As(err, &ete) {
		return 255
	}
	var eoe *ExecOutcomeError
	if errors.As(err, &eoe) {
		return mapExecOutcomeExitCode(eoe.Outcome, eoe.ExitCode)
	}
	var bu *BadUsageError
	if errors.As(err, &bu) {
		return exitBadUsage
	}
	var wt *WaitTimeoutError
	if errors.As(err, &wt) {
		return exitWaitTimeout
	}
	var ce *ConfigError
	if errors.As(err, &ce) {
		return exitConfigError
	}
	var ae *AuthError
	if errors.As(err, &ae) {
		return exitAuthError
	}
	var ne *NetworkError
	if errors.As(err, &ne) {
		return exitNetworkError
	}
	var ma *MissionAbnormalError
	if errors.As(err, &ma) {
		return exitMissionAbnormal
	}
	// Cobra/pflag usage errors that escape the typed-error mechanisms
	// (FlagErrorFunc, badUsageArgs) still belong to the bad-usage class.
	if isCobraUsageError(err) {
		return exitBadUsage
	}
	// Classify raw client errors that weren't wrapped into the typed
	// errors above. Without this, HTTP 401/403 and transport failures from
	// dispatch/run/ctl/apply all fell through to generic exit 1 instead of the
	// codes 5 (auth) / 4 (network).
	var he *lettsclient.HTTPError
	if errors.As(err, &he) && (he.Status == http.StatusUnauthorized || he.Status == http.StatusForbidden) {
		return exitAuthError
	}
	var ue *url.Error
	if errors.As(err, &ue) {
		return exitNetworkError
	}
	var nerr net.Error
	if errors.As(err, &nerr) {
		return exitNetworkError
	}
	return exitFailure
}

// cobraUsageErrorPrefixes lists the plain (untyped) error texts cobra and
// pflag emit for command-line usage mistakes. Two of those escape our typed
// mechanisms entirely and NEED this matcher: the root-level "unknown
// command" from Find/legacyArgs, and "required flag(s) ... not set" from
// ValidateRequiredFlags (cobra produces both inside Execute, before any RunE
// where we could wrap them). The rest are normally intercepted by
// SetFlagErrorFunc / badUsageArgs and are listed only as a safety net for a
// future command wired up without those hooks.
//
// Matching on error text is a textual contract with a third-party library
// and inherently brittle. To contain that: the list lives in this one
// helper, every entry is an anchored PREFIX (never a substring) copied
// verbatim from cobra/args.go, cobra/command.go and pflag/flag.go, and these
// exact strings have been stable across cobra 1.x. If a cobra upgrade
// reworks its messages the worst case is a usage error mapping back to the
// generic exit 1 — never a wrong success.
var cobraUsageErrorPrefixes = []string{
	"unknown command ",         // cobra command.go legacyArgs / args.go NoArgs
	"unknown flag: ",           // pflag flag.go (safety net; FlagErrorFunc wraps it)
	"unknown shorthand flag: ", // pflag flag.go (safety net; FlagErrorFunc wraps it)
	"required flag(s) ",        // cobra command.go ValidateRequiredFlags
	"accepts ",                 // cobra args.go Exact/Maximum/RangeArgs (safety net; badUsageArgs wraps them)
	"requires at least ",       // cobra args.go MinimumNArgs (safety net)
}

// isCobraUsageError reports whether err is one of the plain usage errors
// described on cobraUsageErrorPrefixes. Deliberately conservative: only
// top-level prefix matches count, so daemon/client errors (which carry
// method/path or typed prefixes) can't be misclassified.
func isCobraUsageError(err error) bool {
	msg := err.Error()
	for _, p := range cobraUsageErrorPrefixes {
		if strings.HasPrefix(msg, p) {
			return true
		}
	}
	return false
}
