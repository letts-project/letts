// Package main is the letts CLI entry point (admin/CLI/UI).
package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// Exit codes.
const (
	exitOK              = 0
	exitFailure         = 1
	exitBadUsage        = 2
	exitConfigError     = 3
	exitNetworkError    = 4
	exitAuthError       = 5
	exitWaitTimeout     = 124
	exitMissionAbnormal = 125
)

func newRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:           "letts",
		Short:         "Letts admin and dispatch CLI",
		SilenceUsage:  true, // bad usage exits 2 with a one-line stderr message, no usage dump
		SilenceErrors: true, // we map errors → exit codes in main()
	}
	// Flag-parse failures (unknown flag, unknown shorthand, bad value for a
	// typed flag, missing flag argument) come out of cobra as plain errors;
	// wrap them in BadUsageError here so mapErrorToExit yields the documented
	// exit 2 instead of the generic 1. Subcommands inherit the root's
	// FlagErrorFunc, so one hook covers the whole tree.
	root.SetFlagErrorFunc(func(_ *cobra.Command, err error) error {
		return NewBadUsageError(err.Error())
	})
	root.PersistentFlags().String("config", "", "path to letts.yaml (default: auto-discovery)")
	root.PersistentFlags().BoolP("verbose", "v", false, "debug logging")
	root.PersistentFlags().BoolP("quiet", "q", false, "suppress informational output")
	root.PersistentFlags().StringP("output", "o", "text", "output format: text|json|yaml|ndjson")
	root.PersistentFlags().Bool("insecure-config-permissions", false, "skip letts.yaml 0600/0400 check (DEV ONLY)")
	root.PersistentFlags().Bool("ignore-proxy", false, "ignore per-dugdale SOCKS5 proxy and dial directly")
	root.AddCommand(newVersionCmd())
	root.AddCommand(newCtlCmd())
	root.AddCommand(newApplyCmd())
	root.AddCommand(newDispatchCmd())
	root.AddCommand(newEventsCmd())
	root.AddCommand(newLogsCmd())
	root.AddCommand(newRunCmd())
	root.AddCommand(newExecCmd())
	return root
}

func main() {
	if err := newRootCmd().Execute(); err != nil {
		if shouldReportErr(err) {
			fmt.Fprintf(os.Stderr, "letts: %v\n", err)
		}
		os.Exit(mapErrorToExit(err))
	}
}

// shouldReportErr reports whether err carries a message worth printing to
// stderr beyond the exit code itself. *ExecOutcomeError is a typed carrier
// for the remote exit code, not a letts-side failure — the command's own
// stderr (already streamed through) explains any non-zero outcome.
func shouldReportErr(err error) bool {
	var eoe *ExecOutcomeError
	return !errors.As(err, &eoe)
}
