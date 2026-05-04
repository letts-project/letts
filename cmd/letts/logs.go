package main

import (
	"github.com/spf13/cobra"
)

// newLogsCmd builds `letts logs <mission_id>` — a top-level alias for
// `letts ctl missions output`. Same /output endpoint, same flags. Mirrors
// the convenience of top-level `letts events <id>`.
//
// Defaults: --stream=combined, --follow=false. Logic delegates entirely to
// runCtlMissionsOutput so there's zero duplication with the ctl path.
func newLogsCmd() *cobra.Command {
	var host string
	var match []string
	var stream string
	var follow bool
	c := &cobra.Command{
		Use:   "logs <mission_id>",
		Short: "Stream mission stdout/stderr/combined (alias for `ctl missions output`)",
		Args:  exactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ac, _, err := setupAppCtx(cmd)
			if err != nil {
				return err
			}
			defer ac.Close()
			return runCtlMissionsOutput(cmd, ac, args[0], host, stream, follow, match)
		},
	}
	c.Flags().StringVar(&host, "host", "", "dugdale id (else by-id fan-out)")
	c.Flags().StringSliceVar(&match, "match", nil, "label filter for fan-out")
	c.Flags().StringVar(&stream, "stream", "combined", "stdout|stderr|combined")
	c.Flags().BoolVar(&follow, "follow", false, "follow live output")
	return c
}
