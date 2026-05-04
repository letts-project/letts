package main

import "github.com/spf13/cobra"

func newCtlCmd() *cobra.Command {
	c := &cobra.Command{Use: "ctl", Short: "Control / inspection subcommands"}
	c.AddCommand(newCtlDugdalesCmd())
	c.AddCommand(newCtlLanesCmd())
	c.AddCommand(newCtlMissionsCmd())
	c.AddCommand(newCtlStagingCmd())
	c.AddCommand(newCtlExecCmd())
	return c
}
