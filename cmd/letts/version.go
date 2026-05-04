package main

import (
	"github.com/spf13/cobra"

	"letts/internal/version"
)

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print letts version",
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.Printf("letts %s\n", version.Version)
			return nil
		},
	}
}
