package main

import (
	"fmt"

	"github.com/spf13/cobra"

	"letts/internal/version"
)

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print letts version",
		RunE: func(cmd *cobra.Command, args []string) error {
			formatStr, _ := cmd.Root().PersistentFlags().GetString("output")
			f, err := ParseFormat(formatStr)
			if err != nil {
				return NewBadUsageError(err.Error())
			}
			if ok, err := printRecord(cmd.OutOrStdout(), f, map[string]string{"version": version.Version}); ok {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "letts %s\n", version.Version)
			return nil
		},
	}
}
