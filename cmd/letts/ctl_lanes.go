package main

import (
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"letts/pkg/lettsclient"
	"letts/pkg/lettsconfig"
)

func newCtlLanesCmd() *cobra.Command {
	c := &cobra.Command{Use: "lanes", Short: "Lane control"}
	c.AddCommand(newCtlLanesListCmd())
	c.AddCommand(newCtlLanesPauseCmd())
	c.AddCommand(newCtlLanesContinueCmd())
	return c
}

func newCtlLanesListCmd() *cobra.Command {
	var host string
	c := &cobra.Command{
		Use: "list", Short: "List lanes (GET /v1/lanes)",
		RunE: func(cmd *cobra.Command, args []string) error {
			if host == "" {
				return NewBadUsageError("--host is required")
			}
			ac, format, err := setupAppCtx(cmd)
			if err != nil {
				return err
			}
			defer ac.Close()
			return runCtlLanesList(ac, cmd.OutOrStdout(), host, format)
		},
	}
	c.Flags().StringVar(&host, "host", "", "dugdale id")
	return c
}

func runCtlLanesList(ac *appCtx, w io.Writer, host string, f Format) error {
	c, err := ac.ClientForHost(host, lettsconfig.ScopeDispatch)
	if err != nil {
		return err
	}
	lanes, err := lettsclient.ListLanes(c)
	if err != nil {
		return err
	}
	switch f {
	case FormatJSON:
		return PrintJSON(w, lanes)
	case FormatYAML:
		return PrintYAML(w, lanes)
	default:
		_, _ = fmt.Fprintf(w, "%-20s  %-12s  %-6s  %-8s  %-8s\n", "LANE", "CONCURRENCY", "PAUSED", "QUEUED", "RUNNING")
		for _, l := range lanes {
			_, _ = fmt.Fprintf(w, "%-20s  %-12d  %-6t  %-8d  %-8d\n", l.Name, l.Concurrency, l.Paused, l.Queued, l.Running)
		}
		return nil
	}
}

func newCtlLanesPauseCmd() *cobra.Command {
	var host, lane string
	c := &cobra.Command{
		Use: "pause", Short: "Pause a lane",
		RunE: func(cmd *cobra.Command, args []string) error {
			if host == "" || lane == "" {
				return NewBadUsageError("--host and --lane required")
			}
			ac, _, err := setupAppCtx(cmd)
			if err != nil {
				return err
			}
			defer ac.Close()
			return runCtlLanesPause(ac, host, lane)
		},
	}
	c.Flags().StringVar(&host, "host", "", "dugdale id")
	c.Flags().StringVar(&lane, "lane", "", "lane name")
	return c
}

func runCtlLanesPause(ac *appCtx, host, lane string) error {
	c, err := ac.ClientForHost(host, lettsconfig.ScopeAdmin)
	if err != nil {
		return err
	}
	return lettsclient.PauseLane(c, lane)
}

func newCtlLanesContinueCmd() *cobra.Command {
	var host, lane string
	c := &cobra.Command{
		Use: "continue", Short: "Resume a lane",
		RunE: func(cmd *cobra.Command, args []string) error {
			if host == "" || lane == "" {
				return NewBadUsageError("--host and --lane required")
			}
			ac, _, err := setupAppCtx(cmd)
			if err != nil {
				return err
			}
			defer ac.Close()
			return runCtlLanesContinue(ac, host, lane)
		},
	}
	c.Flags().StringVar(&host, "host", "", "dugdale id")
	c.Flags().StringVar(&lane, "lane", "", "lane name")
	return c
}

func runCtlLanesContinue(ac *appCtx, host, lane string) error {
	c, err := ac.ClientForHost(host, lettsconfig.ScopeAdmin)
	if err != nil {
		return err
	}
	return lettsclient.ContinueLane(c, lane)
}
