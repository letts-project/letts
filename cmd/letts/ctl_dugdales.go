package main

import (
	"fmt"
	"io"
	"sort"

	"github.com/spf13/cobra"

	"letts/pkg/lettsclient"
	"letts/pkg/lettsconfig"
)

func newCtlDugdalesCmd() *cobra.Command {
	c := &cobra.Command{Use: "dugdales", Short: "Manage dugdales (admin)"}
	c.AddCommand(newCtlDugdalesListCmd())
	c.AddCommand(newCtlDugdalesInfoCmd())
	c.AddCommand(newCtlDugdalesConfigCmd())
	return c
}

func newCtlDugdalesListCmd() *cobra.Command {
	return &cobra.Command{
		Use: "list", Short: "List dugdales from letts.yaml",
		RunE: func(cmd *cobra.Command, args []string) error {
			ac, format, err := setupAppCtx(cmd)
			if err != nil {
				return err
			}
			defer ac.Close()
			return runCtlDugdalesList(ac, cmd.OutOrStdout(), format)
		},
	}
}

func runCtlDugdalesList(ac *appCtx, w io.Writer, f Format) error {
	type row struct {
		ID     string   `json:"id" yaml:"id"`
		Host   string   `json:"host,omitempty" yaml:"host,omitempty"`
		URL    string   `json:"url,omitempty" yaml:"url,omitempty"`
		Labels []string `json:"labels,omitempty" yaml:"labels,omitempty"`
		Lanes  []string `json:"lanes,omitempty" yaml:"lanes,omitempty"`
	}
	out := make([]row, 0, len(ac.Config.Dugdales))
	for _, d := range ac.Config.Dugdales {
		lanes := make([]string, 0, len(d.Lanes))
		for n := range d.Lanes {
			lanes = append(lanes, n)
		}
		sort.Strings(lanes)
		out = append(out, row{ID: d.ID, Host: d.Host, URL: d.URL, Labels: d.Labels, Lanes: lanes})
	}
	switch f {
	case FormatJSON:
		return PrintJSON(w, out)
	case FormatYAML:
		return PrintYAML(w, out)
	default:
		for _, r := range out {
			_, _ = fmt.Fprintf(w, "%-12s  host=%-25s  lanes=%v  labels=%v\n", r.ID, r.Host, r.Lanes, r.Labels)
		}
		return nil
	}
}

func newCtlDugdalesInfoCmd() *cobra.Command {
	var host string
	c := &cobra.Command{
		Use: "info", Short: "Get dugdale runtime info",
		RunE: func(cmd *cobra.Command, args []string) error {
			if host == "" {
				return NewBadUsageError("--host is required")
			}
			ac, format, err := setupAppCtx(cmd)
			if err != nil {
				return err
			}
			defer ac.Close()
			return runCtlDugdalesInfo(ac, cmd.OutOrStdout(), host, format)
		},
	}
	c.Flags().StringVar(&host, "host", "", "dugdale id")
	return c
}

func runCtlDugdalesInfo(ac *appCtx, w io.Writer, host string, f Format) error {
	c, err := ac.ClientForHost(host, lettsconfig.ScopeDispatch)
	if err != nil {
		return err
	}
	info, err := lettsclient.GetDugdaleInfo(c)
	if err != nil {
		return err
	}
	switch f {
	case FormatJSON:
		return PrintJSON(w, info)
	case FormatYAML:
		return PrintYAML(w, info)
	default:
		_, _ = fmt.Fprintf(w, "version:   %s\n", info.Version)
		_, _ = fmt.Fprintf(w, "uptime_s:  %.1f\n", info.UptimeSeconds)
		_, _ = fmt.Fprintf(w, "queue:     queued=%d running=%d\n", info.QueueSummary.Queued, info.QueueSummary.Running)
		return nil
	}
}

func newCtlDugdalesConfigCmd() *cobra.Command {
	var host string
	c := &cobra.Command{
		Use: "config", Short: "Get applied state (GET /v1/admin/state)",
		RunE: func(cmd *cobra.Command, args []string) error {
			if host == "" {
				return NewBadUsageError("--host is required")
			}
			ac, format, err := setupAppCtx(cmd)
			if err != nil {
				return err
			}
			defer ac.Close()
			return runCtlDugdalesConfig(ac, cmd.OutOrStdout(), host, format)
		},
	}
	c.Flags().StringVar(&host, "host", "", "dugdale id")
	return c
}

func runCtlDugdalesConfig(ac *appCtx, w io.Writer, host string, f Format) error {
	c, err := ac.ClientForHost(host, lettsconfig.ScopeAdmin)
	if err != nil {
		return err
	}
	st, err := GetState(c)
	if err != nil {
		return err
	}
	if f == FormatYAML {
		return PrintYAML(w, st)
	}
	return PrintJSON(w, st)
}

// setupAppCtx reads global flags and builds appCtx.
func setupAppCtx(cmd *cobra.Command) (*appCtx, Format, error) {
	cfgPath, _ := cmd.Root().PersistentFlags().GetString("config")
	formatStr, _ := cmd.Root().PersistentFlags().GetString("output")
	insecure, _ := cmd.Root().PersistentFlags().GetBool("insecure-config-permissions")
	verbose, _ := cmd.Root().PersistentFlags().GetBool("verbose")
	quiet, _ := cmd.Root().PersistentFlags().GetBool("quiet")
	f, err := ParseFormat(formatStr)
	if err != nil {
		return nil, 0, NewBadUsageError(err.Error())
	}
	ac, err := newAppCtx(appCtxOpts{ConfigPath: cfgPath, Insecure: insecure, Verbose: verbose, Stderr: cmd.ErrOrStderr()})
	if err != nil {
		return nil, 0, err
	}
	ac.Quiet = quiet
	return ac, f, nil
}
