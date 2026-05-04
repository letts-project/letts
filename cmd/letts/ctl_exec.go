package main

import (
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/spf13/cobra"

	"letts/pkg/lettsclient"
	"letts/pkg/lettsconfig"
)

// newCtlExecCmd is the `letts ctl exec` group. Mirrors `ctl missions` but
// pins kind=exec and adds --group on list, and validates kind=exec on
// show before rendering.
func newCtlExecCmd() *cobra.Command {
	c := &cobra.Command{Use: "exec", Short: "Exec invocation control / inspection"}
	c.AddCommand(newCtlExecListCmd())
	c.AddCommand(newCtlExecShowCmd())
	c.AddCommand(newCtlExecOutputCmd())
	c.AddCommand(newCtlExecRestartCmd())
	c.AddCommand(newCtlExecKillCmd())
	c.AddCommand(newCtlExecDeleteCmd())
	return c
}

// newCtlExecListCmd binds `letts ctl exec list`. Like `ctl missions list`
// it requires --host (admin-scoped, no cross-host aggregation) and adds
// --group for filtering by group_id. Kind is pinned to "exec" so the
// daemon never sees a named mission in the response set.
func newCtlExecListCmd() *cobra.Command {
	var host, status, outcome, lane, group, since, until, cursor string
	var limit int
	c := &cobra.Command{
		Use:   "list",
		Short: "List exec invocations (kind=exec)",
		RunE: func(cmd *cobra.Command, args []string) error {
			if host == "" {
				return NewBadUsageError("--host is required for ctl exec list")
			}
			ac, format, err := setupAppCtx(cmd)
			if err != nil {
				return err
			}
			defer ac.Close()
			opts := lettsclient.ListMissionsOpts{
				Kind:    "exec",
				GroupID: group,
				Status:  status,
				Outcome: outcome,
				Lane:    lane,
				Cursor:  cursor,
				Limit:   limit,
			}
			if since != "" {
				ms, perr := parseSinceTime(since, time.Now())
				if perr != nil {
					return NewBadUsageError("--since: " + perr.Error())
				}
				opts.SinceMs = ms
			}
			if until != "" {
				ms, perr := parseSinceTime(until, time.Now())
				if perr != nil {
					return NewBadUsageError("--until: " + perr.Error())
				}
				opts.UntilMs = ms
			}
			return runCtlMissionsList(ac, cmd.OutOrStdout(), host, opts, format)
		},
	}
	c.Flags().StringVar(&host, "host", "", "dugdale id (required)")
	c.Flags().StringVar(&group, "group", "", "filter by group_id")
	c.Flags().StringVar(&status, "status", "", "queued|running|done|deleting")
	c.Flags().StringVar(&outcome, "outcome", "", "success|failed|killed|timeout|oom|crashed|lost")
	c.Flags().StringVar(&lane, "lane", "", "filter lane")
	c.Flags().StringVar(&since, "since", "", "absolute ms or relative duration (-1h)")
	c.Flags().StringVar(&until, "until", "", "absolute ms or relative duration")
	c.Flags().IntVar(&limit, "limit", 0, "max rows")
	c.Flags().StringVar(&cursor, "cursor", "", "pagination cursor")
	return c
}

// newCtlExecShowCmd binds `letts ctl exec show <id>`. Without --host the
// command falls back to by-id fan-out across all dugdales, identical to
// `ctl missions show`; the difference is the kind=exec gate enforced
// post-fetch by runCtlExecShow.
func newCtlExecShowCmd() *cobra.Command {
	var host string
	var match []string
	c := &cobra.Command{
		Use:   "show <exec_id>",
		Short: "Get exec record",
		Args:  exactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ac, format, err := setupAppCtx(cmd)
			if err != nil {
				return err
			}
			defer ac.Close()
			return runCtlExecShow(ac, cmd.OutOrStdout(), args[0], host, match, format)
		},
	}
	c.Flags().StringVar(&host, "host", "", "dugdale id (else by-id fan-out)")
	c.Flags().StringSliceVar(&match, "match", nil, "label filter for fan-out")
	return c
}

// runCtlExecShow fetches the mission record (single-host or fan-out) and
// rejects it as BadUsage if kind != "exec". This guards against typos like
// `ctl exec show <mission_id>` silently rendering a named-mission record
// using the mission renderer. The error message points at the correct
// subcommand so the user can re-issue without guessing.
func runCtlExecShow(ac *appCtx, w io.Writer, id, host string, match []string, f Format) error {
	m, err := fetchMissionForExec(ac, id, host, match)
	if err != nil {
		return err
	}
	if m.Kind != "exec" {
		kind := m.Kind
		if kind == "" {
			kind = "mission"
		}
		return NewBadUsageError(fmt.Sprintf("%s is not an exec mission (kind=%s); use 'letts ctl missions show'", id, kind))
	}
	return printMission(w, m, f)
}

// fetchMissionForExec resolves an exec_id to a single client and record.
// Mirrors runCtlMissionsShow's branching but returns the *Mission so
// callers can validate kind before rendering or acting.
func fetchMissionForExec(ac *appCtx, id, host string, match []string) (*lettsclient.Mission, error) {
	if host != "" {
		// By-id exec read with explicit --host accepts exec scope
		// too (admin fallback), matching the fan-out path below.
		c, err := clientForFirstAvailableScope(ac, host, []lettsconfig.Scope{lettsconfig.ScopeExec, lettsconfig.ScopeAdmin})
		if err != nil {
			return nil, err
		}
		return lettsclient.GetMission(c, id)
	}
	// Prefer exec scope for kind=exec reads so a CLI with
	// only an exec token still works; fall back to admin internally.
	m, _, err := FanOutByIDForScope(ac, match, lettsconfig.ScopeExec, func(c *lettsclient.Client) (*lettsclient.Mission, error) {
		return lettsclient.GetMission(c, id)
	})
	return m, err
}

// newCtlExecOutputCmd binds `letts ctl exec output <exec_id>`. The /output
// endpoint is kind-agnostic at the daemon, so we delegate to
// runCtlMissionsOutput which already handles single-host vs by-id fan-out,
// streaming via io.Copy, and --follow.
func newCtlExecOutputCmd() *cobra.Command {
	var host, stream string
	var follow bool
	var match []string
	c := &cobra.Command{
		Use:   "output <exec_id>",
		Short: "Stream exec stdout/stderr/combined",
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
	c.Flags().StringVar(&stream, "stream", "combined", "stdout|stderr|combined")
	c.Flags().BoolVar(&follow, "follow", false, "stream live")
	c.Flags().StringSliceVar(&match, "match", nil, "label filter for fan-out")
	return c
}

// newCtlExecRestartCmd binds `letts ctl exec restart <exec_id>`. Unlike
// `ctl missions restart` this command intentionally requires --host (no
// by-id fan-out for exec restart) and renders the 409
// input_artifacts_expired error with a user-friendly message.
func newCtlExecRestartCmd() *cobra.Command {
	var host string
	c := &cobra.Command{
		Use:   "restart <exec_id>",
		Short: "Restart an exec (creates new exec_id; admin-only)",
		Args:  exactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ac, format, err := setupAppCtx(cmd)
			if err != nil {
				return err
			}
			defer ac.Close()
			return runCtlExecRestart(ac, cmd.OutOrStdout(), args[0], host, format)
		},
	}
	c.Flags().StringVar(&host, "host", "", "dugdale id (required)")
	return c
}

// runCtlExecRestart POSTs /v1/missions/{id}/restart and renders the
// RestartResponse. On 409 input_artifacts_expired it formats a friendly
// multi-line message naming the expired staging_id and role and returns a
// non-nil error so the CLI exits non-zero. All other transport errors
// flow through wrapExecTransport for the standard exec exit-code mapping
// (→ 255).
func runCtlExecRestart(ac *appCtx, w io.Writer, id, host string, f Format) error {
	if host == "" {
		return NewBadUsageError("--host is required for ctl exec restart")
	}
	c, err := ac.ClientForHost(host, lettsconfig.ScopeAdmin)
	if err != nil {
		return wrapExecTransport(err)
	}
	resp, err := lettsclient.RestartMission(c, id)
	if err != nil {
		var he *lettsclient.HTTPError
		if errors.As(err, &he) && he.Status == 409 && he.Code == "input_artifacts_expired" {
			_, _ = fmt.Fprintf(w, "Cannot restart %s — input artifacts have expired:\n", id)
			if details, ok := he.Details.(map[string]any); ok {
				staging, _ := details["staging_id"].(string)
				role, _ := details["role"].(string)
				if role == "" {
					role = "?"
				}
				if staging != "" {
					_, _ = fmt.Fprintf(w, "  - %s: %s\n", role, staging)
				}
			}
			_, _ = fmt.Fprintln(w, "Original exec was retained for exec_failed_ttl; to retry, dispatch a fresh exec with the original payload.")
			return fmt.Errorf("input artifacts expired")
		}
		return wrapExecTransport(err)
	}
	switch f {
	case FormatJSON:
		return PrintJSON(w, resp)
	case FormatYAML:
		return PrintYAML(w, resp)
	default:
		_, _ = fmt.Fprintf(w, "%s\t%s\trestarted_from=%s\n", resp.MissionID, resp.Status, resp.RestartedFrom)
		return nil
	}
}

// newCtlExecKillCmd binds `letts ctl exec kill <exec_id>`. The /kill
// endpoint is kind-agnostic; delegate to runCtlMissionsKill.
func newCtlExecKillCmd() *cobra.Command {
	var host, signal string
	c := &cobra.Command{
		Use:   "kill <exec_id>",
		Short: "Kill running exec (admin-only)",
		Args:  exactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ac, _, err := setupAppCtx(cmd)
			if err != nil {
				return err
			}
			defer ac.Close()
			return runCtlMissionsKill(ac, args[0], host, signal, nil)
		},
	}
	c.Flags().StringVar(&host, "host", "", "dugdale id (else by-id fan-out)")
	c.Flags().StringVar(&signal, "signal", "", "TERM|KILL (default server choice)")
	return c
}

// newCtlExecDeleteCmd binds `letts ctl exec delete <exec_id>`. The
// DELETE /v1/missions/{id} endpoint is kind-agnostic; delegate to
// runCtlMissionsDelete. force=false: exec records are normally already
// terminal so a forced delete shouldn't be necessary.
func newCtlExecDeleteCmd() *cobra.Command {
	var host string
	c := &cobra.Command{
		Use:   "delete <exec_id>",
		Short: "Delete exec record and outputs (admin-only)",
		Args:  exactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ac, _, err := setupAppCtx(cmd)
			if err != nil {
				return err
			}
			defer ac.Close()
			return runCtlMissionsDelete(ac, args[0], host, false, nil)
		},
	}
	c.Flags().StringVar(&host, "host", "", "dugdale id (else by-id fan-out)")
	return c
}
