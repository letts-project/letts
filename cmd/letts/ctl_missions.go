package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"letts/pkg/lettsclient"
	"letts/pkg/lettsconfig"
)

// newCtlMissionsCmd is the `letts ctl missions` group: list/show/output and
// restart/kill/delete (single-id).
func newCtlMissionsCmd() *cobra.Command {
	c := &cobra.Command{Use: "missions", Short: "Mission control"}
	c.AddCommand(newCtlMissionsListCmd())
	c.AddCommand(newCtlMissionsShowCmd())
	c.AddCommand(newCtlMissionsOutputCmd())
	c.AddCommand(newCtlMissionsRestartCmd())
	c.AddCommand(newCtlMissionsKillCmd())
	c.AddCommand(newCtlMissionsDeleteCmd())
	return c
}

// newCtlMissionsListCmd binds `letts ctl missions list`. --host is required
// because listing is admin-scoped and the daemon doesn't aggregate across
// hosts — by-id fan-out is a separate code path.
//
// --kind defaults to "mission" so default listings stay clean of exec
// rows; --kind=all opts back into the unfiltered view. Unknown values are
// rejected as BadUsage so typos don't silently fall through to a daemon
// error.
func newCtlMissionsListCmd() *cobra.Command {
	var host, status, outcome, lane, mission, missionPrefix, kind, since, until, cursor string
	var limit int
	c := &cobra.Command{
		Use: "list", Short: "List missions",
		RunE: func(cmd *cobra.Command, args []string) error {
			if host == "" {
				return NewBadUsageError("--host is required for ctl missions list")
			}
			// Validate --kind before setupAppCtx so usage errors don't require
			// a loaded config (mirrors the --host check above).
			switch kind {
			case "mission", "exec":
				// keep kind as-is; threaded into opts below.
			case "all":
				kind = ""
			default:
				return NewBadUsageError(`--kind: must be one of "mission", "exec", "all"`)
			}
			ac, format, err := setupAppCtx(cmd)
			if err != nil {
				return err
			}
			defer ac.Close()
			opts := lettsclient.ListMissionsOpts{
				Status:        status,
				Outcome:       outcome,
				Lane:          lane,
				Mission:       mission,
				MissionPrefix: missionPrefix,
				Kind:          kind,
				Cursor:        cursor,
				Limit:         limit,
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
			return runCtlMissionsList(ac, cmd.OutOrStdout(), cmd.ErrOrStderr(), host, opts, format)
		},
	}
	c.Flags().StringVar(&host, "host", "", "dugdale id (required)")
	c.Flags().StringVar(&status, "status", "", "filter status (queued|running|done|deleting)")
	c.Flags().StringVar(&outcome, "outcome", "", "filter outcome (success|failed|killed|timeout|oom|crashed|lost)")
	c.Flags().StringVar(&lane, "lane", "", "filter lane")
	c.Flags().StringVar(&mission, "mission", "", "filter by mission name substring")
	c.Flags().StringVar(&missionPrefix, "mission-prefix", "", "filter mission name by prefix")
	c.Flags().StringVar(&kind, "kind", "mission", "kind filter: mission|exec|all")
	c.Flags().StringVar(&since, "since", "", "absolute ms or relative duration (-1h, -7d)")
	c.Flags().StringVar(&until, "until", "", "absolute ms or relative duration")
	c.Flags().IntVar(&limit, "limit", 0, "max rows (default daemon-side)")
	c.Flags().StringVar(&cursor, "cursor", "", "pagination cursor from a previous next_cursor")
	return c
}

// runCtlMissionsList performs the admin GET /v1/missions and renders.
// Split out so tests can drive without parsing flags.
func runCtlMissionsList(ac *appCtx, w, errW io.Writer, host string, opts lettsclient.ListMissionsOpts, f Format) error {
	c, err := ac.ClientForHost(host, lettsconfig.ScopeAdmin)
	if err != nil {
		return err
	}
	resp, err := lettsclient.ListMissions(c, opts)
	if err != nil {
		return err
	}
	if ok, err := printList(w, f, resp, resp.Missions); ok {
		if err == nil && f == FormatNDJSON && resp.NextCursor != "" {
			_, _ = fmt.Fprintf(errW, "cursor: %s\n", resp.NextCursor)
		}
		return err
	}
	// Conditionally widen the table with DISPLAY_NAME / GROUP_ID columns:
	// only show them when at least one row has the value populated, so
	// plain mission listings stay narrow and exec listings surface the
	// group and display preview automatically.
	anyGroup, anyDisplay := false, false
	for _, m := range resp.Missions {
		if m.GroupID != "" {
			anyGroup = true
		}
		if m.DisplayName != "" {
			anyDisplay = true
		}
	}
	header := []string{"MISSION_ID", "STATUS", "OUTCOME", "LANE", "MISSION", "TIME_CREATED"}
	widths := []int{40, 10, 10, 12, 25, 13}
	if anyDisplay {
		header = append(header, "DISPLAY_NAME")
		widths = append(widths, 30)
	}
	if anyGroup {
		header = append(header, "GROUP_ID")
		widths = append(widths, 40)
	}
	for i, h := range header {
		if i > 0 {
			_, _ = fmt.Fprintf(w, "  ")
		}
		_, _ = fmt.Fprintf(w, "%-*s", widths[i], h)
	}
	_, _ = fmt.Fprintln(w)
	for _, m := range resp.Missions {
		cols := []string{m.MissionID, m.Status, m.Outcome, m.Lane, m.MissionName, strconv.FormatInt(m.TimeCreatedMs, 10)}
		if anyDisplay {
			cols = append(cols, m.DisplayName)
		}
		if anyGroup {
			cols = append(cols, m.GroupID)
		}
		for i, c := range cols {
			if i > 0 {
				_, _ = fmt.Fprintf(w, "  ")
			}
			_, _ = fmt.Fprintf(w, "%-*s", widths[i], c)
		}
		_, _ = fmt.Fprintln(w)
	}
	if resp.NextCursor != "" {
		_, _ = fmt.Fprintf(w, "\ncursor: %s\n", resp.NextCursor)
	}
	return nil
}

// newCtlMissionsShowCmd binds `letts ctl missions show <id>`. Without --host
// the command falls back to by-id fan-out across configured dugdales.
func newCtlMissionsShowCmd() *cobra.Command {
	var host string
	var match []string
	c := &cobra.Command{
		Use:   "show <mission_id>",
		Short: "Get mission record",
		Args:  exactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id := args[0]
			ac, format, err := setupAppCtx(cmd)
			if err != nil {
				return err
			}
			defer ac.Close()
			return runCtlMissionsShow(ac, cmd.OutOrStdout(), id, host, match, format)
		},
	}
	c.Flags().StringVar(&host, "host", "", "dugdale id (else by-id fan-out)")
	c.Flags().StringSliceVar(&match, "match", nil, "label filter for fan-out")
	return c
}

// runCtlMissionsShow GETs /v1/missions/{id} either on a single explicit host
// or via by-id fan-out, then prints the mission record using the active
// format.
func runCtlMissionsShow(ac *appCtx, w io.Writer, id, host string, match []string, f Format) error {
	m, err := getMissionByID(ac, readScopesMission, id, host, match)
	if err != nil {
		return err
	}
	return printMission(w, m, f)
}

// getMissionByID GETs mission id on host, or on the single dugdale that has
// it, trying tokens in scopes order (see callWithKindScopes).
func getMissionByID(ac *appCtx, scopes []lettsconfig.Scope, id, host string, match []string) (*lettsclient.Mission, error) {
	get := func(hostID string) (*lettsclient.Mission, error) {
		return callWithKindScopes(ac, hostID, scopes, func(c *lettsclient.Client) (*lettsclient.Mission, error) {
			return lettsclient.GetMission(c, id)
		})
	}
	if host != "" {
		return get(host)
	}
	m, _, err := fanOutByIDHosts(ac, match, get)
	return m, err
}

// printMission renders a *Mission in the chosen format. Text mode falls back
// to JSON because mission records are nested structs that don't tabulate.
func printMission(w io.Writer, m *lettsclient.Mission, f Format) error {
	if ok, err := printRecord(w, f, m); ok {
		return err
	}
	return PrintJSON(w, m)
}

// newCtlMissionsOutputCmd binds `letts ctl missions output <id>`. Streams
// stdout/stderr/combined; honours --follow for live tailing.
func newCtlMissionsOutputCmd() *cobra.Command {
	var host, stream string
	var match []string
	var follow bool
	c := &cobra.Command{
		Use:   "output <mission_id>",
		Short: "Stream mission stdout/stderr/combined",
		Args:  exactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id := args[0]
			if stream == "" {
				stream = "combined"
			}
			ac, format, err := setupAppCtx(cmd)
			if err != nil {
				return err
			}
			defer ac.Close()
			return runCtlMissionsOutput(cmd, ac, format, readScopesMission, id, host, stream, follow, match)
		},
	}
	c.Flags().StringVar(&host, "host", "", "dugdale id (else by-id fan-out)")
	c.Flags().StringSliceVar(&match, "match", nil, "label filter for fan-out")
	c.Flags().StringVar(&stream, "stream", "combined", "stdout|stderr|combined")
	c.Flags().BoolVar(&follow, "follow", false, "follow live output")
	return c
}

// runCtlMissionsOutput opens the /output stream on --host or by by-id fan-out
// and renders it to stdout in format f (see renderOutput). Tokens are tried in
// scopes order per host (see callWithKindScopes); conflicts and 404 semantics
// are identical to show.
func runCtlMissionsOutput(cmd *cobra.Command, ac *appCtx, f Format, scopes []lettsconfig.Scope, id, host, stream string, follow bool, match []string) error {
	openOn := func(hostID string) (io.ReadCloser, error) {
		return callWithKindScopes(ac, hostID, scopes, func(c *lettsclient.Client) (io.ReadCloser, error) {
			return lettsclient.OpenOutput(cmd.Context(), c, id, lettsclient.OutputOpts{Stream: stream, Follow: follow})
		})
	}
	var rc io.ReadCloser
	var err error
	if host != "" {
		rc, err = openOn(host)
	} else {
		rc, _, err = fanOutByIDHosts(ac, match, openOn)
	}
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()
	return renderOutput(cmd.OutOrStdout(), rc, stream, f)
}

// newCtlMissionsRestartCmd binds `letts ctl missions restart`. Two modes:
//   - single: `restart <id>` posts /restart on either an explicit --host
//     or via by-id fan-out.
//   - bulk:   `restart --selector=...` lists missions matching the selector
//     then POSTs /v1/missions/bulk-restart. --host is required (no fan-out
//     for bulk; selectors are scoped per-dugdale).
//
// Mode is chosen by --selector presence; mixing it with a positional id
// returns BadUsageError so the daemon never sees an ambiguous request.
func newCtlMissionsRestartCmd() *cobra.Command {
	var host, selector string
	var match []string
	var limit int
	var dryRun, yes bool
	c := &cobra.Command{
		Use:   "restart [<mission_id>]",
		Short: "Restart a done mission (single or --selector bulk)",
		Args:  maximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			// Validate mode BEFORE setupAppCtx so usage errors don't depend
			// on config presence (mirrors list's --host check).
			if selector != "" {
				if len(args) > 0 {
					return NewBadUsageError("--selector cannot be combined with a positional <mission_id>")
				}
				if host == "" {
					return NewBadUsageError("--host is required for --selector bulk operations")
				}
			} else if len(args) != 1 {
				return NewBadUsageError("either <mission_id> or --selector is required")
			}
			ac, format, err := setupAppCtx(cmd)
			if err != nil {
				return err
			}
			defer ac.Close()
			if selector != "" {
				return runCtlMissionsBulkRestart(ac, cmd.OutOrStdout(), cmd.ErrOrStderr(), cmd.InOrStdin(), format, host, selector, limit, dryRun, yes)
			}
			if dryRun {
				return runCtlMissionsDryRunSingle(ac, cmd.OutOrStdout(), format, "restart", args[0], host, match)
			}
			return runCtlMissionsRestart(ac, cmd.OutOrStdout(), args[0], host, match, format)
		},
	}
	c.Flags().StringVar(&host, "host", "", "dugdale id (else by-id fan-out for single mode; required for --selector)")
	c.Flags().StringSliceVar(&match, "match", nil, "label filter for fan-out (single mode)")
	c.Flags().StringVar(&selector, "selector", "", "bulk select (e.g. outcome=failed,since=-1h)")
	c.Flags().IntVar(&limit, "limit", 0, "max missions to bulk-act on (0 = daemon default)")
	c.Flags().BoolVar(&dryRun, "dry-run", false, "list the mission(s) that would be acted on, without acting")
	c.Flags().BoolVar(&yes, "yes", false, "skip interactive confirmation")
	return c
}

// runCtlMissionsRestart POSTs /v1/missions/{id}/restart either on a single
// explicit host or via locate-then-act fan-out and prints the new mission
// id. Text mode emits just the new id on its own line so shell pipelines can
// grab it.
func runCtlMissionsRestart(ac *appCtx, w io.Writer, id, host string, match []string, f Format) error {
	doRestart := func(c *lettsclient.Client) (*lettsclient.RestartResponse, error) {
		return lettsclient.RestartMission(c, id)
	}
	var resp *lettsclient.RestartResponse
	var err error
	if host != "" {
		c, cerr := ac.ClientForHost(host, lettsconfig.ScopeAdmin)
		if cerr != nil {
			return cerr
		}
		resp, err = doRestart(c)
	} else {
		// Restart is destructive: locate the single owning host first so an
		// id duplicated across hosts can't spawn one new mission per host.
		resp, _, err = LocateThenActByID(ac, id, match, doRestart)
	}
	if err != nil {
		return err
	}
	if ok, err := printRecord(w, f, resp); ok {
		return err
	}
	_, _ = fmt.Fprintf(w, "%s\n", resp.MissionID)
	return nil
}

// newCtlMissionsKillCmd binds `letts ctl missions kill <id>`. The --signal
// flag (TERM|KILL) is forwarded to the daemon; today it's advisory but the
// transport already supports it.
func newCtlMissionsKillCmd() *cobra.Command {
	var host, signal string
	var match []string
	c := &cobra.Command{
		Use:   "kill <mission_id>",
		Short: "Kill running mission",
		Args:  exactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id := args[0]
			ac, _, err := setupAppCtx(cmd)
			if err != nil {
				return err
			}
			defer ac.Close()
			return runCtlMissionsKill(ac, id, host, signal, match)
		},
	}
	c.Flags().StringVar(&host, "host", "", "dugdale id (else by-id fan-out)")
	c.Flags().StringVar(&signal, "signal", "TERM", "TERM|KILL")
	c.Flags().StringSliceVar(&match, "match", nil, "label filter for fan-out")
	return c
}

// runCtlMissionsKill POSTs /v1/missions/{id}/kill. Kill is fire-and-forget
// from the CLI's perspective; we don't render the daemon's status body — exit
// code 0 means the request was accepted.
func runCtlMissionsKill(ac *appCtx, id, host, signal string, match []string) error {
	doKill := func(c *lettsclient.Client) (struct{}, error) {
		return struct{}{}, lettsclient.KillMission(c, id, signal)
	}
	var err error
	if host != "" {
		c, cerr := ac.ClientForHost(host, lettsconfig.ScopeAdmin)
		if cerr != nil {
			return cerr
		}
		_, err = doKill(c)
	} else {
		// Kill is destructive: locate first so a duplicated id can't get a
		// signal delivered on every host that happens to carry it.
		_, _, err = LocateThenActByID(ac, id, match, doKill)
	}
	return err
}

// newCtlMissionsDeleteCmd binds `letts ctl missions delete`. Like restart,
// it accepts either a positional <mission_id> (single mode,
// fan-out capable) or --selector for a bulk POST /v1/missions/bulk-delete.
// --force is passed through to either path.
func newCtlMissionsDeleteCmd() *cobra.Command {
	var host, selector string
	var match []string
	var force, dryRun, yes bool
	var limit int
	c := &cobra.Command{
		Use:   "delete [<mission_id>]",
		Short: "Delete mission (single or --selector bulk)",
		Args:  maximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if selector != "" {
				if len(args) > 0 {
					return NewBadUsageError("--selector cannot be combined with a positional <mission_id>")
				}
				if host == "" {
					return NewBadUsageError("--host is required for --selector bulk operations")
				}
			} else if len(args) != 1 {
				return NewBadUsageError("either <mission_id> or --selector is required")
			}
			ac, format, err := setupAppCtx(cmd)
			if err != nil {
				return err
			}
			defer ac.Close()
			if selector != "" {
				return runCtlMissionsBulkDelete(ac, cmd.OutOrStdout(), cmd.ErrOrStderr(), cmd.InOrStdin(), format, host, selector, limit, force, dryRun, yes)
			}
			if dryRun {
				return runCtlMissionsDryRunSingle(ac, cmd.OutOrStdout(), format, "delete", args[0], host, match)
			}
			return runCtlMissionsDelete(ac, args[0], host, force, match)
		},
	}
	c.Flags().StringVar(&host, "host", "", "dugdale id (else by-id fan-out for single mode; required for --selector)")
	c.Flags().StringSliceVar(&match, "match", nil, "label filter for fan-out (single mode)")
	c.Flags().BoolVar(&force, "force", false, "force delete running missions (?force=true / body.force)")
	c.Flags().StringVar(&selector, "selector", "", "bulk select (e.g. outcome=success,since=-7d)")
	c.Flags().IntVar(&limit, "limit", 0, "max missions to bulk-act on (0 = daemon default)")
	c.Flags().BoolVar(&dryRun, "dry-run", false, "list the mission(s) that would be acted on, without acting")
	c.Flags().BoolVar(&yes, "yes", false, "skip interactive confirmation")
	return c
}

// runCtlMissionsDryRunSingle checks with the admin token that mission id
// exists (on host, or on exactly one dugdale by by-id fan-out) and lists it
// as the restart/delete would-act set without acting.
func runCtlMissionsDryRunSingle(ac *appCtx, w io.Writer, f Format, verb, id, host string, match []string) error {
	get := func(c *lettsclient.Client) (*lettsclient.Mission, error) {
		return lettsclient.GetMission(c, id)
	}
	if host != "" {
		c, err := ac.ClientForHost(host, lettsconfig.ScopeAdmin)
		if err != nil {
			return err
		}
		if _, err := get(c); err != nil {
			return err
		}
	} else if _, _, err := FanOutByID(ac, match, get); err != nil {
		return err
	}
	return printDryRun(w, f, verb, []string{id})
}

// runCtlMissionsDelete issues DELETE /v1/missions/{id}. Like kill, the daemon
// response body isn't rendered — exit code 0 signals deletion_pending.
func runCtlMissionsDelete(ac *appCtx, id, host string, force bool, match []string) error {
	doDelete := func(c *lettsclient.Client) (struct{}, error) {
		return struct{}{}, lettsclient.DeleteMission(c, id, force)
	}
	var err error
	if host != "" {
		c, cerr := ac.ClientForHost(host, lettsconfig.ScopeAdmin)
		if cerr != nil {
			return cerr
		}
		_, err = doDelete(c)
	} else {
		// Delete is destructive: locate first so a duplicated id is removed
		// from exactly one host (or refused with a conflict), never from all.
		_, _, err = LocateThenActByID(ac, id, match, doDelete)
	}
	return err
}

// runCtlMissionsBulkRestart implements `restart --selector=...`: it lists the
// missions matching the selector on host, then (after --dry-run listing or
// the y/N confirmation unless --yes) POSTs /v1/missions/bulk-restart. Any
// per-id failure makes it return an aggregate error so the exit code reflects
// partial failure; a "no" at the prompt returns a generic "aborted" error.
func runCtlMissionsBulkRestart(ac *appCtx, w, errW io.Writer, stdin io.Reader, f Format, host, selector string, limit int, dryRun, yes bool) error {
	return runCtlMissionsBulk(ac, w, errW, stdin, f, "restart", host, selector, limit, dryRun, yes,
		func(c *lettsclient.Client, ids []string) (*lettsclient.BulkResponse, error) {
			return lettsclient.BulkRestart(c, ids)
		})
}

// runCtlMissionsBulkDelete is runCtlMissionsBulkRestart for bulk-delete,
// forwarding --force.
func runCtlMissionsBulkDelete(ac *appCtx, w, errW io.Writer, stdin io.Reader, f Format, host, selector string, limit int, force, dryRun, yes bool) error {
	return runCtlMissionsBulk(ac, w, errW, stdin, f, "delete", host, selector, limit, dryRun, yes,
		func(c *lettsclient.Client, ids []string) (*lettsclient.BulkResponse, error) {
			return lettsclient.BulkDelete(c, ids, force)
		})
}

func runCtlMissionsBulk(ac *appCtx, w, errW io.Writer, stdin io.Reader, f Format, verb, host, selector string, limit int, dryRun, yes bool,
	act func(*lettsclient.Client, []string) (*lettsclient.BulkResponse, error)) error {
	sel, err := ParseSelector(selector, time.Now())
	if err != nil {
		return NewBadUsageError(err.Error())
	}
	c, err := ac.ClientForHost(host, lettsconfig.ScopeAdmin)
	if err != nil {
		return err
	}
	opts := sel.ToListOpts()
	if limit > 0 {
		opts.Limit = limit
	}
	list, err := lettsclient.ListMissions(c, opts)
	if err != nil {
		return err
	}
	ids := make([]string, 0, len(list.Missions))
	for _, m := range list.Missions {
		ids = append(ids, m.MissionID)
	}
	if list.NextCursor != "" {
		_, _ = fmt.Fprintf(errW, "warning: more missions match the selector; only the first %d are included (raise --limit or re-run)\n", len(ids))
	}
	if dryRun {
		return printDryRun(w, f, verb, ids)
	}
	if len(ids) == 0 {
		_, _ = fmt.Fprintf(errW, "no missions match; nothing to %s\n", verb)
		return printBulkResults(w, errW, f, &lettsclient.BulkResponse{Results: []lettsclient.BulkResult{}}, verb)
	}
	if !yes {
		if err := confirmBulk(errW, stdin, verb, len(ids)); err != nil {
			return err
		}
	}
	resp, err := act(c, ids)
	if err != nil {
		return err
	}
	return printBulkResults(w, errW, f, resp, verb)
}

// confirmBulk prints "{verb} N missions? [y/N]: " to errW and reads a single
// token from stdin. Only "y" / "Y" (case-insensitive, trimmed) accepts;
// anything else — including empty input / EOF — returns the canonical
// "aborted" error so callers exit non-zero without bubbling a usage error.
func confirmBulk(errW io.Writer, stdin io.Reader, verb string, n int) error {
	_, _ = fmt.Fprintf(errW, "%s %d missions? [y/N]: ", verb, n)
	br := bufio.NewReader(stdin)
	line, _ := br.ReadString('\n')
	if strings.ToLower(strings.TrimSpace(line)) != "y" {
		return fmt.Errorf("aborted")
	}
	return nil
}

type dryRunResult struct {
	DryRun bool     `json:"dry_run"`
	IDs    []string `json:"ids"`
}

// printDryRun lists the mission ids a restart/delete would act on.
func printDryRun(w io.Writer, f Format, verb string, ids []string) error {
	switch f {
	case FormatJSON:
		return PrintJSON(w, dryRunResult{DryRun: true, IDs: ids})
	case FormatYAML:
		return PrintYAML(w, dryRunResult{DryRun: true, IDs: ids})
	case FormatNDJSON:
		for _, id := range ids {
			if err := PrintNDJSON(w, map[string]string{"id": id}); err != nil {
				return err
			}
		}
		return nil
	}
	_, _ = fmt.Fprintf(w, "would %s %d missions:\n", verb, len(ids))
	for _, id := range ids {
		_, _ = fmt.Fprintln(w, id)
	}
	return nil
}

// printBulkResults renders a bulk response and returns an aggregate error
// when any id failed. Text mode writes successful ids one per line to stdout
// (the new id for restart) and FAIL lines to stderr; json/yaml write the
// whole response, ndjson one result per line.
func printBulkResults(w, errW io.Writer, f Format, resp *lettsclient.BulkResponse, verb string) error {
	failed := 0
	for _, r := range resp.Results {
		if !r.OK {
			failed++
		}
	}
	var err error
	switch f {
	case FormatJSON:
		err = PrintJSON(w, resp)
	case FormatYAML:
		err = PrintYAML(w, resp)
	case FormatNDJSON:
		for _, r := range resp.Results {
			if err = PrintNDJSON(w, r); err != nil {
				break
			}
		}
	default:
		for _, r := range resp.Results {
			if !r.OK {
				_, _ = fmt.Fprintf(errW, "  FAIL %s — %s\n", r.ID, r.Error)
				continue
			}
			out := r.MissionID
			if out == "" {
				out = r.ID
			}
			_, _ = fmt.Fprintln(w, out)
		}
	}
	if err != nil {
		return err
	}
	if failed > 0 {
		return fmt.Errorf("%d of %d %s operations failed", failed, len(resp.Results), verb)
	}
	return nil
}

// parseSinceTime accepts either a relative duration prefixed by `-` (e.g.
// `-1h`, `-30m`, `-7d`) or an absolute Unix millisecond timestamp. Days
// (`d`) are normalised manually since time.ParseDuration tops out at hours.
func parseSinceTime(s string, now time.Time) (int64, error) {
	if strings.HasPrefix(s, "-") {
		raw := strings.TrimPrefix(s, "-")
		if strings.HasSuffix(raw, "d") {
			n, err := strconv.Atoi(strings.TrimSuffix(raw, "d"))
			if err != nil {
				return 0, err
			}
			d := time.Duration(n) * 24 * time.Hour
			return now.Add(-d).UnixMilli(), nil
		}
		d, err := time.ParseDuration(raw)
		if err != nil {
			return 0, err
		}
		return now.Add(-d).UnixMilli(), nil
	}
	return strconv.ParseInt(s, 10, 64)
}
