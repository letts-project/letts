package main

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/spf13/cobra"

	"letts/internal/apply"
	"letts/pkg/lettsconfig"
)

func newApplyCmd() *cobra.Command {
	var files []string
	var hosts, match []string
	var dryRun, force, prune, forcePrune bool
	c := &cobra.Command{
		Use:   "apply",
		Short: "Reconcile dugdales with letts.yaml",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(files) == 0 {
				return NewBadUsageError("at least one -f / --file is required")
			}
			format, err := ParseFormat(getRootFlagString(cmd, "output"))
			if err != nil {
				return NewBadUsageError(err.Error())
			}
			verbose, _ := cmd.Root().PersistentFlags().GetBool("verbose")
			quiet, _ := cmd.Root().PersistentFlags().GetBool("quiet")
			ignoreProxy, _ := cmd.Root().PersistentFlags().GetBool("ignore-proxy")
			merged, err := loadAndMergeFiles(files)
			if err != nil {
				return err
			}
			// Syntax of the merged config (pre-extends); structure is checked
			// after extends fills template-inherited fields.
			if err := lettsconfig.ValidateSyntax(merged); err != nil {
				return NewConfigError(err.Error())
			}
			if err := lettsconfig.ResolveExtends(merged); err != nil {
				return NewConfigError(err.Error())
			}
			// Full validation (incl. host/url presence, route/alias
			// resolution) on the merged and extends-resolved config.
			if err := lettsconfig.Validate(merged); err != nil {
				return NewConfigError(err.Error())
			}
			// Warn (don't fail) on plain-text tokens — but
			// only for PRIVILEGED scopes (admin/exec). The dispatch token is
			// widely distributed (app code, every app server), so plain text in
			// letts.yaml is expected and not worth a nag. Dispatch-scope
			// locations end in ".token"; admin/exec end in "_token".
			var plainPriv []string
			for _, loc := range lettsconfig.PlainTokenLocations(merged) {
				if !strings.HasSuffix(loc, ".token") {
					plainPriv = append(plainPriv, loc)
				}
			}
			if !quiet && len(plainPriv) > 0 {
				_, _ = fmt.Fprintf(cmd.ErrOrStderr(),
					"letts apply: warning — plain token(s) in letts.yaml at: %s\n",
					strings.Join(plainPriv, ", "))
				_, _ = fmt.Fprintln(cmd.ErrOrStderr(),
					"  prefer ${VAR} substitution to keep secrets out of the YAML file")
			}
			// --force-prune implies --prune (force-prune without prune is
			// meaningless — there are no lanes to remove without prune).
			if forcePrune {
				prune = true
			}
			// Build appCtx from auto-discovery, then override config with merged
			// (CLI uses provided -f files, not auto-discovered config).
			ac, err := newAppCtxBare()
			if err != nil {
				return err
			}
			ac.Config = merged
			ac.Verbose = verbose
			ac.Quiet = quiet
			ac.IgnoreProxy = ignoreProxy
			ac.Stderr = cmd.ErrOrStderr()
			defer ac.Close()
			if dryRun {
				return runApplyDryRun(ac, cmd.OutOrStdout(), hosts, match, format)
			}
			return runApply(ac, cmd.OutOrStdout(), hosts, match, force, prune, forcePrune, format)
		},
	}
	c.Flags().StringSliceVarP(&files, "file", "f", nil, "letts.yaml (repeatable)")
	c.Flags().StringSliceVar(&hosts, "host", nil, "comma-separated dugdale ids (empty = all)")
	c.Flags().StringSliceVar(&match, "match", nil, "label filter (AND)")
	c.Flags().BoolVar(&dryRun, "dry-run", false, "show diff, do not apply")
	c.Flags().BoolVar(&force, "force", false, "allow destructive runtime changes")
	c.Flags().BoolVar(&prune, "prune", false, "remove lanes not present in the applied config")
	c.Flags().BoolVar(&forcePrune, "force-prune", false, "with --prune: also terminate queued/running missions in removed lanes")
	return c
}

// newAppCtxBare returns an appCtx without auto-discovering config (used by
// `apply` which loads explicit -f files instead).
func newAppCtxBare() (*appCtx, error) {
	return &appCtx{
		Getenv:  func(k string) (string, bool) { return os.LookupEnv(k) },
		clients: map[clientKey]*hostClient{},
	}, nil
}

func loadAndMergeFiles(files []string) (*lettsconfig.Config, error) {
	var merged *lettsconfig.Config
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return nil, NewConfigError("read " + f + ": " + err.Error())
		}
		c, err := lettsconfig.Load(b)
		if err != nil {
			return nil, NewConfigError(f + ": " + err.Error())
		}
		// Validate each file individually before merging so a
		// typo in base's lane name can't be masked by an overlay that
		// redefines the same key correctly. Only SYNTAX per file —
		// structural rules (host/url presence, route/alias resolution) span
		// the merged config and would wrongly reject a host-less delta overlay
		// that inherits host from the base file. Structure is checked once on
		// the merged and extends-resolved config (see runApply).
		if err := lettsconfig.ValidateSyntax(c); err != nil {
			return nil, NewConfigError(f + ": " + err.Error())
		}
		merged = MergeConfigs(merged, c)
	}
	return merged, nil
}

type applyResult struct {
	ID  string
	Res *ApplyResult
	Err error
}

// runApply is the testable core (no cobra dependency).
func runApply(ac *appCtx, w io.Writer, hosts, match []string, force, prune, forcePrune bool, f Format) error {
	targets := selectApplyTargets(ac.Config, hosts, match)
	if len(targets) == 0 {
		return fmt.Errorf("no dugdales selected (host=%v match=%v)", hosts, match)
	}
	results := make([]applyResult, len(targets))
	var wg sync.WaitGroup
	for i, t := range targets {
		i, t := i, t
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := ac.ClientForHost(t.ID, lettsconfig.ScopeAdmin)
			if err != nil {
				results[i] = applyResult{ID: t.ID, Err: err}
				return
			}
			r, err := Apply(c, BuildAppliedState(t), ApplyOptions{
				Force: force, Prune: prune, ForcePrune: forcePrune,
			})
			results[i] = applyResult{ID: t.ID, Res: r, Err: err}
		}()
	}
	wg.Wait()
	return formatApplyResults(w, results, f)
}

func selectApplyTargets(c *lettsconfig.Config, hosts, match []string) []*lettsconfig.Dugdale {
	wantIDs := map[string]bool{}
	for _, h := range hosts {
		wantIDs[h] = true
	}
	out := make([]*lettsconfig.Dugdale, 0, len(c.Dugdales))
	for i := range c.Dugdales {
		d := &c.Dugdales[i]
		if len(wantIDs) > 0 && !wantIDs[d.ID] {
			continue
		}
		if len(match) > 0 {
			ok := true
			for _, m := range match {
				if !d.HasLabel(m) {
					ok = false
					break
				}
			}
			if !ok {
				continue
			}
		}
		out = append(out, d)
	}
	return out
}

func formatApplyResults(w io.Writer, results []applyResult, f Format) error {
	// The aggregate failure is format-independent: whatever rendering the
	// user asked for, a host that failed to apply must drive a non-zero exit.
	// The structured summary is still printed first (stdout) so consumers can
	// parse the per-host rows; the error itself only reaches stderr via
	// main(), exactly like the text path.
	anyErr := false
	for _, r := range results {
		if r.Err != nil {
			anyErr = true
		}
	}
	applyErr := func() error {
		if anyErr {
			return fmt.Errorf("apply failed on at least one dugdale")
		}
		return nil
	}

	switch f {
	case FormatJSON, FormatYAML:
		type outRow struct {
			Host  string      `json:"host" yaml:"host"`
			OK    bool        `json:"ok" yaml:"ok"`
			Error string      `json:"error,omitempty" yaml:"error,omitempty"`
			Diff  *apply.Diff `json:"diff,omitempty" yaml:"diff,omitempty"`
		}
		summary := make([]outRow, len(results))
		for i, r := range results {
			summary[i].Host = r.ID
			if r.Err != nil {
				summary[i].Error = r.Err.Error()
			} else {
				summary[i].OK = true
				summary[i].Diff = &r.Res.Diff
			}
		}
		var printErr error
		if f == FormatJSON {
			printErr = PrintJSON(w, summary)
		} else {
			printErr = PrintYAML(w, summary)
		}
		if printErr != nil {
			return printErr
		}
		return applyErr()
	default:
		for _, r := range results {
			if r.Err != nil {
				_, _ = fmt.Fprintf(w, "[FAIL] %s — %v\n", r.ID, r.Err)
				continue
			}
			_, _ = fmt.Fprintf(w, "[OK]   %s — started=%v stopped=%v resized=%v\n",
				r.ID, strings.Join(r.Res.Started, ","), strings.Join(r.Res.Stopped, ","), strings.Join(r.Res.Resized, ","))
		}
		return applyErr()
	}
}

// dryRunRow is the structured per-host record used for JSON/YAML output
// from runApplyDryRun. Mirrors the live-apply summary's outRow shape
// so consumers can parse both with the same code path.
type dryRunRow struct {
	Host  string      `json:"host" yaml:"host"`
	OK    bool        `json:"ok" yaml:"ok"`
	Error string      `json:"error,omitempty" yaml:"error,omitempty"`
	Diff  *ClientDiff `json:"diff,omitempty" yaml:"diff,omitempty"`
}

func runApplyDryRun(ac *appCtx, w io.Writer, hosts, match []string, f Format) error {
	targets := selectApplyTargets(ac.Config, hosts, match)
	if len(targets) == 0 {
		return fmt.Errorf("no dugdales selected (host=%v match=%v)", hosts, match)
	}
	rows := make([]dryRunRow, 0, len(targets))
	anyErr := false
	for _, t := range targets {
		row := dryRunRow{Host: t.ID}
		c, err := ac.ClientForHost(t.ID, lettsconfig.ScopeAdmin)
		if err != nil {
			row.Error = err.Error()
			rows = append(rows, row)
			anyErr = true
			continue
		}
		st, err := GetState(c)
		if err != nil {
			row.Error = "get state: " + err.Error()
			rows = append(rows, row)
			anyErr = true
			continue
		}
		desired := BuildAppliedState(t)
		d := DiffAppliedState(st.State, desired)
		row.OK = true
		row.Diff = &d
		rows = append(rows, row)
	}

	// Output per requested format.
	switch f {
	case FormatJSON:
		if err := PrintJSON(w, rows); err != nil {
			return err
		}
	case FormatYAML:
		if err := PrintYAML(w, rows); err != nil {
			return err
		}
	default:
		for _, row := range rows {
			if row.Error != "" {
				_, _ = fmt.Fprintf(w, "[FAIL] %s — %s\n", row.Host, row.Error)
				continue
			}
			printClientDiff(w, row.Host, *row.Diff)
		}
	}

	// The exit-code contract expects non-zero on host failures
	// regardless of output format.
	if anyErr {
		return fmt.Errorf("dry-run failed on at least one dugdale")
	}
	return nil
}

func printClientDiff(w io.Writer, host string, d ClientDiff) {
	_, _ = fmt.Fprintf(w, "== %s ==\n", host)
	if len(d.AddedLanes) > 0 {
		_, _ = fmt.Fprintf(w, "  add lanes:    %v\n", d.AddedLanes)
	}
	if len(d.RemovedLanes) > 0 {
		_, _ = fmt.Fprintf(w, "  remove lanes: %v\n", d.RemovedLanes)
	}
	for _, c := range d.ChangedLanes {
		_, _ = fmt.Fprintf(w, "  lane %-15s concurrency=%d->%d paused=%t->%t\n",
			c.Name, c.OldConcurrency, c.NewConcurrency, c.OldPaused, c.NewPaused)
	}
	if d.MissionDirChange != nil {
		_, _ = fmt.Fprintf(w, "  mission_dir:  %q -> %q\n", d.MissionDirChange.Old, d.MissionDirChange.New)
	}
	if d.LabelsChange != nil {
		_, _ = fmt.Fprintf(w, "  labels:       %v -> %v\n", d.LabelsChange.Old, d.LabelsChange.New)
	}
	if d.RuntimeChange != nil {
		_, _ = fmt.Fprintf(w, "  runtime:      command_template %v -> %v ; mission_path %q -> %q\n",
			d.RuntimeChange.OldCommandTemplate, d.RuntimeChange.NewCommandTemplate,
			d.RuntimeChange.OldMissionPathTemplate, d.RuntimeChange.NewMissionPathTemplate)
	}
}

func getRootFlagString(cmd *cobra.Command, name string) string {
	v, _ := cmd.Root().PersistentFlags().GetString(name)
	return v
}
