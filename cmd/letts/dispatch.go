package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"letts/internal/ids"
	"letts/pkg/lettsclient"
	"letts/pkg/lettsconfig"
)

// dispatchFlags holds parsed flags for `letts dispatch`.
type dispatchFlags struct {
	route     string
	host      string
	match     []string
	lane      string
	mission   string
	input     string
	inputFile string
	files     []string // role=path
	timeout   string
	missionID string
}

func newDispatchCmd() *cobra.Command {
	df := &dispatchFlags{}
	c := &cobra.Command{
		Use:   "dispatch",
		Short: "Dispatch a mission (no follow)",
		RunE: func(cmd *cobra.Command, args []string) error {
			ac, format, err := setupAppCtx(cmd)
			if err != nil {
				return err
			}
			defer ac.Close()
			return runDispatch(cmd, ac, df, format)
		},
	}
	c.Flags().StringVar(&df.route, "route", "", "symbolic route name")
	c.Flags().StringVar(&df.host, "host", "", "dugdale id")
	c.Flags().StringSliceVar(&df.match, "match", nil, "label filter for auto-select")
	c.Flags().StringVar(&df.lane, "lane", "", "lane name")
	c.Flags().StringVar(&df.mission, "mission", "", "mission name (required)")
	c.Flags().StringVar(&df.input, "input", "", "input JSON literal")
	c.Flags().StringVar(&df.inputFile, "input-file", "", "input JSON file (use - for stdin)")
	c.Flags().StringSliceVar(&df.files, "file", nil, "input file role=path (repeatable)")
	c.Flags().StringVar(&df.timeout, "timeout", "", "mission timeout, e.g. 5m")
	c.Flags().StringVar(&df.missionID, "mission-id", "", "override mission id (UUID v7)")
	return c
}

func runDispatch(cmd *cobra.Command, ac *appCtx, df *dispatchFlags, f Format) error {
	if df.mission == "" {
		return NewBadUsageError("--mission is required")
	}
	host, lane, err := resolveTarget(ac, df.route, df.host, df.lane, df.match)
	if err != nil {
		return err
	}
	input, err := loadInput(cmd.InOrStdin(), df.input, df.inputFile)
	if err != nil {
		return err
	}

	c, err := ac.ClientForHost(host, lettsconfig.ScopeDispatch)
	if err != nil {
		return err
	}

	files, err := stageFiles(c, df.files)
	if err != nil {
		return err
	}

	missionID := df.missionID
	if missionID == "" {
		missionID = ids.NewUUIDv7()
	}
	req := lettsclient.DispatchRequest{
		MissionID: missionID,
		Mission:   df.mission,
		Lane:      lane,
		Input:     input,
		Files:     files,
		Timeout:   df.timeout,
	}
	resp, err := lettsclient.Dispatch(c, req)
	if err != nil {
		return err
	}
	switch f {
	case FormatJSON:
		return PrintJSON(cmd.OutOrStdout(), resp)
	case FormatYAML:
		return PrintYAML(cmd.OutOrStdout(), resp)
	default:
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "%s\t%s\n", resp.MissionID, resp.Status)
		return nil
	}
}

// resolveTarget picks (host_id, lane) from flags. Precedence:
//  1. --route → ResolveRoute
//  2. --host (+ --lane) → ResolveHost; --match ignored when --host explicit
//  3. --lane alone → auto-select via PickOne (uses --match or selector.match)
func resolveTarget(ac *appCtx, route, host, lane string, match []string) (string, string, error) {
	if route != "" {
		// The target spec is XOR —
		// (--route) | (--host --lane). Accepting "--route X --lane Y"
		// and silently using X's lane would hide the ambiguity;
		// reject so the operator notices.
		if host != "" {
			return "", "", NewBadUsageError("--route and --host are mutually exclusive")
		}
		if lane != "" {
			return "", "", NewBadUsageError("--route and --lane are mutually exclusive (the route carries its lane)")
		}
		return lettsconfig.ResolveRoute(ac.Config, route, ac.Getenv)
	}
	if host != "" {
		h, err := lettsconfig.ResolveHost(ac.Config, host, ac.Getenv)
		if err != nil {
			return "", "", err
		}
		if lane == "" {
			return "", "", NewBadUsageError("--lane required when --host given")
		}
		return h, lane, nil
	}
	if lane == "" {
		return "", "", NewBadUsageError("one of --route, (--host --lane), or --lane (with auto-select) is required")
	}
	useMatch := match
	if len(useMatch) == 0 {
		useMatch = ac.Config.Selector.Match
	}
	d, err := lettsconfig.PickOne(ac.Config, lane, useMatch, nil)
	if err != nil {
		return "", "", err
	}
	return d.ID, lane, nil
}

// loadInput returns canonical JSON bytes for the mission input.
//   - --input and --input-file are mutually exclusive
//   - --input-file=- reads stdin
//   - no flag → empty JSON object {}
func loadInput(stdin io.Reader, literal, file string) (json.RawMessage, error) {
	if literal != "" && file != "" {
		return nil, NewBadUsageError("--input and --input-file are mutually exclusive")
	}
	switch {
	case literal != "":
		if err := validateJSON([]byte(literal)); err != nil {
			return nil, NewBadUsageError("--input is not valid JSON: " + err.Error())
		}
		return json.RawMessage(literal), nil
	case file == "-":
		b, err := io.ReadAll(stdin)
		if err != nil {
			return nil, err
		}
		if err := validateJSON(b); err != nil {
			return nil, NewBadUsageError("stdin is not valid JSON: " + err.Error())
		}
		return json.RawMessage(b), nil
	case file != "":
		b, err := os.ReadFile(file)
		if err != nil {
			return nil, err
		}
		if err := validateJSON(b); err != nil {
			return nil, NewBadUsageError(file + " is not valid JSON: " + err.Error())
		}
		return json.RawMessage(b), nil
	default:
		return json.RawMessage("{}"), nil
	}
}

func validateJSON(b []byte) error {
	var v any
	return json.Unmarshal(b, &v)
}

// stageFiles uploads each role=path pair via HEAD+PUT and returns the
// dispatch files list. Allocates a new staging UUIDv7 per file.
func stageFiles(c *lettsclient.Client, pairs []string) ([]lettsclient.DispatchedFile, error) {
	out := make([]lettsclient.DispatchedFile, 0, len(pairs))
	for _, p := range pairs {
		idx := strings.Index(p, "=")
		if idx < 1 {
			return nil, NewBadUsageError("--file expects role=path, got " + p)
		}
		role, path := p[:idx], p[idx+1:]
		stagingID := ids.NewUUIDv7()
		if _, _, _, err := lettsclient.UploadFile(c, stagingID, path); err != nil {
			return nil, err
		}
		out = append(out, lettsclient.DispatchedFile{Role: role, StagingID: stagingID})
	}
	return out, nil
}
