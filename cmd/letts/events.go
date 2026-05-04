package main

import (
	"context"
	"encoding/json"
	"io"

	"github.com/spf13/cobra"

	"letts/pkg/lettsclient"
	"letts/pkg/lettsconfig"
)

// newEventsCmd builds `letts events <mission_id>` — a low-level helper that
// streams /v1/missions/{id}/events to stdout as NDJSON.
//
// --host is required: by-id fan-out across hosts is not implemented for this
// command.
func newEventsCmd() *cobra.Command {
	var host string
	var follow bool
	var from int64
	c := &cobra.Command{
		Use:   "events <mission_id>",
		Short: "Stream mission events as NDJSON",
		Args:  exactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ac, _, err := setupAppCtx(cmd)
			if err != nil {
				return err
			}
			defer ac.Close()
			return runEvents(cmd, ac, args[0], host, follow, from)
		},
	}
	c.Flags().StringVar(&host, "host", "", "dugdale id (required; by-id fan-out not implemented)")
	c.Flags().BoolVar(&follow, "follow", false, "follow live events")
	c.Flags().Int64Var(&from, "from", 0, "starting seq cursor")
	return c
}

// runEvents performs the events stream against a single dugdale and writes
// each event as one NDJSON line to cmd.OutOrStdout(). Split from the cobra
// closure so tests can drive it without parsing flags.
func runEvents(cmd *cobra.Command, ac *appCtx, missionID, host string, follow bool, from int64) error {
	if host == "" {
		return NewBadUsageError("--host is required for events (by-id fan-out not implemented)")
	}
	c, err := ac.ClientForHost(host, lettsconfig.ScopeDispatch)
	if err != nil {
		return err
	}
	return streamEventsToStdout(cmd.Context(), c, missionID, follow, from, cmd.OutOrStdout())
}

// streamEventsToStdout writes each Event from the stream as one NDJSON line.
func streamEventsToStdout(ctx context.Context, c *lettsclient.Client, missionID string, follow bool, from int64, w io.Writer) error {
	return lettsclient.StreamEvents(ctx, c, missionID,
		lettsclient.StreamOpts{Follow: follow, From: from},
		func(ev lettsclient.Event) error {
			return writeRawEventLine(w, ev)
		})
}

// writeRawEventLine writes an event's original on-the-wire bytes verbatim (plus
// a newline), preserving fields the typed struct would drop via omitempty —
// most importantly a progress value of 0.0, which re-encoding would render as a
// missing "value" key (the ndjson format must emit events verbatim). Falls
// back to marshalling the struct only if the raw line wasn't captured.
func writeRawEventLine(w io.Writer, ev lettsclient.Event) error {
	line := ev.Raw
	if len(line) == 0 {
		b, err := json.Marshal(ev)
		if err != nil {
			return err
		}
		line = b
	}
	if _, err := w.Write(line); err != nil {
		return err
	}
	_, err := w.Write([]byte("\n"))
	return err
}
