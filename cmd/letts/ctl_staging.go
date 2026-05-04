package main

import (
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"

	"letts/internal/ids"
	"letts/pkg/lettsclient"
	"letts/pkg/lettsconfig"
)

// newCtlStagingCmd is the `letts ctl staging` group. Staging is
// always per-host; there is no fan-out path because staging ids are not
// portable across dugdales.
func newCtlStagingCmd() *cobra.Command {
	c := &cobra.Command{Use: "staging", Short: "Staging artifact control"}
	c.AddCommand(newCtlStagingUploadCmd())
	c.AddCommand(newCtlStagingDownloadCmd())
	c.AddCommand(newCtlStagingDeleteCmd())
	c.AddCommand(newCtlStagingListCmd())
	return c
}

// newCtlStagingUploadCmd binds `letts ctl staging upload <local-path>`. We
// mint a fresh UUIDv7 client-side so the daemon (and the user) can refer to
// the artifact before the body lands.
func newCtlStagingUploadCmd() *cobra.Command {
	var host string
	c := &cobra.Command{
		Use:   "upload <local-path>",
		Short: "Upload a file to staging",
		Args:  exactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if host == "" {
				return NewBadUsageError("--host is required for ctl staging upload")
			}
			ac, _, err := setupAppCtx(cmd)
			if err != nil {
				return err
			}
			defer ac.Close()
			return runCtlStagingUpload(ac, cmd.OutOrStdout(), host, args[0])
		},
	}
	c.Flags().StringVar(&host, "host", "", "dugdale id (required)")
	return c
}

// runCtlStagingUpload mints a UUIDv7, HEADs and PUTs the file via
// lettsclient.UploadFile, and prints "staging_id\tsha256\tsize" so shell
// pipelines can grab any of the three with `cut`.
func runCtlStagingUpload(ac *appCtx, w io.Writer, host, path string) error {
	// PUT/HEAD /v1/staging is dispatch/exec/admin (admin
	// is a superset). Pick the first scope the operator has a token
	// for so dispatch/exec-only users can upload.
	c, err := clientForFirstAvailableScope(ac, host,
		[]lettsconfig.Scope{lettsconfig.ScopeDispatch, lettsconfig.ScopeExec, lettsconfig.ScopeAdmin})
	if err != nil {
		return err
	}
	stagingID := ids.NewUUIDv7()
	id, sha, size, err := lettsclient.UploadFile(c, stagingID, path)
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(w, "%s\t%s\t%d\n", id, sha, size)
	return nil
}

// newCtlStagingDownloadCmd binds `letts ctl staging download <staging_id>`.
// --output="-" or unset streams to stdout; any other value is a destination
// path.
func newCtlStagingDownloadCmd() *cobra.Command {
	var host, outPath string
	c := &cobra.Command{
		Use:   "download <staging_id>",
		Short: "Download a staging artifact",
		Args:  exactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if host == "" {
				return NewBadUsageError("--host is required for ctl staging download")
			}
			ac, _, err := setupAppCtx(cmd)
			if err != nil {
				return err
			}
			defer ac.Close()
			return runCtlStagingDownload(ac, cmd.OutOrStdout(), host, args[0], outPath)
		},
	}
	c.Flags().StringVar(&host, "host", "", "dugdale id (required)")
	c.Flags().StringVar(&outPath, "output", "", "destination path (default/'-': stdout)")
	return c
}

// runCtlStagingDownload GETs /v1/staging/{id} and io.Copy's the body either
// to w (when outPath is empty or "-") or to a freshly created file.
func runCtlStagingDownload(ac *appCtx, w io.Writer, host, id, outPath string) error {
	// GET /v1/staging/{id} is dispatch/exec/admin.
	c, err := clientForFirstAvailableScope(ac, host,
		[]lettsconfig.Scope{lettsconfig.ScopeDispatch, lettsconfig.ScopeExec, lettsconfig.ScopeAdmin})
	if err != nil {
		return err
	}
	rc, _, err := lettsclient.GetStaging(c, id, "")
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()
	if outPath == "" || outPath == "-" {
		_, err := io.Copy(w, rc)
		return err
	}
	f, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	_, err = io.Copy(f, rc)
	return err
}

// newCtlStagingDeleteCmd binds `letts ctl staging delete <staging_id>`.
// --force cascades to missions referencing the artifact (?force=true).
func newCtlStagingDeleteCmd() *cobra.Command {
	var host string
	var force bool
	c := &cobra.Command{
		Use:   "delete <staging_id>",
		Short: "Delete a staging artifact",
		Args:  exactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if host == "" {
				return NewBadUsageError("--host is required for ctl staging delete")
			}
			ac, _, err := setupAppCtx(cmd)
			if err != nil {
				return err
			}
			defer ac.Close()
			return runCtlStagingDelete(ac, host, args[0], force)
		},
	}
	c.Flags().StringVar(&host, "host", "", "dugdale id (required)")
	c.Flags().BoolVar(&force, "force", false, "cascade-delete missions referencing this staging")
	return c
}

// runCtlStagingDelete DELETEs /v1/staging/{id}. Like missions delete the
// daemon's status body isn't rendered — exit code 0 means accepted.
func runCtlStagingDelete(ac *appCtx, host, id string, force bool) error {
	c, err := ac.ClientForHost(host, lettsconfig.ScopeAdmin)
	if err != nil {
		return err
	}
	return lettsclient.DeleteStaging(c, id, force)
}

// newCtlStagingListCmd binds `letts ctl staging list`. The common case is
// just --mission-id, but ref-kind/cursor/limit are accepted because the
// daemon supports them and it keeps the CLI in sync with the HTTP surface.
func newCtlStagingListCmd() *cobra.Command {
	var host, missionID, refKind, cursor string
	var limit int
	c := &cobra.Command{
		Use:   "list",
		Short: "List staging artifacts",
		RunE: func(cmd *cobra.Command, args []string) error {
			if host == "" {
				return NewBadUsageError("--host is required for ctl staging list")
			}
			ac, format, err := setupAppCtx(cmd)
			if err != nil {
				return err
			}
			defer ac.Close()
			opts := lettsclient.ListStagingOpts{
				MissionID: missionID,
				RefKind:   refKind,
				Cursor:    cursor,
				Limit:     limit,
			}
			return runCtlStagingList(ac, cmd.OutOrStdout(), host, opts, format)
		},
	}
	c.Flags().StringVar(&host, "host", "", "dugdale id (required)")
	c.Flags().StringVar(&missionID, "mission-id", "", "filter by mission")
	c.Flags().StringVar(&refKind, "ref-kind", "", "filter by ref kind (input|output|script)")
	c.Flags().IntVar(&limit, "limit", 0, "max rows (default daemon-side)")
	c.Flags().StringVar(&cursor, "cursor", "", "pagination cursor from a previous next_cursor")
	return c
}

// runCtlStagingList GETs /v1/staging and renders. Text mode is a fixed-width
// table sorted by the daemon; cursor footer mirrors the missions list.
func runCtlStagingList(ac *appCtx, w io.Writer, host string, opts lettsclient.ListStagingOpts, f Format) error {
	c, err := ac.ClientForHost(host, lettsconfig.ScopeAdmin)
	if err != nil {
		return err
	}
	resp, err := lettsclient.ListStaging(c, opts)
	if err != nil {
		return err
	}
	switch f {
	case FormatJSON:
		return PrintJSON(w, resp)
	case FormatYAML:
		return PrintYAML(w, resp)
	default:
		_, _ = fmt.Fprintf(w, "%-40s  %-12s  %-12s  %-15s\n", "STAGING_ID", "SIZE", "STATE", "REF_KIND")
		for _, s := range resp.Staging {
			_, _ = fmt.Fprintf(w, "%-40s  %-12d  %-12s  %-15s\n", s.StagingID, s.Size, s.State, s.RefKind)
		}
		if resp.NextCursor != "" {
			_, _ = fmt.Fprintf(w, "\ncursor: %s\n", resp.NextCursor)
		}
		return nil
	}
}
