package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"

	"letts/internal/ids"
	"letts/pkg/lettsclient"
	"letts/pkg/lettsconfig"
)

// uploadOrReuse computes sha256+size of localPath and looks it up on the
// dugdale behind c. Returns the staging_id to reference in the exec request.
// On miss, mints a new UUIDv7 and uploads via lettsclient.UploadFile.
func uploadOrReuse(c *lettsclient.Client, localPath string) (string, error) {
	sha, size, err := sha256AndSize(localPath)
	if err != nil {
		return "", err
	}
	if id, ok, err := lettsclient.StagingByContent(c, sha, size); err != nil {
		return "", err
	} else if ok {
		return id, nil
	}
	id := ids.NewUUIDv7()
	if _, _, _, err := lettsclient.UploadFile(c, id, localPath); err != nil {
		return "", err
	}
	return id, nil
}

// uploadOrReuseBytes is the in-memory equivalent for stdin payloads.
func uploadOrReuseBytes(c *lettsclient.Client, data []byte) (string, error) {
	h := sha256.Sum256(data)
	sha := hex.EncodeToString(h[:])
	size := int64(len(data))
	if id, ok, err := lettsclient.StagingByContent(c, sha, size); err != nil {
		return "", err
	} else if ok {
		return id, nil
	}
	id := ids.NewUUIDv7()
	if err := lettsclient.PutStagingInitial(c, id, sha, size, bytes.NewReader(data)); err != nil {
		return "", err
	}
	return id, nil
}

// sha256AndSize streams localPath through sha256, returning the hex digest
// and total byte count without ever loading the file into memory.
func sha256AndSize(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

// suffixBeforeExt inserts suffix between the filename stem and extension.
// Used by the multi-host fan-out download coordinator to disambiguate
// per-host outputs without collisions. Single-host path uses the caller's
// --out path verbatim.
//
//	suffixBeforeExt("/p/result.png", "-s1") == "/p/result-s1.png"
//	suffixBeforeExt("/p/out",        "-s1") == "/p/out-s1"
//	suffixBeforeExt("/p/x.tar.gz",   "-s1") == "/p/x.tar-s1.gz"
//
// Only the *last* extension is preserved (filepath.Ext semantics); double
// extensions like .tar.gz get split before the trailing .gz.
func suffixBeforeExt(path, suffix string) string {
	ext := filepath.Ext(path)
	base := strings.TrimSuffix(path, ext)
	return base + suffix + ext
}

// fanOutDownloadPlan is one entry per (host, key) the coordinator must
// download. Built from successful per-host results × --out role pairs.
// TmpPath is filled after the download phase creates the sidecar tmp;
// FinalPath is set up-front from suffixBeforeExt(p.Path, "-"+host).
type fanOutDownloadPlan struct {
	Host      string
	Key       string
	StagingID string
	FinalPath string
	TmpPath   string
}

// atomicDownload is one entry in a downloadAllAtomic batch. Client is
// the per-host lettsclient (in single-host mode this is the same client
// for every entry); StagingID is the source on the daemon; FinalPath
// is the local destination after the sidecar tmp is promoted.
type atomicDownload struct {
	Client    *lettsclient.Client
	StagingID string
	FinalPath string
}

// downloadAllAtomic implements the all-or-none coordinator used by both
// the single-host and multi-host (downloadFanOutOutputs) exec --out
// flows. Three phases:
//
//  1. Pre-check no FinalPath already exists (BadUsageError on collision,
//     no bytes touched).
//  2. Download every entry to a sidecar tmp under FinalPath's dir. On
//     any error roll back: remove every tmp written so far.
//  3. Promote each tmp via os.Rename in order. On any failure remove
//     already-promoted finals AND clean remaining tmps.
//
// Either every FinalPath is created with the daemon's bytes, or none are.
func downloadAllAtomic(downloads []atomicDownload) error {
	if len(downloads) == 0 {
		return nil
	}
	// Phase 1: pre-check no existing finals. Strongest guarantee against
	// half-written state after coordinator failure.
	for _, d := range downloads {
		if _, err := os.Stat(d.FinalPath); err == nil {
			return NewBadUsageError("output_exists: " + d.FinalPath)
		}
	}

	// Phase 2: download all to sidecar tmps. cleanup() removes any tmp
	// already written if we bail out — no partial bytes on disk after
	// an error return.
	tmps := make([]string, len(downloads))
	cleanup := func() {
		for _, t := range tmps {
			if t != "" {
				_ = os.Remove(t)
			}
		}
	}
	for i, d := range downloads {
		dir := filepath.Dir(d.FinalPath)
		tmp, err := os.CreateTemp(dir, filepath.Base(d.FinalPath)+".tmp.*")
		if err != nil {
			cleanup()
			return err
		}
		tmps[i] = tmp.Name()

		rc, _, err := lettsclient.GetStaging(d.Client, d.StagingID, "")
		if err != nil {
			_ = tmp.Close()
			cleanup()
			return err
		}
		if _, err := io.Copy(tmp, rc); err != nil {
			_ = tmp.Close()
			_ = rc.Close()
			cleanup()
			return err
		}
		_ = rc.Close()
		if err := tmp.Sync(); err != nil {
			_ = tmp.Close()
			cleanup()
			return err
		}
		if err := tmp.Close(); err != nil {
			cleanup()
			return err
		}
	}

	// Phase 3: promote (re-check and rename). On any failure, undo
	// previously promoted files AND clean remaining tmps. The re-check
	// narrows the TOCTOU window between download and rename.
	promoted := make([]string, 0, len(downloads))
	for i, d := range downloads {
		if _, err := os.Stat(d.FinalPath); err == nil {
			for _, p := range promoted {
				_ = os.Remove(p)
			}
			cleanup()
			return NewBadUsageError("output_exists: " + d.FinalPath)
		}
		if err := os.Rename(tmps[i], d.FinalPath); err != nil {
			for _, p := range promoted {
				_ = os.Remove(p)
			}
			cleanup()
			return err
		}
		promoted = append(promoted, d.FinalPath)
		tmps[i] = "" // promoted — don't try to cleanup
	}
	return nil
}

// downloadFanOutOutputs implements the multi-host all-or-none coordinator.
// Pre-checks no existing finals; downloads all to per-host sidecar tmp
// files; only after every download succeeds does it promote them via
// os.Rename. On any failure: cleans up tmps and rolls back already-promoted
// files. Returns nil on full success.
//
// Skips hosts with HasErr or non-success done (no outputs to download).
// Clients are looked up via ac.ClientForHost lazily — the same cached
// instances the dispatch goroutines used, so no extra wire calls.
//
// Naming: per-host outputs get suffixBeforeExt(p.Path, "-"+host), e.g.
// "r.png" with host "s1" → "r-s1.png" — disambiguation against the same key
// on sibling hosts.
func downloadFanOutOutputs(ac *appCtx, results []execFanOutResult, outFlags []string) error {
	if len(outFlags) == 0 {
		return nil
	}
	pairs, err := parseExecKV(outFlags, "--out")
	if err != nil {
		return err
	}

	// Build plan and pre-check no existing finals. A pre-check failure here
	// returns before any download happens — strongest guarantee against
	// partial state on disk after coordinator failure.
	plans := make([]fanOutDownloadPlan, 0)
	for _, r := range results {
		if r.HasErr || r.DoneEv == nil || r.DoneEv.Outcome != "success" {
			continue
		}
		for _, p := range pairs {
			eo, ok := r.DoneEv.Outputs[p.Key]
			if !ok {
				return NewBadUsageError("server promised success but missing output " + p.Key + " for " + r.Host)
			}
			final := suffixBeforeExt(p.Path, "-"+r.Host)
			if _, err := os.Stat(final); err == nil {
				return NewBadUsageError("output_exists: " + final)
			}
			plans = append(plans, fanOutDownloadPlan{
				Host:      r.Host,
				Key:       p.Key,
				StagingID: eo.StagingID,
				FinalPath: final,
			})
		}
	}

	// First pass: download all to tmp sidecars. cleanup() removes any tmp
	// already written when we bail out — no partial bytes on disk after
	// an error return.
	tmpWritten := make([]string, 0, len(plans))
	cleanup := func() {
		for _, t := range tmpWritten {
			_ = os.Remove(t)
		}
	}
	for i, pl := range plans {
		c, err := ac.ClientForHost(pl.Host, lettsconfig.ScopeExec)
		if err != nil {
			cleanup()
			return err
		}
		dir := filepath.Dir(pl.FinalPath)
		tmp, err := os.CreateTemp(dir, filepath.Base(pl.FinalPath)+".tmp.*")
		if err != nil {
			cleanup()
			return err
		}
		plans[i].TmpPath = tmp.Name()
		tmpWritten = append(tmpWritten, tmp.Name())

		rc, _, err := lettsclient.GetStaging(c, pl.StagingID, "")
		if err != nil {
			_ = tmp.Close()
			cleanup()
			return err
		}
		if _, err := io.Copy(tmp, rc); err != nil {
			_ = tmp.Close()
			_ = rc.Close()
			cleanup()
			return err
		}
		_ = rc.Close()
		if err := tmp.Sync(); err != nil {
			_ = tmp.Close()
			cleanup()
			return err
		}
		if err := tmp.Close(); err != nil {
			cleanup()
			return err
		}
	}

	// Second pass: promote (re-check and rename). On any failure, undo
	// previously promoted files AND clean remaining tmps. The re-check
	// narrows the TOCTOU window between download and rename.
	promoted := make([]string, 0, len(plans))
	for _, pl := range plans {
		if _, err := os.Stat(pl.FinalPath); err == nil {
			for _, p := range promoted {
				_ = os.Remove(p)
			}
			cleanup()
			return NewBadUsageError("output_exists: " + pl.FinalPath)
		}
		if err := os.Rename(pl.TmpPath, pl.FinalPath); err != nil {
			for _, p := range promoted {
				_ = os.Remove(p)
			}
			cleanup()
			return err
		}
		promoted = append(promoted, pl.FinalPath)
	}
	return nil
}
