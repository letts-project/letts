package mission

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"letts/internal/storage"
)

// ResolvedInput is one input ready for materialization.
type ResolvedInput struct {
	Role       string
	SourcePath string
	Sha256     string
	Size       int64
}

// PrepareWorkdir creates work/<missionID>/{in,out,tmp} (cleaning any existing
// directory for this id) and materializes inputs from staging into in/<role>.
// Returns the absolute workdir path.
func PrepareWorkdir(dataDir, missionID string, inputs []ResolvedInput) (string, error) {
	work := filepath.Join(dataDir, "work", missionID)

	// Remove any stale workdir from a previous run.
	if err := os.RemoveAll(work); err != nil {
		return "", fmt.Errorf("remove stale workdir: %w", err)
	}

	// Create directory tree.
	for _, sub := range []string{"", "in", "out", "tmp"} {
		dir := filepath.Join(work, sub)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return "", fmt.Errorf("mkdir %s: %w", sub, err)
		}
	}

	// Materialize inputs.
	for _, in := range inputs {
		dst := filepath.Join(work, "in", in.Role)
		if err := copyFile(in.SourcePath, dst); err != nil {
			return "", fmt.Errorf("materialize %s: %w", in.Role, err)
		}
		if err := os.Chmod(dst, 0o444); err != nil {
			return "", fmt.Errorf("chmod %s: %w", in.Role, err)
		}
	}

	return work, nil
}

// CleanupWorkdir removes the per-mission work directory.
func CleanupWorkdir(dataDir, missionID string) error {
	return os.RemoveAll(filepath.Join(dataDir, "work", missionID))
}

// writeStdinEnvelope writes the mission stdin payload to <workdir>/input.json
// and returns its absolute path.
//
// The wire payload on stdin is exactly the user-input JSON (or `null` when
// input is empty) — NOT an envelope: the JSON input never carries `files`.
// File refs are dugdale-level metadata, not business payload, and the
// mission-side `Mission::input()` returns only the user-provided JSON.
//
// File metadata (path/sha256/size) is delivered via env vars
// LETTS_IN_<role>{,__SHA256,__SIZE}; the mission lib reads those without
// parsing stdin. This function therefore takes `input` only — the `inputs`
// slice (file refs) is consumed earlier by PrepareWorkdir's materialization
// step.
func writeStdinEnvelope(workdir string, input []byte) (string, error) {
	body := input
	if len(body) == 0 {
		body = []byte("null")
	}
	// Validate the body is decodable JSON so a corrupt missions.input
	// blob is caught here rather than at the mission process. Cheap
	// because storage caps the size via max_mission_input_size.
	var probe any
	if err := json.Unmarshal(body, &probe); err != nil {
		return "", fmt.Errorf("invalid input JSON: %w", err)
	}

	// Open, write, and fsync explicitly so the mission can re-read the
	// input after a crash and restart-from-source if any future cleanup
	// flow ever truncates this file eagerly. The DB blob is the source
	// of truth today, so this is hardening rather than required for
	// correctness.
	path := filepath.Join(workdir, "input.json")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o400)
	if err != nil {
		return "", fmt.Errorf("open %s: %w", path, err)
	}
	if _, err := f.Write(body); err != nil {
		_ = f.Close()
		return "", fmt.Errorf("write %s: %w", path, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return "", fmt.Errorf("fsync %s: %w", path, err)
	}
	if err := f.Close(); err != nil {
		return "", fmt.Errorf("close %s: %w", path, err)
	}
	return path, nil
}

// LoadInputs resolves a mission's input refs from storage, returning a
// ResolvedInput slice with absolute source paths.
//
// StagingFile.Path is relative to dataDir (e.g. "staging/ab/c1/23"), so the
// absolute source path is filepath.Join(dataDir, st.Path).
func LoadInputs(ctx context.Context, db storage.DBOrConn, dataDir, missionID string) ([]ResolvedInput, error) {
	refs, err := storage.RefsByMission(ctx, db, missionID)
	if err != nil {
		return nil, err
	}

	var out []ResolvedInput
	for _, r := range refs {
		if r.RefKind != storage.RefInput {
			continue
		}
		st, err := storage.GetStaging(ctx, db, r.StagingID)
		if err != nil {
			return nil, fmt.Errorf("get staging %s: %w", r.StagingID, err)
		}
		out = append(out, ResolvedInput{
			Role:       r.Role,
			SourcePath: filepath.Join(dataDir, st.Path),
			Sha256:     st.Sha256,
			Size:       st.Size,
		})
	}
	return out, nil
}
