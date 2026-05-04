// Package repair runs at startup before the HTTP listener opens, completing
// any mission/staging state left half-finished by an unclean shutdown.
package repair

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	"letts/internal/config"
	"letts/internal/criticalerr"
	"letts/internal/eventfile"
	"letts/internal/fsutil"
	"letts/internal/ids"
	"letts/internal/mission"
	"letts/internal/storage"
)

// RepairFinalizeIntents processes every row in mission_finalize_intents
// according to the recovery matrix:
//
//   - phase=prepared, outputs=[] → CommitFromIntent (fast path).
//   - phase=prepared, outputs declared, all tmp present → ContinuePhaseB.
//   - phase=prepared, outputs declared, any tmp missing → RevertIntentToFailed
//     (output_commit_failed).
//   - phase=committing → for each output: rename tmp→final if pending; if
//     both missing → RevertIntentToFailed (output_commit_corrupt). Then
//     CommitFromIntent.
//
// One bad intent is logged and skipped so it doesn't block the rest.
func RepairFinalizeIntents(ctx context.Context, cfg *config.DugdaleConfig, db *sql.DB, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}
	intents, err := storage.QueryAllFinalizeIntents(ctx, db)
	if err != nil {
		return err
	}
	for i := range intents {
		in := intents[i]
		if err := repairOne(ctx, cfg, db, &in, logger); err != nil {
			logger.Warn("repair finalize intent failed",
				"mission_id", in.MissionID, "phase", in.Phase, "err", err)
			// Surface the "unrecoverable
			// consistency error" via the sticky readyz flag.
			if errors.Is(err, eventfile.ErrTerminalEventConflict) {
				criticalerr.Trip(criticalerr.Detail{
					Kind:      "terminal_event_conflict",
					MissionID: in.MissionID,
					Op:        "repair.RepairFinalizeIntents",
				})
			}
		}
	}
	return nil
}

func repairOne(ctx context.Context, cfg *config.DugdaleConfig, db *sql.DB, in *storage.FinalizeIntent, logger *slog.Logger) error {
	shard, err := ids.ShardPath(in.MissionID)
	if err != nil {
		return err
	}
	parentDir := filepath.Join(cfg.DataDir, "output", shard)

	ew, err := eventfile.Open(parentDir, in.MissionID)
	if errors.Is(err, os.ErrNotExist) {
		// The durable intent is the authoritative outcome record; a missing
		// events file must not strand the mission — the running→lost sweep
		// deliberately skips intent-carrying rows, so skipping here would
		// leave the row 'running' (and re-warn) on every boot. Recreate the
		// file and proceed: the commit path appends the done event at the
		// intent's done_seq. ensureEventsFile is best-effort; if creation
		// also failed, the re-Open error falls through to the caller's
		// warn-and-skip.
		ensureEventsFile(parentDir, in.MissionID)
		ew, err = eventfile.Open(parentDir, in.MissionID)
	}
	if err != nil {
		return err
	}
	defer func() { _ = ew.Close() }()
	ew.SetLimits(eventfile.Limits{
		MaxEventsBuffer:  cfg.Limits.MaxEventsBuffer,
		MaxEventLineSize: cfg.Limits.MaxEventLineSize,
	})

	finCfg := buildFinalizeCfg(cfg)
	outputs := decodeIntentOutputs(in.Outputs, cfg.DataDir)

	switch in.Phase {
	case storage.PhasePrepared:
		if len(outputs) == 0 {
			logger.Info("repair: commit prepared (fast path)", "mission_id", in.MissionID)
			return mission.CommitFromIntent(ctx, db, ew, in, finCfg)
		}
		// Verify each tmp file is still on disk before continuing Phase B.
		for _, op := range outputs {
			if _, err := os.Stat(op.TmpPath); err != nil {
				logger.Warn("repair: tmp missing → revert to failed",
					"mission_id", in.MissionID, "tmp", op.TmpPath)
				return mission.RevertIntentToFailed(ctx, db, ew, in, finCfg,
					"output_commit_failed", "tmp missing for "+op.Role)
			}
		}
		logger.Info("repair: continue Phase B", "mission_id", in.MissionID, "outputs", len(outputs))
		return mission.ContinuePhaseB(ctx, db, ew, in, finCfg)

	case storage.PhaseCommitting:
		// Each output may have been renamed already in a prior attempt. Catch
		// up the unfinished ones; abort if both copies are gone OR if a
		// pre-renamed final file fails sha256 verification against the
		// intent (per the recovery matrix).
		for _, op := range outputs {
			_, errTmp := os.Stat(op.TmpPath)
			_, errFinal := os.Stat(op.FinalPath)
			tmpExists := errTmp == nil
			finalExists := errFinal == nil
			switch {
			case finalExists:
				// A committing-phase intent for a real output must carry a
				// declared sha. An empty sha means a corrupt/legacy
				// intent we cannot verify — fail closed rather than commit an
				// unverified final (per the recovery matrix).
				if op.Sha256 == "" {
					return mission.RevertIntentToFailed(ctx, db, ew, in, finCfg,
						"output_commit_corrupt", "missing declared sha for "+op.Role)
				}
				// Rename happened in a previous attempt. We must
				// re-verify sha256: a partial write or pre-existing junk
				// at the final path would otherwise commit as success.
				if mismatch, err := verifyFinalSha(op.FinalPath, op.Sha256); err != nil {
					logger.Warn("repair: sha verify failed → revert",
						"mission_id", in.MissionID, "role", op.Role, "err", err)
					return mission.RevertIntentToFailed(ctx, db, ew, in, finCfg,
						"output_commit_corrupt", "sha verify error for "+op.Role+": "+err.Error())
				} else if mismatch {
					logger.Warn("repair: sha mismatch → revert",
						"mission_id", in.MissionID, "role", op.Role, "expected", op.Sha256)
					return mission.RevertIntentToFailed(ctx, db, ew, in, finCfg,
						"output_commit_corrupt", "sha mismatch for "+op.Role)
				}
				// A prior attempt renamed tmp→final but crashed
				// before processing; a leftover *.tmp with a now-complete
				// staging row would never be swept (orphan sweep only removes
				// tmp files whose staging row is gone). Remove it here.
				if tmpExists {
					_ = os.Remove(op.TmpPath)
				}
			case tmpExists:
				if err := os.Rename(op.TmpPath, op.FinalPath); err != nil {
					return mission.RevertIntentToFailed(ctx, db, ew, in, finCfg,
						"output_commit_failed", "rename failed for "+op.Role+": "+err.Error())
				}
				// Make the redo-rename durable, mirroring the normal Phase B
				// path (finalize.go). Without fsync(parent_dir) a power loss
				// right after repair can roll the rename back, leaving a
				// committed staging ref pointing at a missing file.
				if serr := fsutil.SyncDir(filepath.Dir(op.FinalPath)); serr != nil {
					logger.Warn("repair: fsync after redo-rename failed",
						"mission_id", in.MissionID, "role", op.Role, "err", serr)
				}
			default:
				return mission.RevertIntentToFailed(ctx, db, ew, in, finCfg,
					"output_commit_corrupt", "tmp+final missing for "+op.Role)
			}
		}
		logger.Info("repair: commit committing", "mission_id", in.MissionID, "outputs", len(outputs))
		return mission.CommitFromIntent(ctx, db, ew, in, finCfg)

	default:
		return errors.New("unknown phase: " + string(in.Phase))
	}
}

// verifyFinalSha streams the file at path through sha256 and returns
// (mismatch, error). Empty expected → no comparison performed, only
// the read error path is meaningful.
func verifyFinalSha(path, expected string) (bool, error) {
	if expected == "" {
		return false, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return false, err
	}
	got := hex.EncodeToString(h.Sum(nil))
	return got != expected, nil
}

// decodeIntentOutputs resolves the persisted Outputs JSON to in-process
// CollectedOutput values with absolute Tmp/Final paths anchored at
// cfg.DataDir. Legacy rows that stored absolute paths still
// work; the decoder prefers the relative form when both are present.
func decodeIntentOutputs(raw []byte, dataDir string) []mission.CollectedOutput {
	if len(raw) == 0 {
		return nil
	}
	out, err := mission.DecodeIntentOutputsForDataDir(raw, dataDir)
	if err != nil {
		return nil
	}
	return out
}

func buildFinalizeCfg(cfg *config.DugdaleConfig) mission.FinalizeConfig {
	return mission.FinalizeConfig{
		DataDir:        cfg.DataDir,
		MaxReturnValue: cfg.Limits.MaxReturnValueSize,
		MaxFailMessage: cfg.Limits.MaxFailMessageSize,
		MaxFailDetails: cfg.Limits.MaxFailDetailsSize,
		TTL: storage.TTLPolicy{
			MissionSuccess: cfg.Cleanup.SuccessTTL,
			MissionFailed:  cfg.Cleanup.FailedTTL,
			ExecSuccess:    cfg.Exec.ExecSuccessTTL,
			ExecFailed:     cfg.Exec.ExecFailedTTL,
			StagingTTL:     cfg.Cleanup.StagingTTL,
			DownloadGrace:  cfg.Cleanup.DownloadedGrace,
		},
	}
}
