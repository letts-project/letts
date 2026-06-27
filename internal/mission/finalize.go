package mission

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"letts/internal/criticalerr"
	"letts/internal/eventfile"
	"letts/internal/fsutil"
	"letts/internal/ids"
	"letts/internal/metrics"
	"letts/internal/storage"
)

// FinalizeInputs is what Finalize needs to commit a terminal outcome for one
// mission. Outputs is empty on any non-success path; the caller is expected
// to clear it before calling Finalize for failed/killed/timeout/crashed/oom.
//
// TimeStartedMs is the time the mission process was spawned (per
// missions.time_started column). When > 0 it lets Finalize emit
// `duration_ms` = time_finished - time_started on the done event. When 0
// (e.g. spawn_failed before MarkRunning), duration_ms is omitted.
//
// ProgressDropped is the sum of progress events dropped at every stage of
// the fd3 pipeline (reader channel-full, writer rate-limit/append-error,
// eventfile per-line/per-buffer caps). Surfaced in the done event as
// `progress_dropped: <N>` when > 0 so consumers can detect missed progress
// signals.
type FinalizeInputs struct {
	MissionID string
	// Kind ("mission"|"exec") and Lane label the missions_total and
	// mission_duration_seconds metrics emitted by commitFinalize. Repair
	// callers that don't have these in scope use storage.GetMissionLabels
	// to look them up at intent-replay time.
	Kind            string
	Lane            string
	Outcome         OutcomeResult
	Outputs         []CollectedOutput
	Cfg             FinalizeConfig
	Now             func() time.Time
	TimeStartedMs   int64
	ProgressDropped int64
}

// FinalizeConfig carries config bits used by Finalize to truncate oversize
// fields per size policies and to recalc output staging TTLs to the
// policy ceiling once Phase B commits.
type FinalizeConfig struct {
	DataDir        string
	MaxReturnValue int64
	MaxFailMessage int64
	MaxFailDetails int64
	// TTL drives the post-commit RecalcStagingTTL pass for output staging
	// rows. A zero policy skips the recalc and leaves the 24h sentinel —
	// only acceptable for tests that don't depend on staging retention.
	TTL storage.TTLPolicy
}

// stagingTTL24h is the initial TTL for pending_output rows; a staging ref
// recalc adjusts it to the policy ceiling once the mission row references it.
const stagingTTL24h = 24 * 3600 * 1000

// Finalize executes the two-phase commit (Phase A2 durable intent →
// optional Phase B output rename → public done event → final SQL update).
// On rename failure during Phase B, the intent is durably converted to a
// failed/output_commit_failed outcome and finalized via the fast path.
func Finalize(ctx context.Context, db *sql.DB, in FinalizeInputs) error {
	nowFn := in.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	nowMs := nowFn().UnixMilli()

	shard, err := ids.ShardPath(in.MissionID)
	if err != nil {
		return fmt.Errorf("shard %s: %w", in.MissionID, err)
	}
	parentDir := filepath.Join(in.Cfg.DataDir, "output", shard)

	o := capOutcome(in.Outcome, in.Cfg)

	// If capping demoted a success to a failure (e.g. return_too_large), the
	// mission did not actually succeed — its collected output files must NOT
	// be committed to staging (a non-success outcome never transfers
	// outputs). Drop them and remove the orphan tmp copies so the finalize runs
	// as a plain failed/outputs=[] fast path. Output collection only happens on
	// success, so a non-success outcome with non-empty Outputs can only be this
	// demotion case.
	if o.Outcome != "success" && len(in.Outputs) > 0 {
		for _, op := range in.Outputs {
			_ = os.Remove(op.TmpPath)
		}
		in.Outputs = nil
	}

	ew, err := eventfile.Open(parentDir, in.MissionID)
	if err != nil {
		return fmt.Errorf("open events for finalize: %w", err)
	}
	defer func() { _ = ew.Close() }()
	doneSeq := ew.LastSeq() + 1

	doneFields := buildDoneFields(o, in.Outputs, nowMs, in.TimeStartedMs, in.ProgressDropped)
	doneEventJSON, err := json.Marshal(withSeqAndEvent(doneFields, doneSeq))
	if err != nil {
		return fmt.Errorf("marshal done event: %w", err)
	}
	outputsJSON, err := marshalIntentOutputs(in.Outputs, in.Cfg.DataDir)
	if err != nil {
		return fmt.Errorf("marshal outputs: %w", err)
	}
	if in.Outputs == nil {
		outputsJSON = []byte("[]")
	}

	intent := storage.FinalizeIntent{
		MissionID:     in.MissionID,
		Phase:         storage.PhasePrepared,
		Outcome:       o.Outcome,
		ReturnValue:   nilIfEmpty(o.Return),
		FailReason:    nullStr(o.FailReason),
		FailMessage:   nullStr(o.FailMessage),
		FailDetails:   nullStr(string(o.FailDetails)),
		ExitCode:      sql.NullInt64{Int64: int64(o.ExitCode), Valid: true},
		Signal:        nullStr(o.Signal),
		Outputs:       outputsJSON,
		DoneSeq:       doneSeq,
		DoneEvent:     string(doneEventJSON),
		TimeCreatedMs: nowMs,
	}

	// Phase A2: durable intent and pending_output rows.
	// WithWriterRetry (not WithWriter): a dropped terminal outcome strands the
	// mission in status='running' forever with its process already gone, so a
	// transient lock must be retried, never surfaced as a finalize failure.
	if err := storage.WithWriterRetry(ctx, db, func(c *sql.Conn) error {
		for _, op := range in.Outputs {
			sf := &storage.StagingFile{
				StagingID:     op.StagingID,
				State:         storage.StagingPendingOutput,
				Sha256:        op.Sha256,
				Size:          op.Size,
				BytesReceived: op.Size,
				Path:          relPathFromDataDir(in.Cfg.DataDir, op.TmpPath),
				TimeCreatedMs: nowMs,
				TimeUpdatedMs: nowMs,
				TimeExpiresMs: nowMs + stagingTTL24h,
			}
			if err := storage.InsertStaging(ctx, c, sf); err != nil {
				return err
			}
		}
		return storage.InsertFinalizeIntent(ctx, c, &intent)
	}); err != nil {
		// A2 failed: drop tmp files we created and propagate.
		for _, op := range in.Outputs {
			_ = os.Remove(op.TmpPath)
		}
		return fmt.Errorf("phase A2: %w", err)
	}

	// Fast path: no outputs to commit.
	if len(in.Outputs) == 0 {
		return commitFinalize(ctx, db, ew, &intent, nil, in.Cfg, in.Kind, in.Lane, in.TimeStartedMs)
	}

	// Phase B step 1: transition staging rows and intent phase to committing.
	if err := storage.WithWriterRetry(ctx, db, func(c *sql.Conn) error {
		for _, op := range in.Outputs {
			if _, err := c.ExecContext(ctx,
				`UPDATE staging_files SET state='committing'
				 WHERE staging_id=? AND state='pending_output'`, op.StagingID); err != nil {
				return err
			}
		}
		return storage.UpdateFinalizePhase(ctx, c, in.MissionID, storage.PhaseCommitting)
	}); err != nil {
		return fmt.Errorf("phase B step 1: %w", err)
	}

	// Phase B step 2: rename tmp → final, fsync parent dir per output.
	for _, op := range in.Outputs {
		if err := os.Rename(op.TmpPath, op.FinalPath); err != nil {
			// Pass the FULL outputs slice to revertFailedCommit —
			// every staging row was already moved to 'committing' in
			// Phase B step 1 above. The previous slice [:i+1] left
			// Outputs[i+1:] stuck in 'committing' with orphan tmp
			// files; regular cleanup doesn't transition committing
			// rows.
			return revertFailedCommit(ctx, db, ew, &intent, in.Outputs, nowFn,
				in.Cfg, in.Kind, in.Lane, in.TimeStartedMs,
				"output_commit_failed",
				fmt.Errorf("rename %s: %w", op.TmpPath, err))
		}
		// Critical: this fsync makes the rename(tmp→final) durable.
		// Crashing between rename and fsync can roll the rename back
		// on next mount. Log+count failures so operators see them
		// instead of silently swallowing.
		metrics.ObserveSyncDir(
			fsutil.SyncDir(filepath.Dir(op.FinalPath)),
			nil, "output_commit")
	}

	return commitFinalize(ctx, db, ew, &intent, in.Outputs, in.Cfg, in.Kind, in.Lane, in.TimeStartedMs)
}

// CommitFromIntent re-enters the commit path from a stored intent. It
// unmarshals intent.Outputs, calls commitFinalize. Used by startup repair
// to finish a Phase B step 3-4 (or fast path) sequence after a crash.
func CommitFromIntent(ctx context.Context, db *sql.DB, ew *eventfile.Writer, intent *storage.FinalizeIntent, cfg FinalizeConfig) error {
	kind, lane, ts, err := storage.GetMissionLabels(ctx, db, intent.MissionID)
	if err != nil {
		return fmt.Errorf("metric labels: %w", err)
	}
	outputs := unmarshalIntentOutputs(intent.Outputs, cfg.DataDir)
	return commitFinalize(ctx, db, ew, intent, outputs, cfg, kind, lane, ts)
}

// ContinuePhaseB resumes Phase B from a 'prepared' intent: marks staging rows
// committing, renames tmp→final per output, then commits. If no outputs,
// falls through to the fast-path commit. Used by startup repair after
// verifying tmp files survived the crash.
func ContinuePhaseB(ctx context.Context, db *sql.DB, ew *eventfile.Writer, intent *storage.FinalizeIntent, cfg FinalizeConfig) error {
	kind, lane, ts, err := storage.GetMissionLabels(ctx, db, intent.MissionID)
	if err != nil {
		return fmt.Errorf("metric labels: %w", err)
	}
	outputs := unmarshalIntentOutputs(intent.Outputs, cfg.DataDir)
	if len(outputs) == 0 {
		return commitFinalize(ctx, db, ew, intent, nil, cfg, kind, lane, ts)
	}
	if err := storage.WithWriterRetry(ctx, db, func(c *sql.Conn) error {
		for _, op := range outputs {
			if _, err := c.ExecContext(ctx,
				`UPDATE staging_files SET state='committing'
				 WHERE staging_id=? AND state='pending_output'`, op.StagingID); err != nil {
				return err
			}
		}
		return storage.UpdateFinalizePhase(ctx, c, intent.MissionID, storage.PhaseCommitting)
	}); err != nil {
		return fmt.Errorf("phase B step 1: %w", err)
	}
	for _, op := range outputs {
		if err := os.Rename(op.TmpPath, op.FinalPath); err != nil {
			return revertFailedCommit(ctx, db, ew, intent, outputs, time.Now,
				cfg, kind, lane, ts,
				"output_commit_failed",
				fmt.Errorf("rename %s: %w", op.TmpPath, err))
		}
		metrics.ObserveSyncDir(
			fsutil.SyncDir(filepath.Dir(op.FinalPath)),
			nil, "output_commit")
	}
	return commitFinalize(ctx, db, ew, intent, outputs, cfg, kind, lane, ts)
}

// RevertIntentToFailed converts a stored intent into a durable
// failed outcome, drops staging tmp/final files, and finalizes via the
// fast path. Used by startup repair when intent's outputs are corrupt
// or missing.
//
// reason is stored as fail_message verbatim. failReason is one of the
// fail-reason values — typically "output_commit_failed" (tmp
// missing, rename failed) or "output_commit_corrupt" (sha mismatch on
// pre-renamed final). The taxonomy split lets operators distinguish
// "we never wrote successfully" from "we wrote then disk content
// drifted" in audit logs and metrics.
func RevertIntentToFailed(ctx context.Context, db *sql.DB, ew *eventfile.Writer, intent *storage.FinalizeIntent, cfg FinalizeConfig, failReason, reason string) error {
	kind, lane, ts, err := storage.GetMissionLabels(ctx, db, intent.MissionID)
	if err != nil {
		return fmt.Errorf("metric labels: %w", err)
	}
	outputs := unmarshalIntentOutputs(intent.Outputs, cfg.DataDir)
	return revertFailedCommit(ctx, db, ew, intent, outputs, time.Now, cfg, kind, lane, ts, failReason, errors.New(reason))
}

// unmarshalIntentOutputs decodes the persisted Outputs JSON. A corrupt
// blob trips the readyz critical-error flag so the daemon surfaces a
// manual-repair signal rather than silently treating the intent as
// outputs=[] and finalizing the mission with stale state.
//
// dataDir is used to resolve relative tmp/final paths (the persisted form
// stores paths relative to data_dir so a directory rename between crash and
// restart doesn't strand repair). Legacy rows that wrote absolute paths into
// the JSON are still accepted.
func unmarshalIntentOutputs(raw []byte, dataDir string) []CollectedOutput {
	if len(raw) == 0 {
		return nil
	}
	outputs, err := decodeIntentOutputsForDataDir(raw, dataDir)
	if err != nil {
		criticalerr.Trip(criticalerr.Detail{
			Kind: "corrupt_intent_outputs",
			Op:   "unmarshalIntentOutputs",
		})
		slog.Default().Error("intent outputs JSON corrupt", "err", err, "audit", true)
		return nil
	}
	return outputs
}

// intentOutputJSON is the on-disk shape persisted to
// mission_finalize_intents.outputs. Paths are stored relative to data_dir.
// Legacy absolute tmp_path/final_path columns are still read for backward
// compatibility with rows written before this commit.
type intentOutputJSON struct {
	Role      string `json:"role"`
	StagingID string `json:"staging_id"`
	TmpRel    string `json:"tmp_rel,omitempty"`
	FinalRel  string `json:"final_rel,omitempty"`
	// Legacy fields (predecessor encoding). New writers leave these
	// empty; decoders fall back to them when *Rel is empty.
	TmpPathAbs   string `json:"tmp_path,omitempty"`
	FinalPathAbs string `json:"final_path,omitempty"`
	Sha256       string `json:"sha256"`
	Size         int64  `json:"size"`
}

// MarshalIntentOutputsForTest exposes the relative-path JSON encoder for
// test fixtures that need to seed mission_finalize_intents.outputs in the
// relative-path shape. Production code uses marshalIntentOutputs directly.
func MarshalIntentOutputsForTest(outputs []CollectedOutput, dataDir string) ([]byte, error) {
	return marshalIntentOutputs(outputs, dataDir)
}

func marshalIntentOutputs(outputs []CollectedOutput, dataDir string) ([]byte, error) {
	if len(outputs) == 0 {
		return []byte("[]"), nil
	}
	xs := make([]intentOutputJSON, len(outputs))
	for i, o := range outputs {
		xs[i] = intentOutputJSON{
			Role:      o.Role,
			StagingID: o.StagingID,
			TmpRel:    relPathFromDataDir(dataDir, o.TmpPath),
			FinalRel:  relPathFromDataDir(dataDir, o.FinalPath),
			Sha256:    o.Sha256,
			Size:      o.Size,
		}
	}
	return json.Marshal(xs)
}

// DecodeIntentOutputsForDataDir is the exported sibling of
// unmarshalIntentOutputs (used by package repair). Same legacy/relative
// handling; returns the decode error instead of tripping criticalerr so
// callers can decide.
func DecodeIntentOutputsForDataDir(raw []byte, dataDir string) ([]CollectedOutput, error) {
	return decodeIntentOutputsForDataDir(raw, dataDir)
}

func decodeIntentOutputsForDataDir(raw []byte, dataDir string) ([]CollectedOutput, error) {
	var xs []intentOutputJSON
	if err := json.Unmarshal(raw, &xs); err != nil {
		return nil, err
	}
	out := make([]CollectedOutput, len(xs))
	for i, j := range xs {
		out[i] = CollectedOutput{
			Role:      j.Role,
			StagingID: j.StagingID,
			TmpPath:   resolveIntentPath(dataDir, j.TmpRel, j.TmpPathAbs),
			FinalPath: resolveIntentPath(dataDir, j.FinalRel, j.FinalPathAbs),
			Sha256:    j.Sha256,
			Size:      j.Size,
		}
	}
	return out, nil
}

// resolveIntentPath maps the persisted (rel, legacyAbs) pair to an absolute
// path against dataDir. Preference order: relative > legacy absolute. Empty
// rel and empty abs returns empty (a degenerate intent that callers will
// fail to stat — same as today).
func resolveIntentPath(dataDir, rel, legacyAbs string) string {
	if rel != "" {
		return filepath.Join(dataDir, rel)
	}
	return legacyAbs
}

// commitFinalize executes Phase B steps 3–4 (or the fast path's only steps):
// append the durable done event, then a single SQL transaction marks mission
// done, staging rows complete (with path updated to the post-rename final
// location), refs inserted, intent removed.
//
// On successful commit, emits Prometheus letts_missions_total and
// letts_mission_duration_seconds labelled with kind/lane/outcome. Repair
// paths look up kind/lane via storage.GetMissionLabels before calling this.
//
// cfg.TTL drives the post-commit RecalcStagingTTL pass that replaces the
// 24h Phase-A2 sentinel with the policy ceiling for the just-finished
// mission's outputs. A zero policy skips the recalc.
func commitFinalize(ctx context.Context, db *sql.DB, ew *eventfile.Writer, intent *storage.FinalizeIntent, outputs []CollectedOutput, cfg FinalizeConfig, kind, lane string, timeStartedMs int64) error {
	dataDir := cfg.DataDir
	fields, err := extractDoneFields(intent.DoneEvent)
	if err != nil {
		return fmt.Errorf("decode done event: %w", err)
	}
	if err := ew.AppendDoneIdempotent(fields, intent.DoneSeq); err != nil {
		if errors.Is(err, eventfile.ErrTerminalEventConflict) {
			// An existing terminal `done` event that disagrees with the
			// intent is an unrecoverable consistency error — the public
			// stream may already have been consumed by a client with the
			// OLD outcome. Do NOT overwrite the DB row. The intent stays
			// in mission_finalize_intents so operators can inspect; startup
			// repair will likewise refuse to advance.
			slog.Error("finalize: terminal event conflict; blocking DB update",
				"mission_id", intent.MissionID,
				"intent_outcome", intent.Outcome,
				"audit", true,
			)
			// Trip the sticky critical-error flag so /v1/readyz reports
			// 503 until operator resolves.
			criticalerr.Trip(criticalerr.Detail{
				Kind:      "terminal_event_conflict",
				MissionID: intent.MissionID,
				Op:        "commitFinalize",
			})
			return fmt.Errorf("append done: %w", err)
		}
		return fmt.Errorf("append done: %w", err)
	}
	// Keep DB time_finished in lockstep with the done event's
	// time_finished. The done event was constructed at Finalize entry
	// using `nowMs := nowFn().UnixMilli()`; pulling that same value out
	// of intent.DoneEvent here means /v1/missions/{id} duration_ms ==
	// /events done.duration_ms exactly, instead of drifting by the
	// length of the success-with-outputs Phase B sequence (intent
	// insert, staging UPDATE, rename loop, fsync, done append).
	finishedMs := doneEventTimeFinished(fields)
	if finishedMs <= 0 {
		// Defensive fallback for malformed/older intents — better to
		// over-report duration than to write 0.
		finishedMs = time.Now().UnixMilli()
	}
	var missionGone bool
	// WithWriterRetry: this is the single UPDATE that moves the row out of
	// 'running' to 'done'. A transient lock here is exactly what stranded
	// missions in the 2026-06-27 incident, so it must retry rather than fail.
	if err := storage.WithWriterRetry(ctx, db, func(c *sql.Conn) error {
		// Status guard: only 'running' (the normal in-flight state) and
		// 'queued' (the queued-kill path commits terminal outcomes for rows
		// that never started) may transition to 'done'. An unguarded UPDATE
		// would resurrect a row an admin flipped to 'deleting' mid-finalize
		// (the staging force-delete cascade does this to running missions) —
		// un-doing a deletion that was already acknowledged with 202
		// deletion_pending — or no-op against a hard-deleted row while the
		// staging completions below still committed.
		res, err := c.ExecContext(ctx, `UPDATE missions SET status='done', outcome=?,
			fail_reason=?, fail_message=?, fail_details=?, exit_code=?, signal=?,
			return_value=?, time_finished=? WHERE mission_id=? AND status IN ('queued','running')`,
			intent.Outcome,
			intent.FailReason,
			intent.FailMessage,
			intent.FailDetails,
			intent.ExitCode,
			intent.Signal,
			intent.ReturnValue,
			finishedMs,
			intent.MissionID,
		)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			// The row is 'deleting' or already gone: the outcome has nowhere
			// to land, so its outputs are discarded. Flip THIS finalize's
			// staging rows from their Phase-B state straight to 'deleting',
			// pointing path at the post-rename final file so the staging GC
			// tombstones the file that actually exists. This is a direct
			// state-transition UPDATE on purpose: MarkStagingDeleting refuses
			// pending_output/committing rows to protect Phase B from OTHER
			// actors, and commitFinalize IS Phase B. Refs are skipped — a
			// ref to a gone row would violate the FK, and one to a deleting
			// row would only be cascade-dropped by cleanup moments later.
			// The intent delete is a no-op when the row's cascade already
			// removed it. The done event appended above needs no rollback:
			// the events file is removed with the mission's cleanup.
			missionGone = true
			for _, op := range outputs {
				finalRel := relPathFromDataDir(dataDir, op.FinalPath)
				if _, err := c.ExecContext(ctx,
					`UPDATE staging_files SET state='deleting', path=?
					 WHERE staging_id=? AND state IN ('pending_output','committing')`,
					finalRel, op.StagingID); err != nil {
					return err
				}
			}
			return storage.DeleteFinalizeIntent(ctx, c, intent.MissionID)
		}
		for _, op := range outputs {
			finalRel := relPathFromDataDir(dataDir, op.FinalPath)
			if err := storage.MarkStagingCompleteWithPath(ctx, c, op.StagingID, finalRel); err != nil {
				return err
			}
			if err := storage.InsertRef(ctx, c, storage.StagingRef{
				MissionID: intent.MissionID,
				StagingID: op.StagingID,
				RefKind:   storage.RefOutput,
				Role:      op.Role,
			}); err != nil {
				return err
			}
		}
		return storage.DeleteFinalizeIntent(ctx, c, intent.MissionID)
	}); err != nil {
		return err
	}
	if missionGone {
		// Return success: the finalize protocol completed (durable done
		// event, intent cleared, outputs handed to the GC); the outcome just
		// had no row to land on. Callers must not retry or trip the
		// critical-error state over an administrative deletion.
		slog.Default().Warn("finalize: mission deleted while finalizing; outcome discarded, outputs marked deleting",
			"mission_id", intent.MissionID, "outcome", intent.Outcome, "outputs", len(outputs))
		return nil
	}
	// Replace the 24h Phase-A2 sentinel with the policy-ceiling TTL for
	// each just-committed output. Without this the staging row expires
	// before the mission row when success_ttl > 24h. A zero policy means
	// the caller is a test that doesn't care about retention.
	//
	// The SELECT-compute-UPDATE must live in a writer tx so a concurrent
	// cleanup's recalc on the same staging_id can't overwrite ours.
	if len(outputs) > 0 && cfg.TTL != (storage.TTLPolicy{}) {
		for _, op := range outputs {
			err := storage.WithWriter(ctx, db, func(c *sql.Conn) error {
				_, e := storage.RecalcStagingTTL(ctx, c, op.StagingID, cfg.TTL, finishedMs)
				return e
			})
			if err != nil {
				slog.Default().Warn("recalc output staging ttl",
					"mission_id", intent.MissionID, "staging_id", op.StagingID, "err", err)
			}
		}
	}
	var duration time.Duration
	if timeStartedMs > 0 {
		// Same finishedMs used for DB and metric — DB and metric agree.
		duration = time.Duration(finishedMs-timeStartedMs) * time.Millisecond
	}
	metrics.ObserveMissionDone(kind, lane, intent.Outcome, duration)
	// Structured INFO line so log-only observers see every terminal
	// mission outcome (without polling /v1/events).
	slog.Default().Info("mission", "phase", "finished",
		"mission_id", intent.MissionID, "kind", kind, "lane", lane,
		"outcome", intent.Outcome, "duration_ms", duration.Milliseconds())
	return nil
}

// revertFailedCommit converts a partially-committed intent into a durable
// failed outcome and finalizes via the fast path. The done_event in the
// intent is rewritten so startup repair sees a consistent failed state
// if dugdale crashes mid-revert.
//
// failReason is one of the fail-reason values, normally
// "output_commit_failed" (tmp missing, rename failed) or
// "output_commit_corrupt" (sha mismatch on pre-renamed final).
//
// kind/lane/timeStartedMs are forwarded to commitFinalize so the resulting
// missions_total/duration metrics carry correct labels for the revert path.
func revertFailedCommit(ctx context.Context, db *sql.DB, ew *eventfile.Writer, intent *storage.FinalizeIntent, outputs []CollectedOutput, nowFn func() time.Time, cfg FinalizeConfig, kind, lane string, timeStartedMs int64, failReason string, cause error) error {
	for _, op := range outputs {
		_ = os.Remove(op.TmpPath)
		_ = os.Remove(op.FinalPath)
	}

	if failReason == "" {
		failReason = "output_commit_failed"
	}
	failed := OutcomeResult{
		Outcome:     "failed",
		FailReason:  failReason,
		FailMessage: cause.Error(),
		ExitCode:    int(intent.ExitCode.Int64),
		Signal:      intent.Signal.String,
		DropReturn:  true,
	}
	nowMs := nowFn().UnixMilli()
	newSeq := ew.LastSeq() + 1
	// revertFailedCommit doesn't know the original time_started; omit
	// duration_ms by passing 0. The original done event in the intent had
	// it (if applicable), but a partial-commit revert produces a new
	// terminal event whose timing semantics are about the revert moment,
	// not the mission's run duration.
	// progress_dropped intentionally not propagated to the revert event:
	// the failed-commit path is not a normal mission outcome and the
	// original count belonged to the prior intent, which is being
	// superseded by output_commit_failed.
	newDoneFields := buildDoneFields(failed, nil, nowMs, 0, 0)
	newDoneJSON, err := json.Marshal(withSeqAndEvent(newDoneFields, newSeq))
	if err != nil {
		return errors.Join(cause, err)
	}

	if err := storage.WithWriterRetry(ctx, db, func(c *sql.Conn) error {
		if _, err := c.ExecContext(ctx, `UPDATE mission_finalize_intents
			SET phase='prepared', outcome='failed', fail_reason=?,
			    fail_message=?, fail_details=NULL, return_value=NULL,
			    outputs='[]', done_seq=?, done_event=?
			WHERE mission_id=?`,
			failReason, failed.FailMessage, newSeq, string(newDoneJSON), intent.MissionID,
		); err != nil {
			return err
		}
		for _, op := range outputs {
			if _, err := c.ExecContext(ctx,
				`UPDATE staging_files SET state='deleting' WHERE staging_id=?`, op.StagingID); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return errors.Join(cause, err)
	}

	intent.Phase = storage.PhasePrepared
	intent.Outcome = "failed"
	intent.FailReason = nullStr(failReason)
	intent.FailMessage = nullStr(failed.FailMessage)
	intent.FailDetails = sql.NullString{}
	intent.ReturnValue = nil
	intent.Outputs = []byte("[]")
	intent.DoneSeq = newSeq
	intent.DoneEvent = string(newDoneJSON)

	return commitFinalize(ctx, db, ew, intent, nil, cfg, kind, lane, timeStartedMs)
}

// BuildDoneFields exposes the canonical done-event payload builder to the
// repair package, whose terminal-event consistency pass reconstructs done
// events from mission rows and output refs. A single builder guarantees a
// reconstructed event is shaped identically to one produced by Finalize.
func BuildDoneFields(o OutcomeResult, outputs []CollectedOutput, nowMs, timeStartedMs, progressDropped int64) map[string]any {
	return buildDoneFields(o, outputs, nowMs, timeStartedMs, progressDropped)
}

// buildDoneFields assembles the public done-event payload (without seq/event
// keys, which Append populates). Shape:
//   - time_finished (not "time")
//   - duration_ms = time_finished - time_started, omitted when timeStartedMs == 0
//   - outputs keyed by role with {staging_id, sha256, size}
//   - progress_dropped (when > 0)
func buildDoneFields(o OutcomeResult, outputs []CollectedOutput, nowMs, timeStartedMs, progressDropped int64) map[string]any {
	f := map[string]any{
		"time_finished": nowMs,
		"outcome":       o.Outcome,
		"exit_code":     o.ExitCode,
	}
	if timeStartedMs > 0 {
		f["duration_ms"] = nowMs - timeStartedMs
	}
	if o.Signal != "" {
		f["signal"] = o.Signal
	}
	if !o.DropReturn && len(o.Return) > 0 {
		f["return"] = json.RawMessage(o.Return)
	}
	if o.FailReason != "" {
		f["fail_reason"] = o.FailReason
	}
	if o.FailMessage != "" {
		f["fail_message"] = o.FailMessage
	}
	if len(o.FailDetails) > 0 {
		f["fail_details"] = json.RawMessage(o.FailDetails)
	}
	if len(outputs) > 0 {
		f["outputs"] = summarizeOutputs(outputs)
	}
	if progressDropped > 0 {
		f["progress_dropped"] = progressDropped
	}
	return f
}

func summarizeOutputs(outs []CollectedOutput) map[string]map[string]any {
	result := make(map[string]map[string]any, len(outs))
	for _, op := range outs {
		result[op.Role] = map[string]any{
			"staging_id": op.StagingID,
			"sha256":     op.Sha256,
			"size":       op.Size,
		}
	}
	return result
}

// withSeqAndEvent returns a copy of fields with seq and event keys populated.
// Used only when serialising the intent's done_event for storage; the live
// append via Writer.Append re-applies these keys.
func withSeqAndEvent(fields map[string]any, seq int64) map[string]any {
	out := make(map[string]any, len(fields)+2)
	for k, v := range fields {
		out[k] = v
	}
	out["seq"] = seq
	out["event"] = string(eventfile.KindDone)
	return out
}

// extractDoneFields decodes a stored done_event JSON back into a fields map
// suitable for AppendDoneIdempotent. Strips seq/event keys (the Writer sets
// them).
func extractDoneFields(stored string) (map[string]any, error) {
	if stored == "" {
		return map[string]any{}, nil
	}
	var raw map[string]any
	if err := json.Unmarshal([]byte(stored), &raw); err != nil {
		return nil, err
	}
	delete(raw, "seq")
	delete(raw, "event")
	return raw, nil
}

// doneEventTimeFinished pulls the time_finished field out of the parsed
// done-event map. Returns 0 when the field is missing/malformed —
// callers should fall back to time.Now() in that case.
func doneEventTimeFinished(fields map[string]any) int64 {
	if fields == nil {
		return 0
	}
	switch v := fields["time_finished"].(type) {
	case float64:
		return int64(v)
	case int64:
		return v
	case int:
		return int64(v)
	default:
		return 0
	}
}

// capOutcome enforces size truncation rules on return/fail_message/
// fail_details. Oversize return drops the return and switches outcome to
// failed/return_too_large. Oversize fail_message truncates with a suffix.
// Oversize fail_details replaces with the truncated marker.
func capOutcome(o OutcomeResult, cfg FinalizeConfig) OutcomeResult {
	out := o

	if cfg.MaxReturnValue > 0 && int64(len(out.Return)) > cfg.MaxReturnValue {
		out.Outcome = "failed"
		out.FailReason = "return_too_large"
		out.Return = nil
		out.DropReturn = true
	}
	if out.DropReturn {
		out.Return = nil
	}

	if cfg.MaxFailMessage > 0 && int64(len(out.FailMessage)) > cfg.MaxFailMessage {
		const suffix = "…[truncated]"
		head := cfg.MaxFailMessage - int64(len(suffix))
		if head < 0 {
			head = 0
		}
		out.FailMessage = out.FailMessage[:head] + suffix
	}

	if cfg.MaxFailDetails > 0 && int64(len(out.FailDetails)) > cfg.MaxFailDetails {
		out.FailDetails = json.RawMessage(`{"truncated":true,"reason":"fail_details_too_large"}`)
	}

	return out
}

func nullStr(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}

func nilIfEmpty(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return b
}

// relPathFromDataDir trims the data_dir prefix from an absolute path so it
// can be persisted as a relative storage path.
func relPathFromDataDir(dataDir, abs string) string {
	rel, err := filepath.Rel(dataDir, abs)
	if err != nil || strings.HasPrefix(rel, "..") {
		return abs
	}
	return rel
}
