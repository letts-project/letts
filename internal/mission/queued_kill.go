package mission

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"letts/internal/eventfile"
	"letts/internal/ids"
	"letts/internal/storage"
)

// ErrMissionNotQueued is returned when the row's status changed between the
// caller's check and the writer transaction's re-check (e.g. the lane runner
// picked it up first).
var ErrMissionNotQueued = errors.New("mission no longer queued")

// KillQueued atomically transitions a queued mission to done(killed) using
// the durable-finalize path: writer lock, re-check status, insert finalize
// intent, append durable terminal `done` event to the events file, UPDATE
// missions, delete intent. Pickup cannot claim the mission mid-flight
// because it shares the same writer lock.
//
// This is the single shared implementation behind both:
//   - POST /v1/missions/{id}/kill  (failReason="killed_by_api")
//   - apply ForcePrune lane removal (failReason="lane_removed")
//
// Both queued-kill callers MUST go through this — a raw UPDATE
// without an events-file append violates the terminal `done` event before
// DB `status='done'` rule and the `lane_removed`/`killed_by_api` fail_reason
// taxonomy with durable journal.
//
// dataDir is the daemon's data_dir; the events file is opened at
// dataDir/output/<shard>/<missionID>-events. Callers normally have this from
// dugdale config.
func KillQueued(ctx context.Context, dataDir string, db *sql.DB, m *storage.Mission, failReason string) error {
	if failReason == "" {
		return errors.New("KillQueued: failReason required")
	}
	shard, err := ids.ShardPath(m.ID)
	if err != nil {
		return fmt.Errorf("shard: %w", err)
	}
	parentDir := filepath.Join(dataDir, "output", shard)
	ew, err := eventfile.Open(parentDir, m.ID)
	if err != nil {
		return fmt.Errorf("open events: %w", err)
	}
	defer func() { _ = ew.Close() }()

	nowMs := time.Now().UnixMilli()
	doneSeq := ew.LastSeq() + 1

	// time_finished (not "time").
	// duration_ms is omitted because the mission never started.
	doneFields := map[string]any{
		"time_finished": nowMs,
		"outcome":       "killed",
		"exit_code":     int64(0),
		"fail_reason":   failReason,
	}
	full := map[string]any{}
	for k, v := range doneFields {
		full[k] = v
	}
	full["seq"] = doneSeq
	full["event"] = string(eventfile.KindDone)
	doneEventJSON, err := json.Marshal(full)
	if err != nil {
		return err
	}

	intent := storage.FinalizeIntent{
		MissionID:     m.ID,
		Phase:         storage.PhasePrepared,
		Outcome:       "killed",
		FailReason:    sql.NullString{String: failReason, Valid: true},
		ExitCode:      sql.NullInt64{Int64: 0, Valid: true},
		Outputs:       []byte("[]"),
		DoneSeq:       doneSeq,
		DoneEvent:     string(doneEventJSON),
		TimeCreatedMs: nowMs,
	}

	// Phase A2: commit the durable intent in its OWN writer tx
	// BEFORE the events-file fsync. The single-tx version fsynced the `done`
	// event inside the same tx — if COMMIT then failed (disk full / IO error),
	// the event was durable on disk while the intent insert and mission UPDATE
	// rolled back, leaving "events says done, DB says queued, no intent": the
	// runner re-ran the mission and tripped a terminal-event conflict. Now the
	// intent survives any later failure, so startup repair completes the kill.
	//
	// The status re-check and intent insert share one writer lock so pickup
	// can't claim the row between them; once the intent exists, PickQueuedForLane
	// skips the row (NOT EXISTS guard) for the window until the final UPDATE.
	if err := storage.WithWriter(ctx, db, func(c *sql.Conn) error {
		var status string
		if err := c.QueryRowContext(ctx,
			`SELECT status FROM missions WHERE mission_id=?`, m.ID).Scan(&status); err != nil {
			return err
		}
		if status != string(storage.StatusQueued) {
			return ErrMissionNotQueued
		}
		// Idempotency vs a concurrent kill of the same queued row: if an intent
		// already exists the kill is already underway — don't double-insert.
		if _, gerr := storage.GetFinalizeIntent(ctx, c, m.ID); gerr == nil {
			return ErrMissionNotQueued
		} else if !errors.Is(gerr, storage.ErrNotFound) {
			return gerr
		}
		return storage.InsertFinalizeIntent(ctx, c, &intent)
	}); err != nil {
		return err
	}

	// Phase B: append the durable terminal `done` event (idempotent; trips the
	// sticky critical-error flag on a terminal-event conflict so /v1/readyz
	// reports 503), then the final UPDATE, intent delete, and metric — all via the
	// shared, proven commitFinalize tail (outputs=[] fast path). No TTL recalc
	// runs because there are no outputs, so an empty FinalizeConfig.TTL is fine.
	return commitFinalize(ctx, db, ew, &intent, nil,
		FinalizeConfig{DataDir: dataDir}, string(m.Kind), m.Lane, 0)
}
