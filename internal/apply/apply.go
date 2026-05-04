// Package apply implements declarative config apply.
package apply

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"letts/internal/lane"
	"letts/internal/mission"
	"letts/internal/storage"
)

// AppliedState is the desired system configuration stored in the config table.
type AppliedState struct {
	MissionDir string             `json:"mission_dir"`
	Labels     []string           `json:"labels"`
	Lanes      map[string]LaneCfg `json:"lanes"`
	Runtime    Runtime            `json:"runtime"`
}

// LaneCfg is the per-lane configuration block.
//
// PausedBy carries the provenance of a Paused=true state: "yaml"
// when the most recent pause came from `letts apply` and "ctl" when it came
// from `letts ctl lanes pause`. Empty when Paused is false (no provenance to
// track when not paused) or when the persisted row predates this field (a
// legacy Paused=true with empty PausedBy is treated as ctl-origin so the
// existing preservation behaviour holds for already-running installs).
//
// Apply derives PausedBy server-side — never trust a value provided by the
// client over HTTP. See PauseLaneOrigin / pauseProvenance for the
// derivation rules.
type LaneCfg struct {
	Concurrency int    `json:"concurrency"`
	Paused      bool   `json:"paused,omitempty"`
	PausedBy    string `json:"paused_by,omitempty"`
}

// Pause provenance markers stored in LaneCfg.PausedBy.
const (
	PausedByYAML = "yaml"
	PausedByCtl  = "ctl"
)

// Runtime holds execution configuration.
type Runtime struct {
	MissionPathTemplate string   `json:"mission_path_template,omitempty"`
	CommandTemplate     []string `json:"command_template,omitempty"`
	ValidateMissionFile bool     `json:"validate_mission_file"`
}

// Options controls apply behaviour.
//
// Pruning model:
//   - Default (Prune=false): lanes present in current but absent from
//     desired are PRESERVED (merged forward). Operators applying a
//     partial overlay don't lose lanes they didn't touch.
//   - Prune=true:               lanes absent from desired are REMOVED.
//     If any of those lanes have queued/running missions, the apply
//     fails with ConflictError unless ForcePrune is also set.
//   - Prune=true with ForcePrune=true: queued missions are marked
//     done(killed, lane_removed) and running missions are signalled
//     with KillLaneRemoved; the lane runner is then stopped.
type Options struct {
	Force      bool
	Prune      bool
	ForcePrune bool
	Source     string
	// DataDir is the daemon's data_dir. Required by ForcePrune to open
	// each queued mission's events file and append the durable terminal
	// `done(killed,lane_removed)` event through the finalize-intent
	// journal (queued-kill path). Optional when no
	// queued missions need terminating.
	DataDir string
	// Killer is consulted by ForcePrune to signal running missions in
	// removed lanes. Optional — when nil, queued missions are still
	// marked done(killed) but running ones are not signalled (they
	// continue to natural completion).
	Killer Killer
}

// Killer is the subset of runtime.Runtime that ForcePrune needs to signal
// running missions in lanes being removed. Defined as an interface so
// callers can stub for tests. Production wiring uses *runtime.Runtime,
// which already satisfies this signature.
type Killer interface {
	SignalKill(missionID string, reason mission.ExternalKillReason) bool
}

// Result summarises what the apply changed.
type Result struct {
	Diff    Diff     `json:"diff"`
	Started []string `json:"started"`
	Stopped []string `json:"stopped"`
	Resized []string `json:"resized"`
}

// ErrConflict is returned when applying would disrupt active missions without
// the appropriate force flags.
var ErrConflict = errors.New("apply conflict")

// ConflictDetail contains human-readable details about the conflict.
type ConflictDetail struct {
	Reason      string   `json:"reason"`
	BlockedLane []string `json:"blocked_lanes,omitempty"`
}

// ConflictError wraps ErrConflict with detail.
type ConflictError struct {
	Detail ConflictDetail
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("apply conflict: %s blocked_lanes=%v", e.Detail.Reason, e.Detail.BlockedLane)
}

func (e *ConflictError) Unwrap() error { return ErrConflict }

// Apply applies desired state, reconciles the Manager, and stores config.
// It returns ErrConflict (wrapped as *ConflictError) when force flags are
// insufficient.
func Apply(ctx context.Context, db *sql.DB, mgr *lane.Manager, desired AppliedState, opts Options) (*Result, error) {
	// 1. Read current state.
	var current AppliedState
	existing, err := storage.GetAppliedConfig(ctx, db)
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return nil, fmt.Errorf("read config: %w", err)
	}
	if existing != nil {
		if err := json.Unmarshal(existing.Data, &current); err != nil {
			return nil, fmt.Errorf("unmarshal current config: %w", err)
		}
	}

	// 1a. Without --prune: preserve lanes that exist in current but
	// were omitted from desired. Operators applying a partial overlay don't
	// lose lanes they didn't touch. With --prune, missing lanes flow into
	// diff.LanesRemoved and (subject to ForcePrune) get reaped.
	//
	// Clone the caller's Lanes map before merging so we don't surprise the
	// caller (and test fixtures) by retroactively populating their input.
	if !opts.Prune {
		merged := make(map[string]LaneCfg, len(desired.Lanes)+len(current.Lanes))
		for name, cfg := range desired.Lanes {
			merged[name] = cfg
		}
		for name, cfg := range current.Lanes {
			if _, present := merged[name]; !present {
				merged[name] = cfg
			}
		}
		desired.Lanes = merged
	}

	// 1b. Reconcile pause state with provenance.
	//
	// Provenance answers "who paused this lane": "yaml" (an earlier apply
	// set paused:true) or "ctl" (`letts ctl lanes pause`). YAML drives
	// yaml-origin pauses in both directions; ctl-origin pauses can only
	// be cleared by `letts ctl lanes continue` (otherwise an operator's
	// runtime pause silently vanishes on the next re-apply).
	//
	// Conversely, an operator editing `paused: true` → `paused: false`
	// on a yaml-origin pause and re-applying expects the lane to actually
	// unpause; the fix incorrectly preserved it.
	//
	// Legacy persisted rows (Paused=true, empty PausedBy) are treated as
	// ctl-origin so the existing behaviour holds for already-running
	// installs; PausedBy converges to a real value the next time the lane
	// is paused via apply or ctl.
	//
	// Apply also normalises PausedBy on desired itself: an HTTP caller can
	// stuff arbitrary values into the request body, so we strip and
	// re-derive rather than trusting the wire. (Paused=false ⇒ PausedBy
	// always cleared.)
	for name, desiredLane := range desired.Lanes {
		currentLane, hadCurrent := current.Lanes[name]

		if !desiredLane.Paused {
			// Desired wants paused=false. Preserve only ctl-origin pauses
			// (treat legacy empty-PausedBy as ctl for backwards-compat).
			if hadCurrent && currentLane.Paused && currentLane.PausedBy != PausedByYAML {
				desiredLane.Paused = true
				desiredLane.PausedBy = PausedByCtl
			} else {
				desiredLane.PausedBy = ""
			}
			desired.Lanes[name] = desiredLane
			continue
		}

		// Desired wants paused=true. Derive provenance:
		//   - if currently ctl-paused, keep it ctl (more authoritative).
		//   - otherwise this is a yaml-driven pause.
		if hadCurrent && currentLane.Paused && currentLane.PausedBy == PausedByCtl {
			desiredLane.PausedBy = PausedByCtl
		} else {
			desiredLane.PausedBy = PausedByYAML
		}
		desired.Lanes[name] = desiredLane
	}

	// 2. Compute diff.
	diff := ComputeDiff(current, desired)

	// 3. Runtime/mission_dir change with active missions — need Force.
	if (diff.RuntimeChanged || diff.MissionDirChanged) && !opts.Force {
		if hasQueuedOrRunning(ctx, db) {
			return nil, &ConflictError{Detail: ConflictDetail{Reason: "runtime_or_mission_dir_changed_with_active_missions"}}
		}
	}

	// 4. Removed lanes with active missions — need ForcePrune.
	if !opts.ForcePrune && len(diff.LanesRemoved) > 0 {
		var blocked []string
		for _, name := range diff.LanesRemoved {
			if hasActiveInLane(ctx, db, name) {
				blocked = append(blocked, name)
			}
		}
		if len(blocked) > 0 {
			return nil, &ConflictError{Detail: ConflictDetail{Reason: "lanes_with_active_missions", BlockedLane: blocked}}
		}
	}

	// reconciled is set true after the step-6 mgr.Apply runs; the force-prune
	// defer uses it to decide whether the removed runners still
	// need stopping on an early-return error path.
	reconciled := false

	// 4a. ForcePrune with lanes being removed: terminate queued (done/killed/
	// lane_removed) and signal running missions before the lane runner is
	// stopped. Without this, the runners go away but the rows linger
	// (queued forever; running until natural completion). This transition
	// is required, AND requires the lane runner
	// to be marked "removing" FIRST (acked before terminate begins) so
	// no concurrent dispatch can land a fresh queued row that the
	// runner picks mid-terminate.
	if opts.ForcePrune && len(diff.LanesRemoved) > 0 {
		if opts.DataDir == "" {
			return nil, fmt.Errorf("apply: ForcePrune requires Options.DataDir for terminal done-event append")
		}
		// Mark each removed lane as "removing" and wait for
		// its runner to ack. Best-effort — a lane that's not currently
		// managed (e.g. apply called against a fresh DB where the
		// runner never started) returns an error from MarkLaneRemoving,
		// which we tolerate silently because there's no pickup risk
		// from a non-existent runner.
		for _, name := range diff.LanesRemoved {
			_ = mgr.MarkLaneRemoving(name)
		}
		// Once marked removing, a runner blocks on ctx.Done and
		// never re-checks the flag — so if any later step errors before the
		// normal mgr.Apply (step 6) stops it, it strands as a zombie and
		// IsLaneRemoving 503s the lane forever until restart. This defer stops
		// the removed runners on any early-return; the success path sets
		// reconciled=true after mgr.Apply so the defer is a no-op.
		defer func() {
			if !reconciled {
				mgr.StopLanes(diff.LanesRemoved)
			}
		}()
		if err := terminateLaneMissions(ctx, db, opts.DataDir, diff.LanesRemoved, opts.Killer); err != nil {
			return nil, fmt.Errorf("terminate lane missions: %w", err)
		}
	}

	// 5. Persist desired state.
	data, err := json.Marshal(desired)
	if err != nil {
		return nil, fmt.Errorf("marshal desired: %w", err)
	}
	now := time.Now().UnixMilli()
	src := sql.NullString{}
	if opts.Source != "" {
		src = sql.NullString{String: opts.Source, Valid: true}
	}
	if err := storage.SetAppliedConfig(ctx, db, storage.AppliedConfig{
		Data:      data,
		AppliedAt: now,
		Source:    src,
	}); err != nil {
		return nil, fmt.Errorf("set applied config: %w", err)
	}

	// 6. Reconcile lane manager.
	specs := make([]lane.LaneSpec, 0, len(desired.Lanes))
	for name, cfg := range desired.Lanes {
		specs = append(specs, lane.LaneSpec{
			Name:        name,
			Concurrency: cfg.Concurrency,
			Paused:      cfg.Paused,
		})
	}
	started, stopped, resized := mgr.Apply(specs)
	reconciled = true // disarm the force-prune StopLanes defer

	return &Result{
		Diff:    diff,
		Started: started,
		Stopped: stopped,
		Resized: resized,
	}, nil
}

// terminateLaneMissions implements the force-prune transition: for
// each lane being removed it (a) flips queued rows to done(killed/
// lane_removed) THROUGH the durable-finalize-intent path (intent → fsync'ed
// terminal done event → UPDATE missions → delete intent), and (b) signals
// running rows so their kill supervisor SIGTERM/grace/SIGKILLs the process
// group. Returns the first hard error; soft failures (a running row whose
// kill signal can't be queued because it just finished) are logged-and-
// skipped at the caller.
//
// A raw UPDATE missions SET status='done' without
// a prior terminal `done` event in the events file is strictly forbidden. The previous bulk-
// UPDATE implementation violated that: live /v1/missions/{id}/
// events consumers for force-pruned missions never saw the done event and
// crash mid-UPDATE left no intent to recover from.
func terminateLaneMissions(ctx context.Context, db *sql.DB, dataDir string, lanes []string, killer Killer) error {
	for _, name := range lanes {
		// (a) Queued rows → done(killed, lane_removed) via the shared
		// finalize-intent journal. One writer txn per mission keeps
		// each transition atomic with its events-file append.
		rows, err := db.QueryContext(ctx,
			`SELECT mission_id FROM missions WHERE lane=? AND status='queued'`, name)
		if err != nil {
			return fmt.Errorf("list queued in lane %q: %w", name, err)
		}
		var queuedIDs []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				_ = rows.Close()
				return fmt.Errorf("scan queued id: %w", err)
			}
			queuedIDs = append(queuedIDs, id)
		}
		_ = rows.Close()
		for _, id := range queuedIDs {
			m, err := storage.GetMission(ctx, db, id)
			if err != nil {
				if errors.Is(err, storage.ErrNotFound) {
					continue // raced with cleanup
				}
				return fmt.Errorf("get queued %s: %w", id, err)
			}
			if err := mission.KillQueued(ctx, dataDir, db, m, "lane_removed"); err != nil {
				if errors.Is(err, mission.ErrMissionNotQueued) {
					continue // raced with pickup; running path handles it
				}
				return fmt.Errorf("kill queued %s: %w", id, err)
			}
		}

		// (b) Running rows: signal via the Killer. Skip when no Killer
		// wired — apply still removes the lane but running missions
		// finish naturally.
		if killer == nil {
			continue
		}
		runningRows, err := db.QueryContext(ctx,
			`SELECT mission_id FROM missions WHERE lane=? AND status='running'`, name)
		if err != nil {
			return fmt.Errorf("list running in lane %q: %w", name, err)
		}
		var ids []string
		for runningRows.Next() {
			var id string
			if err := runningRows.Scan(&id); err != nil {
				_ = runningRows.Close()
				return fmt.Errorf("scan running id: %w", err)
			}
			ids = append(ids, id)
		}
		_ = runningRows.Close()
		for _, id := range ids {
			// SignalKill returns false when the mission's kill channel is
			// already full or the mission just finished — both benign.
			_ = killer.SignalKill(id, mission.KillLaneRemoved)
		}
	}
	return nil
}

// hasQueuedOrRunning returns true if any missions are queued or running.
func hasQueuedOrRunning(ctx context.Context, db *sql.DB) bool {
	var count int
	_ = db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM missions WHERE status IN ('queued','running')`).Scan(&count)
	return count > 0
}

// hasActiveInLane returns true if any missions in the named lane are queued or running.
func hasActiveInLane(ctx context.Context, db *sql.DB, laneName string) bool {
	var count int
	_ = db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM missions WHERE lane=? AND status IN ('queued','running')`, laneName).Scan(&count)
	return count > 0
}
