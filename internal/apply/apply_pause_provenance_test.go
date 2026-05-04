package apply_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"

	"letts/internal/apply"
	"letts/internal/storage"
)

// readPersistedLane reads the named lane from the persisted AppliedState.
// Used by provenance tests to assert what Apply stored.
func readPersistedLane(t *testing.T, db *sql.DB, name string) apply.LaneCfg {
	t.Helper()
	cfg, err := storage.GetAppliedConfig(context.Background(), db)
	if err != nil {
		t.Fatalf("GetAppliedConfig: %v", err)
	}
	var s apply.AppliedState
	if err := json.Unmarshal(cfg.Data, &s); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	lc, ok := s.Lanes[name]
	if !ok {
		t.Fatalf("lane %q not found in persisted state", name)
	}
	return lc
}

// TestApplyPauseProvenance_YAMLPausesFresh: a YAML with paused:true on a lane
// that was previously unpaused (or absent) stores PausedBy="yaml" so a later
// YAML edit to paused:false can reverse it.
func TestApplyPauseProvenance_YAMLPausesFresh(t *testing.T) {
	db := setupDB(t)
	mgr := newManager(t, db)
	ctx := context.Background()

	desired := apply.AppliedState{
		Lanes: map[string]apply.LaneCfg{"work": {Concurrency: 1, Paused: true}},
	}
	if _, err := apply.Apply(ctx, db, mgr, desired, apply.Options{}); err != nil {
		t.Fatalf("apply: %v", err)
	}

	got := readPersistedLane(t, db, "work")
	if !got.Paused {
		t.Fatalf("Paused=%v want true", got.Paused)
	}
	if got.PausedBy != "yaml" {
		t.Errorf("PausedBy=%q want yaml", got.PausedBy)
	}
}

// TestApplyPauseProvenance_YAMLUnpausesYAMLOrigin: operator
// edits paused:true → paused:false on a yaml-origin pause and re-applies.
// The lane must actually unpause.
func TestApplyPauseProvenance_YAMLUnpausesYAMLOrigin(t *testing.T) {
	db := setupDB(t)
	mgr := newManager(t, db)
	ctx := context.Background()

	// Step 1: YAML pauses the lane.
	first := apply.AppliedState{
		Lanes: map[string]apply.LaneCfg{"work": {Concurrency: 1, Paused: true}},
	}
	if _, err := apply.Apply(ctx, db, mgr, first, apply.Options{}); err != nil {
		t.Fatalf("first apply: %v", err)
	}

	// Step 2: YAML unpauses the lane (paused: false).
	second := apply.AppliedState{
		Lanes: map[string]apply.LaneCfg{"work": {Concurrency: 1, Paused: false}},
	}
	if _, err := apply.Apply(ctx, db, mgr, second, apply.Options{}); err != nil {
		t.Fatalf("second apply: %v", err)
	}

	got := readPersistedLane(t, db, "work")
	if got.Paused {
		t.Errorf("after paused:false re-apply, Paused=%v want false", got.Paused)
	}
	if got.PausedBy != "" {
		t.Errorf("PausedBy=%q want empty after unpause", got.PausedBy)
	}
}

// TestApplyPauseProvenance_PreservesCtlOrigin: ctl-paused lane cannot be
// unpaused via YAML (operator must use `letts ctl lanes continue`).
func TestApplyPauseProvenance_PreservesCtlOrigin(t *testing.T) {
	db := setupDB(t)
	mgr := newManager(t, db)
	ctx := context.Background()

	// Persist a ctl-origin pause directly (simulates `letts ctl lanes pause`).
	stored := apply.AppliedState{
		Lanes: map[string]apply.LaneCfg{
			"work": {Concurrency: 1, Paused: true, PausedBy: "ctl"},
		},
	}
	data, _ := json.Marshal(stored)
	if err := storage.SetAppliedConfig(ctx, db, storage.AppliedConfig{
		Data: data, AppliedAt: 1,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// YAML re-apply with paused:false. Must NOT unpause (it's ctl-origin).
	desired := apply.AppliedState{
		Lanes: map[string]apply.LaneCfg{"work": {Concurrency: 1, Paused: false}},
	}
	if _, err := apply.Apply(ctx, db, mgr, desired, apply.Options{}); err != nil {
		t.Fatalf("apply: %v", err)
	}

	got := readPersistedLane(t, db, "work")
	if !got.Paused {
		t.Errorf("ctl-origin pause must survive YAML re-apply, got Paused=%v", got.Paused)
	}
	if got.PausedBy != "ctl" {
		t.Errorf("PausedBy=%q want ctl preserved", got.PausedBy)
	}
}

// TestApplyPauseProvenance_LegacyMissingPausedByStaysSticky: AppliedState
// rows from before this commit lack PausedBy; for backward compatibility
// they are treated as ctl-origin (i.e. sticky, preserved across apply).
// Operators wanting to unpause must `letts ctl lanes continue` once.
func TestApplyPauseProvenance_LegacyMissingPausedByStaysSticky(t *testing.T) {
	db := setupDB(t)
	mgr := newManager(t, db)
	ctx := context.Background()

	// Persist a legacy AppliedState: Paused=true, no PausedBy field.
	stored := apply.AppliedState{
		Lanes: map[string]apply.LaneCfg{"work": {Concurrency: 1, Paused: true}},
	}
	data, _ := json.Marshal(stored)
	if err := storage.SetAppliedConfig(ctx, db, storage.AppliedConfig{
		Data: data, AppliedAt: 1,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// YAML re-apply with paused:false — preserves (legacy sticky).
	desired := apply.AppliedState{
		Lanes: map[string]apply.LaneCfg{"work": {Concurrency: 1, Paused: false}},
	}
	if _, err := apply.Apply(ctx, db, mgr, desired, apply.Options{}); err != nil {
		t.Fatalf("apply: %v", err)
	}

	got := readPersistedLane(t, db, "work")
	if !got.Paused {
		t.Errorf("legacy paused (no PausedBy) must stay sticky, got Paused=%v", got.Paused)
	}
}

// TestApplyPauseProvenance_ClientCannotForgeCtlOrigin: a malicious admin
// HTTP request that sets paused_by="ctl" on a fresh apply must not be
// trusted; provenance is derived server-side from current state and desired
// transition, never from request body.
func TestApplyPauseProvenance_ClientCannotForgeCtlOrigin(t *testing.T) {
	db := setupDB(t)
	mgr := newManager(t, db)
	ctx := context.Background()

	// Caller asserts ctl provenance for a fresh pause. Server must overwrite
	// to yaml because this came in through Apply (yaml path), not ctl.
	desired := apply.AppliedState{
		Lanes: map[string]apply.LaneCfg{
			"work": {Concurrency: 1, Paused: true, PausedBy: "ctl"},
		},
	}
	if _, err := apply.Apply(ctx, db, mgr, desired, apply.Options{}); err != nil {
		t.Fatalf("apply: %v", err)
	}

	got := readPersistedLane(t, db, "work")
	if got.PausedBy != "yaml" {
		t.Errorf("PausedBy=%q want yaml (Apply must override client-supplied ctl)", got.PausedBy)
	}
}

// TestApplyPauseProvenance_PausedFalseHasNoProvenance: a fresh apply with
// paused:false stores PausedBy="" — provenance only meaningful when paused.
func TestApplyPauseProvenance_PausedFalseHasNoProvenance(t *testing.T) {
	db := setupDB(t)
	mgr := newManager(t, db)
	ctx := context.Background()

	desired := apply.AppliedState{
		Lanes: map[string]apply.LaneCfg{
			"work": {Concurrency: 1, Paused: false, PausedBy: "ctl"},
		},
	}
	if _, err := apply.Apply(ctx, db, mgr, desired, apply.Options{}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	got := readPersistedLane(t, db, "work")
	if got.PausedBy != "" {
		t.Errorf("PausedBy=%q want empty for unpaused lane", got.PausedBy)
	}
}
