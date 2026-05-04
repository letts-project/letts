package repair_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"letts/internal/eventfile"
	"letts/internal/ids"
	"letts/internal/mission"
	"letts/internal/repair"
	"letts/internal/storage"
)

// TestRepairResolvesIntentPathsAcrossDataDirRename checks data_dir-rename
// survival: the persisted intent stores Tmp/Final paths relative to data_dir;
// if an operator renames data_dir between crash and restart, repair must still
// find the tmp files at their new absolute location.
//
// With absolute paths (anchored at the OLD data_dir), a rename would leave
// them pointing at a non-existent location → repair would revert valid
// commits to failed.
func TestRepairResolvesIntentPathsAcrossDataDirRename(t *testing.T) {
	db := setupRepairDB(t)
	oldDataDir := filepath.Join(t.TempDir(), "old")
	if err := os.MkdirAll(oldDataDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Stage the mission and tmp output under the OLD data_dir.
	id := ids.NewUUIDv7()
	m := storage.Mission{
		ID: id, Kind: storage.KindMission, Lane: "normal",
		MissionName: "RenameFixture", Status: storage.StatusRunning,
		Input: []byte(`{}`), InputFingerprint: "fp",
		TimeCreatedMs: time.Now().UnixMilli(),
	}
	if err := storage.InsertMission(context.Background(), db, &m); err != nil {
		t.Fatal(err)
	}
	shard, _ := ids.ShardPath(id)
	oldParent := filepath.Join(oldDataDir, "output", shard)
	if err := os.MkdirAll(oldParent, 0o755); err != nil {
		t.Fatal(err)
	}
	ew, _ := eventfile.Create(oldParent, id)
	_, _ = ew.Append(eventfile.KindRunning, map[string]any{"time": time.Now().UnixMilli()}, false)
	_ = ew.Close()

	out := stageOutput(t, db, oldDataDir, id)

	// Insert intent using the marshaller (relative paths).
	outsJSON, err := mission.MarshalIntentOutputsForTest([]mission.CollectedOutput{out}, oldDataDir)
	if err != nil {
		t.Fatalf("marshalIntentOutputs: %v", err)
	}
	doneJSON, _ := json.Marshal(map[string]any{
		"seq": int64(2), "event": "done", "outcome": "success",
		"exit_code": int64(0), "time": time.Now().UnixMilli(),
	})
	if err := storage.InsertFinalizeIntent(context.Background(), db, &storage.FinalizeIntent{
		MissionID: id, Phase: storage.PhasePrepared, Outcome: "success",
		ExitCode:      sql.NullInt64{Int64: 0, Valid: true},
		Outputs:       outsJSON,
		DoneSeq:       2,
		DoneEvent:     string(doneJSON),
		TimeCreatedMs: time.Now().UnixMilli(),
	}); err != nil {
		t.Fatal(err)
	}

	// Simulate operator renaming data_dir. Move every file over.
	newDataDir := filepath.Join(t.TempDir(), "new")
	if err := os.Rename(oldDataDir, newDataDir); err != nil {
		t.Fatalf("rename data_dir: %v", err)
	}
	cfg := repairCfg(newDataDir)

	if err := repair.RepairFinalizeIntents(context.Background(), cfg, db, slog.Default()); err != nil {
		t.Fatalf("repair: %v", err)
	}

	// Repair should have renamed tmp→final under the NEW data_dir.
	newFinal := filepath.Join(newDataDir, "staging", shard, out.StagingID)
	if _, err := os.Stat(newFinal); err != nil {
		t.Errorf("final missing under new data_dir: %v (path=%s)", err, newFinal)
	}
	got, err := storage.GetMission(context.Background(), db, id)
	if err != nil {
		t.Fatalf("get mission: %v", err)
	}
	if got.Outcome.String != "success" {
		t.Errorf("outcome=%q want success (rename-survival broken)", got.Outcome.String)
	}
}

// TestRepairAcceptsLegacyAbsoluteIntentPaths covers the backward-compat
// path: rows persisted before the relative-paths commit still contain
// `tmp_path`/`final_path` absolute fields. As long as data_dir matches
// what the legacy write used, repair must succeed.
func TestRepairAcceptsLegacyAbsoluteIntentPaths(t *testing.T) {
	db := setupRepairDB(t)
	dataDir := t.TempDir()
	cfg := repairCfg(dataDir)
	id, _ := repairFixture(t, db, dataDir)

	out := stageOutput(t, db, dataDir, id)

	// Write the legacy JSON shape: only tmp_path/final_path absolute.
	type legacy struct {
		Role      string `json:"role"`
		StagingID string `json:"staging_id"`
		TmpPath   string `json:"tmp_path"`
		FinalPath string `json:"final_path"`
		Sha256    string `json:"sha256"`
		Size      int64  `json:"size"`
	}
	legacyJSON, _ := json.Marshal([]legacy{{
		Role: out.Role, StagingID: out.StagingID,
		TmpPath: out.TmpPath, FinalPath: out.FinalPath,
		Sha256: out.Sha256, Size: out.Size,
	}})
	doneJSON, _ := json.Marshal(map[string]any{
		"seq": int64(2), "event": "done", "outcome": "success",
		"exit_code": int64(0), "time": time.Now().UnixMilli(),
	})
	if err := storage.InsertFinalizeIntent(context.Background(), db, &storage.FinalizeIntent{
		MissionID: id, Phase: storage.PhasePrepared, Outcome: "success",
		ExitCode:      sql.NullInt64{Int64: 0, Valid: true},
		Outputs:       legacyJSON,
		DoneSeq:       2,
		DoneEvent:     string(doneJSON),
		TimeCreatedMs: time.Now().UnixMilli(),
	}); err != nil {
		t.Fatal(err)
	}

	if err := repair.RepairFinalizeIntents(context.Background(), cfg, db, slog.Default()); err != nil {
		t.Fatalf("repair: %v", err)
	}
	if _, err := os.Stat(out.FinalPath); err != nil {
		t.Errorf("legacy abs path repair did not produce final: %v", err)
	}
}
