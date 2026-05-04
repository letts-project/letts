package repair_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"letts/internal/config"
	"letts/internal/eventfile"
	"letts/internal/ids"
	"letts/internal/mission"
	"letts/internal/repair"
	"letts/internal/storage"
)

func setupRepairDB(t *testing.T) *sql.DB {
	t.Helper()
	dir := t.TempDir()
	db, err := storage.Open(filepath.Join(dir, "state.db"), storage.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := storage.Migrate(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	return db
}

func repairCfg(dataDir string) *config.DugdaleConfig {
	return &config.DugdaleConfig{
		DataDir: dataDir,
		Cleanup: config.CleanupConfig{
			SuccessTTL: time.Hour, FailedTTL: 24 * time.Hour,
			StagingTTL: time.Hour, DownloadedGrace: time.Hour,
			LostCleanupGrace: 10 * time.Minute,
		},
		Limits: config.LimitsConfig{
			MaxEventsBuffer:    1024,
			MaxEventLineSize:   1024,
			MaxReturnValueSize: 64 * 1024,
			MaxFailMessageSize: 64 * 1024,
			MaxFailDetailsSize: 64 * 1024,
		},
		Exec: config.ExecConfig{
			ExecSuccessTTL: time.Hour, ExecFailedTTL: 24 * time.Hour,
		},
	}
}

// repairFixture inserts a running mission row and an events file with a running
// event and returns (mission_id, parentDir).
func repairFixture(t *testing.T, db *sql.DB, dataDir string) (string, string) {
	t.Helper()
	id := ids.NewUUIDv7()
	m := storage.Mission{
		ID: id, Kind: storage.KindMission, Lane: "normal",
		MissionName: "RepairFixture", Status: storage.StatusRunning,
		Input: []byte(`{}`), InputFingerprint: "fp",
		TimeCreatedMs: time.Now().UnixMilli(),
	}
	if err := storage.InsertMission(context.Background(), db, &m); err != nil {
		t.Fatal(err)
	}
	shard, _ := ids.ShardPath(id)
	parentDir := filepath.Join(dataDir, "output", shard)
	if err := os.MkdirAll(parentDir, 0o755); err != nil {
		t.Fatal(err)
	}
	w, err := eventfile.Create(parentDir, id)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = w.Append(eventfile.KindRunning, map[string]any{"time": time.Now().UnixMilli()}, false)
	_ = w.Close()
	return id, parentDir
}

// stageOutput drops a tmp staging file, pending_output row, and intent for the
// mission. Returns the CollectedOutput so tests can stat tmp/final.
func stageOutput(t *testing.T, db *sql.DB, dataDir, missionID string) mission.CollectedOutput {
	t.Helper()
	stagingID := ids.NewUUIDv7()
	shard, _ := ids.ShardPath(stagingID)
	stagingDir := filepath.Join(dataDir, "staging", shard)
	if err := os.MkdirAll(stagingDir, 0o755); err != nil {
		t.Fatal(err)
	}
	tmpPath := filepath.Join(stagingDir, stagingID+".tmp")
	finalPath := filepath.Join(stagingDir, stagingID)
	if err := os.WriteFile(tmpPath, []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixMilli()
	if err := storage.InsertStaging(context.Background(), db, &storage.StagingFile{
		StagingID: stagingID, State: storage.StagingPendingOutput,
		Sha256: "sha", Size: 7, BytesReceived: 7,
		Path:          filepath.Join("staging", shard, stagingID+".tmp"),
		TimeCreatedMs: now, TimeUpdatedMs: now, TimeExpiresMs: now + 60_000,
	}); err != nil {
		t.Fatal(err)
	}
	return mission.CollectedOutput{
		Role: "result", StagingID: stagingID,
		TmpPath: tmpPath, FinalPath: finalPath,
		Sha256: "sha", Size: 7,
	}
}

func insertIntent(t *testing.T, db *sql.DB, missionID string, phase storage.FinalizePhase, outputs []mission.CollectedOutput, outcome string) {
	t.Helper()
	outputsJSON, _ := json.Marshal(outputs)
	if outputs == nil {
		outputsJSON = []byte("[]")
	}
	doneFields := map[string]any{
		"seq": int64(2), "event": "done", "outcome": outcome,
		"exit_code": int64(0), "time": time.Now().UnixMilli(),
	}
	doneJSON, _ := json.Marshal(doneFields)
	intent := storage.FinalizeIntent{
		MissionID: missionID, Phase: phase, Outcome: outcome,
		ExitCode: sql.NullInt64{Int64: 0, Valid: true},
		Outputs:  outputsJSON, DoneSeq: 2,
		DoneEvent: string(doneJSON), TimeCreatedMs: time.Now().UnixMilli(),
	}
	if err := storage.InsertFinalizeIntent(context.Background(), db, &intent); err != nil {
		t.Fatal(err)
	}
}

func loadEventsRepair(t *testing.T, parentDir, id string) []map[string]any {
	t.Helper()
	path := filepath.Join(parentDir, id+"-events")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimRight(string(b), "\n"), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("unmarshal %q: %v", line, err)
		}
		out = append(out, m)
	}
	return out
}

func TestRepairPreparedFastPathCommits(t *testing.T) {
	db := setupRepairDB(t)
	dataDir := t.TempDir()
	cfg := repairCfg(dataDir)
	id, parentDir := repairFixture(t, db, dataDir)

	insertIntent(t, db, id, storage.PhasePrepared, nil, "success")

	if err := repair.RepairFinalizeIntents(context.Background(), cfg, db, slog.Default()); err != nil {
		t.Fatalf("repair: %v", err)
	}
	m, err := storage.GetMission(context.Background(), db, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if m.Status != storage.StatusDone || m.Outcome.String != "success" {
		t.Errorf("status=%q outcome=%q", m.Status, m.Outcome.String)
	}
	if _, err := storage.GetFinalizeIntent(context.Background(), db, id); !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("intent still present: %v", err)
	}
	events := loadEventsRepair(t, parentDir, id)
	if len(events) < 2 || events[len(events)-1]["event"] != "done" {
		t.Errorf("done event missing: %v", events)
	}
}

func TestRepairPreparedWithOutputsAllTmpExists(t *testing.T) {
	db := setupRepairDB(t)
	dataDir := t.TempDir()
	cfg := repairCfg(dataDir)
	id, parentDir := repairFixture(t, db, dataDir)

	out := stageOutput(t, db, dataDir, id)
	insertIntent(t, db, id, storage.PhasePrepared, []mission.CollectedOutput{out}, "success")

	if err := repair.RepairFinalizeIntents(context.Background(), cfg, db, slog.Default()); err != nil {
		t.Fatalf("repair: %v", err)
	}
	m, _ := storage.GetMission(context.Background(), db, id)
	if m.Outcome.String != "success" {
		t.Errorf("outcome=%q", m.Outcome.String)
	}
	if _, err := os.Stat(out.FinalPath); err != nil {
		t.Errorf("final missing: %v", err)
	}
	if _, err := os.Stat(out.TmpPath); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("tmp not removed: %v", err)
	}
	sf, _ := storage.GetStaging(context.Background(), db, out.StagingID)
	if sf.State != storage.StagingComplete {
		t.Errorf("staging state=%q", sf.State)
	}
	refs, _ := storage.RefsByMission(context.Background(), db, id)
	if len(refs) != 1 || refs[0].StagingID != out.StagingID || refs[0].RefKind != storage.RefOutput {
		t.Errorf("refs=%v", refs)
	}
	events := loadEventsRepair(t, parentDir, id)
	if events[len(events)-1]["outcome"] != "success" {
		t.Errorf("done outcome=%v", events[len(events)-1]["outcome"])
	}
}

// TestContinuePhaseBUpdatesStagingPath guards against the same regression
// covered by TestFinalizeUpdatesStagingPathAfterRename, but on the startup
// repair path (intent.Phase=prepared with tmp present → mission.ContinuePhaseB).
// Phase A2 stored path=<staging_id>.tmp; after Phase B rename, the
// commitFinalize step must UPDATE path to the post-rename location.
func TestContinuePhaseBUpdatesStagingPath(t *testing.T) {
	db := setupRepairDB(t)
	dataDir := t.TempDir()
	cfg := repairCfg(dataDir)
	id, _ := repairFixture(t, db, dataDir)

	out := stageOutput(t, db, dataDir, id)
	insertIntent(t, db, id, storage.PhasePrepared, []mission.CollectedOutput{out}, "success")

	if err := repair.RepairFinalizeIntents(context.Background(), cfg, db, slog.Default()); err != nil {
		t.Fatalf("repair: %v", err)
	}

	sf, err := storage.GetStaging(context.Background(), db, out.StagingID)
	if err != nil {
		t.Fatalf("get staging: %v", err)
	}
	if sf.State != storage.StagingComplete {
		t.Fatalf("Staging state=%q, want complete", sf.State)
	}
	if strings.HasSuffix(sf.Path, ".tmp") {
		t.Errorf("staging_files.path still points at .tmp: %q", sf.Path)
	}
	shard, _ := ids.ShardPath(out.StagingID)
	wantRel := filepath.Join("staging", shard, out.StagingID)
	if sf.Path != wantRel {
		t.Errorf("staging_files.path=%q, want %q", sf.Path, wantRel)
	}
	abs := filepath.Join(dataDir, sf.Path)
	if _, err := os.Stat(abs); err != nil {
		t.Errorf("file at recorded path missing: %v (abs=%s)", err, abs)
	}
}

func TestRepairPreparedTmpMissingRevertsToFailed(t *testing.T) {
	db := setupRepairDB(t)
	dataDir := t.TempDir()
	cfg := repairCfg(dataDir)
	id, parentDir := repairFixture(t, db, dataDir)

	out := stageOutput(t, db, dataDir, id)
	_ = os.Remove(out.TmpPath) // simulate tmp lost in crash
	insertIntent(t, db, id, storage.PhasePrepared, []mission.CollectedOutput{out}, "success")

	if err := repair.RepairFinalizeIntents(context.Background(), cfg, db, slog.Default()); err != nil {
		t.Fatalf("repair: %v", err)
	}
	m, _ := storage.GetMission(context.Background(), db, id)
	if m.Outcome.String != "failed" || m.FailReason.String != "output_commit_failed" {
		t.Errorf("outcome=%q reason=%q", m.Outcome.String, m.FailReason.String)
	}
	sf, _ := storage.GetStaging(context.Background(), db, out.StagingID)
	if sf.State != storage.StagingDeleting {
		t.Errorf("staging state=%q, want deleting", sf.State)
	}
	if _, err := os.Stat(out.FinalPath); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("final present after revert: %v", err)
	}
	events := loadEventsRepair(t, parentDir, id)
	if events[len(events)-1]["outcome"] != "failed" {
		t.Errorf("expected failed done event, got %v", events[len(events)-1])
	}
}

func TestRepairCommittingFinishesRenames(t *testing.T) {
	db := setupRepairDB(t)
	dataDir := t.TempDir()
	cfg := repairCfg(dataDir)
	id, _ := repairFixture(t, db, dataDir)

	out := stageOutput(t, db, dataDir, id)
	// Simulate crash mid Phase B: staging row in committing, intent in committing.
	if _, err := db.Exec(`UPDATE staging_files SET state='committing' WHERE staging_id=?`, out.StagingID); err != nil {
		t.Fatal(err)
	}
	insertIntent(t, db, id, storage.PhaseCommitting, []mission.CollectedOutput{out}, "success")

	if err := repair.RepairFinalizeIntents(context.Background(), cfg, db, slog.Default()); err != nil {
		t.Fatalf("repair: %v", err)
	}
	if _, err := os.Stat(out.FinalPath); err != nil {
		t.Errorf("rename not completed: %v", err)
	}
	m, _ := storage.GetMission(context.Background(), db, id)
	if m.Outcome.String != "success" {
		t.Errorf("outcome=%q", m.Outcome.String)
	}
	sf, _ := storage.GetStaging(context.Background(), db, out.StagingID)
	if sf.State != storage.StagingComplete {
		t.Errorf("staging state=%q", sf.State)
	}
}

func TestRepairCommittingTmpAndFinalMissingReverts(t *testing.T) {
	db := setupRepairDB(t)
	dataDir := t.TempDir()
	cfg := repairCfg(dataDir)
	id, _ := repairFixture(t, db, dataDir)

	out := stageOutput(t, db, dataDir, id)
	// Simulate worst case: both tmp and final gone (rare disk corruption).
	_ = os.Remove(out.TmpPath)
	if _, err := db.Exec(`UPDATE staging_files SET state='committing' WHERE staging_id=?`, out.StagingID); err != nil {
		t.Fatal(err)
	}
	insertIntent(t, db, id, storage.PhaseCommitting, []mission.CollectedOutput{out}, "success")

	if err := repair.RepairFinalizeIntents(context.Background(), cfg, db, slog.Default()); err != nil {
		t.Fatalf("repair: %v", err)
	}
	m, _ := storage.GetMission(context.Background(), db, id)
	// "tmp+final missing" is corruption, not a
	// rename failure. Reason now distinguishes these.
	if m.Outcome.String != "failed" || m.FailReason.String != "output_commit_corrupt" {
		t.Errorf("outcome=%q reason=%q", m.Outcome.String, m.FailReason.String)
	}
}

func TestRepairCommittingFinalAlreadyExistsSkipsRename(t *testing.T) {
	db := setupRepairDB(t)
	dataDir := t.TempDir()
	cfg := repairCfg(dataDir)
	id, _ := repairFixture(t, db, dataDir)

	out := stageOutput(t, db, dataDir, id)
	// Pre-rename: simulate that a previous repair attempt already renamed.
	if err := os.Rename(out.TmpPath, out.FinalPath); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE staging_files SET state='committing' WHERE staging_id=?`, out.StagingID); err != nil {
		t.Fatal(err)
	}
	// stageOutput defaults Sha256:"sha" (literal). The repair sha
	// verification will reject that — set the real sha256
	// of the file content "payload" so the legit "skip rename and
	// commit" branch is exercised.
	out.Sha256 = sha256Hex([]byte("payload"))
	insertIntent(t, db, id, storage.PhaseCommitting, []mission.CollectedOutput{out}, "success")

	if err := repair.RepairFinalizeIntents(context.Background(), cfg, db, slog.Default()); err != nil {
		t.Fatalf("repair: %v", err)
	}
	m, _ := storage.GetMission(context.Background(), db, id)
	if m.Outcome.String != "success" {
		t.Errorf("outcome=%q", m.Outcome.String)
	}
	if _, err := os.Stat(out.FinalPath); err != nil {
		t.Errorf("final missing: %v", err)
	}
}

// sha256Hex is a test helper that computes the lowercase hex sha256 of b.
func sha256Hex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// TestRepairCommittingShaMismatchReverts enforces the recovery rule for a
// 'committing' staging row whose final file exists (no tmp) alongside a
// finalize intent: recompute the final file's sha256 and compare it with
// the stored declared hash — on match continue the commit from the intent;
// on mismatch mark the intent failed, flip the staging row to 'deleting',
// and finalize the mission done(failed, fail_reason='output_commit_corrupt').
//
// A repair that trusts finalExists and goes straight to CommitFromIntent
// would silently accept a corrupt final (partial write, pre-existing junk,
// etc.).
func TestRepairCommittingShaMismatchReverts(t *testing.T) {
	db := setupRepairDB(t)
	dataDir := t.TempDir()
	cfg := repairCfg(dataDir)
	id, _ := repairFixture(t, db, dataDir)

	out := stageOutput(t, db, dataDir, id)
	// Simulate a successful tmp→final rename from a prior repair attempt.
	if err := os.Rename(out.TmpPath, out.FinalPath); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE staging_files SET state='committing' WHERE staging_id=?`, out.StagingID); err != nil {
		t.Fatal(err)
	}
	// Intent declares a sha256 that does NOT match the final's content.
	// stageOutput sets Sha256:"sha", file content is "payload" → real
	// sha is different. Force a clear mismatch: declare a fake sha that's
	// not the real one of "payload".
	out.Sha256 = "0000000000000000000000000000000000000000000000000000000000000000"
	insertIntent(t, db, id, storage.PhaseCommitting, []mission.CollectedOutput{out}, "success")

	if err := repair.RepairFinalizeIntents(context.Background(), cfg, db, slog.Default()); err != nil {
		t.Fatalf("repair: %v", err)
	}
	m, _ := storage.GetMission(context.Background(), db, id)
	if m.Outcome.String != "failed" {
		t.Errorf("outcome=%q, want failed", m.Outcome.String)
	}
	if !strings.Contains(m.FailReason.String, "output_commit_corrupt") {
		t.Errorf("fail_reason=%q, want output_commit_corrupt", m.FailReason.String)
	}
	// Affected staging row must be in 'deleting'.
	st, _ := storage.GetStaging(context.Background(), db, out.StagingID)
	if st.State != storage.StagingDeleting {
		t.Errorf("staging state=%q, want deleting", st.State)
	}
}

// TestRepairIntentForDeletedMissionDiscardsOutcome: a crash can leave a
// prepared intent for a mission an admin flipped to 'deleting' before the
// restart. Repair must finish the protocol without resurrecting the row:
// the mission stays 'deleting' with no outcome written, the intent is
// cleared, the intent's output staging rows land in 'deleting' (pointing at
// the renamed final files for the staging GC), and no refs are inserted.
func TestRepairIntentForDeletedMissionDiscardsOutcome(t *testing.T) {
	db := setupRepairDB(t)
	dataDir := t.TempDir()
	cfg := repairCfg(dataDir)
	id, _ := repairFixture(t, db, dataDir)

	out := stageOutput(t, db, dataDir, id)
	insertIntent(t, db, id, storage.PhasePrepared, []mission.CollectedOutput{out}, "success")
	if _, err := db.Exec(`UPDATE missions SET status='deleting' WHERE mission_id=?`, id); err != nil {
		t.Fatal(err)
	}

	if err := repair.RepairFinalizeIntents(context.Background(), cfg, db, slog.Default()); err != nil {
		t.Fatalf("repair: %v", err)
	}

	m, err := storage.GetMission(context.Background(), db, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if m.Status != storage.StatusDeleting {
		t.Errorf("status=%q, want deleting (repair must not resurrect)", m.Status)
	}
	if m.Outcome.Valid {
		t.Errorf("outcome=%q written onto a deleting row", m.Outcome.String)
	}
	if _, err := storage.GetFinalizeIntent(context.Background(), db, id); !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("intent still present: %v", err)
	}
	sf, err := storage.GetStaging(context.Background(), db, out.StagingID)
	if err != nil {
		t.Fatalf("get staging: %v", err)
	}
	if sf.State != storage.StagingDeleting {
		t.Errorf("staging state=%q, want deleting", sf.State)
	}
	refs, _ := storage.RefsByMission(context.Background(), db, id)
	if len(refs) != 0 {
		t.Errorf("refs=%v, want none", refs)
	}
	// Phase B continuation renamed tmp→final before the guarded commit; the
	// staging row must point at the surviving final file.
	if _, err := os.Stat(out.FinalPath); err != nil {
		t.Errorf("final missing: %v", err)
	}
	if _, err := os.Stat(out.TmpPath); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("tmp not renamed away: %v", err)
	}
	shard, _ := ids.ShardPath(out.StagingID)
	wantRel := filepath.Join("staging", shard, out.StagingID)
	if sf.Path != wantRel {
		t.Errorf("staging path=%q, want %q", sf.Path, wantRel)
	}
}

// TestRepairIntentMissingEventsFileRecreates: the durable intent is the
// authoritative outcome record; a missing events file must not strand the
// mission in running forever (the running→lost sweep deliberately skips
// intent-carrying rows). The repair recreates the file and finishes the
// commit, with the done event at the intent's done_seq.
func TestRepairIntentMissingEventsFileRecreates(t *testing.T) {
	db := setupRepairDB(t)
	dataDir := t.TempDir()
	cfg := repairCfg(dataDir)
	id, parentDir := repairFixture(t, db, dataDir)
	_ = os.Remove(filepath.Join(parentDir, id+"-events"))
	insertIntent(t, db, id, storage.PhasePrepared, nil, "success")

	if err := repair.RepairFinalizeIntents(context.Background(), cfg, db, slog.Default()); err != nil {
		t.Fatalf("repair: %v", err)
	}

	m, err := storage.GetMission(context.Background(), db, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if m.Status != storage.StatusDone || m.Outcome.String != "success" {
		t.Errorf("status=%q outcome=%q", m.Status, m.Outcome.String)
	}
	if _, err := storage.GetFinalizeIntent(context.Background(), db, id); !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("intent still present: %v", err)
	}
	events := loadEventsRepair(t, parentDir, id)
	last := events[len(events)-1]
	if last["event"] != "done" || last["outcome"] != "success" {
		t.Errorf("last event=%v", last)
	}
	// insertIntent commits done_seq=2; the recreated file's running event
	// takes seq 1, so the done must land exactly at the intent's seq.
	if last["seq"].(float64) != 2 {
		t.Errorf("done seq=%v, want 2 (intent done_seq)", last["seq"])
	}
}

func TestRepairOneBadIntentDoesNotBlockOthers(t *testing.T) {
	db := setupRepairDB(t)
	dataDir := t.TempDir()
	cfg := repairCfg(dataDir)

	// Good intent (will commit).
	goodID, _ := repairFixture(t, db, dataDir)
	insertIntent(t, db, goodID, storage.PhasePrepared, nil, "success")

	// Bad intent: the events file path is occupied by a directory, so the
	// append open fails with a non-ENOENT error (a plain missing file is
	// recoverable — see TestRepairIntentMissingEventsFileRecreates).
	badID, badParent := repairFixture(t, db, dataDir)
	badPath := filepath.Join(badParent, badID+"-events")
	_ = os.Remove(badPath)
	if err := os.Mkdir(badPath, 0o755); err != nil {
		t.Fatal(err)
	}
	insertIntent(t, db, badID, storage.PhasePrepared, nil, "success")

	if err := repair.RepairFinalizeIntents(context.Background(), cfg, db, slog.Default()); err != nil {
		t.Fatalf("repair: %v", err)
	}
	good, _ := storage.GetMission(context.Background(), db, goodID)
	if good.Outcome.String != "success" {
		t.Errorf("good mission not finalized: %+v", good)
	}
	bad, _ := storage.GetMission(context.Background(), db, badID)
	if bad.Status == storage.StatusDone {
		t.Errorf("bad mission unexpectedly finalized: %+v", bad)
	}
}

func TestRepairNoIntentsIsNoop(t *testing.T) {
	db := setupRepairDB(t)
	dataDir := t.TempDir()
	cfg := repairCfg(dataDir)
	if err := repair.RepairFinalizeIntents(context.Background(), cfg, db, slog.Default()); err != nil {
		t.Fatalf("repair: %v", err)
	}
}
