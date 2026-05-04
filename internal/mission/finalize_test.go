package mission

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"letts/internal/eventfile"
	"letts/internal/ids"
	"letts/internal/storage"
)

// finalizeFixture sets up a missions row in status=running plus an open
// events file with one running event. Returns the mission id, data dir, and
// db so individual tests can drive Finalize.
func finalizeFixture(t *testing.T) (id, dataDir string, db *sql.DB) {
	t.Helper()
	db = openTestDB(t)
	id = ids.NewUUIDv7()
	dataDir = t.TempDir()

	m := storage.Mission{
		ID:               id,
		Kind:             storage.KindMission,
		Lane:             "normal",
		MissionName:      "Finalize",
		Status:           storage.StatusRunning,
		Input:            []byte(`{}`),
		InputFingerprint: "fpx",
		TimeCreatedMs:    1700000000000,
	}
	if err := storage.InsertMission(context.Background(), db, &m); err != nil {
		t.Fatalf("insert mission: %v", err)
	}

	shard, err := ids.ShardPath(id)
	if err != nil {
		t.Fatalf("shard: %v", err)
	}
	parentDir := filepath.Join(dataDir, "output", shard)
	if err := os.MkdirAll(parentDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	w, err := eventfile.Create(parentDir, id)
	if err != nil {
		t.Fatalf("eventfile create: %v", err)
	}
	if _, err := w.Append(eventfile.KindRunning, map[string]any{"time": int64(1700000000001)}, false); err != nil {
		t.Fatalf("append running: %v", err)
	}
	_ = w.Close()
	return
}

func loadEvents(t *testing.T, dataDir, missionID string) []map[string]any {
	t.Helper()
	shard, err := ids.ShardPath(missionID)
	if err != nil {
		t.Fatalf("shard: %v", err)
	}
	path := filepath.Join(dataDir, "output", shard, missionID+"-events")
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open events: %v", err)
	}
	defer func() { _ = f.Close() }()
	var out []map[string]any
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	for sc.Scan() {
		var ev map[string]any
		if err := json.Unmarshal(sc.Bytes(), &ev); err != nil {
			t.Fatalf("unmarshal %q: %v", sc.Text(), err)
		}
		out = append(out, ev)
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}
	return out
}

func loadMission(t *testing.T, db *sql.DB, id string) *storage.Mission {
	t.Helper()
	m, err := storage.GetMission(context.Background(), db, id)
	if err != nil {
		t.Fatalf("get mission: %v", err)
	}
	return m
}

func TestFinalizeSuccessNoOutputsFastPath(t *testing.T) {
	id, dataDir, db := finalizeFixture(t)
	in := FinalizeInputs{
		MissionID: id,
		Outcome:   OutcomeResult{Outcome: "success", ExitCode: 0, Return: json.RawMessage(`{"k":"v"}`)},
		Cfg:       FinalizeConfig{DataDir: dataDir},
	}
	if err := Finalize(context.Background(), db, in); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	events := loadEvents(t, dataDir, id)
	if len(events) != 2 {
		t.Fatalf("events=%d, want 2 (running+done)", len(events))
	}
	if events[1]["event"] != "done" {
		t.Errorf("events[1]=%v, want event=done", events[1])
	}
	if events[1]["outcome"] != "success" {
		t.Errorf("outcome=%v, want success", events[1]["outcome"])
	}

	m := loadMission(t, db, id)
	if m.Status != storage.StatusDone {
		t.Errorf("Status=%q, want done", m.Status)
	}
	if !m.Outcome.Valid || m.Outcome.String != "success" {
		t.Errorf("Outcome=%+v", m.Outcome)
	}
	if string(m.ReturnValue) != `{"k":"v"}` {
		t.Errorf("ReturnValue=%q", string(m.ReturnValue))
	}

	if _, err := storage.GetFinalizeIntent(context.Background(), db, id); err == nil {
		t.Error("intent still present, expected ErrNotFound after commit")
	}
}

func TestFinalizeFailedNoOutputsFastPath(t *testing.T) {
	id, dataDir, db := finalizeFixture(t)
	in := FinalizeInputs{
		MissionID: id,
		Outcome: OutcomeResult{
			Outcome:     "failed",
			FailReason:  "explicit",
			FailMessage: "boom",
			FailDetails: json.RawMessage(`{"k":"v"}`),
			ExitCode:    1,
		},
		Cfg: FinalizeConfig{DataDir: dataDir},
	}
	if err := Finalize(context.Background(), db, in); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	m := loadMission(t, db, id)
	if m.Status != storage.StatusDone {
		t.Errorf("Status=%q", m.Status)
	}
	if m.Outcome.String != "failed" {
		t.Errorf("Outcome=%q", m.Outcome.String)
	}
	if m.FailReason.String != "explicit" {
		t.Errorf("FailReason=%q", m.FailReason.String)
	}
	if m.FailMessage.String != "boom" {
		t.Errorf("FailMessage=%q", m.FailMessage.String)
	}
	if m.FailDetails.String != `{"k":"v"}` {
		t.Errorf("FailDetails=%q", m.FailDetails.String)
	}
	if m.ExitCode.Int64 != 1 {
		t.Errorf("ExitCode=%d", m.ExitCode.Int64)
	}
	events := loadEvents(t, dataDir, id)
	if len(events) != 2 || events[1]["outcome"] != "failed" {
		t.Errorf("events=%v", events)
	}
}

func TestFinalizeSuccessWithOutputsCommitsFinalPath(t *testing.T) {
	id, dataDir, db := finalizeFixture(t)

	// Stage one tmp output as if CollectOutputs ran.
	stagingID := ids.NewUUIDv7()
	shard, _ := ids.ShardPath(stagingID)
	stagingDir := filepath.Join(dataDir, "staging", shard)
	if err := os.MkdirAll(stagingDir, 0o755); err != nil {
		t.Fatalf("mkdir staging: %v", err)
	}
	tmpPath := filepath.Join(stagingDir, stagingID+".tmp")
	finalPath := filepath.Join(stagingDir, stagingID)
	if err := os.WriteFile(tmpPath, []byte("payload"), 0o644); err != nil {
		t.Fatalf("write tmp: %v", err)
	}
	co := CollectedOutput{
		Role: "result", StagingID: stagingID,
		TmpPath: tmpPath, FinalPath: finalPath,
		Sha256: "deadbeef", Size: 7,
	}

	in := FinalizeInputs{
		MissionID: id,
		Outcome:   OutcomeResult{Outcome: "success", ExitCode: 0},
		Outputs:   []CollectedOutput{co},
		Cfg:       FinalizeConfig{DataDir: dataDir},
	}
	if err := Finalize(context.Background(), db, in); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	if _, err := os.Stat(finalPath); err != nil {
		t.Errorf("final missing: %v", err)
	}
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Errorf("tmp still present (err=%v)", err)
	}

	m := loadMission(t, db, id)
	if m.Outcome.String != "success" {
		t.Errorf("Outcome=%q", m.Outcome.String)
	}

	sf, err := storage.GetStaging(context.Background(), db, stagingID)
	if err != nil {
		t.Fatalf("get staging: %v", err)
	}
	if sf.State != storage.StagingComplete {
		t.Errorf("Staging state=%q, want complete", sf.State)
	}

	refs, err := storage.RefsByMission(context.Background(), db, id)
	if err != nil {
		t.Fatalf("refs: %v", err)
	}
	if len(refs) != 1 || refs[0].StagingID != stagingID || refs[0].RefKind != storage.RefOutput || refs[0].Role != "result" {
		t.Errorf("refs=%v", refs)
	}

	if _, err := storage.GetFinalizeIntent(context.Background(), db, id); err == nil {
		t.Error("intent still present after commit")
	}

	events := loadEvents(t, dataDir, id)
	doneEv := events[len(events)-1]
	if doneEv["event"] != "done" {
		t.Fatalf("last event not done: %v", doneEv)
	}
	outs, _ := doneEv["outputs"].(map[string]any)
	if len(outs) != 1 {
		t.Errorf("outputs=%v", outs)
	}
}

// TestFinalizePartialCommitRevertsAllOutputs reproduces the case where
// Phase B step 2 (rename loop) fails at output index i: the
// previous revertFailedCommit invocation received outputs[:i+1] —
// only outputs up to and including the failing one. Outputs[i+1:]
// stayed in state='committing' with their tmp files orphaned on
// disk; the regular cleanup sweep doesn't transition 'committing'
// rows so they leaked forever (until manual repair).
//
// With 3 outputs and the FIRST rename failing (i=0), outputs at
// indexes 1 and 2 are the leaked ones the bug exhibits.
func TestFinalizePartialCommitRevertsAllOutputs(t *testing.T) {
	id, dataDir, db := finalizeFixture(t)

	type out struct {
		stagingID string
		tmpPath   string
		finalPath string
	}
	mk := func(role string) (CollectedOutput, out) {
		sid := ids.NewUUIDv7()
		shard, _ := ids.ShardPath(sid)
		stagingDir := filepath.Join(dataDir, "staging", shard)
		if err := os.MkdirAll(stagingDir, 0o755); err != nil {
			t.Fatal(err)
		}
		tmp := filepath.Join(stagingDir, sid+".tmp")
		final := filepath.Join(stagingDir, sid)
		if err := os.WriteFile(tmp, []byte("payload-"+role), 0o644); err != nil {
			t.Fatal(err)
		}
		return CollectedOutput{
			Role: role, StagingID: sid,
			TmpPath: tmp, FinalPath: final,
			Sha256: "sha-" + role, Size: int64(len("payload-" + role)),
		}, out{stagingID: sid, tmpPath: tmp, finalPath: final}
	}
	co0, o0 := mk("first")
	co1, o1 := mk("second")
	co2, o2 := mk("third")

	// Force rename of o0 to fail: put a non-empty directory at the
	// final path. os.Rename(file, dir) returns EISDIR/ENOTDIR.
	if err := os.MkdirAll(o0.finalPath, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(o0.finalPath, "block"), []byte{0}, 0o644); err != nil {
		t.Fatal(err)
	}

	in := FinalizeInputs{
		MissionID: id,
		Outcome:   OutcomeResult{Outcome: "success", ExitCode: 0},
		Outputs:   []CollectedOutput{co0, co1, co2},
		Cfg:       FinalizeConfig{DataDir: dataDir},
	}
	// Finalize returns the revert error from Phase B; we ignore it but
	// expect the mission to be marked failed-via-revert (not success).
	_ = Finalize(context.Background(), db, in)

	m := loadMission(t, db, id)
	if m.Outcome.String != "failed" || m.FailReason.String != "output_commit_failed" {
		t.Errorf("outcome=%q reason=%q, want failed/output_commit_failed",
			m.Outcome.String, m.FailReason.String)
	}

	// All three staging rows must transition to 'deleting' — the bug
	// would leave o1 and o2 in 'committing'.
	for _, o := range []out{o0, o1, o2} {
		sf, err := storage.GetStaging(context.Background(), db, o.stagingID)
		if err != nil {
			t.Errorf("%s: get staging: %v", o.stagingID, err)
			continue
		}
		if sf.State != storage.StagingDeleting {
			t.Errorf("staging %s state=%q, want deleting", o.stagingID, sf.State)
		}
	}

	// And the un-renamed tmp files (o1, o2) should have been cleaned up
	// rather than orphaned.
	for _, o := range []out{o1, o2} {
		if _, err := os.Stat(o.tmpPath); err == nil {
			t.Errorf("tmp %s still present after revert (orphan)", o.tmpPath)
		}
	}
}

func TestFinalizeIntentDurableBeforeFastPath(t *testing.T) {
	// Insert intent but bypass commit by stubbing the events file path to a
	// nonexistent dir (so AppendDoneIdempotent fails). We verify that A2
	// completed (intent present) before the failure.
	id, dataDir, db := finalizeFixture(t)

	// Remove the events file's parent so eventfile.Open fails.
	shard, _ := ids.ShardPath(id)
	if err := os.RemoveAll(filepath.Join(dataDir, "output", shard)); err != nil {
		t.Fatalf("rm: %v", err)
	}

	err := Finalize(context.Background(), db, FinalizeInputs{
		MissionID: id,
		Outcome:   OutcomeResult{Outcome: "success"},
		Cfg:       FinalizeConfig{DataDir: dataDir},
	})
	if err == nil {
		t.Fatal("expected open events failure")
	}
	// Phase A2 happens AFTER eventfile.Open in our impl, so no intent present.
	if _, err := storage.GetFinalizeIntent(context.Background(), db, id); err == nil {
		t.Error("intent should not be present after open-events failure")
	}
}

func TestFinalizeCapOutcomeReturnTooLarge(t *testing.T) {
	id, dataDir, db := finalizeFixture(t)
	big := strings.Repeat("x", 1024)
	ret := json.RawMessage(`"` + big + `"`)
	in := FinalizeInputs{
		MissionID: id,
		Outcome:   OutcomeResult{Outcome: "success", Return: ret},
		Cfg:       FinalizeConfig{DataDir: dataDir, MaxReturnValue: 100},
	}
	if err := Finalize(context.Background(), db, in); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	m := loadMission(t, db, id)
	if m.Outcome.String != "failed" {
		t.Errorf("Outcome=%q, want failed (return_too_large)", m.Outcome.String)
	}
	if m.FailReason.String != "return_too_large" {
		t.Errorf("FailReason=%q", m.FailReason.String)
	}
	if len(m.ReturnValue) != 0 {
		t.Errorf("ReturnValue not dropped: %d bytes", len(m.ReturnValue))
	}
}

// TestFinalizeReturnTooLargeDoesNotCommitOutputs pins the demotion rule: when
// capOutcome demotes a success to failed/return_too_large, the mission's
// collected output files must NOT be committed to staging (a
// non-success outcome never transfers outputs). A failed mission must end up
// with no committed staging row, no output ref, no final file on disk, and no
// "outputs" field in its done event — only the tmp copies removed.
func TestFinalizeReturnTooLargeDoesNotCommitOutputs(t *testing.T) {
	id, dataDir, db := finalizeFixture(t)

	stagingID := ids.NewUUIDv7()
	shard, _ := ids.ShardPath(stagingID)
	stagingDir := filepath.Join(dataDir, "staging", shard)
	if err := os.MkdirAll(stagingDir, 0o755); err != nil {
		t.Fatalf("mkdir staging: %v", err)
	}
	tmpPath := filepath.Join(stagingDir, stagingID+".tmp")
	finalPath := filepath.Join(stagingDir, stagingID)
	if err := os.WriteFile(tmpPath, []byte("payload"), 0o644); err != nil {
		t.Fatalf("write tmp: %v", err)
	}
	co := CollectedOutput{
		Role: "result", StagingID: stagingID,
		TmpPath: tmpPath, FinalPath: finalPath,
		Sha256: "deadbeef", Size: 7,
	}

	big := strings.Repeat("x", 1024)
	in := FinalizeInputs{
		MissionID: id,
		Outcome:   OutcomeResult{Outcome: "success", Return: json.RawMessage(`"` + big + `"`)},
		Outputs:   []CollectedOutput{co},
		Cfg:       FinalizeConfig{DataDir: dataDir, MaxReturnValue: 100},
	}
	if err := Finalize(context.Background(), db, in); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	m := loadMission(t, db, id)
	if m.Outcome.String != "failed" || m.FailReason.String != "return_too_large" {
		t.Fatalf("outcome=%q reason=%q, want failed/return_too_large", m.Outcome.String, m.FailReason.String)
	}

	// No output ref must be committed.
	refs, err := storage.RefsByMission(context.Background(), db, id)
	if err != nil {
		t.Fatalf("refs: %v", err)
	}
	if len(refs) != 0 {
		t.Errorf("refs=%v, want none (failed mission must not commit outputs)", refs)
	}

	// No staging row, no final file.
	if _, err := storage.GetStaging(context.Background(), db, stagingID); err == nil {
		t.Error("staging row present, want none (output dropped on demotion)")
	}
	if _, err := os.Stat(finalPath); !os.IsNotExist(err) {
		t.Errorf("final file present (err=%v), want absent", err)
	}

	// done event must not carry an outputs object.
	events := loadEvents(t, dataDir, id)
	doneEv := events[len(events)-1]
	if doneEv["event"] != "done" || doneEv["outcome"] != "failed" {
		t.Fatalf("last event=%v, want done/failed", doneEv)
	}
	if outs, ok := doneEv["outputs"].(map[string]any); ok && len(outs) != 0 {
		t.Errorf("done event carries outputs=%v on a failed mission", outs)
	}
}

// TestFinalizeDeletedMissionDiscardsOutcome: an admin delete can flip the
// row to status='deleting' while the process is finishing (the staging
// force-delete cascade does this to running missions). The terminal commit
// must not resurrect such a row to 'done' — its deletion was already
// acknowledged with 202 deletion_pending. Instead the outcome is discarded:
// the row keeps 'deleting', no mission fields are written, the intent is
// cleared, this finalize's output staging rows flip straight to 'deleting'
// (path pointed at the renamed final file so the staging GC tombstones the
// file that actually exists), no refs are inserted, and Finalize reports
// success so callers don't retry.
func TestFinalizeDeletedMissionDiscardsOutcome(t *testing.T) {
	id, dataDir, db := finalizeFixture(t)

	stagingID := ids.NewUUIDv7()
	shard, _ := ids.ShardPath(stagingID)
	stagingDir := filepath.Join(dataDir, "staging", shard)
	if err := os.MkdirAll(stagingDir, 0o755); err != nil {
		t.Fatalf("mkdir staging: %v", err)
	}
	tmpPath := filepath.Join(stagingDir, stagingID+".tmp")
	finalPath := filepath.Join(stagingDir, stagingID)
	if err := os.WriteFile(tmpPath, []byte("payload"), 0o644); err != nil {
		t.Fatalf("write tmp: %v", err)
	}
	co := CollectedOutput{
		Role: "result", StagingID: stagingID,
		TmpPath: tmpPath, FinalPath: finalPath,
		Sha256: "deadbeef", Size: 7,
	}

	// Admin delete wins the race before the terminal commit.
	if _, err := db.ExecContext(context.Background(),
		`UPDATE missions SET status='deleting' WHERE mission_id=?`, id); err != nil {
		t.Fatalf("flip to deleting: %v", err)
	}

	if err := Finalize(context.Background(), db, FinalizeInputs{
		MissionID: id,
		Outcome:   OutcomeResult{Outcome: "success", ExitCode: 0},
		Outputs:   []CollectedOutput{co},
		Cfg:       FinalizeConfig{DataDir: dataDir},
	}); err != nil {
		t.Fatalf("Finalize must absorb the deleted row, got: %v", err)
	}

	m := loadMission(t, db, id)
	if m.Status != storage.StatusDeleting {
		t.Errorf("Status=%q, want deleting (resurrected)", m.Status)
	}
	if m.Outcome.Valid {
		t.Errorf("Outcome=%q written onto a deleting row", m.Outcome.String)
	}
	if m.TimeFinishedMs.Valid {
		t.Errorf("time_finished=%d written onto a deleting row", m.TimeFinishedMs.Int64)
	}

	if _, err := storage.GetFinalizeIntent(context.Background(), db, id); err == nil {
		t.Error("intent still present, want deleted")
	}

	refs, err := storage.RefsByMission(context.Background(), db, id)
	if err != nil {
		t.Fatalf("refs: %v", err)
	}
	if len(refs) != 0 {
		t.Errorf("refs=%v, want none", refs)
	}

	sf, err := storage.GetStaging(context.Background(), db, stagingID)
	if err != nil {
		t.Fatalf("get staging: %v", err)
	}
	if sf.State != storage.StagingDeleting {
		t.Errorf("staging state=%q, want deleting", sf.State)
	}
	// Phase B already renamed tmp→final; the row must point at the final
	// file so the GC tombstone pass finds something to rename.
	wantRel := filepath.Join("staging", shard, stagingID)
	if sf.Path != wantRel {
		t.Errorf("staging path=%q, want %q", sf.Path, wantRel)
	}
	if _, err := os.Stat(finalPath); err != nil {
		t.Errorf("final file missing (GC needs it at the recorded path): %v", err)
	}
}

// TestFinalizeDeletedMissionFastPathNoError covers the same guard on the
// outputs=[] fast path (the shape crashed/lost/queued-kill finalizes take):
// nil error, the row keeps 'deleting', and no intent is left behind.
func TestFinalizeDeletedMissionFastPathNoError(t *testing.T) {
	id, dataDir, db := finalizeFixture(t)
	if _, err := db.ExecContext(context.Background(),
		`UPDATE missions SET status='deleting' WHERE mission_id=?`, id); err != nil {
		t.Fatalf("flip to deleting: %v", err)
	}
	if err := Finalize(context.Background(), db, FinalizeInputs{
		MissionID: id,
		Outcome:   OutcomeResult{Outcome: "failed", FailReason: "explicit", FailMessage: "boom", ExitCode: 1},
		Cfg:       FinalizeConfig{DataDir: dataDir},
	}); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	m := loadMission(t, db, id)
	if m.Status != storage.StatusDeleting || m.Outcome.Valid {
		t.Errorf("status=%q outcome-valid=%v, want deleting row untouched", m.Status, m.Outcome.Valid)
	}
	if _, err := storage.GetFinalizeIntent(context.Background(), db, id); err == nil {
		t.Error("intent still present, want deleted")
	}
}

func TestFinalizeCapOutcomeFailMessageTruncated(t *testing.T) {
	id, dataDir, db := finalizeFixture(t)
	big := strings.Repeat("a", 200)
	in := FinalizeInputs{
		MissionID: id,
		Outcome: OutcomeResult{
			Outcome:     "failed",
			FailReason:  "explicit",
			FailMessage: big,
			ExitCode:    1,
		},
		Cfg: FinalizeConfig{DataDir: dataDir, MaxFailMessage: 50},
	}
	if err := Finalize(context.Background(), db, in); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	m := loadMission(t, db, id)
	if int64(len(m.FailMessage.String)) != 50 {
		t.Errorf("FailMessage len=%d, want 50", len(m.FailMessage.String))
	}
	if !strings.HasSuffix(m.FailMessage.String, "[truncated]") {
		t.Errorf("FailMessage missing suffix: %q", m.FailMessage.String)
	}
}

func TestFinalizeCapOutcomeFailDetailsReplaced(t *testing.T) {
	id, dataDir, db := finalizeFixture(t)
	big := strings.Repeat("d", 1024)
	in := FinalizeInputs{
		MissionID: id,
		Outcome: OutcomeResult{
			Outcome:     "failed",
			FailReason:  "explicit",
			FailMessage: "x",
			FailDetails: json.RawMessage(`"` + big + `"`),
			ExitCode:    1,
		},
		Cfg: FinalizeConfig{DataDir: dataDir, MaxFailDetails: 100},
	}
	if err := Finalize(context.Background(), db, in); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	m := loadMission(t, db, id)
	if !strings.Contains(m.FailDetails.String, `"truncated":true`) {
		t.Errorf("FailDetails not replaced: %q", m.FailDetails.String)
	}
}

// TestFinalizeUpdatesStagingPathAfterRename guards against the regression
// where Phase A2 records staging_files.path = op.TmpPath but commitFinalize
// only flipped state to 'complete' without UPDATEing path. The result was a
// stale .tmp path that GET /v1/staging/{id} would then try to open and 500.
// TestFinalizeSetsOutputStagingTTLPerPolicy: output staging rows had
// time_expires hard-coded to nowMs+24h during Phase A2 and nothing
// recalculated them post-commit. Operators with success_ttl>24h then lost
// output downloads at 24h while the mission row lived on.
//
// Asserts: with success_ttl=72h, the staging row's time_expires after a
// successful commit lands inside the policy window, not at the 24h
// sentinel.
func TestFinalizeSetsOutputStagingTTLPerPolicy(t *testing.T) {
	id, dataDir, db := finalizeFixture(t)

	stagingID := ids.NewUUIDv7()
	shard, _ := ids.ShardPath(stagingID)
	stagingDir := filepath.Join(dataDir, "staging", shard)
	if err := os.MkdirAll(stagingDir, 0o755); err != nil {
		t.Fatalf("mkdir staging: %v", err)
	}
	tmpPath := filepath.Join(stagingDir, stagingID+".tmp")
	finalPath := filepath.Join(stagingDir, stagingID)
	if err := os.WriteFile(tmpPath, []byte("payload"), 0o644); err != nil {
		t.Fatalf("write tmp: %v", err)
	}

	successTTL := 72 * time.Hour
	now := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)

	co := CollectedOutput{
		Role: "result", StagingID: stagingID,
		TmpPath: tmpPath, FinalPath: finalPath,
		Sha256: "deadbeef", Size: 7,
	}
	if err := Finalize(context.Background(), db, FinalizeInputs{
		MissionID: id,
		Kind:      string(storage.KindMission),
		Lane:      "normal",
		Outcome:   OutcomeResult{Outcome: "success", ExitCode: 0},
		Outputs:   []CollectedOutput{co},
		Now:       func() time.Time { return now },
		Cfg: FinalizeConfig{
			DataDir: dataDir,
			TTL: storage.TTLPolicy{
				MissionSuccess: successTTL,
				MissionFailed:  7 * 24 * time.Hour,
				ExecSuccess:    time.Hour,
				ExecFailed:     24 * time.Hour,
				StagingTTL:     time.Hour,
				DownloadGrace:  time.Hour,
			},
		},
	}); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	sf, err := storage.GetStaging(context.Background(), db, stagingID)
	if err != nil {
		t.Fatalf("get staging: %v", err)
	}
	wantMs := now.Add(successTTL).UnixMilli()
	// Tolerate skew from time.Now() called inside commitFinalize for time_finished.
	const skewMs = 5_000
	if sf.TimeExpiresMs < wantMs-skewMs || sf.TimeExpiresMs > wantMs+skewMs {
		t.Errorf("time_expires=%d (delta=%dms from policy window @ %d) — looks like the 24h hardcode",
			sf.TimeExpiresMs, sf.TimeExpiresMs-wantMs, wantMs)
	}
}

// TestFinalizeSetsExecOutputStagingTTLPerPolicy: the recalc landed only on
// the mission path. runExec passed an empty TTLPolicy and the
// commitFinalize gate keyed on cfg.TTL.MissionSuccess>0, so exec output staging
// kept the 24h sentinel even when exec_success_ttl was larger. Here
// MissionSuccess=0 but ExecSuccess=48h: a kind='exec' success commit must land
// the staging time_expires in the exec policy window, not at 24h.
func TestFinalizeSetsExecOutputStagingTTLPerPolicy(t *testing.T) {
	id, dataDir, db := finalizeFixture(t)
	// Flip the seeded row to kind='exec' so RecalcStagingTTL picks ExecSuccess.
	if _, err := db.ExecContext(context.Background(),
		`UPDATE missions SET kind='exec' WHERE mission_id=?`, id); err != nil {
		t.Fatalf("set kind=exec: %v", err)
	}

	stagingID := ids.NewUUIDv7()
	shard, _ := ids.ShardPath(stagingID)
	stagingDir := filepath.Join(dataDir, "staging", shard)
	if err := os.MkdirAll(stagingDir, 0o755); err != nil {
		t.Fatalf("mkdir staging: %v", err)
	}
	tmpPath := filepath.Join(stagingDir, stagingID+".tmp")
	finalPath := filepath.Join(stagingDir, stagingID)
	if err := os.WriteFile(tmpPath, []byte("payload"), 0o644); err != nil {
		t.Fatalf("write tmp: %v", err)
	}

	execSuccessTTL := 48 * time.Hour
	now := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)

	co := CollectedOutput{
		Role: "png", StagingID: stagingID,
		TmpPath: tmpPath, FinalPath: finalPath,
		Sha256: "deadbeef", Size: 7,
	}
	if err := Finalize(context.Background(), db, FinalizeInputs{
		MissionID: id,
		Kind:      string(storage.KindExec),
		Lane:      "normal",
		Outcome:   OutcomeResult{Outcome: "success", ExitCode: 0},
		Outputs:   []CollectedOutput{co},
		Now:       func() time.Time { return now },
		Cfg: FinalizeConfig{
			DataDir: dataDir,
			TTL: storage.TTLPolicy{
				MissionSuccess: 0, // exercises the gate bug: mission TTL unset
				MissionFailed:  0,
				ExecSuccess:    execSuccessTTL,
				ExecFailed:     24 * time.Hour,
				StagingTTL:     time.Hour,
				DownloadGrace:  time.Hour,
			},
		},
	}); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	sf, err := storage.GetStaging(context.Background(), db, stagingID)
	if err != nil {
		t.Fatalf("get staging: %v", err)
	}
	wantMs := now.Add(execSuccessTTL).UnixMilli()
	const skewMs = 5_000
	if sf.TimeExpiresMs < wantMs-skewMs || sf.TimeExpiresMs > wantMs+skewMs {
		t.Errorf("exec output time_expires=%d (delta=%dms from exec policy window @ %d) — 24h hardcode / gate skip",
			sf.TimeExpiresMs, sf.TimeExpiresMs-wantMs, wantMs)
	}
}

func TestFinalizeUpdatesStagingPathAfterRename(t *testing.T) {
	id, dataDir, db := finalizeFixture(t)

	stagingID := ids.NewUUIDv7()
	shard, _ := ids.ShardPath(stagingID)
	stagingDir := filepath.Join(dataDir, "staging", shard)
	if err := os.MkdirAll(stagingDir, 0o755); err != nil {
		t.Fatalf("mkdir staging: %v", err)
	}
	tmpPath := filepath.Join(stagingDir, stagingID+".tmp")
	finalPath := filepath.Join(stagingDir, stagingID)
	if err := os.WriteFile(tmpPath, []byte("payload"), 0o644); err != nil {
		t.Fatalf("write tmp: %v", err)
	}
	co := CollectedOutput{
		Role: "result", StagingID: stagingID,
		TmpPath: tmpPath, FinalPath: finalPath,
		Sha256: "deadbeef", Size: 7,
	}

	if err := Finalize(context.Background(), db, FinalizeInputs{
		MissionID: id,
		Outcome:   OutcomeResult{Outcome: "success", ExitCode: 0},
		Outputs:   []CollectedOutput{co},
		Cfg:       FinalizeConfig{DataDir: dataDir},
	}); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	sf, err := storage.GetStaging(context.Background(), db, stagingID)
	if err != nil {
		t.Fatalf("get staging: %v", err)
	}
	if sf.State != storage.StagingComplete {
		t.Fatalf("Staging state=%q, want complete", sf.State)
	}
	if strings.HasSuffix(sf.Path, ".tmp") {
		t.Errorf("staging_files.path still points at .tmp: %q", sf.Path)
	}
	wantRel := filepath.Join("staging", shard, stagingID)
	if sf.Path != wantRel {
		t.Errorf("staging_files.path=%q, want %q", sf.Path, wantRel)
	}
	// The file referenced by the stored path must exist (the bug let the
	// download handler open a path that os.Rename had already moved away).
	abs := filepath.Join(dataDir, sf.Path)
	if _, err := os.Stat(abs); err != nil {
		t.Errorf("file at recorded path missing: %v (abs=%s)", err, abs)
	}
}

func TestFinalizeRevertOnRenameFailure(t *testing.T) {
	id, dataDir, db := finalizeFixture(t)

	stagingID := ids.NewUUIDv7()
	shard, _ := ids.ShardPath(stagingID)
	stagingDir := filepath.Join(dataDir, "staging", shard)
	if err := os.MkdirAll(stagingDir, 0o755); err != nil {
		t.Fatalf("mkdir staging: %v", err)
	}
	tmpPath := filepath.Join(stagingDir, stagingID+".tmp")
	if err := os.WriteFile(tmpPath, []byte("data"), 0o644); err != nil {
		t.Fatalf("write tmp: %v", err)
	}
	// Force rename failure: make finalPath an existing directory.
	finalPath := filepath.Join(stagingDir, stagingID)
	if err := os.MkdirAll(finalPath, 0o755); err != nil {
		t.Fatalf("mkdir final: %v", err)
	}
	if err := os.WriteFile(filepath.Join(finalPath, "blocker"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write blocker: %v", err)
	}

	co := CollectedOutput{
		Role: "result", StagingID: stagingID,
		TmpPath: tmpPath, FinalPath: finalPath,
		Sha256: "feedface", Size: 4,
	}

	err := Finalize(context.Background(), db, FinalizeInputs{
		MissionID: id,
		Outcome:   OutcomeResult{Outcome: "success"},
		Outputs:   []CollectedOutput{co},
		Cfg:       FinalizeConfig{DataDir: dataDir},
	})
	if err != nil {
		t.Fatalf("Finalize should durably revert: %v", err)
	}

	m := loadMission(t, db, id)
	if m.Outcome.String != "failed" {
		t.Errorf("Outcome=%q, want failed", m.Outcome.String)
	}
	if m.FailReason.String != "output_commit_failed" {
		t.Errorf("FailReason=%q, want output_commit_failed", m.FailReason.String)
	}
	sf, err := storage.GetStaging(context.Background(), db, stagingID)
	if err != nil {
		t.Fatalf("get staging: %v", err)
	}
	if sf.State != storage.StagingDeleting {
		t.Errorf("staging state=%q, want deleting", sf.State)
	}
	if _, err := storage.GetFinalizeIntent(context.Background(), db, id); err == nil {
		t.Error("intent should be deleted after final commit of revert")
	}
	events := loadEvents(t, dataDir, id)
	if events[len(events)-1]["outcome"] != "failed" {
		t.Errorf("done event outcome=%v, want failed", events[len(events)-1]["outcome"])
	}
}

// TestFinalizeDoneEventSpecShape verifies the done event shape exactly:
//   - time_finished (not "time")
//   - duration_ms = time_finished - time_started (when TimeStartedMs > 0)
//   - outputs is a JSON object keyed by role with {staging_id, sha256, size}
func TestFinalizeDoneEventSpecShape(t *testing.T) {
	id, dataDir, db := finalizeFixture(t)

	stagingID := ids.NewUUIDv7()
	shard, _ := ids.ShardPath(stagingID)
	stagingDir := filepath.Join(dataDir, "staging", shard)
	if err := os.MkdirAll(stagingDir, 0o755); err != nil {
		t.Fatalf("mkdir staging: %v", err)
	}
	tmpPath := filepath.Join(stagingDir, stagingID+".tmp")
	finalPath := filepath.Join(stagingDir, stagingID)
	if err := os.WriteFile(tmpPath, []byte("payload"), 0o644); err != nil {
		t.Fatalf("write tmp: %v", err)
	}
	co := CollectedOutput{
		Role: "result", StagingID: stagingID,
		TmpPath: tmpPath, FinalPath: finalPath,
		Sha256: "deadbeef", Size: 7,
	}

	const startedMs int64 = 1714600044000
	const finishedMs int64 = 1714600045234 // duration = 1234ms
	fixedNow := func() time.Time { return time.UnixMilli(finishedMs) }

	if err := Finalize(context.Background(), db, FinalizeInputs{
		MissionID:     id,
		Outcome:       OutcomeResult{Outcome: "success", ExitCode: 0, Return: json.RawMessage(`{"files_processed":42}`)},
		Outputs:       []CollectedOutput{co},
		Cfg:           FinalizeConfig{DataDir: dataDir},
		Now:           fixedNow,
		TimeStartedMs: startedMs,
	}); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	events := loadEvents(t, dataDir, id)
	doneEv := events[len(events)-1]
	if doneEv["event"] != "done" {
		t.Fatalf("last event not done: %v", doneEv)
	}

	// time_finished present, not "time"
	if _, ok := doneEv["time"]; ok {
		t.Errorf(`done event still uses "time"; want "time_finished" only: %v`, doneEv)
	}
	tf, ok := doneEv["time_finished"].(float64)
	if !ok {
		t.Fatalf("time_finished missing or wrong type: %v (%T)", doneEv["time_finished"], doneEv["time_finished"])
	}
	if int64(tf) != finishedMs {
		t.Errorf("time_finished=%d, want %d", int64(tf), finishedMs)
	}

	// duration_ms present and = time_finished - time_started.
	dm, ok := doneEv["duration_ms"].(float64)
	if !ok {
		t.Fatalf("duration_ms missing or wrong type: %v (%T)", doneEv["duration_ms"], doneEv["duration_ms"])
	}
	if int64(dm) != finishedMs-startedMs {
		t.Errorf("duration_ms=%d, want %d", int64(dm), finishedMs-startedMs)
	}

	// outputs is a map keyed by role with staging_id/sha256/size.
	outsRaw := doneEv["outputs"]
	outs, ok := outsRaw.(map[string]any)
	if !ok {
		t.Fatalf("outputs is %T %v, want JSON object", outsRaw, outsRaw)
	}
	got, ok := outs["result"].(map[string]any)
	if !ok {
		t.Fatalf("outputs.result missing or wrong type: %v", outs["result"])
	}
	if got["staging_id"] != stagingID {
		t.Errorf("outputs.result.staging_id=%v, want %s", got["staging_id"], stagingID)
	}
	if got["sha256"] != "deadbeef" {
		t.Errorf("outputs.result.sha256=%v, want deadbeef", got["sha256"])
	}
	if sz, _ := got["size"].(float64); int64(sz) != 7 {
		t.Errorf("outputs.result.size=%v, want 7", got["size"])
	}
	// no leaked "role" field — role is the map key
	if _, hasRole := got["role"]; hasRole {
		t.Errorf("outputs.result still carries redundant role key: %v", got)
	}
}

// TestFinalizeObservesMissionDoneMetric verifies that a successful Finalize
// call increments letts_missions_total{kind,lane,outcome} via the Prometheus
// observer wired in commitFinalize.
func TestFinalizeObservesMissionDoneMetric(t *testing.T) {
	id, dataDir, db := finalizeFixture(t)
	const startMs = 1_000_000
	in := FinalizeInputs{
		MissionID:     id,
		Kind:          "mission",
		Lane:          "normal",
		Outcome:       OutcomeResult{Outcome: "success", ExitCode: 0},
		Cfg:           FinalizeConfig{DataDir: dataDir},
		Now:           func() time.Time { return time.UnixMilli(startMs + 250) },
		TimeStartedMs: startMs,
	}

	before := readCounter(t, "letts_missions_total", map[string]string{
		"kind": "mission", "lane": "normal", "outcome": "success",
	})
	if err := Finalize(context.Background(), db, in); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	after := readCounter(t, "letts_missions_total", map[string]string{
		"kind": "mission", "lane": "normal", "outcome": "success",
	})
	if got := after - before; got != 1 {
		t.Fatalf("missions_total delta = %v, want 1", got)
	}
}

func readCounter(t *testing.T, name string, labels map[string]string) float64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			lbl := map[string]string{}
			for _, p := range m.GetLabel() {
				lbl[p.GetName()] = p.GetValue()
			}
			match := true
			for k, v := range labels {
				if lbl[k] != v {
					match = false
					break
				}
			}
			if match && m.Counter != nil {
				return m.Counter.GetValue()
			}
		}
	}
	return 0
}

// TestFinalizeDoneEventOmitsDurationWhenNoStart verifies that when
// TimeStartedMs == 0 (e.g. spawn_failed before MarkRunning), duration_ms is
// omitted rather than emitted with a nonsense value.
func TestFinalizeDoneEventOmitsDurationWhenNoStart(t *testing.T) {
	id, dataDir, db := finalizeFixture(t)
	if err := Finalize(context.Background(), db, FinalizeInputs{
		MissionID:     id,
		Outcome:       OutcomeResult{Outcome: "crashed", FailReason: "spawn_failed", FailMessage: "x"},
		Cfg:           FinalizeConfig{DataDir: dataDir},
		TimeStartedMs: 0,
	}); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	events := loadEvents(t, dataDir, id)
	doneEv := events[len(events)-1]
	if _, hasDuration := doneEv["duration_ms"]; hasDuration {
		t.Errorf("done event carries duration_ms when time_started unknown: %v", doneEv)
	}
	if _, hasTimeFinished := doneEv["time_finished"]; !hasTimeFinished {
		t.Errorf("done event missing time_finished: %v", doneEv)
	}
}
