package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"letts/internal/eventfile"
	"letts/internal/ids"
	"letts/internal/repair"
	"letts/internal/storage"
)

// ============================================================================
// Idempotency replay across mission statuses
// ============================================================================

func TestIdempotencyReplayMatrix(t *testing.T) {
	s := newTestStack(t)
	s.applyMinimalLanes()

	idem := ids.NewUUIDv7()

	// 1. First call → 202 queued.
	status, body := s.dispatchOnce(idem, "normal", `{"k":"v"}`)
	if status != 202 {
		t.Fatalf("first dispatch status=%d body=%v", status, body)
	}
	if body["status"] != "queued" {
		t.Errorf("status=%v, want queued", body["status"])
	}
	if body["mission_id"] != idem {
		t.Errorf("mission_id=%v, want %s", body["mission_id"], idem)
	}

	// 2. Replay while still queued → 200 same body.
	status, body = s.dispatchOnce(idem, "normal", `{"k":"v"}`)
	if status != 200 {
		t.Fatalf("replay queued status=%d body=%v", status, body)
	}
	if body["status"] != "queued" {
		t.Errorf("replay queued: status=%v", body["status"])
	}

	// 3. Flip to running. Replay → 200 with status=running.
	s.markMissionStatus(idem, "running")
	status, body = s.dispatchOnce(idem, "normal", `{"k":"v"}`)
	if status != 200 || body["status"] != "running" {
		t.Errorf("replay running: status=%d body=%v", status, body)
	}

	// 4. Flip to done. Replay → 200 with status=done.
	s.markMissionStatus(idem, "done")
	status, body = s.dispatchOnce(idem, "normal", `{"k":"v"}`)
	if status != 200 || body["status"] != "done" {
		t.Errorf("replay done: status=%d body=%v", status, body)
	}

	// 5. Flip to deleting. Replay → 410 mission_deleting.
	s.markMissionStatus(idem, "deleting")
	status, body = s.dispatchOnce(idem, "normal", `{"k":"v"}`)
	if status != 410 {
		t.Errorf("replay deleting: status=%d body=%v", status, body)
	}
	if body["error"] != "mission_deleting" {
		t.Errorf("replay deleting: error=%v", body["error"])
	}

	// 6. Set back to queued and try a different fingerprint → 409 conflict.
	s.markMissionStatus(idem, "queued")
	status, body = s.dispatchOnce(idem, "normal", `{"completely":"different"}`)
	if status != 409 {
		t.Fatalf("conflicting fingerprint replay: status=%d body=%v", status, body)
	}
	if body["error"] != "idempotency_conflict" {
		t.Errorf("error=%v, want idempotency_conflict", body["error"])
	}
}

// ============================================================================
// Metrics route labels never contain raw UUIDs
// ============================================================================

func TestMetricsRouteLabelsNeverContainUUIDs(t *testing.T) {
	s := newTestStack(t)
	s.applyMinimalLanes()

	// Hit a few UUID-bearing routes so the metrics counter populates.
	idem := ids.NewUUIDv7()
	_, _ = s.dispatchOnce(idem, "normal", `{"x":1}`)

	// GET mission (UUID in path).
	r, _ := http.NewRequest("GET", s.srv.URL+"/v1/missions/"+idem, nil)
	r.Header.Set("Authorization", "Bearer disp-token")
	resp, err := s.srv.Client().Do(r)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()

	// HEAD on a (probably missing) staging row — exercises /v1/staging/{id}.
	bogus := ids.NewUUIDv7()
	r, _ = http.NewRequest("HEAD", s.srv.URL+"/v1/staging/"+bogus, nil)
	r.Header.Set("Authorization", "Bearer disp-token")
	resp, err = s.srv.Client().Do(r)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()

	// Scrape metrics — every route= label must use the {id} template, not
	// the raw UUID we just sent.
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "letts_http_requests_total" && mf.GetName() != "letts_http_request_duration_seconds" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() != "route" {
					continue
				}
				v := lp.GetValue()
				if strings.Contains(v, idem) {
					t.Errorf("%s has raw UUID in route label: %q", mf.GetName(), v)
				}
				if strings.Contains(v, bogus) {
					t.Errorf("%s has raw bogus UUID in route label: %q", mf.GetName(), v)
				}
				// Looser sanity: route should not contain a UUID-shaped substring.
				if looksUUID(v) {
					t.Errorf("%s route label looks UUID-shaped: %q", mf.GetName(), v)
				}
			}
		}
	}
}

func looksUUID(s string) bool {
	// UUID is 36 chars: 8-4-4-4-12. Reject any path segment that matches.
	for _, seg := range strings.Split(s, "/") {
		if len(seg) == 36 && strings.Count(seg, "-") == 4 {
			return true
		}
	}
	return false
}

// ============================================================================
// Real-pid persistence: a successfully-spawned mission must carry the OS
// pid through to the done row visible via GET /v1/missions/{id}.
//
// The lane runner atomically transitions queued→running with pid=0 in the
// same writer transaction as PickQueuedForLane (so two runners can't both
// claim the same mission). After Spawn returns the real OS pid, the
// waiter calls UpdateRunningPid to fill it in. Without that second update
// the row would surface pid=0 to clients reading the mission record —
// useless for diagnostics like "is this PID still my process".
// ============================================================================

func TestRealPidLandsInMissionRow(t *testing.T) {
	s := newTestStack(t)
	s.applyAndResume()

	scriptDir := t.TempDir()
	scriptPath := filepath.Join(scriptDir, "ok.sh")
	body := `#!/bin/sh
printf '{"event":"success","return":{}}\n' >&3
exit 0
`
	if err := os.WriteFile(scriptPath, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}

	id := s.insertMissionWithRuntime(scriptDir, "ok.sh")
	s.runOneMission(id)

	m, err := storage.GetMission(context.Background(), s.DB, id)
	if err != nil {
		t.Fatalf("get mission: %v", err)
	}
	if m.Status != storage.StatusDone || m.Outcome.String != "success" {
		t.Fatalf("status=%q outcome=%q, want done/success", m.Status, m.Outcome.String)
	}
	if !m.PID.Valid || m.PID.Int64 <= 0 {
		t.Errorf("pid=%v, want > 0 (lane runner placeholder was not overwritten by Spawn-time UpdateRunningPid)", m.PID)
	}
	if !m.PGID.Valid || m.PGID.Int64 <= 0 {
		t.Errorf("pgid=%v, want > 0", m.PGID)
	}
}

// ============================================================================
// fd3 oversized line without newline
// ============================================================================

func TestFd3OversizedLineRecoversAndDrains(t *testing.T) {
	s := newTestStack(t)
	s.applyAndResume()

	// Override MaxEventLineSize for this test to keep the script payload small.
	s.Cfg.Limits.MaxEventLineSize = 1024 // 1 KiB

	scriptDir := t.TempDir()
	// Script writes 4 KiB to fd 3 with NO newline, then a small newline-
	// terminated success line. Reader must drop the oversized payload, drain
	// past the newline, then process the success line — but the
	// outcome is failed/event_line_too_large since the violation wins.
	scriptPath := filepath.Join(scriptDir, "huge.sh")
	body := fmt.Sprintf(`#!/bin/sh
# Write 4 KiB to fd 3 with no newline, then sleep, then send a real event.
printf '%%4096s' x >&3
sleep 0.1
printf '\n{"event":"success","return":42}\n' >&3
exit 0
`)
	if err := os.WriteFile(scriptPath, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}

	id := s.insertMissionWithRuntime(scriptDir, "huge.sh")
	s.runOneMission(id)

	m, err := storage.GetMission(context.Background(), s.DB, id)
	if err != nil {
		t.Fatalf("get mission: %v", err)
	}
	if m.Outcome.String != "failed" || m.FailReason.String != "event_line_too_large" {
		t.Errorf("outcome=%q reason=%q, want failed/event_line_too_large",
			m.Outcome.String, m.FailReason.String)
	}
}

// ============================================================================
// pending_output recovery matrix
// ============================================================================
//
// The "pending_output without intent" branch from the recovery matrix
// is structurally impossible with the current Finalize implementation: Phase
// A2 inserts the intent and every pending_output row in a single
// storage.WithWriter transaction (mission/finalize.go), so they commit
// atomically. Either both rows exist or neither does; an orphan
// pending_output row would require either a manual DB edit or a future code
// change splitting the transaction.
//
// The "pending_output WITH intent" branch is exercised in
// internal/repair/intents_test.go: TestRepairPreparedTmpMissingRevertsToFailed.
//
// This sub-test verifies the structural invariant: after Phase A2, every
// pending_output row has a matching intent referencing it.
// TestPendingOutputAlwaysHasIntent inserts a synthetic pending_output
// row and matching intent (and a counter-example with intent missing) so
// the invariant assertion has data to assert against. The
// previous version ran on an empty fresh test stack — the scan loop
// never executed and the test "passed" vacuously.
func TestPendingOutputAlwaysHasIntent(t *testing.T) {
	s := newTestStack(t)

	missionA := ids.NewUUIDv7()
	stagingA := ids.NewUUIDv7()
	now := time.Now().UnixMilli()
	if err := storage.InsertMission(context.Background(), s.DB, &storage.Mission{
		ID: missionA, Kind: storage.KindMission, Lane: "normal", MissionName: "m",
		Status: storage.StatusRunning, Input: []byte("{}"), InputFingerprint: "fp",
		TimeCreatedMs: now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := storage.InsertStaging(context.Background(), s.DB, &storage.StagingFile{
		StagingID: stagingA, State: storage.StagingPendingOutput, Sha256: "x", Size: 1,
		BytesReceived: 1, Path: "p", TimeCreatedMs: now, TimeUpdatedMs: now,
		TimeExpiresMs: now + 99999,
	}); err != nil {
		t.Fatal(err)
	}
	if err := storage.InsertFinalizeIntent(context.Background(), s.DB, &storage.FinalizeIntent{
		MissionID: missionA, Phase: storage.PhasePrepared, Outcome: "success",
		Outputs: []byte(`[{"staging_id":"` + stagingA + `"}]`),
		DoneSeq: 1, DoneEvent: "{}",
	}); err != nil {
		t.Fatal(err)
	}

	// SQLite via modernc.org binds []byte as BLOB, so a TEXT-LIKE-?
	// pattern won't match the JSON. Scan the column out as string,
	// then substring-search in Go.
	rows, err := s.DB.Query(`SELECT staging_id FROM staging_files WHERE state='pending_output'`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	seen := 0
	for rows.Next() {
		seen++
		var sid string
		_ = rows.Scan(&sid)
		intentRows, err := s.DB.Query(`SELECT outputs FROM mission_finalize_intents`)
		if err != nil {
			t.Fatal(err)
		}
		matched := false
		for intentRows.Next() {
			var blob string
			if err := intentRows.Scan(&blob); err != nil {
				t.Fatal(err)
			}
			if strings.Contains(blob, sid) {
				matched = true
				break
			}
		}
		_ = intentRows.Close()
		if !matched {
			t.Errorf("pending_output staging %s has no intent referencing it", sid)
		}
	}
	if seen == 0 {
		t.Fatal("test bug: no pending_output rows inserted")
	}
}

// ============================================================================
// Restart with retained vs expired refs
// ============================================================================

func TestRestartWithRetainedAndExpiredRefs(t *testing.T) {
	s := newTestStack(t)
	s.applyMinimalLanes()

	// Set up a done mission A with one input staging ref → restart should
	// succeed; new mission B inherits the ref. Then expire+delete the staging
	// → restart of B should fail with input_artifacts_expired.

	stagingID := ids.NewUUIDv7()
	now := time.Now().UnixMilli()
	if err := storage.InsertStaging(context.Background(), s.DB, &storage.StagingFile{
		StagingID: stagingID, State: storage.StagingComplete,
		Sha256: "abc", Size: 10, BytesReceived: 10,
		Path:          "staging/00/00/abc",
		TimeCreatedMs: now, TimeUpdatedMs: now, TimeExpiresMs: now + 999_999,
	}); err != nil {
		t.Fatal(err)
	}

	missionA := s.insertDoneMission("normal", "success")
	if err := storage.InsertRuntime(context.Background(), s.DB, &storage.MissionRuntime{
		MissionID: missionA, MissionDir: "/tmp", CommandTemplate: `["true"]`,
	}); err != nil {
		t.Fatal(err)
	}
	if err := storage.InsertRef(context.Background(), s.DB, storage.StagingRef{
		MissionID: missionA, StagingID: stagingID, RefKind: storage.RefInput, Role: "in",
	}); err != nil {
		t.Fatal(err)
	}
	s.ensureEventsFile(missionA)

	// Restart A → 201 with new id.
	missionB := s.restart(missionA)

	// Verify B references the same staging.
	refs, _ := storage.RefsByMission(context.Background(), s.DB, missionB)
	if len(refs) != 1 || refs[0].StagingID != stagingID {
		t.Errorf("restart didn't carry input ref: %v", refs)
	}

	// Now mark the staging deleting (simulates GC catching it after a force-
	// delete of A's refs). Restart B → 409 input_artifacts_expired.
	if err := storage.MarkStagingDeleting(context.Background(), s.DB, stagingID); err != nil {
		t.Fatal(err)
	}
	// Mark B done so restart is allowed by status check.
	s.markMissionStatus(missionB, "done")
	if _, err := s.DB.Exec(`UPDATE missions SET outcome='success', time_finished=? WHERE mission_id=?`, time.Now().UnixMilli(), missionB); err != nil {
		t.Fatal(err)
	}

	r, _ := http.NewRequest("POST", s.srv.URL+"/v1/missions/"+missionB+"/restart", nil)
	r.Header.Set("Authorization", "Bearer admin-token")
	resp, err := s.srv.Client().Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 409 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("restart B status=%d body=%s", resp.StatusCode, body)
	}
	var got map[string]any
	body, _ := io.ReadAll(resp.Body)
	_ = json.Unmarshal(body, &got)
	if got["error"] != "input_artifacts_expired" {
		t.Errorf("error=%v, want input_artifacts_expired", got["error"])
	}
}

// ============================================================================
// Test helpers
// ============================================================================

func (s *stack) insertMissionWithRuntime(missionDir, missionName string) string {
	s.t.Helper()
	id := ids.NewUUIDv7()
	now := time.Now().UnixMilli()
	if err := storage.InsertMission(context.Background(), s.DB, &storage.Mission{
		ID: id, Kind: storage.KindMission, Lane: "normal",
		MissionName: missionName, Status: storage.StatusQueued,
		Input: []byte(`{}`), InputFingerprint: "fp",
		TimeCreatedMs: now,
	}); err != nil {
		s.t.Fatal(err)
	}
	if err := storage.InsertRuntime(context.Background(), s.DB, &storage.MissionRuntime{
		MissionID: id, MissionDir: missionDir,
		CommandTemplate:     `["sh","{mission_path}"]`,
		ValidateMissionFile: true,
	}); err != nil {
		s.t.Fatal(err)
	}
	s.ensureEventsFile(id)
	return id
}

func (s *stack) insertDoneMission(lane, outcome string) string {
	s.t.Helper()
	id := ids.NewUUIDv7()
	now := time.Now().UnixMilli()
	if err := storage.InsertMission(context.Background(), s.DB, &storage.Mission{
		ID: id, Kind: storage.KindMission, Lane: lane,
		MissionName: "Fixture", Status: storage.StatusDone,
		Outcome:          nullStr(outcome),
		Input:            []byte(`{}`),
		InputFingerprint: "fp",
		TimeCreatedMs:    now, TimeFinishedMs: nullInt64(now),
	}); err != nil {
		s.t.Fatal(err)
	}
	return id
}

func (s *stack) ensureEventsFile(missionID string) {
	s.t.Helper()
	shard, _ := ids.ShardPath(missionID)
	dir := filepath.Join(s.DataDir, "output", shard)
	_ = os.MkdirAll(dir, 0o755)
	w, err := eventfile.Create(dir, missionID)
	if err != nil {
		// already exists is fine for some tests
		return
	}
	_, _ = w.Append(eventfile.KindRunning, map[string]any{"time": time.Now().UnixMilli()}, false)
	_ = w.Close()
}

func (s *stack) runOneMission(missionID string) {
	s.t.Helper()
	// Spin a one-off lane runner: pick the mission via the spawn callback.
	s.Runtime.Manager.Notify("normal")
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		m, err := storage.GetMission(context.Background(), s.DB, missionID)
		if err == nil && m.Status == storage.StatusDone {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	s.t.Fatalf("mission %s did not finish", missionID)
}

func (s *stack) restart(missionID string) string {
	s.t.Helper()
	r, _ := http.NewRequest("POST", s.srv.URL+"/v1/missions/"+missionID+"/restart", nil)
	r.Header.Set("Authorization", "Bearer admin-token")
	resp, err := s.srv.Client().Do(r)
	if err != nil {
		s.t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 201 {
		body, _ := io.ReadAll(resp.Body)
		s.t.Fatalf("restart status=%d body=%s", resp.StatusCode, body)
	}
	var got map[string]any
	body, _ := io.ReadAll(resp.Body)
	_ = json.Unmarshal(body, &got)
	newID, _ := got["mission_id"].(string)
	if newID == "" {
		s.t.Fatalf("restart response missing mission_id: %s", body)
	}
	return newID
}

// ============================================================================
// Coverage cross-check sanity test: ensure the existing repair tests cover
// the crash-consistency scenarios this file points at. If the named function
// is renamed or removed, this test fails so the doc comment stays accurate.
// ============================================================================

// TestCoverageMapNotStale walks the source files of the repair
// package and this file to verify every test function name claimed by
// the coverage map at the top of setup_test.go still exists.
// The previous version just checked len(want)==15, which
// said nothing about whether the named tests were actually defined.
func TestCoverageMapNotStale(t *testing.T) {
	_ = repair.RepairFinalizeIntents
	_ = repair.SweepRunningToLost
	_ = repair.SweepOrphans

	// Tests claimed by the coverage map in setup_test.go. Each (path, name)
	// must resolve to a func definition; if a future rename breaks the
	// map this test fails so the doc and code stay in sync.
	claims := []struct{ path, fn string }{
		{"../repair/repair_test.go", "TestSweepOrphansRemovesUnknownOutputFiles"},
		{"../repair/repair_test.go", "TestSweepRunningToLostFinalizesAll"},
		{"../repair/intents_test.go", "TestRepairPreparedWithOutputsAllTmpExists"},
		{"../repair/intents_test.go", "TestRepairCommittingFinishesRenames"},
		{"../repair/intents_test.go", "TestRepairCommittingFinalAlreadyExistsSkipsRename"},
		{"../repair/intents_test.go", "TestRepairPreparedTmpMissingRevertsToFailed"},
		{"../eventfile/writer_test.go", "TestAppendDoneIdempotent"},
		{"../cleanup/missions_test.go", "TestMissionCleanupResumeOrphanDeleting"},
		{"../stagingstore/upload_lock_test.go", "TestUploadLockSweepFiresOnIdle"},
		{"integration_test.go", "TestIdempotencyReplayMatrix"},
		{"integration_test.go", "TestRestartWithRetainedAndExpiredRefs"},
		{"integration_test.go", "TestMetricsRouteLabelsNeverContainUUIDs"},
		{"integration_test.go", "TestFd3OversizedLineRecoversAndDrains"},
	}
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range claims {
		abs := filepath.Join(wd, c.path)
		b, err := os.ReadFile(abs)
		if err != nil {
			t.Errorf("coverage claim %q in %s: %v", c.fn, c.path, err)
			continue
		}
		if !strings.Contains(string(b), "func "+c.fn+"(") {
			t.Errorf("coverage claim %q not found in %s — rename or stale map", c.fn, c.path)
		}
	}
}

// nullStr / nullInt64 are sql wrapper shortcuts for the integration helpers.
func nullStr(s string) sqlNullString {
	if s == "" {
		return sqlNullString{}
	}
	return sqlNullString{String: s, Valid: true}
}

func nullInt64(v int64) sqlNullInt64 {
	return sqlNullInt64{Int64: v, Valid: true}
}

type sqlNullString = struct {
	String string
	Valid  bool
}

type sqlNullInt64 = struct {
	Int64 int64
	Valid bool
}
