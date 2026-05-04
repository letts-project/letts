package handlers_test

import (
	"context"
	"net/http/httptest"
	"sort"
	"testing"
	"time"

	"letts/internal/ids"
	"letts/internal/mission"
	"letts/internal/server/handlers"
	"letts/internal/storage"
)

func doStagingDelete(h *handlers.StagingHandler, id string, force bool) *httptest.ResponseRecorder {
	url := "/v1/staging/" + id
	if force {
		url += "?force=true"
	}
	r := httptest.NewRequest("DELETE", url, nil)
	r.SetPathValue("id", id)
	w := httptest.NewRecorder()
	h.Delete(w, r)
	return w
}

func TestStagingDeleteNoRefsMarksDeleting(t *testing.T) {
	h, db, id := setupStagingGet(t, storage.StagingComplete, []byte("hi"))
	w := doStagingDelete(h, id, false)
	if w.Code != 202 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	sf, _ := storage.GetStaging(context.Background(), db, id)
	if sf.State != storage.StagingDeleting {
		t.Errorf("state=%q", sf.State)
	}
}

func TestStagingDeleteRefsWithoutForceReturns409(t *testing.T) {
	h, db, id := setupStagingGet(t, storage.StagingComplete, []byte("x"))
	mID1 := bulkInsertMission(t, db, h.DataDir, storage.StatusDone)
	mID2 := bulkInsertMission(t, db, h.DataDir, storage.StatusRunning)
	_ = storage.InsertRef(context.Background(), db, storage.StagingRef{
		MissionID: mID1, StagingID: id, RefKind: storage.RefInput, Role: "a",
	})
	_ = storage.InsertRef(context.Background(), db, storage.StagingRef{
		MissionID: mID2, StagingID: id, RefKind: storage.RefOutput, Role: "b",
	})

	w := doStagingDelete(h, id, false)
	if w.Code != 409 {
		t.Fatalf("status=%d", w.Code)
	}
	got := parseJSON(t, w.Body.Bytes())
	if got["error"] != "staging_in_use" {
		t.Errorf("error=%v", got["error"])
	}
	details, _ := got["details"].(map[string]any)
	missionsRaw, _ := details["missions"].([]any)
	missions := make([]string, len(missionsRaw))
	for i, m := range missionsRaw {
		missions[i] = m.(string)
	}
	sort.Strings(missions)
	want := []string{mID1, mID2}
	sort.Strings(want)
	if len(missions) != 2 || missions[0] != want[0] || missions[1] != want[1] {
		t.Errorf("missions=%v, want %v", missions, want)
	}
	// Staging must NOT have been marked deleting.
	sf, _ := storage.GetStaging(context.Background(), db, id)
	if sf.State == storage.StagingDeleting {
		t.Error("staging marked deleting without force")
	}
}

func TestStagingDeleteForceCascades(t *testing.T) {
	h, db, id := setupStagingGet(t, storage.StagingComplete, []byte("x"))
	mID := bulkInsertMission(t, db, h.DataDir, storage.StatusDone)
	_ = storage.InsertRef(context.Background(), db, storage.StagingRef{
		MissionID: mID, StagingID: id, RefKind: storage.RefInput, Role: "a",
	})

	w := doStagingDelete(h, id, true)
	if w.Code != 202 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	sf, _ := storage.GetStaging(context.Background(), db, id)
	if sf.State != storage.StagingDeleting {
		t.Errorf("staging state=%q", sf.State)
	}
	mm, _ := storage.GetMission(context.Background(), db, mID)
	if mm.Status != storage.StatusDeleting {
		t.Errorf("mission state=%q, want deleting", mm.Status)
	}
}

func TestStagingDeleteAlreadyDeletingIsIdempotent(t *testing.T) {
	h, _, id := setupStagingGet(t, storage.StagingDeleting, []byte("x"))
	w := doStagingDelete(h, id, false)
	if w.Code != 202 {
		t.Errorf("status=%d", w.Code)
	}
}

func TestStagingDeleteMissingReturns404(t *testing.T) {
	h, _, _ := setupStagingPut(t)
	w := doStagingDelete(h, ids.NewUUIDv7(), false)
	if w.Code != 404 {
		t.Errorf("status=%d", w.Code)
	}
}

func TestStagingDeleteInvalidIDReturns400(t *testing.T) {
	h, _, _ := setupStagingPut(t)
	w := doStagingDelete(h, "bad", false)
	if w.Code != 400 {
		t.Errorf("status=%d", w.Code)
	}
}

// TestStagingDeleteForceKillsRunningMissionBeforeCascade: force-deleting a
// staging file referenced by a RUNNING mission must deliver a force-delete
// kill and wait for the mission to finalize before flipping anything to
// deleting — same contract as DELETE /v1/missions/{id}?force=true. Flipping
// a live running row would let the cleanup goroutine remove the row and
// files from under the process.
func TestStagingDeleteForceKillsRunningMissionBeforeCascade(t *testing.T) {
	h, db, id := setupStagingGet(t, storage.StagingComplete, []byte("x"))
	mID := bulkInsertMission(t, db, h.DataDir, storage.StatusRunning)
	if err := storage.InsertRef(context.Background(), db, storage.StagingRef{
		MissionID: mID, StagingID: id, RefKind: storage.RefInput, Role: "a",
	}); err != nil {
		t.Fatal(err)
	}
	rt := &fakeRuntime{available: true}
	rt.onSignal = func(missionID string, _ mission.ExternalKillReason) {
		// Simulate the runtime finalizing the killed mission shortly after.
		go func() {
			time.Sleep(40 * time.Millisecond)
			_, _ = db.ExecContext(context.Background(),
				`UPDATE missions SET status='done', outcome='killed', fail_reason='force_delete', exit_code=0 WHERE mission_id=?`, missionID)
		}()
	}
	h.Runtime = rt
	h.ForceDeleteTimeout = 2 * time.Second
	h.ForceDeletePoll = 10 * time.Millisecond

	w := doStagingDelete(h, id, true)
	if w.Code != 202 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	rt.mu.Lock()
	if len(rt.calls) != 1 || rt.calls[0].reason != mission.KillForceDelete {
		t.Errorf("kill calls=%v, want one force_delete kill", rt.calls)
	}
	rt.mu.Unlock()
	mm, _ := storage.GetMission(context.Background(), db, mID)
	if mm.Status != storage.StatusDeleting {
		t.Errorf("mission state=%q, want deleting (after finalize)", mm.Status)
	}
	sf, _ := storage.GetStaging(context.Background(), db, id)
	if sf.State != storage.StagingDeleting {
		t.Errorf("staging state=%q, want deleting", sf.State)
	}
}

// TestStagingDeleteForceTimesOutWhenMissionWontFinalize: when the killed
// mission does not finalize within the bounded wait, the staging
// force-delete answers 504 force_delete_timeout and leaves BOTH the mission
// and the staging row un-flipped — nothing may be handed to cleanup while
// the process is still alive.
func TestStagingDeleteForceTimesOutWhenMissionWontFinalize(t *testing.T) {
	h, db, id := setupStagingGet(t, storage.StagingComplete, []byte("x"))
	mID := bulkInsertMission(t, db, h.DataDir, storage.StatusRunning)
	if err := storage.InsertRef(context.Background(), db, storage.StagingRef{
		MissionID: mID, StagingID: id, RefKind: storage.RefInput, Role: "a",
	}); err != nil {
		t.Fatal(err)
	}
	h.Runtime = &fakeRuntime{available: true} // accepts the kill, never finalizes
	h.ForceDeleteTimeout = 100 * time.Millisecond
	h.ForceDeletePoll = 10 * time.Millisecond

	w := doStagingDelete(h, id, true)
	if w.Code != 504 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if parseJSON(t, w.Body.Bytes())["error"] != "force_delete_timeout" {
		t.Errorf("body=%s", w.Body.String())
	}
	mm, _ := storage.GetMission(context.Background(), db, mID)
	if mm.Status != storage.StatusRunning {
		t.Errorf("mission state=%q, want running (must not flip on timeout)", mm.Status)
	}
	sf, _ := storage.GetStaging(context.Background(), db, id)
	if sf.State == storage.StagingDeleting {
		t.Error("staging marked deleting despite force-delete timeout")
	}
}

// TestStagingDeleteForceConflictsWhenRefStartsRunningMidDelete: refs are
// re-read inside the writer transaction, so a referencing mission that
// enters 'running' after the kill pass (here: a brand-new ref inserted while
// the first kill is being processed) aborts the whole transaction with 409
// instead of being flipped to deleting while live.
func TestStagingDeleteForceConflictsWhenRefStartsRunningMidDelete(t *testing.T) {
	h, db, id := setupStagingGet(t, storage.StagingComplete, []byte("x"))
	mA := bulkInsertMission(t, db, h.DataDir, storage.StatusRunning)
	if err := storage.InsertRef(context.Background(), db, storage.StagingRef{
		MissionID: mA, StagingID: id, RefKind: storage.RefInput, Role: "a",
	}); err != nil {
		t.Fatal(err)
	}
	var mC string
	rt := &fakeRuntime{available: true}
	rt.onSignal = func(missionID string, _ mission.ExternalKillReason) {
		// While mission A is being killed, a new RUNNING mission C gains a
		// ref to the same staging file (e.g. a queued dispatch landing and
		// getting picked up). The kill pass only saw A.
		mC = bulkInsertMission(t, db, h.DataDir, storage.StatusRunning)
		if err := storage.InsertRef(context.Background(), db, storage.StagingRef{
			MissionID: mC, StagingID: id, RefKind: storage.RefInput, Role: "c",
		}); err != nil {
			t.Error(err)
		}
		_, _ = db.ExecContext(context.Background(),
			`UPDATE missions SET status='done', outcome='killed', fail_reason='force_delete', exit_code=0 WHERE mission_id=?`, missionID)
	}
	h.Runtime = rt
	h.ForceDeleteTimeout = 2 * time.Second
	h.ForceDeletePoll = 10 * time.Millisecond

	w := doStagingDelete(h, id, true)
	if w.Code != 409 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	got := parseJSON(t, w.Body.Bytes())
	if got["error"] != "staging_in_use" {
		t.Errorf("error=%v", got["error"])
	}
	details, _ := got["details"].(map[string]any)
	missionsRaw, _ := details["missions"].([]any)
	if len(missionsRaw) != 1 || missionsRaw[0] != mC {
		t.Errorf("conflict missions=%v, want [%s]", missionsRaw, mC)
	}
	// The aborted transaction must leave everything un-flipped.
	sf, _ := storage.GetStaging(context.Background(), db, id)
	if sf.State == storage.StagingDeleting {
		t.Error("staging marked deleting despite still-running ref")
	}
	ma, _ := storage.GetMission(context.Background(), db, mA)
	if ma.Status != storage.StatusDone {
		t.Errorf("mission A state=%q, want done (rollback must not flip it)", ma.Status)
	}
	mc, _ := storage.GetMission(context.Background(), db, mC)
	if mc.Status != storage.StatusRunning {
		t.Errorf("mission C state=%q, want running (untouched)", mc.Status)
	}
}

// TestStagingDeleteForceFlipsQueuedReferencingMission: a queued referencing
// mission needs no kill — it flips straight to deleting (deletion removes
// the record; no terminal event is owed, matching the mission-DELETE
// contract for queued rows).
func TestStagingDeleteForceFlipsQueuedReferencingMission(t *testing.T) {
	h, db, id := setupStagingGet(t, storage.StagingComplete, []byte("x"))
	mID := bulkInsertMission(t, db, h.DataDir, storage.StatusQueued)
	if err := storage.InsertRef(context.Background(), db, storage.StagingRef{
		MissionID: mID, StagingID: id, RefKind: storage.RefInput, Role: "a",
	}); err != nil {
		t.Fatal(err)
	}
	rt := &fakeRuntime{available: true}
	h.Runtime = rt

	w := doStagingDelete(h, id, true)
	if w.Code != 202 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	rt.mu.Lock()
	if len(rt.calls) != 0 {
		t.Errorf("kill sent for queued mission: %v", rt.calls)
	}
	rt.mu.Unlock()
	mm, _ := storage.GetMission(context.Background(), db, mID)
	if mm.Status != storage.StatusDeleting {
		t.Errorf("mission state=%q, want deleting", mm.Status)
	}
}

func TestStagingDeleteForceDedupsMissions(t *testing.T) {
	h, db, id := setupStagingGet(t, storage.StagingComplete, []byte("x"))
	mID := bulkInsertMission(t, db, h.DataDir, storage.StatusDone)
	// Two refs for same mission (e.g., input and output).
	_ = storage.InsertRef(context.Background(), db, storage.StagingRef{
		MissionID: mID, StagingID: id, RefKind: storage.RefInput, Role: "a",
	})
	_ = storage.InsertRef(context.Background(), db, storage.StagingRef{
		MissionID: mID, StagingID: id, RefKind: storage.RefOutput, Role: "b",
	})
	w := doStagingDelete(h, id, true)
	if w.Code != 202 {
		t.Fatalf("status=%d", w.Code)
	}
	mm, _ := storage.GetMission(context.Background(), db, mID)
	if mm.Status != storage.StatusDeleting {
		t.Errorf("mission state=%q", mm.Status)
	}
}
