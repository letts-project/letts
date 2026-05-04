package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"letts/internal/ids"
)

func setupDB(t *testing.T) *sql.DB {
	t.Helper()
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "state.db"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := Migrate(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestInsertGetMission(t *testing.T) {
	db := setupDB(t)
	id := ids.NewUUIDv7()
	m := Mission{
		ID:               id,
		Kind:             KindMission,
		Lane:             "normal",
		MissionName:      "BookCalc",
		Status:           StatusQueued,
		Input:            []byte(`{"k":"v"}`),
		InputFingerprint: "abc",
		TimeCreatedMs:    1700000000000,
	}
	if err := InsertMission(context.Background(), db, &m); err != nil {
		t.Fatal(err)
	}
	got, err := GetMission(context.Background(), db, id)
	if err != nil {
		t.Fatal(err)
	}
	if got.MissionName != "BookCalc" {
		t.Errorf("got name %q", got.MissionName)
	}
	if got.Status != StatusQueued {
		t.Errorf("got status %q", got.Status)
	}
	if string(got.Input) != `{"k":"v"}` {
		t.Errorf("got input %q", got.Input)
	}
}

// TestListMissionsMissionSubstringFilter pins the behavior added
// in 05cd2b2: the `mission` filter is a case-insensitive substring match (SQLite
// LIKE '%x%', case-insensitive for the ASCII-only mission names), not an
// exact match. arby's missions search box relies on this.
func TestListMissionsMissionSubstringFilter(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()
	for _, name := range []string{"ArbyDeploy", "deploy-worker", "BookCalc"} {
		if err := InsertMission(ctx, db, &Mission{
			ID:               ids.NewUUIDv7(),
			Kind:             KindMission,
			Lane:             "normal",
			MissionName:      name,
			Status:           StatusQueued,
			Input:            []byte("{}"),
			InputFingerprint: "x",
			TimeCreatedMs:    1700000000000,
		}); err != nil {
			t.Fatal(err)
		}
	}
	names := func(mission string) []string {
		rows, err := ListMissions(ctx, db, ListFilter{Mission: mission}, nil, 100)
		if err != nil {
			t.Fatal(err)
		}
		out := make([]string, 0, len(rows))
		for _, m := range rows {
			out = append(out, m.MissionName)
		}
		return out
	}
	if got := names("deploy"); len(got) != 2 { // substring, matches both *deploy* names
		t.Errorf(`mission="deploy" → %v, want the two deploy missions`, got)
	}
	if got := names("ARBY"); len(got) != 1 || got[0] != "ArbyDeploy" { // case-insensitive
		t.Errorf(`mission="ARBY" → %v, want [ArbyDeploy]`, got)
	}
	if got := names("zzz"); len(got) != 0 { // no match
		t.Errorf(`mission="zzz" → %v, want none`, got)
	}
}

func TestListMissionsMissionPrefixFilter(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()
	for _, name := range []string{"deploy-web", "deploy-db", "rollback-web"} {
		if err := InsertMission(ctx, db, &Mission{
			ID:               ids.NewUUIDv7(),
			Kind:             KindMission,
			Lane:             "normal",
			MissionName:      name,
			Status:           StatusQueued,
			Input:            []byte("{}"),
			InputFingerprint: "x",
			TimeCreatedMs:    1700000000000,
		}); err != nil {
			t.Fatal(err)
		}
	}
	names := func(prefix string) []string {
		rows, err := ListMissions(ctx, db, ListFilter{MissionPrefix: prefix}, nil, 100)
		if err != nil {
			t.Fatal(err)
		}
		out := make([]string, 0, len(rows))
		for _, m := range rows {
			out = append(out, m.MissionName)
		}
		return out
	}
	// Anchored prefix: both deploy-* names match.
	if got := names("deploy"); len(got) != 2 {
		t.Errorf(`prefix="deploy" → %v, want the two deploy missions`, got)
	}
	// "web" is a substring of two names but a prefix of none → no match. This is
	// what distinguishes mission_prefix from the substring `mission` filter.
	if got := names("web"); len(got) != 0 {
		t.Errorf(`prefix="web" → %v, want none (prefix is anchored at the start)`, got)
	}
}

func TestGetMissionNotFound(t *testing.T) {
	db := setupDB(t)
	_, err := GetMission(context.Background(), db, "nonexistent")
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestMarkRunning(t *testing.T) {
	db := setupDB(t)
	id := ids.NewUUIDv7()
	err := InsertMission(context.Background(), db, &Mission{
		ID:               id,
		Kind:             KindMission,
		Lane:             "normal",
		MissionName:      "test",
		Status:           StatusQueued,
		Input:            []byte("{}"),
		InputFingerprint: "fp",
		TimeCreatedMs:    1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	conn, _ := db.Conn(context.Background())
	defer func() { _ = conn.Close() }()
	_, _ = conn.ExecContext(context.Background(), "BEGIN IMMEDIATE")
	if err := MarkRunning(context.Background(), conn, id, 2000, 1234, 5678, 999); err != nil {
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		t.Fatal(err)
	}
	_, _ = conn.ExecContext(context.Background(), "COMMIT")

	got, _ := GetMission(context.Background(), db, id)
	if got.Status != StatusRunning {
		t.Errorf("expected running, got %q", got.Status)
	}
	if !got.PID.Valid || got.PID.Int64 != 1234 {
		t.Errorf("expected pid=1234, got %v", got.PID)
	}
}

// TestUpdateRunningPid covers the post-spawn pid fill-in: a row that the
// lane runner placed with placeholder pid=0 must accept a real-pid update
// while in status='running'. This guards the bug where the lane runner
// transitions queued→running with pid=0 first (so two runners can't both
// claim a mission), then the spawner needs to fill in the real OS pid —
// without UpdateRunningPid the second MarkRunning was a no-op because of
// its WHERE status='queued' clause.
func TestUpdateRunningPid(t *testing.T) {
	db := setupDB(t)
	id := ids.NewUUIDv7()
	if err := InsertMission(context.Background(), db, &Mission{
		ID: id, Kind: KindMission, Lane: "normal", MissionName: "test",
		Status: StatusQueued, Input: []byte("{}"), InputFingerprint: "fp",
		TimeCreatedMs: 1000,
	}); err != nil {
		t.Fatal(err)
	}
	// Placeholder MarkRunning by the lane runner.
	conn, _ := db.Conn(context.Background())
	defer func() { _ = conn.Close() }()
	_, _ = conn.ExecContext(context.Background(), "BEGIN IMMEDIATE")
	if err := MarkRunning(context.Background(), conn, id, 2000, 0, 0, 0); err != nil {
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		t.Fatal(err)
	}
	_, _ = conn.ExecContext(context.Background(), "COMMIT")

	// Post-spawn update with real OS pid.
	_, _ = conn.ExecContext(context.Background(), "BEGIN IMMEDIATE")
	if err := UpdateRunningPid(context.Background(), conn, id, 4242, 4242, 7777); err != nil {
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		t.Fatal(err)
	}
	_, _ = conn.ExecContext(context.Background(), "COMMIT")

	got, _ := GetMission(context.Background(), db, id)
	if got.Status != StatusRunning {
		t.Errorf("status=%q, want running", got.Status)
	}
	if !got.PID.Valid || got.PID.Int64 != 4242 {
		t.Errorf("pid=%v, want 4242", got.PID)
	}
	if !got.PGID.Valid || got.PGID.Int64 != 4242 {
		t.Errorf("pgid=%v, want 4242", got.PGID)
	}
	if !got.ProcStarttime.Valid || got.ProcStarttime.Int64 != 7777 {
		t.Errorf("proc_starttime=%v, want 7777", got.ProcStarttime)
	}
	// time_started must stay at the MarkRunning value — UpdateRunningPid
	// doesn't touch it.
	if !got.TimeStartedMs.Valid || got.TimeStartedMs.Int64 != 2000 {
		t.Errorf("time_started=%v, want 2000 (unchanged)", got.TimeStartedMs)
	}
}

// TestUpdateRunningPidNotRunning verifies the WHERE status='running' guard:
// calling UpdateRunningPid on a queued (or done) row returns ErrNotFound.
// This is the safety net for a killed-during-spawn race: MarkRunning
// inserts status='running'; an external kill may flip it to done; the
// spawner's UpdateRunningPid must not bring it back to running with a
// real pid (the kill won the race).
func TestUpdateRunningPidNotRunning(t *testing.T) {
	db := setupDB(t)
	id := ids.NewUUIDv7()
	if err := InsertMission(context.Background(), db, &Mission{
		ID: id, Kind: KindMission, Lane: "normal", MissionName: "test",
		Status: StatusQueued, Input: []byte("{}"), InputFingerprint: "fp",
		TimeCreatedMs: 1000,
	}); err != nil {
		t.Fatal(err)
	}
	conn, _ := db.Conn(context.Background())
	defer func() { _ = conn.Close() }()
	_, _ = conn.ExecContext(context.Background(), "BEGIN IMMEDIATE")
	err := UpdateRunningPid(context.Background(), conn, id, 4242, 4242, 7777)
	_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
	if err != ErrNotFound {
		t.Errorf("err=%v, want ErrNotFound (row is queued, not running)", err)
	}
}

// TestUpdateRunningPidAndTimeStarted verifies the spawn path now
// re-stamps time_started so DB and done-event durations match exactly.
func TestUpdateRunningPidAndTimeStarted(t *testing.T) {
	db := setupDB(t)
	id := ids.NewUUIDv7()
	if err := InsertMission(context.Background(), db, &Mission{
		ID: id, Kind: KindMission, Lane: "normal", MissionName: "test",
		Status: StatusQueued, Input: []byte("{}"), InputFingerprint: "fp",
		TimeCreatedMs: 1000,
	}); err != nil {
		t.Fatal(err)
	}
	conn, _ := db.Conn(context.Background())
	defer func() { _ = conn.Close() }()
	_, _ = conn.ExecContext(context.Background(), "BEGIN IMMEDIATE")
	if err := MarkRunning(context.Background(), conn, id, 2000, 0, 0, 0); err != nil {
		t.Fatal(err)
	}
	_, _ = conn.ExecContext(context.Background(), "COMMIT")

	_, _ = conn.ExecContext(context.Background(), "BEGIN IMMEDIATE")
	if err := UpdateRunningPidAndTimeStarted(context.Background(), conn, id, 4242, 4242, 7777, 2500); err != nil {
		t.Fatal(err)
	}
	_, _ = conn.ExecContext(context.Background(), "COMMIT")

	got, _ := GetMission(context.Background(), db, id)
	if got.PID.Int64 != 4242 {
		t.Errorf("pid=%v, want 4242", got.PID)
	}
	if !got.TimeStartedMs.Valid || got.TimeStartedMs.Int64 != 2500 {
		t.Errorf("time_started=%v, want 2500 (overwritten by spawn nowMs)", got.TimeStartedMs)
	}
}

func TestListMissions(t *testing.T) {
	db := setupDB(t)
	// Insert 3 queued, 2 running
	for i := 0; i < 3; i++ {
		_ = InsertMission(context.Background(), db, &Mission{
			ID: ids.NewUUIDv7(), Kind: KindMission, Lane: "x", MissionName: "test",
			Status: StatusQueued, Input: []byte("{}"), InputFingerprint: "f",
			TimeCreatedMs: int64(1000 + i),
		})
	}
	for i := 0; i < 2; i++ {
		_ = InsertMission(context.Background(), db, &Mission{
			ID: ids.NewUUIDv7(), Kind: KindMission, Lane: "x", MissionName: "test",
			Status: StatusRunning, Input: []byte("{}"), InputFingerprint: "f",
			TimeCreatedMs: int64(2000 + i),
		})
	}

	queued, err := ListMissions(context.Background(), db, ListFilter{Status: "queued"}, nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(queued) != 3 {
		t.Errorf("expected 3 queued, got %d", len(queued))
	}

	all, _ := ListMissions(context.Background(), db, ListFilter{}, nil, 100)
	if len(all) != 5 {
		t.Errorf("expected 5 total, got %d", len(all))
	}
}

func TestListMissionsCursorPagination(t *testing.T) {
	db := setupDB(t)
	// Insert 10 missions with sequential times
	for i := 0; i < 10; i++ {
		id := ids.NewUUIDv7()
		_ = InsertMission(context.Background(), db, &Mission{
			ID: id, Kind: KindMission, Lane: "x", MissionName: "test",
			Status: StatusQueued, Input: []byte("{}"), InputFingerprint: "f",
			TimeCreatedMs: int64(1000 + i),
		})
	}

	// Get first page of 5
	page1, err := ListMissions(context.Background(), db, ListFilter{}, nil, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(page1) != 5 {
		t.Fatalf("expected 5, got %d", len(page1))
	}
	last := page1[len(page1)-1]
	cursor := &Cursor{TimeCreatedMs: last.TimeCreatedMs, MissionID: last.ID}

	// Get second page
	page2, err := ListMissions(context.Background(), db, ListFilter{}, cursor, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(page2) != 5 {
		t.Fatalf("expected 5 on page2, got %d", len(page2))
	}

	// No overlap
	seen := map[string]bool{}
	for _, m := range page1 {
		seen[m.ID] = true
	}
	for _, m := range page2 {
		if seen[m.ID] {
			t.Errorf("duplicate mission %s across pages", m.ID)
		}
	}
}

func TestPickQueuedForLane(t *testing.T) {
	db := setupDB(t)
	id1 := ids.NewUUIDv7()
	id2 := ids.NewUUIDv7()
	_ = InsertMission(context.Background(), db, &Mission{
		ID: id1, Kind: KindMission, Lane: "fast", MissionName: "first",
		Status: StatusQueued, Input: []byte("{}"), InputFingerprint: "f",
		TimeCreatedMs: 1000,
	})
	_ = InsertMission(context.Background(), db, &Mission{
		ID: id2, Kind: KindMission, Lane: "fast", MissionName: "second",
		Status: StatusQueued, Input: []byte("{}"), InputFingerprint: "f",
		TimeCreatedMs: 2000,
	})

	conn, _ := db.Conn(context.Background())
	defer func() { _ = conn.Close() }()
	_, _ = conn.ExecContext(context.Background(), "BEGIN IMMEDIATE")
	got, err := PickQueuedForLane(context.Background(), conn, "fast")
	_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
	if err != nil {
		t.Fatal(err)
	}
	if got.MissionName != "first" {
		t.Errorf("expected oldest queued (first), got %q", got.MissionName)
	}
}

// TestPickQueuedSkipsMissionWithFinalizeIntent verifies a queued mission
// with a pending finalize intent (queued-kill committed its Phase-A2 intent but
// hasn't run the final UPDATE yet) must NOT be claimed by the lane runner —
// otherwise it would spawn a mission that is being killed, and the eventual
// final UPDATE / startup repair would race the running process.
func TestPickQueuedSkipsMissionWithFinalizeIntent(t *testing.T) {
	db := setupDB(t)
	id := ids.NewUUIDv7()
	_ = InsertMission(context.Background(), db, &Mission{
		ID: id, Kind: KindMission, Lane: "fast", MissionName: "kill-me",
		Status: StatusQueued, Input: []byte("{}"), InputFingerprint: "f",
		TimeCreatedMs: 1000,
	})
	if err := InsertFinalizeIntent(context.Background(), db, &FinalizeIntent{
		MissionID: id, Phase: PhasePrepared, Outcome: "killed",
		FailReason:    sql.NullString{String: "killed_by_api", Valid: true},
		ExitCode:      sql.NullInt64{Int64: 0, Valid: true},
		Outputs:       []byte("[]"),
		DoneSeq:       2,
		DoneEvent:     `{"seq":2,"event":"done","outcome":"killed"}`,
		TimeCreatedMs: 1000,
	}); err != nil {
		t.Fatal(err)
	}

	conn, _ := db.Conn(context.Background())
	defer func() { _ = conn.Close() }()
	_, _ = conn.ExecContext(context.Background(), "BEGIN IMMEDIATE")
	got, err := PickQueuedForLane(context.Background(), conn, "fast")
	_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound (queued mission with finalize intent must be skipped), got mission=%v err=%v", got, err)
	}
}

func TestListMissionsFilterByGroupID(t *testing.T) {
	db := setupDB(t)

	groupA := "0192aaaa-0000-7000-8000-000000000000"
	groupB := "0192bbbb-0000-7000-8000-000000000000"

	for i, gid := range []string{groupA, groupA, groupA, groupB, ""} {
		m := &Mission{
			ID:            fmt.Sprintf("0192cccc-0000-7000-8000-00000000000%d", i),
			Kind:          KindExec,
			MissionName:   "exec",
			Lane:          "light",
			Status:        StatusQueued,
			Input:         []byte(`{}`),
			TimeCreatedMs: int64(1700000000000 + i),
			GroupID:       sql.NullString{String: gid, Valid: gid != ""},
		}
		if err := InsertMission(context.Background(), db, m); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	rows, err := ListMissions(context.Background(), db, ListFilter{GroupID: groupA}, nil, 100)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("groupA len=%d, want 3", len(rows))
	}
	for _, r := range rows {
		if r.GroupID.String != groupA {
			t.Errorf("got group_id %q, want %q", r.GroupID.String, groupA)
		}
	}

	rowsAll, err := ListMissions(context.Background(), db, ListFilter{}, nil, 100)
	if err != nil {
		t.Fatalf("list all: %v", err)
	}
	if len(rowsAll) != 5 {
		t.Fatalf("all len=%d, want 5", len(rowsAll))
	}
}

func TestListMissionsOrderFinished(t *testing.T) {
	db := setupDB(t)
	mk := func(createdMs, finishedMs int64) string {
		id := ids.NewUUIDv7()
		if err := InsertMission(context.Background(), db, &Mission{
			ID: id, Kind: KindMission, Lane: "x", MissionName: "m",
			Status: StatusDone, Outcome: sql.NullString{String: "success", Valid: true},
			Input: []byte("{}"), InputFingerprint: "f",
			TimeCreatedMs:  createdMs,
			TimeFinishedMs: sql.NullInt64{Int64: finishedMs, Valid: true},
		}); err != nil {
			t.Fatal(err)
		}
		return id
	}
	idLateFinish := mk(1000, 5000)
	idMidFinish := mk(2000, 3000)
	idEarlyMid := mk(3000, 4000)
	_ = InsertMission(context.Background(), db, &Mission{
		ID: ids.NewUUIDv7(), Kind: KindMission, Lane: "x", MissionName: "m",
		Status: StatusQueued, Input: []byte("{}"), InputFingerprint: "f",
		TimeCreatedMs: 9000,
	})

	got, err := ListMissions(context.Background(), db, ListFilter{Order: "finished"}, nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("want 3 finished rows (queued excluded), got %d", len(got))
	}
	want := []string{idLateFinish, idEarlyMid, idMidFinish}
	for i, id := range want {
		if got[i].ID != id {
			t.Errorf("pos %d: got %s want %s", i, got[i].ID, id)
		}
	}
	page1, _ := ListMissions(context.Background(), db, ListFilter{Order: "finished"}, nil, 2)
	if len(page1) != 2 {
		t.Fatalf("page1 len=%d", len(page1))
	}
	last := page1[1]
	page2, _ := ListMissions(context.Background(), db, ListFilter{Order: "finished"},
		&Cursor{TimeFinishedMs: last.TimeFinishedMs.Int64, MissionID: last.ID}, 2)
	if len(page2) != 1 || page2[0].ID != idMidFinish {
		t.Fatalf("page2 want [%s], got %v", idMidFinish, page2)
	}
}

func TestListMissionsExcludesDeletingByDefault(t *testing.T) {
	db := setupDB(t)
	done := ids.NewUUIDv7()
	if err := InsertMission(context.Background(), db, &Mission{
		ID: done, Kind: KindMission, Lane: "x", MissionName: "m",
		Status: StatusDone, Input: []byte("{}"), InputFingerprint: "f", TimeCreatedMs: 1000,
	}); err != nil {
		t.Fatal(err)
	}
	del := ids.NewUUIDv7()
	if err := InsertMission(context.Background(), db, &Mission{
		ID: del, Kind: KindMission, Lane: "x", MissionName: "m",
		Status: StatusDeleting, Input: []byte("{}"), InputFingerprint: "f", TimeCreatedMs: 2000,
	}); err != nil {
		t.Fatal(err)
	}

	all, err := ListMissions(context.Background(), db, ListFilter{}, nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 || all[0].ID != done {
		t.Fatalf("default listing must exclude deleting; got %d rows %+v", len(all), all)
	}

	onlyDel, err := ListMissions(context.Background(), db, ListFilter{Status: string(StatusDeleting)}, nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(onlyDel) != 1 || onlyDel[0].ID != del {
		t.Fatalf("explicit status=deleting must return deleting rows; got %d", len(onlyDel))
	}
}

func TestPickupUsesPartialIndex(t *testing.T) {
	db := setupDB(t)
	rows, err := db.Query(`EXPLAIN QUERY PLAN
		SELECT mission_id FROM missions
		WHERE status='queued' AND lane=?
		ORDER BY time_created, mission_id LIMIT 1`, "normal")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var detail string
	for rows.Next() {
		var i, p, n int
		var d string
		_ = rows.Scan(&i, &p, &n, &d)
		detail += d + " | "
	}
	if !strings.Contains(detail, "missions_queue") {
		t.Errorf("EXPLAIN missing missions_queue index: %s", detail)
	}
}
