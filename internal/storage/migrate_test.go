package storage

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

// TestMigrateIndexesRestartedFromFK locks in migration 002: the
// self-referential missions.restarted_from foreign key MUST be index-backed.
// Without the index, the FK's ON DELETE SET NULL forces a full table scan on
// every mission delete, so cleanup batch deletes hold the write lock for
// seconds and starve dispatch writers (the recurrence after the connection-init
// fix).
func TestMigrateIndexesRestartedFromFK(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(filepath.Join(dir, "state.db"), Options{})
	defer func() { _ = db.Close() }()
	if err := Migrate(context.Background(), db); err != nil {
		t.Fatal(err)
	}

	var ver int
	if err := db.QueryRow("PRAGMA user_version").Scan(&ver); err != nil {
		t.Fatal(err)
	}
	if ver < 2 {
		t.Fatalf("user_version=%d, want >=2 (migration 002 not applied)", ver)
	}

	var name string
	if err := db.QueryRow(
		`SELECT name FROM sqlite_master WHERE type='index' AND name='missions_restarted_from'`,
	).Scan(&name); err != nil {
		t.Fatalf("missions_restarted_from index missing: %v", err)
	}

	// The restarted_from lookup the FK performs on every delete must SEARCH
	// via the index, not SCAN the table.
	plan := explainQueryPlan(t, db, `SELECT 1 FROM missions WHERE restarted_from = ?`, "some-id")
	if !strings.Contains(plan, "missions_restarted_from") {
		t.Errorf("restarted_from lookup is not index-backed; plan:\n%s", plan)
	}
}

func explainQueryPlan(t *testing.T, db *sql.DB, query string, args ...any) string {
	t.Helper()
	rows, err := db.Query("EXPLAIN QUERY PLAN "+query, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	cols, _ := rows.Columns()
	var sb strings.Builder
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		for _, v := range vals {
			_, _ = fmt.Fprintf(&sb, "%v ", v)
		}
		sb.WriteByte('\n')
	}
	return sb.String()
}

func TestMigrateApplies001(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(filepath.Join(dir, "state.db"), Options{})
	defer func() { _ = db.Close() }()

	if err := Migrate(context.Background(), db); err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var names []string
	for rows.Next() {
		var n string
		_ = rows.Scan(&n)
		names = append(names, n)
	}
	want := []string{"config", "mission_finalize_intents", "mission_runtime", "mission_staging_refs", "missions", "staging_files"}
	if len(names) < len(want) {
		t.Fatalf("got tables %v, want at least %v", names, want)
	}
}

func TestMigrateIdempotent(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(filepath.Join(dir, "state.db"), Options{})
	defer func() { _ = db.Close() }()
	if err := Migrate(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	// Run again — must be no-op.
	if err := Migrate(context.Background(), db); err != nil {
		t.Errorf("second Migrate: %v", err)
	}
}

func TestMigrate003DropsMissionKind(t *testing.T) {
	ctx := context.Background()
	db, err := Open(filepath.Join(t.TempDir(), "state.db"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i, name := range []string{"001_initial.sql", "002_restarted_from_index.sql"} {
		if err := applyMigration(ctx, conn, name, i+1); err != nil {
			t.Fatal(err)
		}
	}
	const (
		missionID = "01900000-0000-7000-8000-000000000001"
		otherID   = "01900000-0000-7000-8000-000000000002"
		sharedSt  = "01900000-0000-7000-8000-0000000000a1"
		otherSt   = "01900000-0000-7000-8000-0000000000a2"
	)
	stmts := []string{
		`INSERT INTO missions (mission_id, kind, lane, mission_name, status, input, input_fingerprint, time_created)
		 VALUES ('` + missionID + `', 'mission', 'normal', 'Demo', 'done', '{}', 'fp1', 1000)`,
		`INSERT INTO missions (mission_id, kind, lane, mission_name, display_name, group_id, status, input, input_fingerprint, time_created)
		 VALUES ('` + otherID + `', 'other', 'normal', 'df', 'disk', 'g1', 'running', '{}', 'fp2', 2000)`,
		`INSERT INTO staging_files (staging_id, state, sha256, size, path, time_created, time_updated, time_expires)
		 VALUES ('` + sharedSt + `', 'complete', 'aa', 1, 'p1', 1, 1, 99), ('` + otherSt + `', 'complete', 'bb', 1, 'p2', 1, 1, 99)`,
		`INSERT INTO mission_staging_refs (mission_id, staging_id, ref_kind, role)
		 VALUES ('` + missionID + `', '` + sharedSt + `', 'input', 'a'), ('` + otherID + `', '` + otherSt + `', 'input', 'b')`,
	}
	for _, s := range stmts {
		if _, err := conn.ExecContext(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	_ = conn.Close()

	if err := Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}

	if _, err := GetMission(ctx, db, otherID); err != ErrNotFound {
		t.Errorf("non-mission row: err=%v, want ErrNotFound", err)
	}
	m, err := GetMission(ctx, db, missionID)
	if err != nil {
		t.Fatalf("mission row: %v", err)
	}
	if m.MissionName != "Demo" || m.Status != StatusDone {
		t.Errorf("mission row = %+v", m)
	}

	rows, err := db.Query(`SELECT name FROM pragma_table_info('missions')`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var col string
		_ = rows.Scan(&col)
		if col == "kind" || col == "display_name" || col == "group_id" {
			t.Errorf("column %s still present", col)
		}
	}
	_ = rows.Close()

	var refs int
	if err := db.QueryRow(`SELECT COUNT(*) FROM mission_staging_refs`).Scan(&refs); err != nil {
		t.Fatal(err)
	}
	if refs != 1 {
		t.Errorf("staging refs = %d, want 1", refs)
	}
	var otherExp, sharedExp int64
	_ = db.QueryRow(`SELECT time_expires FROM staging_files WHERE staging_id=?`, otherSt).Scan(&otherExp)
	_ = db.QueryRow(`SELECT time_expires FROM staging_files WHERE staging_id=?`, sharedSt).Scan(&sharedExp)
	if otherExp != 0 || sharedExp != 99 {
		t.Errorf("time_expires other=%d shared=%d, want 0 and 99", otherExp, sharedExp)
	}

	for _, idx := range []string{"missions_by_group", "staging_by_sha"} {
		var n int
		_ = db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name=?`, idx).Scan(&n)
		if n != 0 {
			t.Errorf("index %s still present", idx)
		}
	}
	plan := explainQueryPlan(t, db, `SELECT mission_id FROM missions ORDER BY time_created DESC, mission_id DESC LIMIT 10`)
	if !strings.Contains(plan, "missions_recent") {
		t.Errorf("recent listing is not index-backed; plan:\n%s", plan)
	}
}

func TestMigrateRefusesNewerSchema(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "state.db"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if err := Migrate(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("PRAGMA user_version=999"); err != nil {
		t.Fatal(err)
	}
	err = Migrate(context.Background(), db)
	if err == nil || !strings.Contains(err.Error(), "newer than this build supports") {
		t.Fatalf("Migrate on a newer schema: err=%v", err)
	}
}
