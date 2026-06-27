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
