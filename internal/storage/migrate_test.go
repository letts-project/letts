package storage

import (
	"context"
	"path/filepath"
	"testing"
)

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
