package storage

import (
	"context"
	"path/filepath"
	"testing"
)

// TestOpenEnablesIncrementalAutoVacuum verifies that
// PRAGMA auto_vacuum only takes effect on a fresh DB before any page write, and
// never after journal_mode=WAL has written the header. It must be the FIRST
// pragma on the connector so a new DB is created with auto_vacuum=INCREMENTAL (2)
// — otherwise incremental_vacuum is a permanent no-op and state.db never shrinks.
func TestOpenEnablesIncrementalAutoVacuum(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "state.db"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var av int
	if err := db.QueryRow("PRAGMA auto_vacuum").Scan(&av); err != nil {
		t.Fatal(err)
	}
	if av != 2 { // 2 == INCREMENTAL
		t.Errorf("auto_vacuum = %d, want 2 (INCREMENTAL)", av)
	}
}

func TestOpenAppliesPragmasOnEachConn(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "state.db"), Options{CacheSizeKB: -16000})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	for i := 0; i < 5; i++ {
		conn, err := db.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		var fk int
		if err := conn.QueryRowContext(context.Background(), "PRAGMA foreign_keys").Scan(&fk); err != nil {
			t.Fatal(err)
		}
		if fk != 1 {
			t.Errorf("conn %d: foreign_keys = %d", i, fk)
		}
		var jm string
		_ = conn.QueryRowContext(context.Background(), "PRAGMA journal_mode").Scan(&jm)
		if jm != "wal" {
			t.Errorf("conn %d: journal_mode = %s", i, jm)
		}
		_ = conn.Close()
	}
}
