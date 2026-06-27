package storage

import (
	"context"
	"path/filepath"
	"testing"
	"time"
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

// TestNewConnInitDoesNotNeedWriteLock guards against a "database is locked"
// storm. Per-connection init must NOT acquire
// the write lock, so a connection opened to serve a READ can always come
// online even while a writer holds the write lock. Before the fix the
// connector ran auto_vacuum=INCREMENTAL and incremental_vacuum(1000) on every
// Connect — both take the write lock — so a brand-new connection (even one
// opened only to serve a read) blocked on, and then failed under, a held
// write lock. Heavy read traffic then forced ever more blocked writers,
// turning localized contention into a self-sustaining outage.
func TestNewConnInitDoesNotNeedWriteLock(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "state.db"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec(`CREATE TABLE t (x INTEGER)`); err != nil {
		t.Fatal(err)
	}

	// Pin a connection and hold an open write transaction (the write lock).
	writer, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = writer.Close() }()
	if _, err := writer.ExecContext(context.Background(), "BEGIN IMMEDIATE"); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = writer.ExecContext(context.Background(), "ROLLBACK") }()

	// Open a BRAND-NEW connection (writer is checked out, so the pool must
	// create a fresh physical connection -> runs the connector init) and use
	// it for a read under a short deadline. If init needed the write lock it
	// would block on the held writer and miss the deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	reader, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("opening a new connection while a writer holds the lock failed: %v", err)
	}
	defer func() { _ = reader.Close() }()
	var one int
	if err := reader.QueryRowContext(ctx, "SELECT 1").Scan(&one); err != nil {
		t.Fatalf("read on new connection blocked by held write lock: %v", err)
	}
	if one != 1 {
		t.Fatalf("SELECT 1 = %d", one)
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
