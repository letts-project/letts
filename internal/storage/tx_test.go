package storage

import (
	"context"
	"database/sql"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestWithWriterCanceledCtxDoesNotPoisonPool verifies that if fn fails
// because the caller's ctx is cancelled mid-transaction, WithWriter's ROLLBACK
// must still run so the pooled connection is not returned to the pool inside an
// open transaction (which would make every subsequent BEGIN IMMEDIATE fail with
// "cannot start a transaction within a transaction").
func TestWithWriterCanceledCtxDoesNotPoisonPool(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "state.db"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(1) // force conn reuse so poisoning is observable
	db.SetMaxIdleConns(1)
	if _, err := db.ExecContext(context.Background(), "CREATE TABLE k (v INTEGER)"); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel mid-fn so the rollback runs on a dead ctx.
	_ = WithWriter(ctx, db, func(c *sql.Conn) error {
		cancel()
		_, e := c.ExecContext(ctx, "INSERT INTO k(v) VALUES (1)")
		return e
	})

	// The pooled conn must be healthy for the next writer.
	if err := WithWriter(context.Background(), db, func(c *sql.Conn) error {
		_, e := c.ExecContext(context.Background(), "INSERT INTO k(v) VALUES (2)")
		return e
	}); err != nil {
		t.Fatalf("subsequent WithWriter failed (pool poisoned by cancelled rollback): %v", err)
	}

	// The cancelled tx must not have leaked a committed row.
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM k").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("row count = %d, want 1 (only the healthy v=2 insert; cancelled tx must roll back)", n)
	}
}

// TestWithWriterPanicRollsBackAndKeepsPoolUsable verifies that a panic inside
// fn does not return the pinned connection to the pool with the IMMEDIATE
// transaction still open. net/http recovers handler panics, so the process
// keeps running — a leaked open transaction would make every later
// BEGIN IMMEDIATE on that conn fail with "cannot start a transaction within a
// transaction" and stall all other writers against the abandoned write lock.
// The panic itself must still propagate to the caller.
func TestWithWriterPanicRollsBackAndKeepsPoolUsable(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "state.db"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(1) // force conn reuse so a leaked tx is observable
	db.SetMaxIdleConns(1)
	if _, err := db.ExecContext(context.Background(), "CREATE TABLE k (v INTEGER)"); err != nil {
		t.Fatal(err)
	}

	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("panic in fn did not propagate out of WithWriter")
			}
		}()
		_ = WithWriter(context.Background(), db, func(c *sql.Conn) error {
			if _, err := c.ExecContext(context.Background(), "INSERT INTO k(v) VALUES (1)"); err != nil {
				t.Errorf("insert inside panicking fn: %v", err)
			}
			panic("boom")
		})
	}()

	// The pooled conn must be healthy for the next writer.
	if err := WithWriter(context.Background(), db, func(c *sql.Conn) error {
		_, e := c.ExecContext(context.Background(), "INSERT INTO k(v) VALUES (2)")
		return e
	}); err != nil {
		t.Fatalf("subsequent WithWriter failed (pool poisoned by panic unwind): %v", err)
	}

	// The panicked tx must have rolled back: only v=2 survives.
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM k").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("row count = %d, want 1 (panicked tx must roll back, healthy tx must commit)", n)
	}
}

func TestWithWriterSerializes(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(filepath.Join(dir, "state.db"), Options{})
	defer func() { _ = db.Close() }()
	_ = Migrate(context.Background(), db)
	_, _ = db.ExecContext(context.Background(), "CREATE TABLE counter (n INTEGER)")
	_, _ = db.ExecContext(context.Background(), "INSERT INTO counter VALUES (0)")

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := WithWriter(context.Background(), db, func(c *sql.Conn) error {
				var n int
				if err := c.QueryRowContext(context.Background(), "SELECT n FROM counter").Scan(&n); err != nil {
					return err
				}
				time.Sleep(20 * time.Millisecond)
				_, err := c.ExecContext(context.Background(), "UPDATE counter SET n=?", n+1)
				return err
			})
			if err != nil {
				t.Errorf("writer: %v", err)
			}
		}()
	}
	wg.Wait()
	var n int
	_ = db.QueryRow("SELECT n FROM counter").Scan(&n)
	if n != 5 {
		t.Errorf("got n=%d, want 5 (lost updates)", n)
	}
}
