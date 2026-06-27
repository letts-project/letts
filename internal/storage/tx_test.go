package storage

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// openShortBusy opens a state.db with a short busy_timeout so write-lock
// contention surfaces as SQLITE_BUSY in ~100ms instead of the 5s default,
// keeping the retry tests fast and deterministic. Uses the in-package
// connector directly to override the DSN that Open hardcodes.
func openShortBusy(t *testing.T, path string) *sql.DB {
	t.Helper()
	connector := &lettsConnector{dsn: path + "?_pragma=busy_timeout(100)", opts: Options{}}
	db := sql.OpenDB(connector)
	if err := initDatabaseOnce(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestIsBusy(t *testing.T) {
	if IsBusy(nil) {
		t.Error("IsBusy(nil) = true, want false")
	}
	if IsBusy(errors.New("some other error")) {
		t.Error("IsBusy(generic) = true, want false")
	}

	// A real SQLITE_BUSY: hold the write lock on one conn, attempt a write on
	// another (short busy_timeout makes it fail quickly).
	dir := t.TempDir()
	db := openShortBusy(t, filepath.Join(dir, "state.db"))
	defer func() { _ = db.Close() }()
	if _, err := db.Exec("CREATE TABLE k (v INTEGER)"); err != nil {
		t.Fatal(err)
	}
	holder, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Close() }()
	if _, err := holder.ExecContext(context.Background(), "BEGIN IMMEDIATE"); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = holder.ExecContext(context.Background(), "ROLLBACK") }()

	err = WithWriter(context.Background(), db, func(c *sql.Conn) error {
		_, e := c.ExecContext(context.Background(), "INSERT INTO k(v) VALUES (1)")
		return e
	})
	if err == nil {
		t.Fatal("expected a busy error while the write lock is held")
	}
	if !IsBusy(err) {
		t.Errorf("IsBusy(%v) = false, want true", err)
	}
}

// TestWithWriterRetrySucceedsAfterContention proves the finalize-stranding fix:
// a writer that hits transient SQLITE_BUSY must retry and eventually commit,
// where a single WithWriter would have failed and (for finalize) left the
// mission stuck in status='running' forever.
func TestWithWriterRetrySucceedsAfterContention(t *testing.T) {
	dir := t.TempDir()
	db := openShortBusy(t, filepath.Join(dir, "state.db"))
	defer func() { _ = db.Close() }()
	if _, err := db.Exec("CREATE TABLE k (v INTEGER)"); err != nil {
		t.Fatal(err)
	}

	holder, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := holder.ExecContext(context.Background(), "BEGIN IMMEDIATE"); err != nil {
		t.Fatal(err)
	}
	// Release the write lock after ~350ms — long enough that the first
	// WithWriterRetry attempts (busy_timeout 100ms + backoff) fail busy and
	// must retry.
	go func() {
		time.Sleep(350 * time.Millisecond)
		_, _ = holder.ExecContext(context.Background(), "ROLLBACK")
		_ = holder.Close()
	}()

	start := time.Now()
	if err := WithWriterRetry(context.Background(), db, func(c *sql.Conn) error {
		_, e := c.ExecContext(context.Background(), "INSERT INTO k(v) VALUES (1)")
		return e
	}); err != nil {
		t.Fatalf("WithWriterRetry never committed despite the lock being released: %v", err)
	}
	if d := time.Since(start); d < 300*time.Millisecond {
		t.Errorf("committed in %v — too fast to have retried through the held lock", d)
	}
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM k").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("row count = %d, want 1", n)
	}
}

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
