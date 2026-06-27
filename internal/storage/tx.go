package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"modernc.org/sqlite"
)

// SQLite primary result codes for lock contention. Extended codes (e.g.
// SQLITE_BUSY_SNAPSHOT=517, SQLITE_LOCKED_SHAREDCACHE=262) carry one of these
// in their low byte, which is why IsBusy masks with 0xFF.
const (
	sqliteBusy   = 5 // SQLITE_BUSY
	sqliteLocked = 6 // SQLITE_LOCKED
)

// IsBusy reports whether err (anywhere in its wrap chain) is a transient
// SQLite lock-contention error — SQLITE_BUSY (5) or SQLITE_LOCKED (6),
// including their extended variants such as SQLITE_BUSY_SNAPSHOT (517), which
// busy_timeout does NOT retry on its own. These are safe to retry: the
// statement acquired no partial state (the transaction never started or was
// rolled back). Used by WithWriterRetry so a transient lock never drops a
// terminal mission outcome.
func IsBusy(err error) bool {
	if err == nil {
		return false
	}
	var se *sqlite.Error
	if errors.As(err, &se) {
		switch se.Code() & 0xFF {
		case sqliteBusy, sqliteLocked:
			return true
		}
	}
	return false
}

// WithWriter runs fn inside a BEGIN IMMEDIATE transaction on a pinned conn.
// Returning an error from fn rolls back; nil error commits. Use this for any
// path that mutates state.
func WithWriter(ctx context.Context, db *sql.DB, fn func(*sql.Conn) error) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("pin conn: %w", err)
	}
	defer func() { _ = conn.Close() }()
	// Transaction-control statements (BEGIN/COMMIT/ROLLBACK) must run on a
	// non-cancellable context. If the caller's ctx is cancelled
	// mid-transaction, a ROLLBACK on that dead ctx fails — and database/sql
	// does NOT auto-rollback an explicit BEGIN on a pinned *sql.Conn, so the
	// connection is returned to the pool still inside an open transaction.
	// modernc.org/sqlite doesn't reset it, poisoning the conn: every later
	// BEGIN IMMEDIATE on it fails with "cannot start a transaction within a
	// transaction", taking down the whole writer path. Cancellation must affect
	// only fn's queries, never the tx lifecycle.
	txCtx := context.WithoutCancel(ctx)
	if _, err := conn.ExecContext(txCtx, "BEGIN IMMEDIATE"); err != nil {
		return fmt.Errorf("BEGIN IMMEDIATE: %w", err)
	}
	// finished flips to true once the transaction has been explicitly resolved
	// (ROLLBACK on fn error, or COMMIT / its fallback rollback). The deferred
	// ROLLBACK below is the panic path: net/http recovers handler panics, so a
	// panic in fn would otherwise unwind straight into conn.Close and return
	// the pooled connection with the IMMEDIATE transaction still open. The
	// process keeps running with a poisoned pool — every later BEGIN IMMEDIATE
	// on that conn fails with "cannot start a transaction within a
	// transaction", and all other writers wait out busy_timeout against the
	// leaked write lock. Registered after the conn.Close defer so LIFO order
	// runs the rollback while the conn is still pinned. The panic itself
	// propagates untouched.
	finished := false
	defer func() {
		if !finished {
			_, _ = conn.ExecContext(txCtx, "ROLLBACK")
		}
	}()
	if err := fn(conn); err != nil {
		finished = true
		if _, rbErr := conn.ExecContext(txCtx, "ROLLBACK"); rbErr != nil {
			err = errors.Join(err, fmt.Errorf("rollback: %w", rbErr))
		}
		return err
	}
	if _, err := conn.ExecContext(txCtx, "COMMIT"); err != nil {
		// Leave the conn clean for reuse even if COMMIT failed.
		finished = true
		_, _ = conn.ExecContext(txCtx, "ROLLBACK")
		return fmt.Errorf("COMMIT: %w", err)
	}
	finished = true
	return nil
}

// WithWriterRetry is WithWriter with bounded retry on transient SQLite lock
// contention (see IsBusy). Use it for writes whose failure is unacceptable —
// above all mission finalize: busy_timeout (5s) makes a single BEGIN IMMEDIATE
// give up under a write-lock storm, and BUSY_SNAPSHOT (517) is not retried by
// busy_timeout at all. Without retry a transient lock drops the terminal
// outcome and strands the row in status='running' forever, with its OS process
// already gone (2026-06-27 incident).
//
// Each attempt is a complete, atomic BEGIN IMMEDIATE..COMMIT (or rollback), so
// retrying is idempotent: a failed attempt left no partial state. Non-busy
// errors are returned immediately. The caller's ctx still bounds the wait —
// finalize passes a context.WithoutCancel ctx so shutdown can't abort a
// durable terminal write mid-retry.
func WithWriterRetry(ctx context.Context, db *sql.DB, fn func(*sql.Conn) error) error {
	const maxAttempts = 10
	backoff := 50 * time.Millisecond
	const maxBackoff = 2 * time.Second
	var err error
	for attempt := 1; ; attempt++ {
		err = WithWriter(ctx, db, fn)
		if err == nil || !IsBusy(err) || attempt == maxAttempts {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < maxBackoff {
			if backoff *= 2; backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}
