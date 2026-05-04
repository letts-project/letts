package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

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
