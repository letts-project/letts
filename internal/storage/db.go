// Package storage owns the sqlite database for dugdale state.
// All schema, migrations, and CRUD live in subfiles of this package.
package storage

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	"modernc.org/sqlite"
)

// Options for Open; align with dugdale.yaml limits.cache_size.
type Options struct {
	CacheSizeKB int // negative for KiB absolute, per PRAGMA cache_size convention
}

// Open opens (or creates) a SQLite database at path with WAL and per-conn
// PRAGMA initialization. The returned *sql.DB is configured for
// concurrent reads and serialized writes.
func Open(path string, opts Options) (*sql.DB, error) {
	if opts.CacheSizeKB == 0 {
		opts.CacheSizeKB = -16000
	}
	connector := &lettsConnector{
		dsn:  path + "?_pragma=busy_timeout(5000)",
		opts: opts,
	}
	db := sql.OpenDB(connector)
	// Cap the connection pool so a sudden burst of HTTP requests can't open
	// arbitrarily many connections — each connection holds a sqlite handle
	// and its own per-conn PRAGMA state, and unbounded growth would make
	// busy_timeout enforcement uneven under load. Reads share the pool;
	// the single writer is serialized via WithWriter's BEGIN IMMEDIATE.
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(8)
	// One-time database-level setup, run ONCE at startup before the pool warms
	// up. auto_vacuum and journal_mode=WAL are PERSISTENT database properties —
	// once set they survive every later connection and every restart. Both
	// require the write lock to apply, so they MUST NOT live in the
	// per-connection connector: doing them per-connection meant every new
	// pooled connection — including one opened only to serve a READ — had to
	// take the write lock to come online. Under write contention that turns a
	// localized stall into a self-sustaining "database is locked" storm, where
	// read load forces new connections whose init is itself a blocked writer.
	// Running them here, once, while nothing contends the lock keeps connection
	// init lock-free forever after.
	if err := initDatabaseOnce(context.Background(), db); err != nil {
		_ = db.Close()
		return nil, err
	}
	// Open immediately to surface errors at startup.
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping db: %w", err)
	}
	return db, nil
}

// initDatabaseOnce applies the persistent, write-lock-requiring PRAGMAs a
// single time at startup. Order matters: auto_vacuum only takes effect on a
// fresh DB BEFORE any page is written, and never once journal_mode=WAL has
// written the header — so auto_vacuum must precede WAL. Both are no-ops on an
// already-initialized DB (the production case), so this is cheap on restart.
// Freelist reclamation is left entirely to the periodic Vacuumer; it is no
// longer attempted per-connection.
func initDatabaseOnce(ctx context.Context, db *sql.DB) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("init conn: %w", err)
	}
	defer func() { _ = conn.Close() }()
	for _, p := range []string{
		"PRAGMA auto_vacuum=INCREMENTAL",
		"PRAGMA journal_mode=WAL",
	} {
		if _, err := conn.ExecContext(ctx, p); err != nil {
			return fmt.Errorf("init %s: %w", p, err)
		}
	}
	return nil
}

type lettsConnector struct {
	dsn  string
	opts Options
}

func (c *lettsConnector) Connect(ctx context.Context) (driver.Conn, error) {
	d := &sqlite.Driver{}
	conn, err := d.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	// Per-connection PRAGMAs ONLY. Every statement here must be a
	// connection-level setting that does NOT require the write lock — these
	// run on every new pooled connection, including read-only ones, and a
	// new connection must always be able to come online even while a writer
	// holds the lock. auto_vacuum, journal_mode=WAL and incremental_vacuum
	// (all of which take the write lock) are deliberately NOT here; they are
	// applied once at startup by initDatabaseOnce. See Open's comment.
	pragmas := []string{
		"PRAGMA synchronous=NORMAL",
		"PRAGMA foreign_keys=ON",
		"PRAGMA temp_store=MEMORY",
		fmt.Sprintf("PRAGMA cache_size=%d", c.opts.CacheSizeKB),
	}
	for _, p := range pragmas {
		execer, ok := conn.(driver.ExecerContext)
		if !ok {
			_ = conn.Close()
			return nil, fmt.Errorf("sqlite conn missing ExecerContext")
		}
		if _, err := execer.ExecContext(ctx, p, nil); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("pragma %s: %w", p, err)
		}
	}
	return conn, nil
}

func (c *lettsConnector) Driver() driver.Driver {
	return &sqlite.Driver{}
}
