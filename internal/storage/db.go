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
	// Open immediately to surface errors at startup.
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping db: %w", err)
	}
	return db, nil
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
	pragmas := []string{
		// auto_vacuum MUST come first: it only
		// takes effect on a fresh DB before any page is written, and never once
		// journal_mode=WAL has written the header. Set after WAL it silently
		// stays NONE, making incremental_vacuum a permanent no-op so state.db
		// never returns freelist pages to the OS.
		"PRAGMA auto_vacuum=INCREMENTAL",
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA foreign_keys=ON",
		"PRAGMA temp_store=MEMORY",
		// Reclaim up to 1000 free pages per connection-init. Without this,
		// auto_vacuum=INCREMENTAL leaves DBs that have just had a giant
		// DELETE bloated until the periodic Vacuumer fires (1h cadence).
		"PRAGMA incremental_vacuum(1000)",
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
