package storage

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Migrate applies missing migrations forward-only. Tracks current version via
// PRAGMA user_version.
func Migrate(ctx context.Context, db *sql.DB) error {
	files, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("read migrations dir: %w", err)
	}
	type mig struct {
		ver  int
		name string
	}
	var migs []mig
	for _, f := range files {
		name := f.Name()
		if !strings.HasSuffix(name, ".sql") {
			continue
		}
		dash := strings.IndexByte(name, '_')
		if dash < 0 {
			continue
		}
		v, err := strconv.Atoi(name[:dash])
		if err != nil {
			continue
		}
		migs = append(migs, mig{ver: v, name: name})
	}
	sort.Slice(migs, func(i, j int) bool { return migs[i].ver < migs[j].ver })

	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	var current int
	if err := conn.QueryRowContext(ctx, "PRAGMA user_version").Scan(&current); err != nil {
		return fmt.Errorf("read user_version: %w", err)
	}

	for _, m := range migs {
		if m.ver <= current {
			continue
		}
		if err := applyMigration(ctx, conn, m.name, m.ver); err != nil {
			return err
		}
	}
	return nil
}

// applyMigration runs one migration script plus its user_version bump inside
// an explicit BEGIN IMMEDIATE on the pinned conn. database/sql does not
// auto-rollback an explicit BEGIN on a *sql.Conn, so every exit that leaves
// the transaction unresolved — script error, PRAGMA error, a failed COMMIT,
// or a driver panic — must roll back before the conn rejoins the pool;
// otherwise the next BEGIN on it fails with "cannot start a transaction
// within a transaction". The single deferred ROLLBACK covers all of those
// paths (same resolve-or-rollback pattern as WithWriter). As in WithWriter,
// the transaction-control statements run on a non-cancellable context: a
// caller's cancellation may abort the migration work itself, but must never
// prevent the resolving ROLLBACK/COMMIT from reaching the database.
func applyMigration(ctx context.Context, conn *sql.Conn, name string, ver int) error {
	body, err := fs.ReadFile(migrationsFS, "migrations/"+name)
	if err != nil {
		return err
	}
	txCtx := context.WithoutCancel(ctx)
	if _, err := conn.ExecContext(txCtx, "BEGIN IMMEDIATE"); err != nil {
		return err
	}
	finished := false
	defer func() {
		if !finished {
			_, _ = conn.ExecContext(txCtx, "ROLLBACK")
		}
	}()
	if _, err := conn.ExecContext(ctx, string(body)); err != nil {
		return fmt.Errorf("apply %s: %w", name, err)
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version=%d", ver)); err != nil {
		return err
	}
	if _, err := conn.ExecContext(txCtx, "COMMIT"); err != nil {
		return fmt.Errorf("commit %s: %w", name, err)
	}
	finished = true
	return nil
}
