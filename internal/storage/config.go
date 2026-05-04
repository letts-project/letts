package storage

import (
	"context"
	"database/sql"
	"errors"
)

// AppliedConfig holds the single-row applied configuration.
type AppliedConfig struct {
	Data      []byte // JSON: {mission_dir, labels, lanes:{...}, runtime:{...}}
	AppliedAt int64
	Source    sql.NullString
}

// GetAppliedConfig reads the applied config row. Returns ErrNotFound if no
// configuration has been applied yet.
func GetAppliedConfig(ctx context.Context, db DBOrConn) (*AppliedConfig, error) {
	var c AppliedConfig
	err := db.QueryRowContext(ctx, `SELECT data, applied_at, source FROM config WHERE id=1`).
		Scan(&c.Data, &c.AppliedAt, &c.Source)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &c, nil
}

// SetAppliedConfig stores (or replaces) the applied config row.
func SetAppliedConfig(ctx context.Context, db DBOrConn, c AppliedConfig) error {
	_, err := db.ExecContext(ctx, `INSERT OR REPLACE INTO config (id, data, applied_at, source)
		VALUES (1, ?, ?, ?)`, c.Data, c.AppliedAt, c.Source)
	return err
}
