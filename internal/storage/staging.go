package storage

import (
	"context"
	"database/sql"
	"errors"
)

// StagingState represents the lifecycle state of a staging file.
type StagingState string

const (
	StagingUploading     StagingState = "uploading"
	StagingPendingOutput StagingState = "pending_output"
	StagingCommitting    StagingState = "committing"
	StagingComplete      StagingState = "complete"
	StagingDeleting      StagingState = "deleting"
)

// StagingFile mirrors the staging_files row.
type StagingFile struct {
	StagingID     string
	State         StagingState
	Sha256        string
	Size          int64
	BytesReceived int64
	Path          string
	TimeCreatedMs int64
	TimeUpdatedMs int64
	TimeExpiresMs int64
	DownloadedAt  sql.NullInt64
}

// InsertStaging inserts a new staging file row.
func InsertStaging(ctx context.Context, db DBOrConn, s *StagingFile) error {
	_, err := db.ExecContext(ctx, `INSERT INTO staging_files (
		staging_id, state, sha256, size, bytes_received, path,
		time_created, time_updated, time_expires, downloaded_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		s.StagingID, s.State, s.Sha256, s.Size, s.BytesReceived, s.Path,
		s.TimeCreatedMs, s.TimeUpdatedMs, s.TimeExpiresMs, s.DownloadedAt)
	return err
}

// GetStaging returns the staging row by ID or ErrNotFound.
func GetStaging(ctx context.Context, db DBOrConn, id string) (*StagingFile, error) {
	var s StagingFile
	err := db.QueryRowContext(ctx, `SELECT staging_id, state, sha256, size, bytes_received,
		path, time_created, time_updated, time_expires, downloaded_at FROM staging_files
		WHERE staging_id=?`, id).Scan(
		&s.StagingID, &s.State, &s.Sha256, &s.Size, &s.BytesReceived, &s.Path,
		&s.TimeCreatedMs, &s.TimeUpdatedMs, &s.TimeExpiresMs, &s.DownloadedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return &s, err
}

// UpdateStagingProgress updates bytes_received and TTL for an uploading file.
// Returns ErrNotFound if the row does not exist or is not in uploading state.
func UpdateStagingProgress(ctx context.Context, db DBOrConn, id string, bytes, nowMs, expiresMs int64) error {
	res, err := db.ExecContext(ctx, `UPDATE staging_files SET
		bytes_received=?, time_updated=?, time_expires=? WHERE staging_id=? AND state='uploading'`,
		bytes, nowMs, expiresMs, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// MarkStagingComplete transitions state from uploading → complete with verified sha256/size.
// Returns ErrNotFound if the row does not exist or is not in uploading state.
func MarkStagingComplete(ctx context.Context, db DBOrConn, id string, sha256 string, size, nowMs, expiresMs int64) error {
	res, err := db.ExecContext(ctx, `UPDATE staging_files SET
		state='complete', sha256=?, size=?, bytes_received=?, time_updated=?, time_expires=?
		WHERE staging_id=? AND state='uploading'`,
		sha256, size, size, nowMs, expiresMs, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// MarkStagingCompleteWithPath transitions a staging row to state='complete'
// and updates its path in the same statement. Used by Phase B commit after a
// successful tmp→final rename so the stored path tracks where the file
// actually lives.
//
// State guard limits the UPDATE to Phase B rows
// (pending_output/committing). Without it a manual SQL or future code
// path could undelete a tombstoned row by writing complete back over
// deleting. Symmetric with the MarkStagingDeleting guard.
func MarkStagingCompleteWithPath(ctx context.Context, db DBOrConn, id, path string) error {
	res, err := db.ExecContext(ctx, `UPDATE staging_files SET
		state='complete', path=?
		WHERE staging_id=? AND state IN ('pending_output','committing')`, path, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// ErrStagingFinalizing is returned by MarkStagingDeleting when the row is
// in a Phase B state (pending_output / committing) and therefore owned by
// mission.commitFinalize. Admin force-delete must wait or fail loudly
// rather than racing Phase B.
var ErrStagingFinalizing = errors.New("staging file is mid-finalize (Phase B)")

// MarkStagingDeleting transitions a staging file to the deleting state.
//
// State guard: only rows in 'uploading' or 'complete' transition
// to 'deleting'. A row already in 'deleting' is treated as a no-op (idempotent).
// Rows in 'pending_output' or 'committing' are owned by Phase B output-commit
// and must NOT be reaped from under it — return ErrStagingFinalizing so
// callers can surface 409 Conflict and retry.
//
// Returns:
//   - nil on successful transition (or idempotent no-op for already-deleting).
//   - ErrNotFound if the row does not exist.
//   - ErrStagingFinalizing if the row is mid-Phase-B.
func MarkStagingDeleting(ctx context.Context, db DBOrConn, id string) error {
	res, err := db.ExecContext(ctx,
		`UPDATE staging_files SET state='deleting'
		 WHERE staging_id=? AND state IN ('uploading','complete')`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n > 0 {
		return nil
	}
	// 0 rows affected: distinguish missing vs wrong state.
	st, gerr := GetStaging(ctx, db, id)
	if errors.Is(gerr, ErrNotFound) {
		return ErrNotFound
	}
	if gerr != nil {
		return gerr
	}
	if st.State == StagingDeleting {
		return nil // idempotent
	}
	return ErrStagingFinalizing
}

// MarkStagingDeletingIfExpired is the GC variant: in addition to the
// state guard, requires the row's time_expires to still be in the past.
// A concurrent dispatch can promote the row to live
// (time_expires=MaxInt64) between expireTTLs' SELECT and per-row UPDATE.
// Without the time_expires guard, the GC flip races over the dispatch's
// recalc and reaps a row referenced by a fresh queued mission.
//
// Returns nil for both the "successfully marked" and "no-longer-expired
// — skip" cases (so the caller's metric/log stays accurate, no error
// to log on the benign promotion race). Returns ErrStagingFinalizing
// for Phase-B states like MarkStagingDeleting, and ErrNotFound when the
// row is gone.
func MarkStagingDeletingIfExpired(ctx context.Context, db DBOrConn, id string, nowMs int64) error {
	res, err := db.ExecContext(ctx,
		`UPDATE staging_files SET state='deleting'
		 WHERE staging_id=? AND state IN ('uploading','complete')
		   AND time_expires > 0 AND time_expires <= ?`, id, nowMs)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n > 0 {
		return nil
	}
	st, gerr := GetStaging(ctx, db, id)
	if errors.Is(gerr, ErrNotFound) {
		return ErrNotFound
	}
	if gerr != nil {
		return gerr
	}
	if st.State == StagingPendingOutput || st.State == StagingCommitting {
		return ErrStagingFinalizing
	}
	// Row exists but no longer expired (promoted by concurrent dispatch)
	// or already deleting — both are benign for the GC caller.
	return nil
}

// LookupStagingByContent returns a complete staging file matching sha256+size.
// Uses the partial index staging_by_sha (state='complete' only).
// Returns ErrNotFound if no matching complete row exists.
func LookupStagingByContent(ctx context.Context, db DBOrConn, sha256 string, size int64) (*StagingFile, error) {
	var s StagingFile
	err := db.QueryRowContext(ctx, `SELECT staging_id, state, sha256, size, bytes_received,
		path, time_created, time_updated, time_expires, downloaded_at FROM staging_files
		WHERE state='complete' AND sha256=? AND size=? LIMIT 1`, sha256, size).Scan(
		&s.StagingID, &s.State, &s.Sha256, &s.Size, &s.BytesReceived, &s.Path,
		&s.TimeCreatedMs, &s.TimeUpdatedMs, &s.TimeExpiresMs, &s.DownloadedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return &s, err
}

// StagingWithRef pairs a staging file with the ref kind/role linking it to a
// mission. Returned by ListStagingByMission.
type StagingWithRef struct {
	StagingFile
	RefKind RefKind
	Role    string
}

// StagingCursor is the opaque pagination cursor for ListStagingByMission.
type StagingCursor struct {
	TimeCreatedMs int64  `json:"time_created"`
	StagingID     string `json:"staging_id"`
}

// ListStagingByMission returns staging files referenced by the mission,
// optionally filtered by ref_kind ("" means no filter). Ordered by
// (time_created DESC, staging_id DESC) for stable cursor pagination.
func ListStagingByMission(ctx context.Context, db DBOrConn, missionID string, kindFilter string, after *StagingCursor, limit int) ([]StagingWithRef, error) {
	q := `SELECT sf.staging_id, sf.state, sf.sha256, sf.size, sf.bytes_received,
	             sf.path, sf.time_created, sf.time_updated, sf.time_expires,
	             sf.downloaded_at, r.ref_kind, r.role
	      FROM staging_files sf
	      INNER JOIN mission_staging_refs r ON sf.staging_id = r.staging_id
	      WHERE r.mission_id = ?`
	args := []any{missionID}
	if kindFilter != "" {
		q += " AND r.ref_kind = ?"
		args = append(args, kindFilter)
	}
	if after != nil {
		q += " AND (sf.time_created, sf.staging_id) < (?, ?)"
		args = append(args, after.TimeCreatedMs, after.StagingID)
	}
	q += " ORDER BY sf.time_created DESC, sf.staging_id DESC LIMIT ?"
	args = append(args, limit)
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []StagingWithRef
	for rows.Next() {
		var s StagingWithRef
		if err := rows.Scan(&s.StagingID, &s.State, &s.Sha256, &s.Size, &s.BytesReceived,
			&s.Path, &s.TimeCreatedMs, &s.TimeUpdatedMs, &s.TimeExpiresMs, &s.DownloadedAt,
			&s.RefKind, &s.Role); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// ListExpiredStaging returns rows where time_expires <= nowMs and state in given set.
// Used by Staging GC and orphan cleanup.
func ListExpiredStaging(ctx context.Context, db DBOrConn, nowMs int64, states []StagingState, limit int) ([]StagingFile, error) {
	if len(states) == 0 {
		return nil, nil
	}
	q := `SELECT staging_id, state, sha256, size, bytes_received, path, time_created,
		time_updated, time_expires, downloaded_at FROM staging_files
		WHERE time_expires <= ? AND state IN (`
	args := []any{nowMs}
	for i, s := range states {
		if i > 0 {
			q += ","
		}
		q += "?"
		args = append(args, s)
	}
	q += ") ORDER BY time_expires LIMIT ?"
	args = append(args, limit)
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []StagingFile
	for rows.Next() {
		var s StagingFile
		if err := rows.Scan(&s.StagingID, &s.State, &s.Sha256, &s.Size, &s.BytesReceived,
			&s.Path, &s.TimeCreatedMs, &s.TimeUpdatedMs, &s.TimeExpiresMs, &s.DownloadedAt); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}
