package storage

import (
	"context"
	"database/sql"
	"math"
	"time"
)

// maxInt64 is used as "effectively infinity" TTL when a ref'd mission is active.
const maxInt64 = int64(math.MaxInt64)

// RefKind classifies the relationship between a mission and a staging file.
type RefKind string

const (
	RefInput  RefKind = "input"
	RefOutput RefKind = "output"
	RefScript RefKind = "script"
)

// StagingRef mirrors a mission_staging_refs row.
type StagingRef struct {
	MissionID string
	StagingID string
	RefKind   RefKind
	Role      string
}

// InsertRef inserts a single mission_staging_refs row.
func InsertRef(ctx context.Context, db DBOrConn, r StagingRef) error {
	_, err := db.ExecContext(ctx, `INSERT INTO mission_staging_refs (
		mission_id, staging_id, ref_kind, role) VALUES (?, ?, ?, ?)`,
		r.MissionID, r.StagingID, r.RefKind, r.Role)
	return err
}

// RefsByMission returns all refs for a given mission.
func RefsByMission(ctx context.Context, db DBOrConn, missionID string) ([]StagingRef, error) {
	rows, err := db.QueryContext(ctx, `SELECT mission_id, staging_id, ref_kind, role
		FROM mission_staging_refs WHERE mission_id=?`, missionID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	return scanRefs(rows)
}

// RefsByStaging returns all refs pointing at the given staging file.
func RefsByStaging(ctx context.Context, db DBOrConn, stagingID string) ([]StagingRef, error) {
	rows, err := db.QueryContext(ctx, `SELECT mission_id, staging_id, ref_kind, role
		FROM mission_staging_refs WHERE staging_id=?`, stagingID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	return scanRefs(rows)
}

func scanRefs(rows *sql.Rows) ([]StagingRef, error) {
	var out []StagingRef
	for rows.Next() {
		var r StagingRef
		if err := rows.Scan(&r.MissionID, &r.StagingID, &r.RefKind, &r.Role); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// TTLPolicy carries config-derived durations needed by RecalcStagingTTL.
type TTLPolicy struct {
	MissionSuccess time.Duration
	MissionFailed  time.Duration
	ExecSuccess    time.Duration
	ExecFailed     time.Duration
	StagingTTL     time.Duration
	DownloadGrace  time.Duration
}

// RecalcStagingTTL applies the TTL formula and stores the new time_expires.
//   - If any ref'd mission is queued/running/deleting → time_expires = MaxInt64 (infinity)
//   - Else time_expires = max(time_finished + ttl_for(kind, outcome)) over refs
//   - If no refs:
//     if downloaded_at set → min(time_created + staging_ttl, downloaded_at + downloaded_grace)
//     else → time_created + staging_ttl
//
// Returns the computed time_expires value.
func RecalcStagingTTL(ctx context.Context, db DBOrConn, stagingID string, p TTLPolicy, nowMs int64) (int64, error) {
	st, err := GetStaging(ctx, db, stagingID)
	if err != nil {
		return 0, err
	}

	rows, err := db.QueryContext(ctx, `SELECT m.kind, m.status, m.outcome, m.time_finished
		FROM mission_staging_refs r
		JOIN missions m ON m.mission_id = r.mission_id
		WHERE r.staging_id = ?`, stagingID)
	if err != nil {
		return 0, err
	}
	defer func() { _ = rows.Close() }()

	var maxExp int64 = -1
	hasRefs := false
	isInfinity := false

	for rows.Next() {
		hasRefs = true
		var kind, status string
		var outcome sql.NullString
		var finishedMs sql.NullInt64
		if err := rows.Scan(&kind, &status, &outcome, &finishedMs); err != nil {
			return 0, err
		}
		if status == string(StatusQueued) || status == string(StatusRunning) || status == string(StatusDeleting) {
			isInfinity = true
			continue
		}
		// done — compute TTL based on kind+outcome
		var ttl time.Duration
		isExec := kind == string(KindExec)
		if outcome.Valid && outcome.String == string(OutcomeSuccess) {
			if isExec {
				ttl = p.ExecSuccess
			} else {
				ttl = p.MissionSuccess
			}
		} else {
			if isExec {
				ttl = p.ExecFailed
			} else {
				ttl = p.MissionFailed
			}
		}
		if finishedMs.Valid {
			exp := finishedMs.Int64 + ttl.Milliseconds()
			if exp > maxExp {
				maxExp = exp
			}
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	var newExp int64
	switch {
	case isInfinity:
		newExp = maxInt64
	case hasRefs:
		newExp = maxExp
	default:
		// No refs — orphan; use staging_ttl or downloaded_grace
		newExp = st.TimeCreatedMs + p.StagingTTL.Milliseconds()
		if st.DownloadedAt.Valid {
			grace := st.DownloadedAt.Int64 + p.DownloadGrace.Milliseconds()
			if grace < newExp {
				newExp = grace
			}
		}
	}

	// Filter by state so a recalc never writes time_expires
	// onto rows in 'deleting' or Phase B states. Today expireTTLs and
	// tombstoneDeleting re-filter independently so a stale write was
	// harmless, but a fresh TTL on a 'deleting' row obscures intent.
	if _, err := db.ExecContext(ctx,
		`UPDATE staging_files SET time_expires=?
		 WHERE staging_id=? AND state IN ('uploading','complete')`,
		newExp, stagingID); err != nil {
		return 0, err
	}
	return newExp, nil
}

// FindStagingNeedingRecalc returns staging_ids whose time_expires=0 (set by
// the AFTER DELETE trigger on mission_staging_refs after CASCADE). The Go
// cleanup goroutine drains these.
func FindStagingNeedingRecalc(ctx context.Context, db DBOrConn, limit int) ([]string, error) {
	rows, err := db.QueryContext(ctx, `SELECT staging_id FROM staging_files
		WHERE time_expires=0 LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		result = append(result, id)
	}
	return result, rows.Err()
}
