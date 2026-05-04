package storage

import (
	"context"
	"database/sql"
	"errors"
)

// FinalizePhase tracks the durability phase of a finalization intent.
type FinalizePhase string

const (
	PhasePrepared   FinalizePhase = "prepared"
	PhaseCommitting FinalizePhase = "committing"
)

// FinalizeIntent mirrors the mission_finalize_intents row.
type FinalizeIntent struct {
	MissionID     string
	Phase         FinalizePhase
	Outcome       string
	ReturnValue   []byte
	FailReason    sql.NullString
	FailMessage   sql.NullString
	FailDetails   sql.NullString
	ExitCode      sql.NullInt64
	Signal        sql.NullString
	Outputs       []byte // JSON array
	DoneSeq       int64
	DoneEvent     string
	TimeCreatedMs int64
}

// InsertFinalizeIntent inserts a new finalization intent.
func InsertFinalizeIntent(ctx context.Context, db DBOrConn, in *FinalizeIntent) error {
	_, err := db.ExecContext(ctx, `INSERT INTO mission_finalize_intents (
		mission_id, phase, outcome, return_value, fail_reason, fail_message,
		fail_details, exit_code, signal, outputs, done_seq, done_event, time_created
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		in.MissionID, in.Phase, in.Outcome, in.ReturnValue,
		in.FailReason, in.FailMessage, in.FailDetails, in.ExitCode, in.Signal,
		in.Outputs, in.DoneSeq, in.DoneEvent, in.TimeCreatedMs)
	return err
}

// UpdateFinalizePhase transitions the intent to the given phase.
func UpdateFinalizePhase(ctx context.Context, db DBOrConn, missionID string, phase FinalizePhase) error {
	res, err := db.ExecContext(ctx, `UPDATE mission_finalize_intents SET phase=? WHERE mission_id=?`,
		phase, missionID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// DeleteFinalizeIntent removes the intent after successful finalization.
func DeleteFinalizeIntent(ctx context.Context, db DBOrConn, missionID string) error {
	_, err := db.ExecContext(ctx, `DELETE FROM mission_finalize_intents WHERE mission_id=?`, missionID)
	return err
}

// ListFinalizeIntents returns all intents in the given phase.
func ListFinalizeIntents(ctx context.Context, db DBOrConn, phase FinalizePhase) ([]FinalizeIntent, error) {
	return queryFinalizeIntents(ctx, db,
		`SELECT mission_id, phase, outcome, return_value,
		fail_reason, fail_message, fail_details, exit_code, signal, outputs, done_seq,
		done_event, time_created FROM mission_finalize_intents WHERE phase=?`, phase)
}

// QueryAllFinalizeIntents returns all intents regardless of phase (startup repair).
func QueryAllFinalizeIntents(ctx context.Context, db DBOrConn) ([]FinalizeIntent, error) {
	return queryFinalizeIntents(ctx, db,
		`SELECT mission_id, phase, outcome, return_value,
		fail_reason, fail_message, fail_details, exit_code, signal, outputs, done_seq,
		done_event, time_created FROM mission_finalize_intents`)
}

func queryFinalizeIntents(ctx context.Context, db DBOrConn, q string, args ...any) ([]FinalizeIntent, error) {
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []FinalizeIntent
	for rows.Next() {
		var in FinalizeIntent
		if err := rows.Scan(&in.MissionID, &in.Phase, &in.Outcome, &in.ReturnValue,
			&in.FailReason, &in.FailMessage, &in.FailDetails, &in.ExitCode, &in.Signal,
			&in.Outputs, &in.DoneSeq, &in.DoneEvent, &in.TimeCreatedMs); err != nil {
			return nil, err
		}
		out = append(out, in)
	}
	return out, rows.Err()
}

// GetFinalizeIntent retrieves a single intent (for per-mission post-crash recovery).
func GetFinalizeIntent(ctx context.Context, db DBOrConn, missionID string) (*FinalizeIntent, error) {
	var in FinalizeIntent
	err := db.QueryRowContext(ctx, `SELECT mission_id, phase, outcome, return_value,
		fail_reason, fail_message, fail_details, exit_code, signal, outputs, done_seq,
		done_event, time_created FROM mission_finalize_intents WHERE mission_id=?`, missionID).
		Scan(&in.MissionID, &in.Phase, &in.Outcome, &in.ReturnValue,
			&in.FailReason, &in.FailMessage, &in.FailDetails, &in.ExitCode, &in.Signal,
			&in.Outputs, &in.DoneSeq, &in.DoneEvent, &in.TimeCreatedMs)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return &in, err
}
