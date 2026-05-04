package storage

import (
	"context"
	"database/sql"
	"errors"
)

// MissionRuntime mirrors the mission_runtime row.
type MissionRuntime struct {
	MissionID           string
	MissionDir          string
	CommandTemplate     string // JSON array
	MissionPathTemplate string
	ValidateMissionFile bool
}

// InsertRuntime inserts a mission_runtime row.
func InsertRuntime(ctx context.Context, db DBOrConn, r *MissionRuntime) error {
	_, err := db.ExecContext(ctx, `INSERT INTO mission_runtime (
		mission_id, mission_dir, command_template, mission_path_template, validate_mission_file
	) VALUES (?, ?, ?, ?, ?)`,
		r.MissionID, r.MissionDir, r.CommandTemplate, r.MissionPathTemplate, boolToInt(r.ValidateMissionFile))
	return err
}

// GetRuntime returns the runtime row for the given mission or ErrNotFound.
func GetRuntime(ctx context.Context, db DBOrConn, id string) (*MissionRuntime, error) {
	var r MissionRuntime
	var v int
	err := db.QueryRowContext(ctx, `SELECT mission_id, mission_dir, command_template,
		mission_path_template, validate_mission_file FROM mission_runtime WHERE mission_id=?`, id).
		Scan(&r.MissionID, &r.MissionDir, &r.CommandTemplate, &r.MissionPathTemplate, &v)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	r.ValidateMissionFile = v != 0
	return &r, nil
}
