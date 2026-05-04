package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// escapeLikePattern escapes SQL LIKE metacharacters (\ % _) so a user-supplied
// substring matches literally under `LIKE ? ESCAPE '\'`.
func escapeLikePattern(s string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(s)
}

// Status/Kind/Outcome string constants — keep these as Go-typed constants for
// compile-time safety; values match DB column text.
type Status string
type Kind string
type Outcome string

const (
	StatusQueued   Status = "queued"
	StatusRunning  Status = "running"
	StatusDone     Status = "done"
	StatusDeleting Status = "deleting"

	KindMission Kind = "mission"
	KindExec    Kind = "exec"

	OutcomeSuccess Outcome = "success"
	OutcomeFailed  Outcome = "failed"
	OutcomeKilled  Outcome = "killed"
	OutcomeTimeout Outcome = "timeout"
	OutcomeCrashed Outcome = "crashed"
	OutcomeLost    Outcome = "lost"
	OutcomeOOM     Outcome = "oom"
)

// Mission mirrors the missions row.
type Mission struct {
	ID               string
	Kind             Kind
	Lane             string
	MissionName      string
	DisplayName      sql.NullString
	GroupID          sql.NullString
	Status           Status
	Outcome          sql.NullString
	FailReason       sql.NullString
	FailMessage      sql.NullString
	FailDetails      sql.NullString
	ExitCode         sql.NullInt64
	Signal           sql.NullString
	PID              sql.NullInt64
	PGID             sql.NullInt64
	ProcStarttime    sql.NullInt64
	Input            []byte
	InputFingerprint string
	ReturnValue      []byte
	TruncatedStdout  bool
	TruncatedStderr  bool
	TimeCreatedMs    int64
	TimeStartedMs    sql.NullInt64
	TimeFinishedMs   sql.NullInt64
	TimeoutMs        sql.NullInt64
	RestartedFrom    sql.NullString
}

// ErrNotFound is returned when a row lookup misses.
var ErrNotFound = errors.New("not found")

// DBOrConn lets us share queries between *sql.DB and *sql.Conn (for tx callers).
type DBOrConn interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// InsertMission inserts a new row. Caller is responsible for transaction;
// for stand-alone insert use db, for atomic insert with refs use *sql.Conn.
func InsertMission(ctx context.Context, db DBOrConn, m *Mission) error {
	const q = `INSERT INTO missions (
		mission_id, kind, lane, mission_name, display_name, group_id, status,
		outcome, fail_reason, fail_message, fail_details, exit_code, signal,
		pid, pgid, proc_starttime, input, input_fingerprint, return_value,
		truncated_stdout, truncated_stderr,
		time_created, time_started, time_finished, timeout_ms, restarted_from
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	_, err := db.ExecContext(ctx, q,
		m.ID, m.Kind, m.Lane, m.MissionName, m.DisplayName, m.GroupID, m.Status,
		m.Outcome, m.FailReason, m.FailMessage, m.FailDetails, m.ExitCode, m.Signal,
		m.PID, m.PGID, m.ProcStarttime, m.Input, m.InputFingerprint, m.ReturnValue,
		boolToInt(m.TruncatedStdout), boolToInt(m.TruncatedStderr),
		m.TimeCreatedMs, m.TimeStartedMs, m.TimeFinishedMs, m.TimeoutMs, m.RestartedFrom,
	)
	if err != nil {
		return fmt.Errorf("insert mission %s: %w", m.ID, err)
	}
	return nil
}

const missionSelectCols = `mission_id, kind, lane, mission_name, display_name, group_id,
	status, outcome, fail_reason, fail_message, fail_details, exit_code, signal,
	pid, pgid, proc_starttime, input, input_fingerprint, return_value,
	truncated_stdout, truncated_stderr, time_created, time_started, time_finished,
	timeout_ms, restarted_from`

func scanMission(row interface {
	Scan(dest ...any) error
}) (*Mission, error) {
	var m Mission
	var truncOut, truncErr int
	err := row.Scan(
		&m.ID, &m.Kind, &m.Lane, &m.MissionName, &m.DisplayName, &m.GroupID,
		&m.Status, &m.Outcome, &m.FailReason, &m.FailMessage, &m.FailDetails,
		&m.ExitCode, &m.Signal, &m.PID, &m.PGID, &m.ProcStarttime,
		&m.Input, &m.InputFingerprint, &m.ReturnValue,
		&truncOut, &truncErr, &m.TimeCreatedMs, &m.TimeStartedMs, &m.TimeFinishedMs,
		&m.TimeoutMs, &m.RestartedFrom,
	)
	if err != nil {
		return nil, err
	}
	m.TruncatedStdout = truncOut != 0
	m.TruncatedStderr = truncErr != 0
	return &m, nil
}

// GetMission returns the mission by ID or ErrNotFound.
func GetMission(ctx context.Context, db DBOrConn, id string) (*Mission, error) {
	q := `SELECT ` + missionSelectCols + ` FROM missions WHERE mission_id=?`
	m, err := scanMission(db.QueryRowContext(ctx, q, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return m, nil
}

// GetMissionLabels returns kind/lane/time_started_ms for one mission. Used
// by the repair paths to populate Prometheus labels at intent-replay time
// when the original FinalizeInputs are not in memory.
func GetMissionLabels(ctx context.Context, db DBOrConn, id string) (kind, lane string, timeStartedMs int64, err error) {
	row := db.QueryRowContext(ctx,
		`SELECT kind, lane, COALESCE(time_started, 0) FROM missions WHERE mission_id = ?`, id)
	err = row.Scan(&kind, &lane, &timeStartedMs)
	if errors.Is(err, sql.ErrNoRows) {
		err = ErrNotFound
	}
	return
}

// PickQueuedForLane returns the oldest queued mission for the lane, or
// ErrNotFound if empty. Caller must hold writer transaction.
func PickQueuedForLane(ctx context.Context, conn *sql.Conn, lane string) (*Mission, error) {
	// Skip any queued mission that already has a finalize intent:
	// a queued-kill commits its Phase-A2 intent under the writer lock while the
	// row is still 'queued', then runs the final UPDATE in a second tx. Between
	// those two steps the runner must not claim the row, or it would spawn a
	// mission that is being killed. The intent is removed once the kill (or
	// startup repair) finalizes the row to done(killed).
	q := `SELECT ` + missionSelectCols + ` FROM missions
		WHERE status='queued' AND lane=?
		  AND NOT EXISTS (
		    SELECT 1 FROM mission_finalize_intents fi
		    WHERE fi.mission_id = missions.mission_id
		  )
		ORDER BY time_created, mission_id LIMIT 1`
	m, err := scanMission(conn.QueryRowContext(ctx, q, lane))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return m, nil
}

// MarkRunning transitions queued → running with time_started=now. Returns
// ErrNotFound if the row no longer matches (e.g., killed concurrently).
func MarkRunning(ctx context.Context, conn *sql.Conn, id string, nowMs int64, pid, pgid, procStart int64) error {
	res, err := conn.ExecContext(ctx, `UPDATE missions
		SET status='running', time_started=?, pid=?, pgid=?, proc_starttime=?
		WHERE mission_id=? AND status='queued'`, nowMs, pid, pgid, procStart, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// UpdateRunningPid sets the OS pid/pgid/proc_starttime on a row that is
// already in status='running'. Lane runners atomically pick a queued
// mission and place a status='running' placeholder with pid=0 in the same
// writer transaction (PickQueuedForLane and MarkRunning), which prevents
// two runners from claiming the same mission. UpdateRunningPid then fills
// in the real OS values once Spawn returns the process info — without
// this step, completed rows would surface pid=0 to anyone querying
// GET /v1/missions/{id}.
//
// Returns ErrNotFound if the row is no longer status='running' (e.g.
// killed concurrently between MarkRunning and the post-Spawn update).
func UpdateRunningPid(ctx context.Context, conn *sql.Conn, id string, pid, pgid, procStart int64) error {
	res, err := conn.ExecContext(ctx, `UPDATE missions
		SET pid=?, pgid=?, proc_starttime=?
		WHERE mission_id=? AND status='running'`, pid, pgid, procStart, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// UpdateRunningPidAndTimeStarted is the spawn-time variant of
// UpdateRunningPid that also overwrites time_started with the spawn
// timestamp. MarkRunning at lane pickup records the row's
// time_started against the pre-spawn moment, but the done event's
// duration_ms uses a later nowMs captured after ResolveCommand /
// LoadInputs / Spawn / eventfile.Open. Aligning the DB value with the
// post-spawn nowMs keeps GET /v1/missions/{id}.duration_ms identical to
// /events done.duration_ms (matches the fix on the time_finished
// side).
func UpdateRunningPidAndTimeStarted(ctx context.Context, conn *sql.Conn, id string, pid, pgid, procStart, timeStartedMs int64) error {
	res, err := conn.ExecContext(ctx, `UPDATE missions
		SET pid=?, pgid=?, proc_starttime=?, time_started=?
		WHERE mission_id=? AND status='running'`, pid, pgid, procStart, timeStartedMs, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// ListFilter narrows a missions listing.
type ListFilter struct {
	Status        string
	Outcome       string
	Lane          string
	Mission       string // substring match on mission name
	MissionPrefix string // anchored prefix match on mission name
	Kind          string
	GroupID       string // exec group filter
	Order         string // "" / "created" (default) | "finished"
	SinceMs       int64
	UntilMs       int64
}

// Cursor for stable pagination. The active field depends on the listing
// order: TimeCreatedMs for the default (created) order, TimeFinishedMs for
// order="finished".
type Cursor struct {
	TimeCreatedMs  int64  `json:"time_created,omitempty"`
	TimeFinishedMs int64  `json:"time_finished,omitempty"`
	MissionID      string `json:"mission_id"`
}

// ListMissions returns up to limit missions matching filter. Results are
// ordered by (time_created DESC, mission_id DESC) by default, or by
// (time_finished DESC, mission_id DESC) when f.Order=="finished" (which also
// excludes rows with NULL time_finished). When no explicit f.Status filter is
// given, internal status='deleting' rows are excluded. Cursor pagination is
// stable on the active ordering column.
func ListMissions(ctx context.Context, db DBOrConn, f ListFilter, after *Cursor, limit int) ([]Mission, error) {
	var conds []string
	var args []any
	if f.Kind != "" {
		conds = append(conds, "kind = ?")
		args = append(args, f.Kind)
	}
	if f.GroupID != "" {
		conds = append(conds, "group_id = ?")
		args = append(args, f.GroupID)
	}
	if f.Status != "" {
		conds = append(conds, "status = ?")
		args = append(args, f.Status)
	} else {
		// Never surface internal 'deleting' rows in default/unfiltered
		// listings — their detail/events 404, so they'd be broken UI rows.
		// Explicit status=deleting still returns them (cleanup debugging).
		conds = append(conds, "status != 'deleting'")
	}
	if f.Outcome != "" {
		conds = append(conds, "outcome = ?")
		args = append(args, f.Outcome)
	}
	if f.Lane != "" {
		conds = append(conds, "lane = ?")
		args = append(args, f.Lane)
	}
	if f.Mission != "" {
		// Substring match on the mission name. Escape LIKE metacharacters so a
		// name containing % or _ is matched literally.
		conds = append(conds, `mission_name LIKE ? ESCAPE '\'`)
		args = append(args, "%"+escapeLikePattern(f.Mission)+"%")
	}
	if f.MissionPrefix != "" {
		// Anchored prefix match on the mission name: only the trailing % is
		// added, so the pattern matches names that START with the prefix.
		conds = append(conds, `mission_name LIKE ? ESCAPE '\'`)
		args = append(args, escapeLikePattern(f.MissionPrefix)+"%")
	}
	finished := f.Order == "finished"
	if finished {
		conds = append(conds, "time_finished IS NOT NULL")
	}
	if f.SinceMs > 0 {
		conds = append(conds, "time_created >= ?")
		args = append(args, f.SinceMs)
	}
	if f.UntilMs > 0 {
		conds = append(conds, "time_created < ?")
		args = append(args, f.UntilMs)
	}
	if after != nil {
		if finished {
			conds = append(conds, "(time_finished, mission_id) < (?, ?)")
			args = append(args, after.TimeFinishedMs, after.MissionID)
		} else {
			conds = append(conds, "(time_created, mission_id) < (?, ?)")
			args = append(args, after.TimeCreatedMs, after.MissionID)
		}
	}
	where := ""
	if len(conds) > 0 {
		where = "WHERE " + strings.Join(conds, " AND ")
	}
	orderBy := "time_created DESC, mission_id DESC"
	if finished {
		orderBy = "time_finished DESC, mission_id DESC"
	}
	q := `SELECT ` + missionSelectCols + ` FROM missions ` + where + `
		ORDER BY ` + orderBy + ` LIMIT ?`
	args = append(args, limit)
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []Mission
	for rows.Next() {
		m, err := scanMission(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *m)
	}
	return out, rows.Err()
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
