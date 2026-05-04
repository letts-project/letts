package repair

import (
	"context"
	"database/sql"
	"log/slog"
	"path/filepath"
	"time"

	"letts/internal/config"
	"letts/internal/eventfile"
	"letts/internal/ids"
	"letts/internal/mission"
)

// SweepRunningToLost finalizes every remaining status='running' mission as
// outcome=lost via the standard outputs=[] fast path. Bulk
// UPDATE is forbidden because it would skip the durable done event.
//
// Best-effort kill is attempted first when the row carries pid/pgid/
// proc_starttime — recovers process groups whose leader is still alive
// after an unclean shutdown.
func SweepRunningToLost(ctx context.Context, cfg *config.DugdaleConfig, db *sql.DB, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}
	// Skip rows that still carry a finalize intent: RepairFinalizeIntents
	// runs first and deletes every intent it resolves, so a surviving intent on a
	// 'running' row means it hit a terminal-event conflict and is pending manual
	// repair (criticalerr tripped). Re-attacking it here would collide on the
	// intent PK and emit a misleading "finalize-as-lost failed" warning; making
	// the ordering invariant explicit is cleaner than relying on that collision.
	rows, err := db.QueryContext(ctx, `SELECT mission_id, kind, lane, pid, pgid, proc_starttime, time_started
		FROM missions
		WHERE status='running'
		  AND NOT EXISTS (
		    SELECT 1 FROM mission_finalize_intents fi WHERE fi.mission_id = missions.mission_id
		  )`)
	if err != nil {
		return err
	}
	type lostRow struct {
		ID            string
		Kind          string
		Lane          string
		PID, PGID     sql.NullInt64
		ProcStarttime sql.NullInt64
		TimeStartedMs sql.NullInt64
	}
	var pending []lostRow
	for rows.Next() {
		var r lostRow
		if err := rows.Scan(&r.ID, &r.Kind, &r.Lane, &r.PID, &r.PGID, &r.ProcStarttime, &r.TimeStartedMs); err != nil {
			_ = rows.Close()
			return err
		}
		pending = append(pending, r)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	finCfg := mission.FinalizeConfig{
		DataDir:        cfg.DataDir,
		MaxReturnValue: cfg.Limits.MaxReturnValueSize,
		MaxFailMessage: cfg.Limits.MaxFailMessageSize,
		MaxFailDetails: cfg.Limits.MaxFailDetailsSize,
	}

	for _, r := range pending {
		if r.PID.Valid && r.PGID.Valid && r.ProcStarttime.Valid {
			killed := BestEffortKillPgid(int(r.PID.Int64), int(r.PGID.Int64), r.ProcStarttime.Int64)
			if killed {
				logger.Info("repair: killed leftover process group",
					"mission_id", r.ID, "pid", r.PID.Int64, "pgid", r.PGID.Int64)
			}
		}

		// Ensure events file exists; if missing (rare), create one so Finalize
		// has somewhere to append the done event.
		shard, _ := ids.ShardPath(r.ID)
		parentDir := filepath.Join(cfg.DataDir, "output", shard)
		ensureEventsFile(parentDir, r.ID)

		// For timeout/lost outcomes fail_reason stays NULL.
		// Leave FailReason empty so clients filtering by fail_reason see the
		// expected absence rather than the redundant "lost" string.
		o := mission.OutcomeResult{Outcome: "lost", ExitCode: 0}
		if err := mission.Finalize(ctx, db, mission.FinalizeInputs{
			MissionID:     r.ID,
			Kind:          r.Kind,
			Lane:          r.Lane,
			Outcome:       o,
			Cfg:           finCfg,
			Now:           time.Now,
			TimeStartedMs: r.TimeStartedMs.Int64, // 0 when NULL → duration_ms omitted
		}); err != nil {
			logger.Warn("repair: finalize-as-lost failed",
				"mission_id", r.ID, "err", err)
			continue
		}
		logger.Info("repair: marked mission as lost", "mission_id", r.ID)
	}
	return nil
}

func ensureEventsFile(parentDir, missionID string) {
	w, err := eventfile.Open(parentDir, missionID)
	if err == nil {
		_ = w.Close()
		return
	}
	// Create best-effort.
	if w, err := eventfile.Create(parentDir, missionID); err == nil {
		_, _ = w.Append(eventfile.KindRunning, map[string]any{"time": time.Now().UnixMilli()}, false)
		_ = w.Close()
	}
}
