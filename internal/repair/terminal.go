package repair

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"letts/internal/config"
	"letts/internal/eventfile"
	"letts/internal/ids"
	"letts/internal/mission"
)

// tailProbeSlack pads the tail-read window past max_event_line_size so the
// window always covers a full done line plus the partial line the window cut
// through at its start.
const tailProbeSlack = 4096

// EnsureTerminalEvents is the startup terminal-event consistency pass: every
// mission with status='done' must have a parseable terminal `done` line in
// its events file, because follow=true streams terminate only on that line
// and archived reads promise it to clients. The normal finalize path appends
// the done durably BEFORE flipping the DB row, so a done row with a done-less
// stream is a corner state — a done line glued unparseably onto a torn
// progress line, a file lost to manual cleanup/disk repair, and similar. For
// each such mission the pass reconstructs a minimal done event from the DB
// row and output refs and appends it at the next seq.
//
// Runs after RepairFinalizeIntents (intents carry the exact outcome and must
// win) and before the running→lost sweep (this pass only touches rows that
// are already done). Per-mission failures are logged and skipped — the pass
// must never block startup.
func EnsureTerminalEvents(ctx context.Context, cfg *config.DugdaleConfig, db *sql.DB, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}

	// List ids only: the data dir can hold a long history of done missions,
	// and holding every row (return_value blobs included) in memory would
	// make the pass cost scale with payload size instead of mission count.
	// The full row is loaded per mission, and only when a repair is needed.
	rows, err := db.QueryContext(ctx, `SELECT mission_id FROM missions WHERE status='done'`)
	if err != nil {
		return err
	}
	var pending []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return err
		}
		pending = append(pending, id)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	// One done line never exceeds max_event_line_size, so a bounded tail
	// read suffices to decide "terminal event present". Fall back to the
	// 1 MiB config default if the limit is unset (test configs).
	window := cfg.Limits.MaxEventLineSize
	if window <= 0 {
		window = 1 << 20
	}
	window += tailProbeSlack

	repaired := 0
	var ctxErr error
	for _, id := range pending {
		// Honour cancellation between missions: unlike the running→lost
		// sweep, whose list only ever holds the few rows that were running
		// at crash time, this loop is sized by every retained done mission,
		// so a long-retention data dir must not pin shutdown behind a full
		// sweep. Stop early, report how far the pass got, and let the next
		// startup resume — the pass is idempotent.
		if err := ctx.Err(); err != nil {
			ctxErr = fmt.Errorf("terminal-event pass stopped after %d repairs: %w", repaired, err)
			break
		}
		shard, err := ids.ShardPath(id)
		if err != nil {
			logger.Warn("repair: terminal-event pass: bad mission id", "mission_id", id, "err", err)
			continue
		}
		parentDir := filepath.Join(cfg.DataDir, "output", shard)
		path := filepath.Join(parentDir, id+"-events")

		found, err := tailHasDoneEvent(path, window)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			logger.Warn("repair: terminal-event probe failed", "mission_id", id, "err", err)
			continue
		}
		if found {
			continue
		}

		outcome, err := appendReconstructedDone(ctx, cfg, db, parentDir, id)
		if err != nil {
			logger.Warn("repair: reconstruct done event failed", "mission_id", id, "err", err)
			continue
		}
		logger.Info("repair: appended reconstructed done event",
			"mission_id", id, "outcome", outcome)
		repaired++
	}
	if repaired > 0 {
		logger.Info("repair: terminal-event consistency pass repaired missions", "count", repaired)
	}
	return ctxErr
}

// tailHasDoneEvent reports whether the tail window of the events file
// contains a complete, parseable `done` line. Reading only the tail keeps
// the pass cheap on long-lived data dirs with many done missions.
func tailHasDoneEvent(path string, window int64) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer func() { _ = f.Close() }()
	st, err := f.Stat()
	if err != nil {
		return false, err
	}
	size := st.Size()
	start := int64(0)
	if size > window {
		start = size - window
	}
	buf := make([]byte, size-start)
	// ReadAt may legally pair io.EOF with a full read at the exact file
	// boundary; only a short read is a real failure here.
	if n, err := f.ReadAt(buf, start); err != nil && !(errors.Is(err, io.EOF) && n == len(buf)) {
		return false, err
	}
	if start > 0 {
		// The window cut through a line: discard up to (and including) the
		// first newline so only complete lines are scanned.
		if i := bytes.IndexByte(buf, '\n'); i >= 0 {
			buf = buf[i+1:]
		} else {
			buf = nil
		}
	}
	for len(buf) > 0 {
		i := bytes.IndexByte(buf, '\n')
		if i < 0 {
			break // trailing partial — not a complete line
		}
		line := buf[:i]
		buf = buf[i+1:]
		if len(line) == 0 || !json.Valid(line) {
			continue // terminated torn-tail junk
		}
		var ev struct {
			Event string `json:"event"`
		}
		if json.Unmarshal(line, &ev) == nil && ev.Event == string(eventfile.KindDone) {
			return true, nil
		}
	}
	return false, nil
}

// appendReconstructedDone rebuilds the done event from the DB row and output
// refs and appends it (fsynced) at the next seq, creating the events file
// first when it is missing. The payload shape must match what Finalize
// emits, so the canonical mission.BuildDoneFields assembles it.
// Returns the row's outcome for logging.
func appendReconstructedDone(ctx context.Context, cfg *config.DugdaleConfig, db *sql.DB, parentDir, missionID string) (string, error) {
	var (
		outcome, failReason, failMessage, signal sql.NullString
		failDetails                              sql.NullString
		exitCode, timeStarted, timeFinished      sql.NullInt64
		returnValue                              []byte
	)
	// Re-check status='done' at load time: the listing snapshot is not a
	// transaction, and only done rows may receive a reconstructed terminal.
	if err := db.QueryRowContext(ctx, `SELECT outcome, fail_reason, fail_message, fail_details,
		exit_code, signal, return_value, time_started, time_finished
		FROM missions WHERE mission_id=? AND status='done'`, missionID).
		Scan(&outcome, &failReason, &failMessage, &failDetails,
			&exitCode, &signal, &returnValue, &timeStarted, &timeFinished); err != nil {
		return "", err
	}

	outputs, err := outputRefsAsCollected(ctx, db, missionID)
	if err != nil {
		return "", err
	}

	o := mission.OutcomeResult{
		Outcome:     outcome.String,
		FailReason:  failReason.String,
		FailMessage: failMessage.String,
		ExitCode:    int(exitCode.Int64),
		Signal:      signal.String,
		Return:      returnValue,
	}
	if failDetails.Valid && failDetails.String != "" {
		o.FailDetails = json.RawMessage(failDetails.String)
	}
	finished := timeFinished.Int64
	if finished <= 0 {
		// Defensive: a done row should always carry time_finished. Better an
		// approximate timestamp than a zero on the public stream.
		finished = time.Now().UnixMilli()
	}
	fields := mission.BuildDoneFields(o, outputs, finished, timeStarted.Int64, 0)

	// When the events file vanished entirely, ensureEventsFile recreates it
	// with a synthetic `running` event stamped with the current time, not the
	// original start — an accepted repair artifact: the terminal done appended
	// below carries the authoritative time_finished.
	ensureEventsFile(parentDir, missionID)
	ew, err := eventfile.Open(parentDir, missionID)
	if err != nil {
		return "", err
	}
	defer func() { _ = ew.Close() }()
	ew.SetLimits(eventfile.Limits{
		MaxEventsBuffer:  cfg.Limits.MaxEventsBuffer,
		MaxEventLineSize: cfg.Limits.MaxEventLineSize,
	})
	_, err = ew.Append(eventfile.KindDone, fields, true)
	return outcome.String, err
}

// outputRefsAsCollected loads the mission's committed output refs in the
// CollectedOutput shape BuildDoneFields summarises (role → staging_id/
// sha256/size). Tmp/final paths are irrelevant for event reconstruction.
func outputRefsAsCollected(ctx context.Context, db *sql.DB, missionID string) ([]mission.CollectedOutput, error) {
	rows, err := db.QueryContext(ctx, `SELECT r.role, r.staging_id, s.sha256, s.size
		FROM mission_staging_refs r
		JOIN staging_files s ON s.staging_id = r.staging_id
		WHERE r.mission_id=? AND r.ref_kind='output'`, missionID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []mission.CollectedOutput
	for rows.Next() {
		var c mission.CollectedOutput
		if err := rows.Scan(&c.Role, &c.StagingID, &c.Sha256, &c.Size); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}
