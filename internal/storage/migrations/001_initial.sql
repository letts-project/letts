-- Audit M14: auto_vacuum is a DATABASE-FILE property and MUST be set
-- before the first CREATE TABLE; setting it per-connection at runtime
-- is silently ignored, leaving the DB in auto_vacuum=NONE mode where
-- the cleanup.Vacuumer's hourly PRAGMA incremental_vacuum is a no-op.
-- modernc.org/sqlite (and stock SQLite) honour this PRAGMA at first
-- DDL on an empty file.
PRAGMA auto_vacuum=INCREMENTAL;

CREATE TABLE missions (
  mission_id      TEXT    NOT NULL PRIMARY KEY,
  kind            TEXT    NOT NULL DEFAULT 'mission',
  lane            TEXT    NOT NULL,
  mission_name    TEXT    NOT NULL,
  display_name    TEXT,
  group_id        TEXT,
  status          TEXT    NOT NULL,
  outcome         TEXT,
  fail_reason     TEXT,
  fail_message    TEXT,
  fail_details    TEXT,
  exit_code       INTEGER,
  signal          TEXT,
  pid             INTEGER,
  pgid            INTEGER,
  proc_starttime  INTEGER,

  input           BLOB    NOT NULL,
  input_fingerprint TEXT  NOT NULL,
  return_value    BLOB,
  truncated_stdout INTEGER NOT NULL DEFAULT 0,
  truncated_stderr INTEGER NOT NULL DEFAULT 0,

  time_created    INTEGER NOT NULL,
  time_started    INTEGER,
  time_finished   INTEGER,
  timeout_ms      INTEGER,

  restarted_from  TEXT REFERENCES missions(mission_id) ON DELETE SET NULL
) WITHOUT ROWID;

CREATE INDEX missions_queue
    ON missions (lane, time_created, mission_id)
    WHERE status = 'queued';

CREATE INDEX missions_running
    ON missions (lane, time_started, mission_id)
    WHERE status = 'running';

CREATE INDEX missions_cleanup
    ON missions (time_finished)
    WHERE status = 'done';

CREATE INDEX missions_recent
    ON missions (kind, time_created DESC, mission_id DESC);

CREATE INDEX missions_by_name
    ON missions (mission_name, time_created DESC);

CREATE INDEX missions_by_group
    ON missions (group_id, time_created)
    WHERE group_id IS NOT NULL;

CREATE TABLE mission_runtime (
  mission_id            TEXT NOT NULL PRIMARY KEY REFERENCES missions(mission_id) ON DELETE CASCADE,
  mission_dir           TEXT NOT NULL,
  command_template      TEXT NOT NULL,
  mission_path_template TEXT NOT NULL,
  validate_mission_file INTEGER NOT NULL
) WITHOUT ROWID;

CREATE TABLE mission_finalize_intents (
  mission_id      TEXT NOT NULL PRIMARY KEY REFERENCES missions(mission_id) ON DELETE CASCADE,
  phase           TEXT NOT NULL,
  outcome         TEXT NOT NULL,
  return_value    BLOB,
  fail_reason     TEXT,
  fail_message    TEXT,
  fail_details    TEXT,
  exit_code       INTEGER,
  signal          TEXT,
  outputs         TEXT NOT NULL,
  done_seq        INTEGER NOT NULL,
  done_event      TEXT    NOT NULL,
  time_created    INTEGER NOT NULL
) WITHOUT ROWID;

CREATE TABLE staging_files (
  staging_id      TEXT    NOT NULL PRIMARY KEY,
  state           TEXT    NOT NULL,
  sha256          TEXT    NOT NULL,
  size            INTEGER NOT NULL,
  bytes_received  INTEGER NOT NULL DEFAULT 0,
  path            TEXT    NOT NULL,
  time_created    INTEGER NOT NULL,
  time_updated    INTEGER NOT NULL,
  time_expires    INTEGER NOT NULL,
  downloaded_at   INTEGER
) WITHOUT ROWID;

CREATE INDEX staging_expires ON staging_files (time_expires);
CREATE INDEX staging_state   ON staging_files (state, time_expires);

CREATE INDEX staging_by_sha
    ON staging_files (sha256, size)
    WHERE state = 'complete';

CREATE TABLE mission_staging_refs (
  mission_id    TEXT NOT NULL REFERENCES missions(mission_id) ON DELETE CASCADE,
  staging_id    TEXT NOT NULL REFERENCES staging_files(staging_id) ON DELETE RESTRICT,
  ref_kind      TEXT NOT NULL,
  role          TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (mission_id, staging_id, ref_kind, role)
) WITHOUT ROWID;

CREATE INDEX msr_by_staging ON mission_staging_refs (staging_id);

CREATE UNIQUE INDEX msr_unique_role
    ON mission_staging_refs (mission_id, ref_kind, role);

CREATE TABLE config (
  id          INTEGER PRIMARY KEY CHECK (id = 1),
  data        TEXT    NOT NULL,
  applied_at  INTEGER NOT NULL,
  source      TEXT
);

CREATE TRIGGER staging_recalc_after_ref_delete
AFTER DELETE ON mission_staging_refs
FOR EACH ROW
BEGIN
  -- Mark the staging row for TTL recalc by zeroing time_expires; the
  -- recalc runs in Go after the cascade completes (refs.RecalcTTL).
  UPDATE staging_files SET time_expires = 0 WHERE staging_id = OLD.staging_id;
END;
