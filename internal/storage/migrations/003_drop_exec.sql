DELETE FROM missions WHERE kind = 'exec';

-- SQLite refuses to drop a column that an index still references.
DROP INDEX missions_recent;
DROP INDEX missions_by_group;
DROP INDEX staging_by_sha;

ALTER TABLE missions DROP COLUMN kind;
ALTER TABLE missions DROP COLUMN display_name;
ALTER TABLE missions DROP COLUMN group_id;

CREATE INDEX missions_recent
    ON missions (time_created DESC, mission_id DESC);
