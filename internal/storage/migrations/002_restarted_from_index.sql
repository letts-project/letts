-- Index the self-referential missions.restarted_from foreign key.
--
-- `restarted_from TEXT REFERENCES missions(mission_id) ON DELETE SET NULL`
-- (see 001) has no covering index. With foreign_keys=ON, deleting ANY mission
-- forces SQLite to find every row whose restarted_from points at it (to set it
-- NULL) — and with no index that is a FULL TABLE SCAN per deleted row. A
-- cleanup batch DELETE is therefore O(rows_deleted * table_size): on a large
-- missions table it holds the single write lock for seconds, starving dispatch
-- writers out past busy_timeout every sweep (~5 min). Measured: deleting 1000
-- of 8000 rows went 887ms -> 8ms with this index.
--
-- Partial: restarted_from is NULL for almost every mission (only restarts set
-- it), so the index stays tiny while still covering the FK's `= <id>` lookups.
CREATE INDEX missions_restarted_from
    ON missions (restarted_from)
    WHERE restarted_from IS NOT NULL;
