-- Iceberg table schema DDL and dependency safety.

DROP EXTENSION IF EXISTS pg_iceberg_am CASCADE;
CREATE EXTENSION IF NOT EXISTS pg_iceberg_am;

CREATE TABLE schema_evolution_t (
    id integer NOT NULL,
    payload text NOT NULL
) USING iceberg;

INSERT INTO schema_evolution_t VALUES (1, 'one'), (2, 'two');

SELECT metadata_location AS loc0
FROM iceberg.iceberg_metadata
WHERE relid = 'schema_evolution_t'::regclass \gset

ALTER TABLE schema_evolution_t ADD COLUMN extra bigint;

SELECT metadata_location <> :'loc0' AS changed
FROM iceberg.iceberg_metadata
WHERE relid = 'schema_evolution_t'::regclass;

INSERT INTO schema_evolution_t (id, payload, extra)
VALUES (3, 'three', 30);

SELECT count(*) AS rows, count(extra) AS extra_values, sum(extra) AS extra_sum
FROM schema_evolution_t;

SELECT metadata_location AS loc_after_add
FROM iceberg.iceberg_metadata
WHERE relid = 'schema_evolution_t'::regclass \gset

ALTER TABLE schema_evolution_t ALTER COLUMN payload DROP NOT NULL;

SELECT metadata_location <> :'loc_after_add' AS changed
FROM iceberg.iceberg_metadata
WHERE relid = 'schema_evolution_t'::regclass;

INSERT INTO schema_evolution_t (id, payload, extra)
VALUES (4, NULL, 40);

SELECT count(*) FILTER (WHERE payload IS NULL) AS null_payloads
FROM schema_evolution_t;

SELECT metadata_location AS loc_after_drop_not_null
FROM iceberg.iceberg_metadata
WHERE relid = 'schema_evolution_t'::regclass \gset

ALTER TABLE schema_evolution_t RENAME COLUMN payload TO body;

SELECT metadata_location <> :'loc_after_drop_not_null' AS changed
FROM iceberg.iceberg_metadata
WHERE relid = 'schema_evolution_t'::regclass;

INSERT INTO schema_evolution_t (id, body, extra)
VALUES (5, 'five', 50);

SELECT count(*) FILTER (WHERE body = 'five') AS renamed_reads
FROM schema_evolution_t;

SELECT metadata_location AS loc_after_rename
FROM iceberg.iceberg_metadata
WHERE relid = 'schema_evolution_t'::regclass \gset

ALTER TABLE schema_evolution_t DROP COLUMN extra;

SELECT metadata_location <> :'loc_after_rename' AS changed
FROM iceberg.iceberg_metadata
WHERE relid = 'schema_evolution_t'::regclass;

INSERT INTO schema_evolution_t (id, body)
VALUES (6, 'six');

SELECT string_agg(attname, ',' ORDER BY attnum) AS live_columns
FROM pg_attribute
WHERE attrelid = 'schema_evolution_t'::regclass
  AND attnum > 0
  AND NOT attisdropped;

SELECT count(*) AS rows, count(body) AS body_values
FROM schema_evolution_t;

CREATE TABLE schema_evolution_tx_t (
    id integer NOT NULL
) USING iceberg;

BEGIN;
ALTER TABLE schema_evolution_tx_t ADD COLUMN tx_value integer;
INSERT INTO schema_evolution_tx_t (id, tx_value) VALUES (1, 100);
SELECT count(*) AS rows, sum(tx_value) AS tx_sum
FROM schema_evolution_tx_t;
COMMIT;

SELECT count(*) AS rows, sum(tx_value) AS tx_sum
FROM schema_evolution_tx_t;

CREATE TABLE schema_evolution_multicmd_t (
    id integer NOT NULL,
    old_value integer
) USING iceberg;

ALTER TABLE schema_evolution_multicmd_t
    ADD COLUMN added_optional integer,
    DROP COLUMN old_value;

INSERT INTO schema_evolution_multicmd_t (id, added_optional)
VALUES (1, NULL), (2, 20);

SELECT string_agg(attname, ',' ORDER BY attnum) AS live_columns
FROM pg_attribute
WHERE attrelid = 'schema_evolution_multicmd_t'::regclass
  AND attnum > 0
  AND NOT attisdropped;

SELECT count(*) AS rows,
       count(added_optional) AS added_values,
       sum(added_optional) AS added_sum
FROM schema_evolution_multicmd_t;

CREATE TABLE schema_evolution_replace_same_stmt_t (
    id integer NOT NULL,
    a text
) USING iceberg;

INSERT INTO schema_evolution_replace_same_stmt_t (id, a)
VALUES (1, 'old');

ALTER TABLE schema_evolution_replace_same_stmt_t
    ADD COLUMN a integer,
    DROP COLUMN a;

INSERT INTO schema_evolution_replace_same_stmt_t (id, a)
VALUES (2, 20);

COPY (
    SELECT string_agg(attname || ':' || atttypid::regtype::text, ',' ORDER BY attnum) AS live_columns
    FROM pg_attribute
    WHERE attrelid = 'schema_evolution_replace_same_stmt_t'::regclass
      AND attnum > 0
      AND NOT attisdropped
) TO STDOUT WITH (FORMAT csv, HEADER true);

COPY (
    SELECT count(*) AS rows, count(a) AS a_values, sum(a) AS a_sum
    FROM schema_evolution_replace_same_stmt_t
) TO STDOUT WITH (FORMAT csv, HEADER true);

CREATE TABLE schema_evolution_drop_required_t (
    id integer NOT NULL,
    dropped_required integer NOT NULL,
    keep_value text
) USING iceberg;

INSERT INTO schema_evolution_drop_required_t
VALUES (1, 10, 'before');

ALTER TABLE schema_evolution_drop_required_t DROP COLUMN dropped_required;

INSERT INTO schema_evolution_drop_required_t (id, keep_value)
VALUES (2, 'after');

SELECT string_agg(attname, ',' ORDER BY attnum) AS live_columns
FROM pg_attribute
WHERE attrelid = 'schema_evolution_drop_required_t'::regclass
  AND attnum > 0
  AND NOT attisdropped;

SELECT count(*) AS rows, string_agg(keep_value, ',' ORDER BY id) AS kept
FROM schema_evolution_drop_required_t;

CREATE TABLE schema_evolution_part_root_t (
    id integer NOT NULL,
    payload text
) PARTITION BY RANGE (id) USING iceberg;

CREATE TABLE schema_evolution_part_root_t_p0
PARTITION OF schema_evolution_part_root_t
FOR VALUES FROM (0) TO (100) USING iceberg;

CREATE TABLE schema_evolution_part_root_t_p1
PARTITION OF schema_evolution_part_root_t
FOR VALUES FROM (100) TO (200) USING iceberg;

INSERT INTO schema_evolution_part_root_t
VALUES (1, 'one'), (101, 'one hundred one');

ALTER TABLE schema_evolution_part_root_t ADD COLUMN extra integer;

INSERT INTO schema_evolution_part_root_t (id, payload, extra)
VALUES (2, 'two', 20), (102, 'one hundred two', 120);

COPY (
    SELECT count(*) AS rows, count(extra) AS extra_values, sum(extra) AS extra_sum
    FROM schema_evolution_part_root_t
) TO STDOUT WITH (FORMAT csv, HEADER true);

ALTER TABLE schema_evolution_part_root_t DROP COLUMN extra;
ALTER TABLE schema_evolution_part_root_t RENAME COLUMN payload TO body;

INSERT INTO schema_evolution_part_root_t (id, body)
VALUES (3, 'three'), (103, 'one hundred three');

COPY (
    SELECT string_agg(attname, ',' ORDER BY attnum) AS live_columns
    FROM pg_attribute
    WHERE attrelid = 'schema_evolution_part_root_t'::regclass
      AND attnum > 0
      AND NOT attisdropped
) TO STDOUT WITH (FORMAT csv, HEADER true);

COPY (
    SELECT count(*) AS rows, count(body) AS body_values
    FROM schema_evolution_part_root_t
) TO STDOUT WITH (FORMAT csv, HEADER true);

COPY (
    SELECT rel::regclass::text AS rel,
           metadata.metadata_location IS NOT NULL AS has_metadata
    FROM (
        VALUES
            ('schema_evolution_part_root_t'::regclass),
            ('schema_evolution_part_root_t_p0'::regclass),
            ('schema_evolution_part_root_t_p1'::regclass)
    ) AS relations(rel)
    LEFT JOIN iceberg.iceberg_metadata AS metadata ON metadata.relid = relations.rel
    ORDER BY rel
) TO STDOUT WITH (FORMAT csv, HEADER true);

CREATE TABLE schema_evolution_mixed_root_t (
    id integer NOT NULL,
    payload text
) PARTITION BY RANGE (id) USING iceberg;

CREATE TABLE schema_evolution_mixed_root_t_iceberg
PARTITION OF schema_evolution_mixed_root_t
FOR VALUES FROM (0) TO (100) USING iceberg;

CREATE TABLE schema_evolution_mixed_root_t_heap
PARTITION OF schema_evolution_mixed_root_t
FOR VALUES FROM (100) TO (200) USING heap;

\set VERBOSITY terse
ALTER TABLE schema_evolution_mixed_root_t ADD COLUMN should_fail integer;
\set VERBOSITY default

DROP TABLE schema_evolution_mixed_root_t;

CREATE TABLE schema_evolution_epoch_t (
    id integer NOT NULL
) USING iceberg;

BEGIN;
ALTER TABLE schema_evolution_epoch_t ADD COLUMN transient integer;
INSERT INTO schema_evolution_epoch_t (id, transient) VALUES (1, 10);
ALTER TABLE schema_evolution_epoch_t DROP COLUMN transient;
SELECT string_agg(attname, ',' ORDER BY attnum) AS live_columns
FROM pg_attribute
WHERE attrelid = 'schema_evolution_epoch_t'::regclass
  AND attnum > 0
  AND NOT attisdropped;
COMMIT;

SELECT count(*) AS rows
FROM schema_evolution_epoch_t;

ALTER TABLE schema_evolution_epoch_t ADD COLUMN transient integer;
INSERT INTO schema_evolution_epoch_t (id, transient) VALUES (2, 20);

SELECT count(*) AS rows,
       count(transient) AS transient_values,
       sum(transient) AS transient_sum
FROM schema_evolution_epoch_t;

CREATE TABLE schema_evolution_savepoint_t (
    id integer NOT NULL,
    must_keep integer NOT NULL
) USING iceberg;

BEGIN;
ALTER TABLE schema_evolution_savepoint_t ADD COLUMN keep_col integer;
SAVEPOINT sp_rollback_add;
ALTER TABLE schema_evolution_savepoint_t ADD COLUMN rolled_col integer;
ROLLBACK TO sp_rollback_add;
INSERT INTO schema_evolution_savepoint_t (id, must_keep, keep_col)
VALUES (1, 1, 10);
COMMIT;

ALTER TABLE schema_evolution_savepoint_t ADD COLUMN rolled_col integer;
INSERT INTO schema_evolution_savepoint_t
    (id, must_keep, keep_col, rolled_col)
VALUES (2, 2, 20, 200);

BEGIN;
SAVEPOINT sp_release;
ALTER TABLE schema_evolution_savepoint_t ADD COLUMN released_col integer;
RELEASE SAVEPOINT sp_release;
SAVEPOINT sp_release_sibling;
ALTER TABLE schema_evolution_savepoint_t ADD COLUMN sibling_rolled integer;
ROLLBACK TO sp_release_sibling;
INSERT INTO schema_evolution_savepoint_t
    (id, must_keep, keep_col, rolled_col, released_col)
VALUES (3, 3, 30, 300, 3000);
COMMIT;

ALTER TABLE schema_evolution_savepoint_t ADD COLUMN sibling_rolled integer;

BEGIN;
SAVEPOINT sp_rename;
ALTER TABLE schema_evolution_savepoint_t RENAME COLUMN keep_col TO keep_renamed;
ROLLBACK TO sp_rename;
INSERT INTO schema_evolution_savepoint_t
    (id, must_keep, keep_col, rolled_col, released_col, sibling_rolled)
VALUES (4, 4, 40, 400, 4000, 40000);
COMMIT;

BEGIN;
SAVEPOINT sp_drop;
ALTER TABLE schema_evolution_savepoint_t DROP COLUMN rolled_col;
ROLLBACK TO sp_drop;
INSERT INTO schema_evolution_savepoint_t
    (id, must_keep, keep_col, rolled_col, released_col, sibling_rolled)
VALUES (5, 5, 50, 500, 5000, 50000);
COMMIT;

BEGIN;
SAVEPOINT sp_drop_not_null;
ALTER TABLE schema_evolution_savepoint_t ALTER COLUMN must_keep DROP NOT NULL;
ROLLBACK TO sp_drop_not_null;
INSERT INTO schema_evolution_savepoint_t
    (id, must_keep, keep_col, rolled_col, released_col, sibling_rolled)
VALUES (6, 6, 60, 600, 6000, 60000);
COMMIT;

SELECT string_agg(attname, ',' ORDER BY attnum) AS live_columns
FROM pg_attribute
WHERE attrelid = 'schema_evolution_savepoint_t'::regclass
  AND attnum > 0
  AND NOT attisdropped;

SELECT count(*) AS rows,
       count(keep_col) AS keep_values,
       count(rolled_col) AS rolled_values,
       count(released_col) AS released_values,
       count(sibling_rolled) AS sibling_values,
       bool_and(must_keep IS NOT NULL) AS must_keep_not_null
FROM schema_evolution_savepoint_t;

\set VERBOSITY terse
ALTER TABLE schema_evolution_t ADD COLUMN bad_required integer NOT NULL;
ALTER TABLE schema_evolution_t ADD COLUMN bad_default integer DEFAULT 1;
ALTER TABLE schema_evolution_t ADD COLUMN bad_identity integer GENERATED ALWAYS AS IDENTITY;
ALTER TABLE schema_evolution_t ADD COLUMN bad_check integer CHECK (bad_check > 0);
ALTER TABLE schema_evolution_t ADD COLUMN IF NOT EXISTS maybe_bad integer;
ALTER TABLE schema_evolution_t ALTER COLUMN id TYPE bigint;
ALTER TABLE schema_evolution_t ALTER COLUMN id SET NOT NULL;
ALTER TABLE schema_evolution_t ALTER COLUMN id SET DEFAULT 1;
ALTER TABLE schema_evolution_t DROP COLUMN IF EXISTS maybe_bad;
ALTER TABLE schema_evolution_t DROP COLUMN body CASCADE;
\set VERBOSITY default

DROP TABLE schema_evolution_savepoint_t;
DROP TABLE schema_evolution_epoch_t;
DROP TABLE schema_evolution_part_root_t;
DROP TABLE schema_evolution_drop_required_t;
DROP TABLE schema_evolution_replace_same_stmt_t;
DROP TABLE schema_evolution_multicmd_t;
DROP TABLE schema_evolution_tx_t;
DROP TABLE schema_evolution_t;

-- Dependency-driven column-drop guard.
-- Only the supported ALTER TABLE path may drop an Iceberg column.
DROP EXTENSION IF EXISTS pg_iceberg_am CASCADE;
CREATE EXTENSION pg_iceberg_am;
CREATE SCHEMA schema_dependency_drop_guard;

CREATE TABLE schema_dependency_drop_guard.controlled_t (
    id integer,
    value text
) USING iceberg;
ALTER TABLE schema_dependency_drop_guard.controlled_t DROP COLUMN value;
SELECT string_agg(attname, ',' ORDER BY attnum) AS live_columns
FROM pg_attribute
WHERE attrelid = 'schema_dependency_drop_guard.controlled_t'::regclass
  AND attnum > 0
  AND NOT attisdropped;

CREATE COLLATION schema_dependency_drop_guard.drop_me FROM "C";
CREATE TABLE schema_dependency_drop_guard.dependency_t (
    id integer,
    value text COLLATE schema_dependency_drop_guard.drop_me
) USING iceberg;
INSERT INTO schema_dependency_drop_guard.dependency_t VALUES (1, 'kept');

\set VERBOSITY terse
DROP COLLATION schema_dependency_drop_guard.drop_me CASCADE;
\set VERBOSITY default

SELECT string_agg(attname, ',' ORDER BY attnum) AS live_columns
FROM pg_attribute
WHERE attrelid = 'schema_dependency_drop_guard.dependency_t'::regclass
  AND attnum > 0
  AND NOT attisdropped;
SELECT count(*) AS rows_after_rejected_dependency_drop
FROM schema_dependency_drop_guard.dependency_t;

DROP SCHEMA schema_dependency_drop_guard CASCADE;
DROP EXTENSION pg_iceberg_am CASCADE;
