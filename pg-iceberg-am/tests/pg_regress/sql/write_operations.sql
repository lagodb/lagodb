-- Core INSERT, UPDATE, DELETE, and MERGE semantics for Iceberg tables.

DROP EXTENSION IF EXISTS pg_iceberg_am CASCADE;
CREATE EXTENSION IF NOT EXISTS pg_iceberg_am;
CREATE SCHEMA dml_lifecycle;

-- INSERT ... SELECT ... RETURNING must not finalize on non-null slots.
CREATE TABLE dml_lifecycle.returning_t (
    id integer,
    label text
) USING iceberg;

INSERT INTO dml_lifecycle.returning_t
SELECT g, 'returning_' || g
FROM generate_series(1, 3) AS g
RETURNING id, label;

SELECT count(*) AS rows_after_returning FROM dml_lifecycle.returning_t;

-- Data-modifying CTEs are exposed through es_auxmodifytables.
CREATE TABLE dml_lifecycle.cte_t (
    id integer,
    label text
) USING iceberg;

WITH inserted AS (
    INSERT INTO dml_lifecycle.cte_t
    SELECT g, 'cte_' || g
    FROM generate_series(10, 12) AS g
    RETURNING id
)
SELECT count(*) AS returned_rows, min(id) AS min_id, max(id) AS max_id
FROM inserted;

SELECT * FROM dml_lifecycle.cte_t ORDER BY id;

WITH updated AS (
    UPDATE dml_lifecycle.cte_t
    SET label = label || '_updated'
    WHERE id = 11
    RETURNING id, label
)
SELECT * FROM updated;

WITH deleted AS (
    DELETE FROM dml_lifecycle.cte_t
    WHERE id = 10
    RETURNING id, label
)
SELECT * FROM deleted;

SELECT * FROM dml_lifecycle.cte_t ORDER BY id;

-- Sibling ModifyTable nodes for the same relation share one executor-query
-- file-ID namespace while retaining independent relation-local write states.
WITH first_update AS (
    UPDATE dml_lifecycle.cte_t
    SET label = label || '_first'
    WHERE id = 11
    RETURNING id, label
), second_update AS (
    UPDATE dml_lifecycle.cte_t
    SET label = label || '_second'
    WHERE id = 12
    RETURNING id, label
)
SELECT *
FROM (
    SELECT * FROM first_update
    UNION ALL
    SELECT * FROM second_update
) AS changed
ORDER BY id;

SELECT * FROM dml_lifecycle.cte_t ORDER BY id;

-- MERGE insert-only still runs as CMD_MERGE in the outer CustomScan.
CREATE TABLE dml_lifecycle.merge_insert_t (
    id integer,
    label text
) USING iceberg;

MERGE INTO dml_lifecycle.merge_insert_t AS target
USING (VALUES (21, 'merge_21'), (22, 'merge_22')) AS source(id, label)
ON target.id = source.id
WHEN NOT MATCHED THEN
    INSERT (id, label) VALUES (source.id, source.label);

SELECT * FROM dml_lifecycle.merge_insert_t ORDER BY id;

-- A constant-false MERGE condition lets PostgreSQL replace the target side
-- with a dummy relation. The remaining NOT MATCHED action is an independent
-- append and must not require a target-scan snapshot that cannot exist.
MERGE INTO dml_lifecycle.merge_insert_t AS target
USING (VALUES (23, 'merge_23'), (24, 'merge_24')) AS source(id, label)
ON FALSE
WHEN NOT MATCHED THEN
    INSERT (id, label) VALUES (source.id, source.label);

SELECT * FROM dml_lifecycle.merge_insert_t ORDER BY id;

-- UPDATE and DELETE use the Modify scan's synthetic ctid and remain visible
-- through Iceberg position deletes plus appended data files.
CREATE TABLE dml_lifecycle.update_delete_t (
    id integer,
    label text
) USING iceberg;

INSERT INTO dml_lifecycle.update_delete_t VALUES
    (1, 'one'),
    (2, 'two'),
    (3, 'three'),
    (4, 'four');

UPDATE dml_lifecycle.update_delete_t
SET label = label || '_updated'
WHERE id IN (2, 4);

SELECT id, label FROM dml_lifecycle.update_delete_t ORDER BY id;

-- tableoid is a logical relation identity and remains usable in DML quals.
UPDATE dml_lifecycle.update_delete_t
SET label = label
WHERE tableoid = 'dml_lifecycle.update_delete_t'::regclass AND id = 999;

-- User-visible heap system columns are not Iceberg row identity. The planner
-- must reject them instead of exposing the executor-local synthetic ctid.
\set VERBOSITY sqlstate
UPDATE dml_lifecycle.update_delete_t
SET label = 'invalid'
WHERE ctid = '(0,1)'::tid;
DELETE FROM dml_lifecycle.update_delete_t
WHERE xmin = '1'::xid;
\set VERBOSITY default

-- Generated columns, CHECK constraints, RETURNING, and prepared statements
-- must retain PostgreSQL's normal ModifyTable semantics.
CREATE TABLE dml_lifecycle.generated_t (
    id integer,
    base integer CHECK (base > 0),
    doubled integer GENERATED ALWAYS AS (base * 2) STORED
) USING iceberg;

INSERT INTO dml_lifecycle.generated_t (id, base)
VALUES (1, 5), (2, 7)
RETURNING id, base, doubled;

PREPARE update_generated(integer, integer) AS
UPDATE dml_lifecycle.generated_t
SET base = $2
WHERE id = $1
RETURNING id, base, doubled;

EXECUTE update_generated(2, 9);
DEALLOCATE update_generated;

\set VERBOSITY terse
UPDATE dml_lifecycle.generated_t SET base = -1 WHERE id = 1;
\set VERBOSITY default

SELECT * FROM dml_lifecycle.generated_t ORDER BY id;

DELETE FROM dml_lifecycle.update_delete_t
WHERE id IN (1, 3);

SELECT id, label FROM dml_lifecycle.update_delete_t ORDER BY id;

-- An unqualified DELETE needs only Iceberg row identity metadata; no business
-- columns should be projected or decoded.
CREATE TABLE dml_lifecycle.identity_only_delete_t (
    id integer,
    label text
) USING iceberg;
INSERT INTO dml_lifecycle.identity_only_delete_t
VALUES (1, 'one'), (2, 'two');
DELETE FROM dml_lifecycle.identity_only_delete_t;
SELECT count(*) AS identity_only_rows
FROM dml_lifecycle.identity_only_delete_t;


-- Iceberg v1 has no position deletes, so row-level UPDATE/DELETE must fail
-- before staging files or reaching commit.
CREATE TABLE dml_lifecycle.v1_dml_t (
    id integer,
    label text
) USING iceberg WITH (
    "format-version" = 1
);

INSERT INTO dml_lifecycle.v1_dml_t VALUES (1, 'one');

\set VERBOSITY terse
DELETE FROM dml_lifecycle.v1_dml_t WHERE id = 1;
UPDATE dml_lifecycle.v1_dml_t SET label = 'updated' WHERE id = 1;
\set VERBOSITY default

-- INSERT-only MERGE plans write no position deletes, even when their join
-- reads the target, so they remain valid appends on an Iceberg v1 table.
MERGE INTO dml_lifecycle.v1_dml_t AS target
USING (VALUES (2, 'two')) AS source(id, label)
ON FALSE
WHEN NOT MATCHED THEN
    INSERT (id, label) VALUES (source.id, source.label);

MERGE INTO dml_lifecycle.v1_dml_t AS target
USING (VALUES (3, 'three')) AS source(id, label)
ON target.id = source.id
WHEN NOT MATCHED THEN
    INSERT (id, label) VALUES (source.id, source.label);

SELECT id, label FROM dml_lifecycle.v1_dml_t ORDER BY id;

-- Matched MERGE branches exercise the same AM update/delete/lock paths through
-- PostgreSQL's MERGE executor.
CREATE TABLE dml_lifecycle.merge_update_t (
    id integer,
    label text
) USING iceberg;

INSERT INTO dml_lifecycle.merge_update_t VALUES (31, 'old_31');

MERGE INTO dml_lifecycle.merge_update_t AS target
USING (VALUES (31, 'new_31')) AS source(id, label)
ON target.id = source.id
WHEN MATCHED THEN
    UPDATE SET label = source.label;

SELECT id, label FROM dml_lifecycle.merge_update_t ORDER BY id;
INSERT INTO dml_lifecycle.merge_update_t VALUES (32, 'after_merge');
SELECT id, label FROM dml_lifecycle.merge_update_t ORDER BY id;

CREATE TABLE dml_lifecycle.merge_delete_t (
    id integer,
    label text
) USING iceberg;

INSERT INTO dml_lifecycle.merge_delete_t VALUES (41, 'old_41');

MERGE INTO dml_lifecycle.merge_delete_t AS target
USING (VALUES (41)) AS source(id)
ON target.id = source.id
WHEN MATCHED THEN
    DELETE;

SELECT id, label FROM dml_lifecycle.merge_delete_t ORDER BY id;
INSERT INTO dml_lifecycle.merge_delete_t VALUES (42, 'after_merge');
SELECT id, label FROM dml_lifecycle.merge_delete_t ORDER BY id;

-- UPDATE ... FROM: only the target scan receives Modify identity binding; the
-- Iceberg source remains a plain join input regardless of chosen scan path.
CREATE TABLE dml_lifecycle.upd_target (
    id integer,
    label text
) USING iceberg;

CREATE TABLE dml_lifecycle.upd_source (
    id integer,
    label text
) USING iceberg;

INSERT INTO dml_lifecycle.upd_target VALUES (1, 't1'), (2, 't2'), (3, 't3');
INSERT INTO dml_lifecycle.upd_source VALUES (2, 's2'), (3, 's3');

UPDATE dml_lifecycle.upd_target AS t
SET label = s.label
FROM dml_lifecycle.upd_source AS s
WHERE t.id = s.id;

SELECT id, label FROM dml_lifecycle.upd_target ORDER BY id;
SELECT id, label FROM dml_lifecycle.upd_source ORDER BY id;

-- A materialized source and self-join exercise PostgreSQL ctid propagation
-- through Materialize/Sort and a second scan of the target relation.
WITH source AS MATERIALIZED (
    SELECT id, label
    FROM dml_lifecycle.upd_source
    ORDER BY id DESC
)
UPDATE dml_lifecycle.upd_target AS target
SET label = source.label || '_materialized'
FROM source
WHERE target.id = source.id;

UPDATE dml_lifecycle.upd_target AS target
SET label = sibling.label || '_self'
FROM dml_lifecycle.upd_target AS sibling
WHERE target.id = sibling.id AND target.id = 2;

SELECT id, label FROM dml_lifecycle.upd_target ORDER BY id;

EXPLAIN (COSTS OFF)
UPDATE dml_lifecycle.upd_target
SET label = label
WHERE id = 999;

-- The GUC controls query optimization only. Modify-purpose CustomScan remains
-- mandatory so row identity and OCC context cannot be disabled.
SET pg_lakebase.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
UPDATE dml_lifecycle.upd_target
SET label = label || '_seqscan'
WHERE id = 1;
UPDATE dml_lifecycle.upd_target
SET label = label || '_seqscan'
WHERE id = 1;
DELETE FROM dml_lifecycle.upd_target
WHERE id = 3;
RESET pg_lakebase.customscan_mode;

SELECT id, label FROM dml_lifecycle.upd_target ORDER BY id;

-- Rewritten view DML retains PostgreSQL WITH CHECK OPTION enforcement.
CREATE VIEW dml_lifecycle.generated_small AS
SELECT id, base, doubled
FROM dml_lifecycle.generated_t
WHERE base < 10
WITH LOCAL CHECK OPTION;

\set VERBOSITY terse
UPDATE dml_lifecycle.generated_small SET base = 12 WHERE id = 1;
\set VERBOSITY default
SELECT * FROM dml_lifecycle.generated_t ORDER BY id;

-- RLS visibility and WITH CHECK policies remain enforced by the forked
-- PostgreSQL control flow.
CREATE ROLE dml_rls_user;
CREATE TABLE dml_lifecycle.rls_t (
    id integer,
    label text
) USING iceberg;
INSERT INTO dml_lifecycle.rls_t VALUES (1, 'visible'), (2, 'hidden');
ALTER TABLE dml_lifecycle.rls_t ENABLE ROW LEVEL SECURITY;
CREATE POLICY rls_t_policy ON dml_lifecycle.rls_t
USING (id = 1)
WITH CHECK (label <> 'blocked');
GRANT USAGE ON SCHEMA dml_lifecycle TO dml_rls_user;
GRANT SELECT, UPDATE ON dml_lifecycle.rls_t TO dml_rls_user;

SET ROLE dml_rls_user;
UPDATE dml_lifecycle.rls_t
SET label = 'updated'
RETURNING id, label;
\set VERBOSITY terse
UPDATE dml_lifecycle.rls_t SET label = 'blocked' WHERE id = 1;
\set VERBOSITY default
RESET ROLE;

SELECT * FROM dml_lifecycle.rls_t ORDER BY id;
DROP OWNED BY dml_rls_user;
DROP ROLE dml_rls_user;


SET client_min_messages = warning;
DROP SCHEMA dml_lifecycle CASCADE;
RESET client_min_messages;
