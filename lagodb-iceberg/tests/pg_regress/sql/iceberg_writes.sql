-- Core INSERT, UPDATE, DELETE, and MERGE semantics for Iceberg tables.

DROP EXTENSION IF EXISTS lagodb_iceberg CASCADE;
CREATE EXTENSION IF NOT EXISTS lagodb_iceberg;
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

-- DELETE RETURNING tableoid must retain its logical relation identity.
DELETE FROM dml_lifecycle.update_delete_t
WHERE id = 1
RETURNING tableoid::regclass =
    'dml_lifecycle.update_delete_t'::regclass AS correct_tableoid;

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

-- Bind parameters anew per execution, including one shared by SET and WHERE.
SET lagodb.customscan_mode = 'off';
SET plan_cache_mode = force_generic_plan;
PREPARE update_generated(integer, integer) AS
UPDATE dml_lifecycle.generated_t
SET id = $1, base = $2
WHERE id = $1
RETURNING id, base, doubled;

EXECUTE update_generated(2, 9);
EXECUTE update_generated(1, 5);
DEALLOCATE update_generated;
RESET plan_cache_mode;
RESET lagodb.customscan_mode;

UPDATE dml_lifecycle.generated_t SET base = -1 WHERE id = 1;

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

DELETE FROM dml_lifecycle.v1_dml_t WHERE id = 1;
UPDATE dml_lifecycle.v1_dml_t SET label = 'updated' WHERE id = 1;

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
-- PostgreSQL's MERGE executor with native target SeqScans.
SET lagodb.customscan_mode = 'off';
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
RESET lagodb.customscan_mode;

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

-- Disabling optional CustomScans selects a native target SeqScan. ModifyTable
-- still supplies its row identity binding and OCC context.
SET lagodb.customscan_mode = 'off';
UPDATE dml_lifecycle.upd_target
SET label = label || '_seqscan'
WHERE id = 1;
DELETE FROM dml_lifecycle.upd_target
WHERE id = 3;
RESET lagodb.customscan_mode;

SELECT id, label FROM dml_lifecycle.upd_target ORDER BY id;

-- Rewritten view DML retains PostgreSQL WITH CHECK OPTION enforcement.
CREATE VIEW dml_lifecycle.generated_small AS
SELECT id, base, doubled
FROM dml_lifecycle.generated_t
WHERE base < 10
WITH LOCAL CHECK OPTION;

UPDATE dml_lifecycle.generated_small SET base = 12 WHERE id = 1;
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
UPDATE dml_lifecycle.rls_t SET label = 'blocked' WHERE id = 1;
RESET ROLE;

SELECT * FROM dml_lifecycle.rls_t ORDER BY id;
DROP OWNED BY dml_rls_user;
DROP ROLE dml_rls_user;

-- PostgreSQL TM_SelfModified semantics for Lake table row identities.

-- A nested SPI command uses a different CommandId and ModifyState. The outer
-- command must therefore receive PostgreSQL's triggered-data-change error,
-- not silently treat the nested mutation as its own duplicate match.
CREATE FUNCTION dml_lifecycle.self_update_before()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.label IN ('outer_update', 'outer_merge') THEN
        EXECUTE format(
            'UPDATE %I.%I SET label = $1 WHERE id = $2',
            TG_TABLE_SCHEMA,
            TG_TABLE_NAME
        ) USING 'trigger_update', OLD.id;
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION dml_lifecycle.self_delete_before()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    EXECUTE format(
        'UPDATE %I.%I SET label = $1 WHERE id = $2',
        TG_TABLE_SCHEMA,
        TG_TABLE_NAME
    ) USING 'trigger_delete', OLD.id;
    RETURN OLD;
END;
$$;

-- A nested SPI command that changes another physical row in the same data
-- file must succeed. Duplicate detection is row-level, not file-level.
CREATE FUNCTION dml_lifecycle.update_different_row_before()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.label = 'outer_different' THEN
        EXECUTE format(
            'UPDATE %I.%I SET label = $1 WHERE id = $2',
            TG_TABLE_SCHEMA,
            TG_TABLE_NAME
        ) USING 'nested_different', OLD.id + 1;
    END IF;
    RETURN NEW;
END;
$$;

-- Force the first MERGE UPDATE action through the Arrow/Parquet flush path
-- before the duplicate target identity raises an error. The failed statement
-- must publish neither its appended data nor its position delete.
SET iceberg.mutation_buffer_flush_mb = 1;

-- Run the complete matrix through the provider CustomScan target path.
SET lagodb.customscan_mode = 'force';

CREATE TABLE dml_lifecycle.force_target (
    id integer,
    label text
) USING iceberg;

INSERT INTO dml_lifecycle.force_target VALUES
    (1, 'original_update'),
    (2, 'original_delete'),
    (3, 'ordinary_update'),
    (4, 'ordinary_delete');

SELECT metadata_location AS force_before_error
FROM iceberg.iceberg_metadata
WHERE relid = 'dml_lifecycle.force_target'::regclass \gset

\set VERBOSITY sqlstate
MERGE INTO dml_lifecycle.force_target AS target
USING (VALUES
    (1, repeat('x', 1100000)),
    (1, repeat('y', 1100000))
) AS source(id, label)
ON target.id = source.id
WHEN MATCHED THEN
    UPDATE SET label = source.label;
\set VERBOSITY default

SELECT id, label, length(label)
FROM dml_lifecycle.force_target
WHERE id = 1;

SELECT metadata_location = :'force_before_error' AS metadata_unchanged
FROM iceberg.iceberg_metadata
WHERE relid = 'dml_lifecycle.force_target'::regclass;

\set VERBOSITY sqlstate
MERGE INTO dml_lifecycle.force_target AS target
USING (VALUES (2), (2)) AS source(id)
ON target.id = source.id
WHEN MATCHED THEN
    DELETE;
\set VERBOSITY default

SELECT id, label
FROM dml_lifecycle.force_target
WHERE id = 2;

SELECT metadata_location = :'force_before_error' AS metadata_unchanged
FROM iceberg.iceberg_metadata
WHERE relid = 'dml_lifecycle.force_target'::regclass;

UPDATE dml_lifecycle.force_target AS target
SET label = source.label
FROM (VALUES (3, 'updated_once'), (3, 'updated_once')) AS source(id, label)
WHERE target.id = source.id
RETURNING target.id;

DELETE FROM dml_lifecycle.force_target AS target
USING (VALUES (4), (4)) AS source(id)
WHERE target.id = source.id
RETURNING target.id;

SELECT id, label
FROM dml_lifecycle.force_target
ORDER BY id;

INSERT INTO dml_lifecycle.force_target VALUES
    (5, 'trigger_update_original'),
    (6, 'trigger_delete_original'),
    (7, 'trigger_merge_original'),
    (8, 'sibling_original'),
    (9, 'different_outer_original'),
    (10, 'different_nested_original');

SELECT metadata_location AS force_before_trigger
FROM iceberg.iceberg_metadata
WHERE relid = 'dml_lifecycle.force_target'::regclass \gset

CREATE TRIGGER force_self_update
BEFORE UPDATE ON dml_lifecycle.force_target
FOR EACH ROW EXECUTE FUNCTION dml_lifecycle.self_update_before();

CREATE TRIGGER force_self_delete
BEFORE DELETE ON dml_lifecycle.force_target
FOR EACH ROW EXECUTE FUNCTION dml_lifecycle.self_delete_before();

CREATE TRIGGER force_update_different
BEFORE UPDATE ON dml_lifecycle.force_target
FOR EACH ROW EXECUTE FUNCTION dml_lifecycle.update_different_row_before();

\set VERBOSITY sqlstate
UPDATE dml_lifecycle.force_target
SET label = 'outer_update'
WHERE id = 5;
DELETE FROM dml_lifecycle.force_target WHERE id = 6;
MERGE INTO dml_lifecycle.force_target AS target
USING (VALUES (7)) AS source(id)
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET label = 'outer_merge';
\set VERBOSITY default

SELECT id, label
FROM dml_lifecycle.force_target
WHERE id BETWEEN 5 AND 7
ORDER BY id;

SELECT metadata_location = :'force_before_trigger' AS metadata_unchanged
FROM iceberg.iceberg_metadata
WHERE relid = 'dml_lifecycle.force_target'::regclass;

UPDATE dml_lifecycle.force_target
SET label = 'outer_different'
WHERE id = 9;

SELECT id, label
FROM dml_lifecycle.force_target
WHERE id IN (9, 10)
ORDER BY id;

-- Two sibling ModifyTable nodes use separate IcebergModifyState values but
-- the same PostgreSQL command ID. Exactly one action may affect the row.
WITH first_update AS (
    UPDATE dml_lifecycle.force_target
    SET label = 'sibling_first'
    WHERE id = 8
    RETURNING id
), second_update AS (
    UPDATE dml_lifecycle.force_target
    SET label = 'sibling_second'
    WHERE id = 8
    RETURNING id
)
SELECT count(*) AS changed_rows
FROM (
    SELECT id FROM first_update
    UNION ALL
    SELECT id FROM second_update
) AS changed;

SELECT count(*), bool_and(label IN ('sibling_first', 'sibling_second'))
FROM dml_lifecycle.force_target
WHERE id = 8;

-- A claim made in an aborted savepoint must not poison the replacement
-- command that subsequently sees the original physical row.
BEGIN;
SAVEPOINT row_claim;
UPDATE dml_lifecycle.force_target
SET label = 'rolled_back'
WHERE id = 5;
ROLLBACK TO SAVEPOINT row_claim;
UPDATE dml_lifecycle.force_target
SET label = 'after_rollback'
WHERE id = 5;
COMMIT;

SELECT id, label
FROM dml_lifecycle.force_target
WHERE id = 5;

-- Repeat on a native target SeqScan. Both target paths reach the same
-- PostgreSQL error conversion, so one error per SQLSTATE covers this path.
SET lagodb.customscan_mode = 'off';

CREATE TABLE dml_lifecycle.seqscan_target (
    id integer,
    label text
) USING iceberg;

INSERT INTO dml_lifecycle.seqscan_target VALUES
    (1, 'original_update'),
    (3, 'ordinary_update'),
    (4, 'ordinary_delete');

SELECT metadata_location AS seqscan_before_error
FROM iceberg.iceberg_metadata
WHERE relid = 'dml_lifecycle.seqscan_target'::regclass \gset

\set VERBOSITY sqlstate
MERGE INTO dml_lifecycle.seqscan_target AS target
USING (VALUES
    (1, repeat('x', 1100000)),
    (1, repeat('y', 1100000))
) AS source(id, label)
ON target.id = source.id
WHEN MATCHED THEN
    UPDATE SET label = source.label;
\set VERBOSITY default

SELECT id, label, length(label)
FROM dml_lifecycle.seqscan_target
WHERE id = 1;

SELECT metadata_location = :'seqscan_before_error' AS metadata_unchanged
FROM iceberg.iceberg_metadata
WHERE relid = 'dml_lifecycle.seqscan_target'::regclass;

UPDATE dml_lifecycle.seqscan_target AS target
SET label = source.label
FROM (VALUES (3, 'updated_once'), (3, 'updated_once')) AS source(id, label)
WHERE target.id = source.id
RETURNING target.id;

DELETE FROM dml_lifecycle.seqscan_target AS target
USING (VALUES (4), (4)) AS source(id)
WHERE target.id = source.id
RETURNING target.id;

SELECT id, label
FROM dml_lifecycle.seqscan_target
ORDER BY id;

INSERT INTO dml_lifecycle.seqscan_target VALUES
    (5, 'trigger_update_original'),
    (8, 'different_outer_original'),
    (9, 'different_nested_original');

SELECT metadata_location AS seqscan_before_trigger
FROM iceberg.iceberg_metadata
WHERE relid = 'dml_lifecycle.seqscan_target'::regclass \gset

CREATE TRIGGER seqscan_self_update
BEFORE UPDATE ON dml_lifecycle.seqscan_target
FOR EACH ROW EXECUTE FUNCTION dml_lifecycle.self_update_before();

CREATE TRIGGER seqscan_update_different
BEFORE UPDATE ON dml_lifecycle.seqscan_target
FOR EACH ROW EXECUTE FUNCTION dml_lifecycle.update_different_row_before();

\set VERBOSITY sqlstate
UPDATE dml_lifecycle.seqscan_target
SET label = 'outer_update'
WHERE id = 5;
\set VERBOSITY default

SELECT id, label
FROM dml_lifecycle.seqscan_target
WHERE id = 5;

SELECT metadata_location = :'seqscan_before_trigger' AS metadata_unchanged
FROM iceberg.iceberg_metadata
WHERE relid = 'dml_lifecycle.seqscan_target'::regclass;

UPDATE dml_lifecycle.seqscan_target
SET label = 'outer_different'
WHERE id = 8;

SELECT id, label
FROM dml_lifecycle.seqscan_target
WHERE id IN (8, 9)
ORDER BY id;

RESET lagodb.customscan_mode;
RESET iceberg.mutation_buffer_flush_mb;

SET client_min_messages = warning;
DROP SCHEMA dml_lifecycle CASCADE;
RESET client_min_messages;
