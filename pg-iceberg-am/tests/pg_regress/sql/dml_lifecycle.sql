-- DML lifecycle frame coverage.

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

-- ROLLBACK TO SAVEPOINT must abort the frame and allow a new frame later.
CREATE TABLE dml_lifecycle.savepoint_t (
    id integer
) USING iceberg;

BEGIN;
SAVEPOINT s1;
INSERT INTO dml_lifecycle.savepoint_t VALUES (1);
ROLLBACK TO SAVEPOINT s1;
INSERT INTO dml_lifecycle.savepoint_t VALUES (2);
COMMIT;

SELECT * FROM dml_lifecycle.savepoint_t ORDER BY id;

-- RELEASE SAVEPOINT smoke test. This DML frame finishes before RELEASE, so
-- it does not directly exercise ResourceOwner reparent; it guards against
-- accidental leak warnings on the release path.
BEGIN;
SAVEPOINT s2;
INSERT INTO dml_lifecycle.savepoint_t VALUES (3);
RELEASE SAVEPOINT s2;
COMMIT;

SELECT * FROM dml_lifecycle.savepoint_t ORDER BY id;

-- Nested subtransaction abort must only discard the inner frame.
BEGIN;
SAVEPOINT outer_sp;
INSERT INTO dml_lifecycle.savepoint_t VALUES (10);
SAVEPOINT inner_sp;
INSERT INTO dml_lifecycle.savepoint_t VALUES (11);
ROLLBACK TO SAVEPOINT inner_sp;
INSERT INTO dml_lifecycle.savepoint_t VALUES (12);
COMMIT;

SELECT * FROM dml_lifecycle.savepoint_t WHERE id >= 10 ORDER BY id;

-- COPY FROM failure must abort the COPY frame and leave no inserted rows.
CREATE TABLE dml_lifecycle.copy_t (
    id integer
) USING iceberg;

\set VERBOSITY terse
COPY dml_lifecycle.copy_t FROM stdin;
1
bad
\.
\set VERBOSITY default

SELECT 'no resource leak' AS check;
SELECT count(*) AS rows_after_failed_copy FROM dml_lifecycle.copy_t;

COPY dml_lifecycle.copy_t FROM stdin;
2
3
\.

SELECT * FROM dml_lifecycle.copy_t ORDER BY id;

-- COPY into a partitioned table must use one frame with multiple relation
-- sessions and finalize all touched partitions.
CREATE TABLE dml_lifecycle.part_t (
    id integer
) PARTITION BY RANGE (id) USING iceberg;

CREATE TABLE dml_lifecycle.part_t_a
PARTITION OF dml_lifecycle.part_t
FOR VALUES FROM (0) TO (100) USING iceberg;

CREATE TABLE dml_lifecycle.part_t_b
PARTITION OF dml_lifecycle.part_t
FOR VALUES FROM (100) TO (200) USING iceberg;

COPY dml_lifecycle.part_t FROM stdin;
10
150
20
\.

SELECT * FROM dml_lifecycle.part_t ORDER BY id;

-- INSERT into a partitioned table must also use one ModifyTable frame with
-- multiple relation sessions.
INSERT INTO dml_lifecycle.part_t VALUES (30), (170), (40);

SELECT * FROM dml_lifecycle.part_t ORDER BY id;

-- Per-partition placement: the per-row fast path keys the cached session on the
-- current relation, so alternating partition routing (a -> b -> a) must land
-- each row in its own leaf relation rather than reusing the previous row's
-- session. Reading each leaf directly would expose a mis-routed row.
SELECT * FROM dml_lifecycle.part_t_a ORDER BY id;
SELECT * FROM dml_lifecycle.part_t_b ORDER BY id;

-- MERGE insert-only still runs as CMD_MERGE and must finalize once at the
-- ModifyTable frame boundary.
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

-- UPDATE and DELETE use scan-synthesized row locations and must remain visible
-- to later statements through Iceberg position deletes plus appended data files.
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

COPY (
    SELECT id, label FROM dml_lifecycle.update_delete_t ORDER BY id
) TO STDOUT WITH (FORMAT csv);

DELETE FROM dml_lifecycle.update_delete_t
WHERE id IN (1, 3);

COPY (
    SELECT id, label FROM dml_lifecycle.update_delete_t ORDER BY id
) TO STDOUT WITH (FORMAT csv);

-- RowDelta validation must use the final transaction-local view. In
-- particular, position deletes may reference data files appended by an
-- earlier statement in the same PostgreSQL transaction.
CREATE TABLE dml_lifecycle.same_tx_dml_t (
    id integer,
    label text
) USING iceberg;

BEGIN;
INSERT INTO dml_lifecycle.same_tx_dml_t VALUES
    (1, 'one'),
    (2, 'two'),
    (3, 'three');
UPDATE dml_lifecycle.same_tx_dml_t
SET label = 'two_updated'
WHERE id = 2;
DELETE FROM dml_lifecycle.same_tx_dml_t
WHERE id IN (1, 2);
COPY (
    SELECT id, label FROM dml_lifecycle.same_tx_dml_t ORDER BY id
) TO STDOUT WITH (FORMAT csv);
COMMIT;

COPY (
    SELECT id, label FROM dml_lifecycle.same_tx_dml_t ORDER BY id
) TO STDOUT WITH (FORMAT csv);

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

-- A target-independent MERGE writes no position deletes, so it remains a
-- valid append on an Iceberg v1 table.
MERGE INTO dml_lifecycle.v1_dml_t AS target
USING (VALUES (2, 'two')) AS source(id, label)
ON FALSE
WHEN NOT MATCHED THEN
    INSERT (id, label) VALUES (source.id, source.label);

COPY (
    SELECT id, label FROM dml_lifecycle.v1_dml_t ORDER BY id
) TO STDOUT WITH (FORMAT csv);

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

COPY (
    SELECT id, label FROM dml_lifecycle.merge_update_t ORDER BY id
) TO STDOUT WITH (FORMAT csv);
INSERT INTO dml_lifecycle.merge_update_t VALUES (32, 'after_merge');
COPY (
    SELECT id, label FROM dml_lifecycle.merge_update_t ORDER BY id
) TO STDOUT WITH (FORMAT csv);

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

COPY (
    SELECT id, label FROM dml_lifecycle.merge_delete_t ORDER BY id
) TO STDOUT WITH (FORMAT csv);
INSERT INTO dml_lifecycle.merge_delete_t VALUES (42, 'after_merge');
COPY (
    SELECT id, label FROM dml_lifecycle.merge_delete_t ORDER BY id
) TO STDOUT WITH (FORMAT csv);

-- UPDATE ... FROM: only the target relation's scan synthesizes row identity
-- (ctid); the Iceberg source relation is scanned as a plain join input and
-- skips row-location work. Both the updated target and the untouched source
-- must be correct.
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

COPY (
    SELECT id, label FROM dml_lifecycle.upd_target ORDER BY id
) TO STDOUT WITH (FORMAT csv);
COPY (
    SELECT id, label FROM dml_lifecycle.upd_source ORDER BY id
) TO STDOUT WITH (FORMAT csv);

-- Partitioned row-level UPDATE/DELETE: each leaf partition is its own result
-- relation, so its scan still synthesizes row identity and routes position
-- deletes plus appended rows to the correct leaf.
CREATE TABLE dml_lifecycle.part_dml (
    id integer,
    label text
) PARTITION BY RANGE (id) USING iceberg;

CREATE TABLE dml_lifecycle.part_dml_a
PARTITION OF dml_lifecycle.part_dml
FOR VALUES FROM (0) TO (100) USING iceberg;

CREATE TABLE dml_lifecycle.part_dml_b
PARTITION OF dml_lifecycle.part_dml
FOR VALUES FROM (100) TO (200) USING iceberg;

INSERT INTO dml_lifecycle.part_dml VALUES
    (10, 'a10'), (20, 'a20'), (110, 'b110'), (120, 'b120');

UPDATE dml_lifecycle.part_dml
SET label = label || '_upd'
WHERE id IN (20, 110);

COPY (
    SELECT id, label FROM dml_lifecycle.part_dml ORDER BY id
) TO STDOUT WITH (FORMAT csv);

DELETE FROM dml_lifecycle.part_dml
WHERE id IN (10, 120);

COPY (
    SELECT id, label FROM dml_lifecycle.part_dml ORDER BY id
) TO STDOUT WITH (FORMAT csv);

-- Trigger/SPI nested DML must use a nested frame and restore the outer frame.
CREATE TABLE dml_lifecycle.trigger_src (
    id integer
) USING iceberg;

CREATE TABLE dml_lifecycle.trigger_audit (
    id integer,
    note text
) USING iceberg;

CREATE FUNCTION dml_lifecycle.audit_insert()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO dml_lifecycle.trigger_audit VALUES (NEW.id, 'inserted');
    RETURN NEW;
END;
$$;

CREATE TRIGGER trigger_src_bi
BEFORE INSERT ON dml_lifecycle.trigger_src
FOR EACH ROW
EXECUTE FUNCTION dml_lifecycle.audit_insert();

INSERT INTO dml_lifecycle.trigger_src VALUES (7), (8);

SELECT * FROM dml_lifecycle.trigger_src ORDER BY id;
SELECT * FROM dml_lifecycle.trigger_audit ORDER BY id;

-- CTAS does not have an Iceberg create lifecycle yet and must fail loudly.
\set VERBOSITY sqlstate
CREATE TABLE dml_lifecycle.ctas_t USING iceberg AS
SELECT 1::integer AS id;
CREATE TABLE dml_lifecycle.ctas_no_data_t USING iceberg AS
SELECT 1::integer AS id WITH NO DATA;
\set VERBOSITY default

-- The unsupported CTAS path must not leave backend-local DML state poisoned.
CREATE TABLE dml_lifecycle.after_ctas_t (
    id integer
) USING iceberg;

INSERT INTO dml_lifecycle.after_ctas_t VALUES (99);
SELECT * FROM dml_lifecycle.after_ctas_t ORDER BY id;

-- Storage identity changes and truncate need explicit Iceberg lifecycle support.
\set VERBOSITY sqlstate
ALTER TABLE dml_lifecycle.after_ctas_t SET ACCESS METHOD heap;
ALTER TABLE dml_lifecycle.after_ctas_t SET TABLESPACE pg_default;
TRUNCATE dml_lifecycle.after_ctas_t;
\set VERBOSITY default

SET client_min_messages = warning;
DROP SCHEMA dml_lifecycle CASCADE;
RESET client_min_messages;
