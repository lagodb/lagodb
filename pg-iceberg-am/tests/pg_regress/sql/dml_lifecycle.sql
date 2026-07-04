-- PG17 Custom ModifyTable lifecycle coverage.

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
COPY (
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
ORDER BY id
) TO STDOUT WITH (FORMAT csv);

COPY (
    SELECT * FROM dml_lifecycle.cte_t ORDER BY id
) TO STDOUT WITH (FORMAT csv);

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

-- COPY into a partitioned table uses an independent COPY lifecycle with
-- multiple relation sessions.
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

-- INSERT into a partitioned table owns multiple stable relation states.
INSERT INTO dml_lifecycle.part_t VALUES (30), (170), (40);

SELECT * FROM dml_lifecycle.part_t ORDER BY id;

-- Alternating partition routing (a -> b -> a) must land each row in its own
-- relation-local state. Reading each leaf exposes a mis-routed row.
SELECT * FROM dml_lifecycle.part_t_a ORDER BY id;
SELECT * FROM dml_lifecycle.part_t_b ORDER BY id;

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

COPY (
    SELECT id, label FROM dml_lifecycle.update_delete_t ORDER BY id
) TO STDOUT WITH (FORMAT csv);

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

COPY (
    SELECT id, label FROM dml_lifecycle.update_delete_t ORDER BY id
) TO STDOUT WITH (FORMAT csv);

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

COPY (
    SELECT id, label FROM dml_lifecycle.upd_target ORDER BY id
) TO STDOUT WITH (FORMAT csv);
COPY (
    SELECT id, label FROM dml_lifecycle.upd_source ORDER BY id
) TO STDOUT WITH (FORMAT csv);

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

COPY (
    SELECT id, label FROM dml_lifecycle.upd_target ORDER BY id
) TO STDOUT WITH (FORMAT csv);

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

COPY (
    SELECT id, label FROM dml_lifecycle.upd_target ORDER BY id
) TO STDOUT WITH (FORMAT csv);

-- A gating Result node between ModifyTable and a partition append must have
-- planner-only ROWID_VAR entries resolved before PostgreSQL setrefs runs.
CREATE TABLE dml_lifecycle.result_gate_t (
    id integer,
    label text
) PARTITION BY RANGE (id) USING iceberg;
CREATE TABLE dml_lifecycle.result_gate_t_a
PARTITION OF dml_lifecycle.result_gate_t
FOR VALUES FROM (0) TO (10) USING iceberg;
CREATE TABLE dml_lifecycle.result_gate_t_b
PARTITION OF dml_lifecycle.result_gate_t
FOR VALUES FROM (10) TO (20) USING iceberg;
INSERT INTO dml_lifecycle.result_gate_t VALUES (1, 'a'), (11, 'b');
UPDATE dml_lifecycle.result_gate_t
SET label = label || '_exists'
WHERE EXISTS (SELECT 1);
DELETE FROM dml_lifecycle.result_gate_t
WHERE id = 11 AND EXISTS (SELECT 1);
COPY (
    SELECT id, label FROM dml_lifecycle.result_gate_t ORDER BY id
) TO STDOUT WITH (FORMAT csv);

-- Partitioned row-level UPDATE/DELETE: each leaf partition is its own result
-- relation, so its scan carries identity and routes position
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

-- A row DELETE trigger defined only on a leaf still requires OLD when the
-- nominal partitioned root has no row triggers.
CREATE TABLE dml_lifecycle.leaf_delete_audit (
    id integer,
    old_label text
);

CREATE FUNCTION dml_lifecycle.audit_leaf_delete()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO dml_lifecycle.leaf_delete_audit VALUES (OLD.id, OLD.label);
    RETURN OLD;
END;
$$;

CREATE TRIGGER part_dml_a_ad
AFTER DELETE ON dml_lifecycle.part_dml_a
FOR EACH ROW
EXECUTE FUNCTION dml_lifecycle.audit_leaf_delete();

INSERT INTO dml_lifecycle.part_dml VALUES (30, 'leaf_trigger');
DELETE FROM dml_lifecycle.part_dml WHERE id = 30;
SELECT * FROM dml_lifecycle.leaf_delete_audit;

-- Only matching result relations get an Iceberg ModifyState. The indexed heap
-- leaf must retain PostgreSQL's native mutation path.
CREATE TABLE dml_lifecycle.mixed_am (
    id integer,
    label text
) PARTITION BY RANGE (id) USING iceberg;

CREATE TABLE dml_lifecycle.mixed_am_iceberg
PARTITION OF dml_lifecycle.mixed_am
FOR VALUES FROM (0) TO (100) USING iceberg;

CREATE TABLE dml_lifecycle.mixed_am_heap
PARTITION OF dml_lifecycle.mixed_am
FOR VALUES FROM (100) TO (200) USING heap;

CREATE INDEX mixed_am_heap_id_idx ON dml_lifecycle.mixed_am_heap (id);

INSERT INTO dml_lifecycle.mixed_am VALUES (10, 'iceberg'), (110, 'heap');
UPDATE dml_lifecycle.mixed_am SET label = label || '_updated';
SELECT id, label, tableoid::regclass FROM dml_lifecycle.mixed_am ORDER BY id;
DELETE FROM dml_lifecycle.mixed_am WHERE id IN (10, 110);
SELECT count(*) AS mixed_rows_after_delete FROM dml_lifecycle.mixed_am;

-- Cross-partition UPDATE may route the destination INSERT before that leaf's
-- target scan executes; relation snapshot context is pinned at outer binding.
SET pg_lakebase.customscan_mode = 'off';
UPDATE dml_lifecycle.part_dml
SET id = 130, label = label || '_moved'
WHERE id = 20
RETURNING id, label;
RESET pg_lakebase.customscan_mode;

COPY (
    SELECT id, label, tableoid::regclass
    FROM dml_lifecycle.part_dml
    ORDER BY id
) TO STDOUT WITH (FORMAT csv);

-- Partitioned MERGE mixes DELETE, cross-partition UPDATE, and INSERT. Matched
-- actions must decode the source leaf's synthetic ctid; the destination action
-- remains normal PostgreSQL partition routing.
MERGE INTO dml_lifecycle.part_dml AS target
USING (VALUES
    (110, 110, 'delete'),
    (130, 30, 'merge_moved'),
    (150, 150, 'merge_inserted')
) AS source(old_id, new_id, label)
ON target.id = source.old_id
WHEN MATCHED AND source.label = 'delete' THEN
    DELETE
WHEN MATCHED THEN
    UPDATE SET id = source.new_id, label = source.label
WHEN NOT MATCHED THEN
    INSERT (id, label) VALUES (source.new_id, source.label);

COPY (
    SELECT id, label, tableoid::regclass
    FROM dml_lifecycle.part_dml
    ORDER BY id
) TO STDOUT WITH (FORMAT csv);

-- Trigger/SPI nested DML owns a nested Custom ModifyTable execution.
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

-- OLD/NEW trigger slots and RETURNING must use PostgreSQL wholerow and the
-- final trigger-adjusted NEW row without a physical-row fetch.
CREATE TABLE dml_lifecycle.trigger_ud (
    id integer,
    label text
) USING iceberg;

CREATE TABLE dml_lifecycle.trigger_ud_audit (
    action text,
    id integer,
    old_label text,
    new_label text
) USING iceberg;

CREATE FUNCTION dml_lifecycle.audit_update_delete()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'UPDATE' THEN
        INSERT INTO dml_lifecycle.trigger_ud_audit
        VALUES ('update', OLD.id, OLD.label, NEW.label);
        NEW.label := NEW.label || '_trigger';
        RETURN NEW;
    END IF;
    INSERT INTO dml_lifecycle.trigger_ud_audit
    VALUES ('delete', OLD.id, OLD.label, NULL);
    RETURN OLD;
END;
$$;

CREATE TRIGGER trigger_ud_bud
BEFORE UPDATE OR DELETE ON dml_lifecycle.trigger_ud
FOR EACH ROW
EXECUTE FUNCTION dml_lifecycle.audit_update_delete();

CREATE TABLE dml_lifecycle.trigger_ud_after_audit (
    action text,
    old_label text,
    new_label text
);
CREATE FUNCTION dml_lifecycle.audit_after_write()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO dml_lifecycle.trigger_ud_after_audit
        VALUES ('insert', NULL, NEW.label);
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO dml_lifecycle.trigger_ud_after_audit
        VALUES ('update', OLD.label, NEW.label);
    ELSE
        INSERT INTO dml_lifecycle.trigger_ud_after_audit
        VALUES ('delete', OLD.label, NULL);
    END IF;
    RETURN NULL;
END;
$$;
CREATE TRIGGER trigger_ud_aiud
AFTER INSERT OR UPDATE OR DELETE ON dml_lifecycle.trigger_ud
FOR EACH ROW
EXECUTE FUNCTION dml_lifecycle.audit_after_write();

INSERT INTO dml_lifecycle.trigger_ud VALUES (1, 'old');
UPDATE dml_lifecycle.trigger_ud
SET label = 'new'
WHERE id = 1
RETURNING id, label;
DELETE FROM dml_lifecycle.trigger_ud
WHERE id = 1
RETURNING id, label;
SELECT * FROM dml_lifecycle.trigger_ud_audit ORDER BY action;
SELECT * FROM dml_lifecycle.trigger_ud_after_audit ORDER BY action;

-- PostgreSQL queues one event per AFTER trigger. The tuplestore adapter must
-- retain both OLD and NEW while multiple events reuse the same row pair.
CREATE TABLE dml_lifecycle.trigger_multi (
    id integer,
    label text
) USING iceberg;
CREATE TABLE dml_lifecycle.trigger_multi_audit (
    trigger_name text,
    old_label text,
    new_label text
);
CREATE FUNCTION dml_lifecycle.audit_multi()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO dml_lifecycle.trigger_multi_audit
    VALUES (TG_NAME, OLD.label, NEW.label);
    RETURN NULL;
END;
$$;
CREATE TRIGGER trigger_multi_a
AFTER UPDATE ON dml_lifecycle.trigger_multi
FOR EACH ROW
EXECUTE FUNCTION dml_lifecycle.audit_multi();
CREATE TRIGGER trigger_multi_b
AFTER UPDATE ON dml_lifecycle.trigger_multi
FOR EACH ROW
EXECUTE FUNCTION dml_lifecycle.audit_multi();
INSERT INTO dml_lifecycle.trigger_multi VALUES (1, 'old_1'), (2, 'old_2');
UPDATE dml_lifecycle.trigger_multi SET label = 'new_' || id;
SELECT * FROM dml_lifecycle.trigger_multi_audit
ORDER BY old_label, trigger_name;

-- FDW-style query-local tuple storage cannot support deferred row events:
-- PostgreSQL destroys the tuplestore at query end. Reject that unsupported
-- lifetime explicitly instead of falling back to object-storage refetch.
CREATE CONSTRAINT TRIGGER trigger_multi_deferred
AFTER UPDATE ON dml_lifecycle.trigger_multi
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION dml_lifecycle.audit_multi();
\set VERBOSITY sqlstate
UPDATE dml_lifecycle.trigger_multi SET label = 'deferred' WHERE id = 1;
\set VERBOSITY default
DROP TRIGGER trigger_multi_deferred ON dml_lifecycle.trigger_multi;

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
