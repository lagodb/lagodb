-- Trigger-visible row images and query-level TriggerRowStore behavior.

DROP EXTENSION IF EXISTS lagodb_iceberg CASCADE;
CREATE EXTENSION lagodb_iceberg;

-- Query-level TriggerRowStore ownership and routing.
CREATE SCHEMA dml_trigger_query_state;

SET lagodb.customscan_mode = 'force';

CREATE TABLE dml_trigger_query_state.target (
    id integer,
    label text
) USING iceberg;
CREATE TABLE dml_trigger_query_state.audit (
    id integer,
    old_label text,
    new_label text
);
CREATE FUNCTION dml_trigger_query_state.audit_update()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO dml_trigger_query_state.audit
    VALUES (NEW.id, OLD.label, NEW.label);
    RETURN NULL;
END;
$$;
CREATE TRIGGER target_audit
AFTER UPDATE ON dml_trigger_query_state.target
FOR EACH ROW EXECUTE FUNCTION dml_trigger_query_state.audit_update();

INSERT INTO dml_trigger_query_state.target VALUES
    (1, 'old_1'), (2, 'old_2'), (3, 'old_3'), (4, 'old_4');

-- Sibling ModifyTable nodes for one relation must share one query-level store
-- without allowing OLD/NEW identities to cross between their result states.
WITH first_update AS (
    UPDATE dml_trigger_query_state.target
    SET label = 'cte_1'
    WHERE id = 1
    RETURNING id
), second_update AS (
    UPDATE dml_trigger_query_state.target
    SET label = 'cte_2'
    WHERE id = 2
    RETURNING id
)
SELECT count(*) AS changed_rows
FROM (
    SELECT id FROM first_update
    UNION ALL
    SELECT id FROM second_update
) AS changed;

SELECT id, old_label, new_label
FROM dml_trigger_query_state.audit
ORDER BY id;

TRUNCATE dml_trigger_query_state.audit;

-- The outer query store remains routable while an AFTER trigger runs a nested
-- SPI UPDATE that creates another query store for the same relation.
CREATE FUNCTION dml_trigger_query_state.nested_update()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.id = 3 AND NEW.label = 'outer_3' THEN
        UPDATE dml_trigger_query_state.target
        SET label = 'nested_4'
        WHERE id = 4;
    END IF;
    RETURN NULL;
END;
$$;
CREATE TRIGGER target_nested
AFTER UPDATE ON dml_trigger_query_state.target
FOR EACH ROW EXECUTE FUNCTION dml_trigger_query_state.nested_update();

UPDATE dml_trigger_query_state.target SET label = 'outer_3' WHERE id = 3;

SELECT id, label
FROM dml_trigger_query_state.target
WHERE id IN (3, 4)
ORDER BY id;
SELECT id, old_label, new_label
FROM dml_trigger_query_state.audit
ORDER BY id;

-- Disabled and WHEN=false events must not fire. A later matching row in the
-- same UPDATE verifies that the tuplestore can skip preserved, unused rows.
CREATE TABLE dml_trigger_query_state.conditional_target (
    id integer,
    label text
) USING iceberg;
CREATE TABLE dml_trigger_query_state.conditional_audit (
    trigger_name text,
    id integer,
    label text
);
CREATE FUNCTION dml_trigger_query_state.audit_conditional()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO dml_trigger_query_state.conditional_audit
    VALUES (TG_NAME, NEW.id, NEW.label);
    RETURN NULL;
END;
$$;
CREATE TRIGGER conditional_when
AFTER UPDATE ON dml_trigger_query_state.conditional_target
FOR EACH ROW WHEN (NEW.label = 'fire')
EXECUTE FUNCTION dml_trigger_query_state.audit_conditional();
CREATE TRIGGER conditional_disabled
AFTER UPDATE ON dml_trigger_query_state.conditional_target
FOR EACH ROW EXECUTE FUNCTION dml_trigger_query_state.audit_conditional();
ALTER TABLE dml_trigger_query_state.conditional_target
DISABLE TRIGGER conditional_disabled;
INSERT INTO dml_trigger_query_state.conditional_target VALUES
    (1, 'old_1'), (2, 'old_2');
UPDATE dml_trigger_query_state.conditional_target
SET label = CASE id WHEN 1 THEN 'skip' ELSE 'fire' END;
SELECT trigger_name, id, label
FROM dml_trigger_query_state.conditional_audit
ORDER BY trigger_name, id;

-- work_mem is deliberately smaller than the retained OLD/NEW rows so the
-- PostgreSQL tuplestore must use its spill path.
SET work_mem = '64kB';
CREATE TABLE dml_trigger_query_state.spill_target (
    id integer,
    payload text
) USING iceberg;
CREATE TABLE dml_trigger_query_state.spill_audit (
    id integer,
    old_length integer,
    new_length integer
);
CREATE FUNCTION dml_trigger_query_state.audit_spill()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO dml_trigger_query_state.spill_audit
    VALUES (NEW.id, length(OLD.payload), length(NEW.payload));
    RETURN NULL;
END;
$$;
CREATE TRIGGER spill_audit
AFTER UPDATE ON dml_trigger_query_state.spill_target
FOR EACH ROW EXECUTE FUNCTION dml_trigger_query_state.audit_spill();
INSERT INTO dml_trigger_query_state.spill_target
SELECT id, repeat('x', 8192)
FROM generate_series(1, 80) AS id;
-- SPI subtransaction cleanup must abandon spilled rows even after the
-- ModifyTable node finished and its AFTER trigger raised an error.
ALTER TABLE dml_trigger_query_state.spill_audit
ADD CONSTRAINT reject_spill_update CHECK (new_length <= 8192);
SET client_min_messages = notice;
DO $$
BEGIN
    UPDATE dml_trigger_query_state.spill_target SET payload = payload || 'y';
    RAISE EXCEPTION 'expected AFTER trigger constraint failure';
EXCEPTION
    WHEN check_violation THEN
        RAISE NOTICE 'caught AFTER trigger constraint failure';
END;
$$;
RESET client_min_messages;
SELECT count(*) = 80 AND bool_and(length(payload) = 8192) AS rows_restored,
       NOT EXISTS (SELECT FROM dml_trigger_query_state.spill_audit) AS audit_empty
FROM dml_trigger_query_state.spill_target;
ALTER TABLE dml_trigger_query_state.spill_audit
DROP CONSTRAINT reject_spill_update;
UPDATE dml_trigger_query_state.spill_target SET payload = payload || 'y';
SELECT count(*), min(old_length), max(new_length)
FROM dml_trigger_query_state.spill_audit;
-- COPY stores tokens with spilled rows, including rows skipped by WHEN.
-- Two triggers reuse each selected row, with a nested COPY between them
-- using a separate COPY owner.
SELECT current_setting('data_directory') || '/copy-trigger-input-' || pg_backend_pid()
       AS copy_trigger_input
\gset
\set copy_trigger_nested :copy_trigger_input '.nested'
COPY (SELECT id, CASE WHEN id <> 89 THEN repeat('c', 8192 + id) END
      FROM generate_series(81, 160) AS id)
TO :'copy_trigger_input';
COPY (SELECT -1, repeat('n', 4096)) TO :'copy_trigger_nested';
TRUNCATE dml_trigger_query_state.spill_audit;
DROP TRIGGER spill_audit ON dml_trigger_query_state.spill_target;
CREATE TRIGGER spill_copy_a
AFTER INSERT ON dml_trigger_query_state.spill_target
FOR EACH ROW WHEN (NEW.id < 0 OR NEW.id % 2 = 1)
EXECUTE FUNCTION dml_trigger_query_state.audit_spill();
CREATE FUNCTION dml_trigger_query_state.nested_copy()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    EXECUTE format('COPY dml_trigger_query_state.spill_target FROM %L',
                   current_setting('data_directory') || '/copy-trigger-input-' ||
                   pg_backend_pid() || '.nested');
    RETURN NULL;
END;
$$;
CREATE TRIGGER spill_copy_nested
AFTER INSERT ON dml_trigger_query_state.spill_target
FOR EACH ROW WHEN (NEW.id = 81) EXECUTE FUNCTION dml_trigger_query_state.nested_copy();
CREATE TRIGGER spill_copy_z
AFTER INSERT ON dml_trigger_query_state.spill_target
FOR EACH ROW WHEN (NEW.id < 0 OR NEW.id % 2 = 1)
EXECUTE FUNCTION dml_trigger_query_state.audit_spill();
COPY dml_trigger_query_state.spill_target FROM :'copy_trigger_input';
SELECT array_agg(id ORDER BY id) = ARRAY[-1] ||
           ARRAY(SELECT generate_series(81, 159, 2)) AS correct_ids,
       bool_and(copies = 2 AND correct_length) AS correct_rows
FROM (
    SELECT id, count(*) AS copies,
           bool_and(new_length IS NOT DISTINCT FROM
                    CASE WHEN id = -1 THEN 4096
                         WHEN id <> 89 THEN 8192 + id END) AS correct_length
    FROM dml_trigger_query_state.spill_audit
    GROUP BY id
) AS audited_rows;

-- An AFTER-trigger error must release spilled COPY state on savepoint rollback.
-- Retrying in the same transaction must not reuse the failed owner's row store.
TRUNCATE dml_trigger_query_state.spill_audit;
ALTER TABLE dml_trigger_query_state.spill_audit
ADD CONSTRAINT reject_copy_row CHECK (id <> 101);
BEGIN;
SAVEPOINT failed_copy;
\set VERBOSITY terse
COPY dml_trigger_query_state.spill_target FROM :'copy_trigger_input';
\set VERBOSITY default
ROLLBACK TO SAVEPOINT failed_copy;
ALTER TABLE dml_trigger_query_state.spill_audit DROP CONSTRAINT reject_copy_row;
COPY dml_trigger_query_state.spill_target FROM :'copy_trigger_input';
COMMIT;
SELECT array_agg(id ORDER BY id) = ARRAY[-1] ||
           ARRAY(SELECT generate_series(81, 159, 2)) AS correct_ids,
       bool_and(copies = 2 AND correct_length) AS correct_rows
FROM (
    SELECT id, count(*) AS copies,
           bool_and(new_length IS NOT DISTINCT FROM
                    CASE WHEN id = -1 THEN 4096
                         WHEN id <> 89 THEN 8192 + id END) AS correct_length
    FROM dml_trigger_query_state.spill_audit
    GROUP BY id
) AS audited_rows;

RESET work_mem;

-- AFTER-trigger materialization must be identical whether query CustomScan
-- optimization is forced or disabled; DML itself always uses CustomScan.
CREATE TABLE dml_trigger_query_state.force_target (
    id integer,
    label text
) USING iceberg;
CREATE TABLE dml_trigger_query_state.seq_target (
    id integer,
    label text
) USING iceberg;
CREATE TABLE dml_trigger_query_state.parity_audit (
    table_name text,
    old_label text,
    new_label text
);
CREATE FUNCTION dml_trigger_query_state.audit_parity()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO dml_trigger_query_state.parity_audit
    VALUES (TG_TABLE_NAME, OLD.label, NEW.label);
    RETURN NULL;
END;
$$;
CREATE TRIGGER force_audit
AFTER UPDATE ON dml_trigger_query_state.force_target
FOR EACH ROW EXECUTE FUNCTION dml_trigger_query_state.audit_parity();
CREATE TRIGGER seq_audit
AFTER UPDATE ON dml_trigger_query_state.seq_target
FOR EACH ROW EXECUTE FUNCTION dml_trigger_query_state.audit_parity();
INSERT INTO dml_trigger_query_state.force_target VALUES (1, 'old');
INSERT INTO dml_trigger_query_state.seq_target VALUES (1, 'old');
UPDATE dml_trigger_query_state.force_target SET label = 'new' WHERE id = 1;
SET lagodb.customscan_mode = 'off';
UPDATE dml_trigger_query_state.seq_target SET label = 'new' WHERE id = 1;
SELECT old_label, new_label, count(*)
FROM dml_trigger_query_state.parity_audit
GROUP BY old_label, new_label;

RESET lagodb.customscan_mode;
SET client_min_messages = warning;
DROP SCHEMA dml_trigger_query_state CASCADE;
RESET client_min_messages;

CREATE SCHEMA dml_lifecycle;
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

-- A BEFORE trigger forces single-row COPY. AFTER sees the adjusted NEW row;
-- a suppressed row must not produce an event.
CREATE FUNCTION dml_lifecycle.adjust_copy_row()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.id < 0 THEN RETURN NULL; END IF;
    NEW.label := NEW.label || '_copy';
    RETURN NEW;
END;
$$;
CREATE TRIGGER copy_before
BEFORE INSERT ON dml_lifecycle.trigger_ud
FOR EACH ROW EXECUTE FUNCTION dml_lifecycle.adjust_copy_row();
TRUNCATE dml_lifecycle.trigger_ud_after_audit;
COPY dml_lifecycle.trigger_ud FROM stdin;
2	input
-1	suppressed
\.
SELECT new_label FROM dml_lifecycle.trigger_ud_after_audit;

-- COPY has the same statement-lifetime restriction as ModifyTable.
CREATE CONSTRAINT TRIGGER copy_deferred
AFTER INSERT ON dml_lifecycle.trigger_ud
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION dml_lifecycle.audit_after_write();
\set VERBOSITY terse
COPY dml_lifecycle.trigger_ud FROM stdin;
3	deferred
\.
\set VERBOSITY default
DROP TRIGGER copy_deferred ON dml_lifecycle.trigger_ud;

-- Standard Iceberg COPY now enters the fork. Publishing precedes both kinds
-- of AFTER event, while row images survive nested UPDATE execution.
CREATE TABLE dml_lifecycle.copy_visible (id integer, label text) USING iceberg;
CREATE TABLE dml_lifecycle.copy_visibility_audit (
    event text, id integer, visible_rows bigint
);
CREATE FUNCTION dml_lifecycle.check_copy_visibility()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_LEVEL = 'ROW' THEN
        INSERT INTO dml_lifecycle.copy_visibility_audit
        SELECT TG_LEVEL, NEW.id, count(*) FROM dml_lifecycle.copy_visible;
        UPDATE dml_lifecycle.copy_visible
        SET label = label || '_after' WHERE id = NEW.id;
    ELSE
        INSERT INTO dml_lifecycle.copy_visibility_audit
        SELECT TG_LEVEL, NULL, count(*) FROM dml_lifecycle.copy_visible;
    END IF;
    RETURN NULL;
END;
$$;
CREATE TRIGGER copy_visible_row
AFTER INSERT ON dml_lifecycle.copy_visible
FOR EACH ROW EXECUTE FUNCTION dml_lifecycle.check_copy_visibility();
CREATE TRIGGER copy_visible_statement
AFTER INSERT ON dml_lifecycle.copy_visible
FOR EACH STATEMENT EXECUTE FUNCTION dml_lifecycle.check_copy_visibility();
COPY dml_lifecycle.copy_visible FROM stdin;
1	one
2	two
\.
SELECT * FROM dml_lifecycle.copy_visibility_audit ORDER BY event, id;

-- Empty COPY still fires its statement trigger without opening an AM writer.
TRUNCATE dml_lifecycle.copy_visibility_audit;
COPY dml_lifecycle.copy_visible FROM stdin;
\.
SELECT * FROM dml_lifecycle.copy_visibility_audit;

-- Input shutdown can still fail after publication and nested trigger writes.
\set VERBOSITY sqlstate
COPY dml_lifecycle.copy_visible FROM PROGRAM 'printf "3\tlate\n"; exit 1';
\set VERBOSITY default
SELECT * FROM dml_lifecycle.copy_visible ORDER BY id;
SELECT count(*) AS audit_rows FROM dml_lifecycle.copy_visibility_audit;

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

SET client_min_messages = warning;
DROP SCHEMA dml_lifecycle CASCADE;
RESET client_min_messages;
