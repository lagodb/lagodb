-- Query-level TriggerRowStore ownership and routing.

DROP EXTENSION IF EXISTS pg_iceberg_am CASCADE;
CREATE EXTENSION pg_iceberg_am;
CREATE SCHEMA dml_trigger_query_state;

SET pg_lakebase.customscan_mode = 'force';

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
COPY (
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
SELECT count(*)
FROM (
    SELECT id FROM first_update
    UNION ALL
    SELECT id FROM second_update
) AS changed
) TO STDOUT WITH (FORMAT csv);

COPY (
    SELECT id, old_label, new_label
    FROM dml_trigger_query_state.audit
    ORDER BY id
) TO STDOUT WITH (FORMAT csv);

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

COPY (
    SELECT id, label
    FROM dml_trigger_query_state.target
    WHERE id IN (3, 4)
    ORDER BY id
) TO STDOUT WITH (FORMAT csv);
COPY (
    SELECT id, old_label, new_label
    FROM dml_trigger_query_state.audit
    ORDER BY id
) TO STDOUT WITH (FORMAT csv);

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
COPY (
    SELECT trigger_name, id, label
    FROM dml_trigger_query_state.conditional_audit
    ORDER BY trigger_name, id
) TO STDOUT WITH (FORMAT csv);

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
UPDATE dml_trigger_query_state.spill_target SET payload = payload || 'y';
COPY (
    SELECT count(*), min(old_length), max(new_length)
    FROM dml_trigger_query_state.spill_audit
) TO STDOUT WITH (FORMAT csv);
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
SET pg_lakebase.customscan_mode = 'force';
UPDATE dml_trigger_query_state.force_target SET label = 'new' WHERE id = 1;
SET pg_lakebase.customscan_mode = 'off';
UPDATE dml_trigger_query_state.seq_target SET label = 'new' WHERE id = 1;
COPY (
    SELECT old_label, new_label, count(*)
    FROM dml_trigger_query_state.parity_audit
    GROUP BY old_label, new_label
) TO STDOUT WITH (FORMAT csv);

RESET pg_lakebase.customscan_mode;
SET client_min_messages = warning;
DROP SCHEMA dml_trigger_query_state CASCADE;
RESET client_min_messages;
