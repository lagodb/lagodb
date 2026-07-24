-- PostgreSQL TM_SelfModified semantics for Lake table row identities.

DROP EXTENSION IF EXISTS pg_iceberg_am CASCADE;
CREATE EXTENSION pg_iceberg_am;

CREATE SCHEMA dml_self_modified;

-- A nested SPI command uses a different CommandId and ModifyState. The outer
-- command must therefore receive PostgreSQL's triggered-data-change error,
-- not silently treat the nested mutation as its own duplicate match.
CREATE FUNCTION dml_self_modified.self_update_before()
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

CREATE FUNCTION dml_self_modified.self_delete_before()
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
CREATE FUNCTION dml_self_modified.update_different_row_before()
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
SET pg_lakebase.customscan_mode = 'force';

CREATE TABLE dml_self_modified.force_target (
    id integer,
    label text
) USING iceberg;

INSERT INTO dml_self_modified.force_target VALUES
    (1, 'original_update'),
    (2, 'original_delete'),
    (3, 'ordinary_update'),
    (4, 'ordinary_delete');

SELECT metadata_location AS force_before_error
FROM iceberg.iceberg_metadata
WHERE relid = 'dml_self_modified.force_target'::regclass \gset

\set VERBOSITY sqlstate
MERGE INTO dml_self_modified.force_target AS target
USING (VALUES
    (1, repeat('x', 1100000)),
    (1, repeat('y', 1100000))
) AS source(id, label)
ON target.id = source.id
WHEN MATCHED THEN
    UPDATE SET label = source.label;
\set VERBOSITY default

COPY (
    SELECT id, label, length(label)
    FROM dml_self_modified.force_target
    WHERE id = 1
) TO STDOUT WITH (FORMAT csv);

COPY (
    SELECT metadata_location = :'force_before_error'
    FROM iceberg.iceberg_metadata
    WHERE relid = 'dml_self_modified.force_target'::regclass
) TO STDOUT WITH (FORMAT csv);

\set VERBOSITY sqlstate
MERGE INTO dml_self_modified.force_target AS target
USING (VALUES (2), (2)) AS source(id)
ON target.id = source.id
WHEN MATCHED THEN
    DELETE;
\set VERBOSITY default

COPY (
    SELECT id, label
    FROM dml_self_modified.force_target
    WHERE id = 2
) TO STDOUT WITH (FORMAT csv);

COPY (
    SELECT metadata_location = :'force_before_error'
    FROM iceberg.iceberg_metadata
    WHERE relid = 'dml_self_modified.force_target'::regclass
) TO STDOUT WITH (FORMAT csv);

UPDATE dml_self_modified.force_target AS target
SET label = source.label
FROM (VALUES (3, 'updated_once'), (3, 'updated_once')) AS source(id, label)
WHERE target.id = source.id
RETURNING target.id;

DELETE FROM dml_self_modified.force_target AS target
USING (VALUES (4), (4)) AS source(id)
WHERE target.id = source.id
RETURNING target.id;

COPY (
    SELECT id, label
    FROM dml_self_modified.force_target
    ORDER BY id
) TO STDOUT WITH (FORMAT csv);

INSERT INTO dml_self_modified.force_target VALUES
    (5, 'trigger_update_original'),
    (6, 'trigger_delete_original'),
    (7, 'trigger_merge_original'),
    (8, 'sibling_original'),
    (9, 'different_outer_original'),
    (10, 'different_nested_original');

SELECT metadata_location AS force_before_trigger
FROM iceberg.iceberg_metadata
WHERE relid = 'dml_self_modified.force_target'::regclass \gset

CREATE TRIGGER force_self_update
BEFORE UPDATE ON dml_self_modified.force_target
FOR EACH ROW EXECUTE FUNCTION dml_self_modified.self_update_before();

CREATE TRIGGER force_self_delete
BEFORE DELETE ON dml_self_modified.force_target
FOR EACH ROW EXECUTE FUNCTION dml_self_modified.self_delete_before();

CREATE TRIGGER force_update_different
BEFORE UPDATE ON dml_self_modified.force_target
FOR EACH ROW EXECUTE FUNCTION dml_self_modified.update_different_row_before();

\set VERBOSITY sqlstate
UPDATE dml_self_modified.force_target
SET label = 'outer_update'
WHERE id = 5;
DELETE FROM dml_self_modified.force_target WHERE id = 6;
MERGE INTO dml_self_modified.force_target AS target
USING (VALUES (7)) AS source(id)
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET label = 'outer_merge';
\set VERBOSITY default

COPY (
    SELECT id, label
    FROM dml_self_modified.force_target
    WHERE id BETWEEN 5 AND 7
    ORDER BY id
) TO STDOUT WITH (FORMAT csv);

COPY (
    SELECT metadata_location = :'force_before_trigger'
    FROM iceberg.iceberg_metadata
    WHERE relid = 'dml_self_modified.force_target'::regclass
) TO STDOUT WITH (FORMAT csv);

UPDATE dml_self_modified.force_target
SET label = 'outer_different'
WHERE id = 9;

COPY (
    SELECT id, label
    FROM dml_self_modified.force_target
    WHERE id IN (9, 10)
    ORDER BY id
) TO STDOUT WITH (FORMAT csv);

-- Two sibling ModifyTable nodes use separate IcebergModifyState values but
-- the same PostgreSQL command ID. Exactly one action may affect the row.
COPY (
WITH first_update AS (
    UPDATE dml_self_modified.force_target
    SET label = 'sibling_first'
    WHERE id = 8
    RETURNING id
), second_update AS (
    UPDATE dml_self_modified.force_target
    SET label = 'sibling_second'
    WHERE id = 8
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
    SELECT count(*), bool_and(label IN ('sibling_first', 'sibling_second'))
    FROM dml_self_modified.force_target
    WHERE id = 8
) TO STDOUT WITH (FORMAT csv);

-- A claim made in an aborted savepoint must not poison the replacement
-- command that subsequently sees the original physical row.
BEGIN;
SAVEPOINT row_claim;
UPDATE dml_self_modified.force_target
SET label = 'rolled_back'
WHERE id = 5;
ROLLBACK TO SAVEPOINT row_claim;
UPDATE dml_self_modified.force_target
SET label = 'after_rollback'
WHERE id = 5;
COMMIT;

COPY (
    SELECT id, label
    FROM dml_self_modified.force_target
    WHERE id = 5
) TO STDOUT WITH (FORMAT csv);

-- Repeat with query CustomScan optimization disabled. Modify-purpose
-- CustomScan remains mandatory correctness infrastructure.
SET pg_lakebase.customscan_mode = 'off';

CREATE TABLE dml_self_modified.seqscan_target (
    id integer,
    label text
) USING iceberg;

INSERT INTO dml_self_modified.seqscan_target VALUES
    (1, 'original_update'),
    (2, 'original_delete'),
    (3, 'ordinary_update'),
    (4, 'ordinary_delete');

SELECT metadata_location AS seqscan_before_error
FROM iceberg.iceberg_metadata
WHERE relid = 'dml_self_modified.seqscan_target'::regclass \gset

\set VERBOSITY sqlstate
MERGE INTO dml_self_modified.seqscan_target AS target
USING (VALUES
    (1, repeat('x', 1100000)),
    (1, repeat('y', 1100000))
) AS source(id, label)
ON target.id = source.id
WHEN MATCHED THEN
    UPDATE SET label = source.label;
\set VERBOSITY default

COPY (
    SELECT id, label, length(label)
    FROM dml_self_modified.seqscan_target
    WHERE id = 1
) TO STDOUT WITH (FORMAT csv);

COPY (
    SELECT metadata_location = :'seqscan_before_error'
    FROM iceberg.iceberg_metadata
    WHERE relid = 'dml_self_modified.seqscan_target'::regclass
) TO STDOUT WITH (FORMAT csv);

\set VERBOSITY sqlstate
MERGE INTO dml_self_modified.seqscan_target AS target
USING (VALUES (2), (2)) AS source(id)
ON target.id = source.id
WHEN MATCHED THEN
    DELETE;
\set VERBOSITY default

COPY (
    SELECT id, label
    FROM dml_self_modified.seqscan_target
    WHERE id = 2
) TO STDOUT WITH (FORMAT csv);

COPY (
    SELECT metadata_location = :'seqscan_before_error'
    FROM iceberg.iceberg_metadata
    WHERE relid = 'dml_self_modified.seqscan_target'::regclass
) TO STDOUT WITH (FORMAT csv);

UPDATE dml_self_modified.seqscan_target AS target
SET label = source.label
FROM (VALUES (3, 'updated_once'), (3, 'updated_once')) AS source(id, label)
WHERE target.id = source.id
RETURNING target.id;

DELETE FROM dml_self_modified.seqscan_target AS target
USING (VALUES (4), (4)) AS source(id)
WHERE target.id = source.id
RETURNING target.id;

COPY (
    SELECT id, label
    FROM dml_self_modified.seqscan_target
    ORDER BY id
) TO STDOUT WITH (FORMAT csv);

INSERT INTO dml_self_modified.seqscan_target VALUES
    (5, 'trigger_update_original'),
    (6, 'trigger_delete_original'),
    (7, 'trigger_merge_original'),
    (8, 'different_outer_original'),
    (9, 'different_nested_original');

SELECT metadata_location AS seqscan_before_trigger
FROM iceberg.iceberg_metadata
WHERE relid = 'dml_self_modified.seqscan_target'::regclass \gset

CREATE TRIGGER seqscan_self_update
BEFORE UPDATE ON dml_self_modified.seqscan_target
FOR EACH ROW EXECUTE FUNCTION dml_self_modified.self_update_before();

CREATE TRIGGER seqscan_self_delete
BEFORE DELETE ON dml_self_modified.seqscan_target
FOR EACH ROW EXECUTE FUNCTION dml_self_modified.self_delete_before();

CREATE TRIGGER seqscan_update_different
BEFORE UPDATE ON dml_self_modified.seqscan_target
FOR EACH ROW EXECUTE FUNCTION dml_self_modified.update_different_row_before();

\set VERBOSITY sqlstate
UPDATE dml_self_modified.seqscan_target
SET label = 'outer_update'
WHERE id = 5;
DELETE FROM dml_self_modified.seqscan_target WHERE id = 6;
MERGE INTO dml_self_modified.seqscan_target AS target
USING (VALUES (7)) AS source(id)
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET label = 'outer_merge';
\set VERBOSITY default

COPY (
    SELECT id, label
    FROM dml_self_modified.seqscan_target
    WHERE id BETWEEN 5 AND 7
    ORDER BY id
) TO STDOUT WITH (FORMAT csv);

COPY (
    SELECT metadata_location = :'seqscan_before_trigger'
    FROM iceberg.iceberg_metadata
    WHERE relid = 'dml_self_modified.seqscan_target'::regclass
) TO STDOUT WITH (FORMAT csv);

UPDATE dml_self_modified.seqscan_target
SET label = 'outer_different'
WHERE id = 8;

COPY (
    SELECT id, label
    FROM dml_self_modified.seqscan_target
    WHERE id IN (8, 9)
    ORDER BY id
) TO STDOUT WITH (FORMAT csv);

RESET pg_lakebase.customscan_mode;
RESET iceberg.mutation_buffer_flush_mb;

SET client_min_messages = warning;
DROP SCHEMA dml_self_modified CASCADE;
RESET client_min_messages;
