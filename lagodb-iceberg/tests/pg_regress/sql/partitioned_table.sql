-- Managed partitioned tables: definitions, PostgreSQL catalog policy, COPY, and DML.
\pset format unaligned
\pset footer off

-- Managed partition definitions, storage identity, and PostgreSQL catalog policy.

DROP EXTENSION IF EXISTS lagodb_iceberg CASCADE;
CREATE EXTENSION lagodb_iceberg;
CREATE SCHEMA partitioned_ddl;

-- PostgreSQL partition syntax is lowered only where its semantics fully
-- determine an Iceberg transform.
CREATE TABLE partitioned_ddl.date_t (event_date date)
PARTITION BY RANGE (event_date) USING iceberg;
CREATE TABLE partitioned_ddl.timestamp_t (event_time timestamp)
PARTITION BY RANGE ((date_trunc('month', event_time))) USING iceberg;
CREATE TABLE partitioned_ddl.timestamptz_t (event_time timestamptz)
PARTITION BY RANGE ((date_trunc('day', event_time, 'UTC'))) USING iceberg;

-- Check the lowered transform, field name, and source identity.
SELECT definition.table_id::text AS relation,
       jsonb_array_length(metadata->'partition-specs'->0->'fields') AS fields,
       metadata->'partition-specs'->0->'fields'->0->>'transform' AS transform,
       metadata->'partition-specs'->0->'fields'->0->>'name' AS field_name,
       metadata->'partition-specs'->0->'fields'->0->>'source-id' AS source_id
FROM (VALUES ('partitioned_ddl.date_t'::regclass),
             ('partitioned_ddl.timestamp_t'::regclass),
             ('partitioned_ddl.timestamptz_t'::regclass)) AS definition(table_id)
JOIN iceberg.iceberg_metadata ON relid = definition.table_id
CROSS JOIN LATERAL (SELECT pg_read_file(metadata_location)::jsonb AS metadata) AS file
ORDER BY relation;

-- Partitioned tables reserve native file numbers without a physical pg_class locator.
SELECT count(*) = 3
       AND count(DISTINCT value) = 3
       AND bool_and(value::oid <> 0::oid)
       AND bool_and(pg_relation_filenode(relid) IS NULL) AS reserved_file_numbers
FROM lagodb.table_option_values
WHERE relid IN (
    'partitioned_ddl.date_t'::regclass,
    'partitioned_ddl.timestamp_t'::regclass,
    'partitioned_ddl.timestamptz_t'::regclass
)
  AND name = 'relfilenumber';

\set VERBOSITY terse
CREATE TABLE partitioned_ddl.unsupported_hash_t (id integer)
PARTITION BY HASH (id) USING iceberg;
-- UUID partition literals cannot currently round-trip through upstream Avro.
CREATE TABLE partitioned_ddl.unsupported_uuid_t (id uuid)
PARTITION BY LIST (id) USING iceberg;
\set VERBOSITY default

DROP TABLE partitioned_ddl.date_t, partitioned_ddl.timestamp_t, partitioned_ddl.timestamptz_t;

-- Partitioned tables must not advertise PostgreSQL uniqueness without enforcement.
CREATE TABLE partitioned_ddl.root_index_t (
    id integer,
    region text
) PARTITION BY LIST (region) USING iceberg;
INSERT INTO partitioned_ddl.root_index_t VALUES (1, 'east'), (2, 'east');
\set VERBOSITY sqlstate
CREATE UNIQUE INDEX root_unique_idx
ON partitioned_ddl.root_index_t (region);
\set VERBOSITY default

-- Rejected indexes leave no metadata and do not poison the COPY lifecycle.
COPY partitioned_ddl.root_index_t FROM stdin WITH (FORMAT csv);
3,west
\.
SELECT count(*) AS root_indexes
FROM pg_index WHERE indrelid = 'partitioned_ddl.root_index_t'::regclass;
SELECT count(*) AS join_rows
FROM partitioned_ddl.root_index_t a
LEFT JOIN partitioned_ddl.root_index_t b ON a.region = b.region;

-- Native partitioned tables retain PostgreSQL's metadata-only index behavior.
CREATE TABLE partitioned_ddl.native_index_t (
    region text
) PARTITION BY LIST (region) USING heap;
CREATE UNIQUE INDEX native_root_unique_idx
ON partitioned_ddl.native_index_t (region);
SELECT indisunique, indisvalid
FROM pg_index
WHERE indexrelid = 'partitioned_ddl.native_root_unique_idx'::regclass;

-- Native child partitions must still route INSERT and COPY after provider registration.
CREATE TABLE partitioned_ddl.native_east
PARTITION OF partitioned_ddl.native_index_t FOR VALUES IN ('east') USING heap;
CREATE TABLE partitioned_ddl.native_west
PARTITION OF partitioned_ddl.native_index_t FOR VALUES IN ('west') USING heap;
INSERT INTO partitioned_ddl.native_index_t VALUES ('east');
COPY partitioned_ddl.native_index_t FROM stdin WITH (FORMAT csv);
west
\.
SELECT region FROM partitioned_ddl.native_index_t ORDER BY region;

-- One representative topology rejection: Iceberg partitions do not use PG child tables.
\set VERBOSITY terse
CREATE TABLE partitioned_ddl.root_index_t_child
PARTITION OF partitioned_ddl.root_index_t
FOR VALUES IN ('east') USING heap;
\set VERBOSITY default

SET client_min_messages = warning;
DROP SCHEMA partitioned_ddl CASCADE;
RESET client_min_messages;

-- Partitioned-table COPY routing, native delegation, and RLS query rewriting.
CREATE SCHEMA partitioned_copy;
CREATE ROLE partitioned_copy_user;
GRANT USAGE ON SCHEMA partitioned_copy TO partitioned_copy_user;

CREATE TABLE partitioned_copy.native_t (id integer PRIMARY KEY, region text);
CREATE TABLE partitioned_copy.root_t (id integer, region text)
PARTITION BY LIST (region) USING iceberg;
INSERT INTO partitioned_copy.root_t VALUES (1, 'east'), (2, 'west');
GRANT SELECT, INSERT ON partitioned_copy.native_t TO partitioned_copy_user;
GRANT SELECT ON partitioned_copy.root_t TO partitioned_copy_user;

-- The provider-owned target route must preserve ordinary COPY execution.
CREATE TABLE partitioned_copy.execution_t (
    id integer,
    region text,
    label text DEFAULT 'base'
) PARTITION BY LIST (region) USING iceberg;
CREATE FUNCTION partitioned_copy.before_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.label := NEW.label || '_before';
    RETURN NEW;
END;
$$;
CREATE TRIGGER execution_before
BEFORE INSERT ON partitioned_copy.execution_t
FOR EACH ROW EXECUTE FUNCTION partitioned_copy.before_insert();
COPY partitioned_copy.execution_t (id, region) FROM stdin
WITH (FORMAT csv) WHERE id >= 2;
1,east
2,west
3,east
\.
COPY (
    SELECT id, region, label FROM partitioned_copy.execution_t ORDER BY id
) TO STDOUT WITH (FORMAT csv);

-- A current-database-qualified root must still select the root consumer.
SELECT current_database() AS copy_database \gset
COPY :"copy_database".partitioned_copy.root_t TO '/dev/null' WITH (FORMAT csv);

ALTER TABLE partitioned_copy.root_t ENABLE ROW LEVEL SECURITY;
CREATE POLICY visible_root_rows ON partitioned_copy.root_t
FOR SELECT TO partitioned_copy_user USING (id = 1);
SET ROLE partitioned_copy_user;

-- A representative endpoint error must precede relation ACL checks.
\set VERBOSITY terse
COPY partitioned_copy.root_t FROM '/dev/null';
\set VERBOSITY default

-- Native relation COPY continues to use the parent executor.
COPY partitioned_copy.native_t FROM stdin WITH (FORMAT csv);
10,native
\.
COPY partitioned_copy.native_t TO STDOUT WITH (FORMAT csv);

-- A temporary native table shadows the persistent root in search_path.
SET search_path = pg_temp, partitioned_copy, public;
CREATE TEMP TABLE root_t (id integer, region text);
COPY root_t FROM stdin WITH (FORMAT csv);
99,temp
\.
COPY root_t TO STDOUT WITH (FORMAT csv);
DROP TABLE root_t;
RESET search_path;

-- RLS rewrites relation COPY TO as a query, while retaining root identity.
COPY partitioned_copy.root_t TO STDOUT WITH (FORMAT csv);
COPY (SELECT id, region FROM partitioned_copy.root_t) TO STDOUT WITH (FORMAT csv);
RESET ROLE;

SET client_min_messages = warning;
DROP SCHEMA partitioned_copy CASCADE;
RESET client_min_messages;
DROP ROLE partitioned_copy_user;

-- Managed Iceberg partitioned table INSERT, UPDATE, DELETE, MERGE, and COPY.

CREATE SCHEMA dml_lifecycle;

CREATE TABLE dml_lifecycle.part_t (
    id integer,
    region text,
    label text
) PARTITION BY LIST (region) USING iceberg;

SELECT relkind = 'p' AND NOT relhassubclass AS partitioned_table_catalog_shape,
       pg_get_partkeydef(oid) = 'LIST (region)' AS pg_partition_key_visible
FROM pg_class
WHERE oid = 'dml_lifecycle.part_t'::regclass;

-- COPY FROM is bound directly to the provider-owned partitioned table; PostgreSQL child
-- partition routing must not run.
COPY dml_lifecycle.part_t FROM stdin WITH (FORMAT csv);
10,east,a
20,west,b
30,east,c
\.

INSERT INTO dml_lifecycle.part_t VALUES (40, 'west', 'd');
UPDATE dml_lifecycle.part_t
SET label = label || '_u'
WHERE id IN (10, 20) AND EXISTS (SELECT 1);

-- Moving an Iceberg identity partition value remains one logical-table
-- update; it must not ask PostgreSQL for a destination child partition.
UPDATE dml_lifecycle.part_t SET region = 'west' WHERE id = 10;
DELETE FROM dml_lifecycle.part_t WHERE id = 30;

COPY (
    SELECT id, region, label,
           tableoid = 'dml_lifecycle.part_t'::regclass AS partitioned_tableoid
    FROM dml_lifecycle.part_t
    ORDER BY id
) TO STDOUT WITH (FORMAT csv);

SELECT count(*) = 3 AS only_partitioned_table_reads_data
FROM ONLY dml_lifecycle.part_t;

MERGE INTO dml_lifecycle.part_t AS target
USING (VALUES
    (20, 'east', 'merged'),
    (40, 'west', 'delete'),
    (50, 'east', 'new')
) AS source(id, region, label)
ON target.id = source.id
WHEN MATCHED AND source.label = 'delete' THEN DELETE
WHEN MATCHED THEN
    UPDATE SET region = source.region, label = source.label
WHEN NOT MATCHED THEN
    INSERT (id, region, label)
    VALUES (source.id, source.region, source.label);

COPY (
    SELECT id, region, label FROM dml_lifecycle.part_t ORDER BY id
) TO STDOUT WITH (FORMAT csv);

BEGIN;
INSERT INTO dml_lifecycle.part_t VALUES (60, 'west', 'tx');
UPDATE dml_lifecycle.part_t SET label = 'tx-updated' WHERE id = 20;
DELETE FROM dml_lifecycle.part_t WHERE id = 10;
SELECT count(*) = 3 AS transaction_delta_visible
FROM dml_lifecycle.part_t;
ROLLBACK;

COPY (
    SELECT id, region, label FROM dml_lifecycle.part_t ORDER BY id
) TO STDOUT WITH (FORMAT csv);

-- Partition-aware v3 deletion vectors use a different delete-file path from v2.
CREATE TABLE dml_lifecycle.part_v3 (
    id integer,
    region text,
    label text
) PARTITION BY LIST (region) USING iceberg WITH ("format-version" = 3);
INSERT INTO dml_lifecycle.part_v3
VALUES (1, 'east', 'one'), (2, 'west', 'two'), (3, 'east', 'three');
UPDATE dml_lifecycle.part_v3 SET region = 'west', label = 'moved' WHERE id = 1;
DELETE FROM dml_lifecycle.part_v3 WHERE id = 2;
COPY (
    SELECT id, region, label FROM dml_lifecycle.part_v3 ORDER BY id
) TO STDOUT WITH (FORMAT csv);

SET client_min_messages = warning;
DROP SCHEMA dml_lifecycle CASCADE;
RESET client_min_messages;

\pset format aligned
\pset footer on
