-- Table DDL, table options, and physical storage lifecycle.

DROP EXTENSION IF EXISTS pg_iceberg_am CASCADE;
CREATE EXTENSION pg_iceberg_am;

--
-- Test 0: Basic CREATE and DROP (Top-level Transaction)
--
CREATE TABLE test_lifecycle (id int) USING iceberg;
INSERT INTO test_lifecycle VALUES (1), (2), (3);

-- capture path info while table exists (pg_relation_filepath returns null after drop)
SELECT pg_relation_filepath('test_lifecycle') || '_iceberg' AS path_1 \gset
-- Verify directory exists
SELECT (pg_stat_file(:'path_1')).isdir as directory_found;
SELECT EXISTS (
    SELECT 1 FROM pg_ls_dir(:'path_1', true, false)
) AS root_has_iceberg_artifacts;

DROP TABLE test_lifecycle;
-- Verify directory is gone
SELECT (pg_stat_file(:'path_1', true)) is null as directory_missing;
SELECT count(*) AS local_drop_remote_items
FROM lakebase.maintenance_queue
WHERE producer = 'iceberg-drop';


--
-- Test 1: Create table in sub-transaction and rollback (Abort Cleanup)
--
BEGIN;
SAVEPOINT s1;
CREATE TABLE test_sub_create (id int) USING iceberg;

-- capture path info inside sub-xact so we can check it after rollback
SELECT pg_relation_filepath('test_sub_create') || '_iceberg' AS path_sub_1 \gset

-- Verify directory exists inside sub-xact
SELECT (pg_stat_file(:'path_sub_1')).isdir as directory_found_in_sub;

ROLLBACK TO SAVEPOINT s1;
COMMIT;

-- Verify directory is gone after rollback
SELECT (pg_stat_file(:'path_sub_1', true)) is null as directory_missing_after_rollback;


--
-- Test 2: Drop table in sub-transaction and rollback (Commit Cleanup - Cancelled)
--
CREATE TABLE test_sub_drop (id int) USING iceberg;
INSERT INTO test_sub_drop VALUES (10), (20);
-- capture path now because we need to verify it still exists after rollback
SELECT pg_relation_filepath('test_sub_drop') || '_iceberg' AS path_sub_2 \gset

BEGIN;
SAVEPOINT s2;
DROP TABLE test_sub_drop;
ROLLBACK TO SAVEPOINT s2;
COMMIT;

-- Verify directory still exists (drop was cancelled)
SELECT (pg_stat_file(:'path_sub_2')).isdir as directory_found_after_rollback;
SELECT array_agg(id ORDER BY id) AS rows_after_drop_rollback FROM test_sub_drop;

-- Cleanup
DROP TABLE test_sub_drop;


--
-- Test 3: Drop table in sub-transaction and commit (Commit Cleanup - Executed)
--
CREATE TABLE test_sub_drop_commit (id int) USING iceberg;
-- capture path now to verify it is gone after commit
SELECT pg_relation_filepath('test_sub_drop_commit') || '_iceberg' AS path_sub_3 \gset

BEGIN;
SAVEPOINT s3;
DROP TABLE test_sub_drop_commit;
RELEASE SAVEPOINT s3;
COMMIT;

-- Verify directory is gone
SELECT (pg_stat_file(:'path_sub_3', true)) is null as directory_missing_after_commit;

-- Test custom table options for Iceberg access method.
-- This test verifies that the IcebergTableHook correctly extracts and persists
-- table options defined in options.rs.
-- ============================================================================
-- Test 1: Create table with default options (no custom options specified)
-- ============================================================================
CREATE TABLE iceberg_default_test (
    id integer,
    name text
) USING iceberg;

-- Verify: No options should be stored (or empty options)
SELECT relid::regclass::text AS table_name, options
FROM lakebase.table_options
WHERE relid = 'iceberg_default_test'::regclass;

DROP TABLE iceberg_default_test;

-- ============================================================================
-- Test 2: Create table with format-version option
-- ============================================================================
CREATE TABLE iceberg_format_test (
    id integer,
    value double precision
) USING iceberg WITH (
    "format-version" = 1
);

-- Verify the option is stored
SELECT relid::regclass::text AS table_name, options
FROM lakebase.table_options
WHERE relid = 'iceberg_format_test'::regclass;

DROP TABLE iceberg_format_test;

-- ============================================================================
-- Test 3: Create table with multiple custom options
-- ============================================================================
CREATE TABLE iceberg_multi_opts_test (
    id integer,
    data jsonb
) USING iceberg WITH (
    "format-version" = 2,
    "write.parquet.compression-codec" = 'zstd',
    "write.format.default" = 'parquet'
);

-- Verify all options are stored correctly
SELECT relid::regclass::text AS table_name, options
FROM lakebase.table_options
WHERE relid = 'iceberg_multi_opts_test'::regclass;

DROP TABLE iceberg_multi_opts_test;

-- ============================================================================
-- Test 4: Create table with different compression codec
-- ============================================================================
CREATE TABLE iceberg_compression_test (
    id integer,
    payload bytea
) USING iceberg WITH (
    "write.parquet.compression-codec" = 'snappy'
);

-- Verify the compression option
SELECT relid::regclass::text AS table_name, options
FROM lakebase.table_options
WHERE relid = 'iceberg_compression_test'::regclass;

DROP TABLE iceberg_compression_test;

-- ============================================================================
-- Test 5: Create table with enum option
-- ============================================================================
CREATE TABLE iceberg_enum_opt_test (
    id integer
) USING iceberg WITH (
    "write.format.default" = 'parquet'
);

-- Verify the enum option
SELECT relid::regclass::text AS table_name, options
FROM lakebase.table_options
WHERE relid = 'iceberg_enum_opt_test'::regclass;

DROP TABLE iceberg_enum_opt_test;

-- ============================================================================
-- Test 6: Persist command-specific DML isolation options
-- ============================================================================
CREATE TABLE iceberg_isolation_opts_test (
    id integer
) USING iceberg WITH (
    "write.delete.isolation-level" = 'snapshot',
    "write.update.isolation-level" = 'serializable',
    "write.merge.isolation-level" = 'snapshot'
);

COPY (
    SELECT options @> ARRAY[
        'write.delete.isolation-level=snapshot',
        'write.update.isolation-level=serializable',
        'write.merge.isolation-level=snapshot'
    ]::text[]
    FROM lakebase.table_options
    WHERE relid = 'iceberg_isolation_opts_test'::regclass
) TO STDOUT;

DROP TABLE iceberg_isolation_opts_test;

-- ============================================================================
-- Test 7: Create table in a specific schema
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS test_schema;

CREATE TABLE test_schema.iceberg_schema_test (
    id integer
) USING iceberg WITH (
    "format-version" = 2
);

-- Verify the option is stored with correct schema-qualified name
SELECT relid::regclass::text AS table_name, options
FROM lakebase.table_options
WHERE relid = 'test_schema.iceberg_schema_test'::regclass;

DROP TABLE test_schema.iceberg_schema_test;
DROP SCHEMA test_schema;

-- ============================================================================
-- Clean up: Verify no orphaned entries remain
-- ============================================================================
SELECT COUNT(*) AS orphan_count FROM lakebase.table_options;

CREATE SCHEMA dml_lifecycle;
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
