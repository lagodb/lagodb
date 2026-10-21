-- Table DDL, table options, and physical storage lifecycle.

DROP EXTENSION IF EXISTS lagodb_iceberg CASCADE;
CREATE EXTENSION lagodb_iceberg;
SET client_min_messages = notice;

-- UNLOGGED disables Iceberg file WAL while retaining ordinary transaction
-- and cleanup behavior, including partitioned tables and the default AM.
CREATE UNLOGGED TABLE unlogged_iceberg (id integer) USING iceberg;
CREATE UNLOGGED TABLE unlogged_partitioned_iceberg (id integer)
PARTITION BY LIST (id) USING iceberg;
SET default_table_access_method = 'iceberg';
CREATE UNLOGGED TABLE unlogged_default_iceberg (id integer);
RESET default_table_access_method;
INSERT INTO unlogged_iceberg VALUES (10), (20);
INSERT INTO unlogged_partitioned_iceberg VALUES (1), (2);
INSERT INTO unlogged_default_iceberg VALUES (9);
SELECT count(*) = 3 AS unlogged_catalog_bootstrapped
FROM pg_class AS c
JOIN iceberg.iceberg_metadata AS m ON m.relid = c.oid
WHERE c.oid IN ('unlogged_iceberg'::regclass,
                'unlogged_partitioned_iceberg'::regclass,
                'unlogged_default_iceberg'::regclass)
  AND c.relpersistence = 'u' AND m.metadata_location IS NOT NULL;
SELECT (SELECT array_agg(id ORDER BY id) FROM unlogged_iceberg) AS table_rows,
       (SELECT array_agg(id ORDER BY id) FROM unlogged_partitioned_iceberg) AS partitioned_rows,
       (SELECT array_agg(id ORDER BY id) FROM unlogged_default_iceberg) AS default_am_rows;
BEGIN;
SAVEPOINT unlogged_change;
INSERT INTO unlogged_iceberg VALUES (30);
TRUNCATE unlogged_iceberg, unlogged_partitioned_iceberg;
INSERT INTO unlogged_iceberg VALUES (99);
INSERT INTO unlogged_partitioned_iceberg VALUES (99);
DROP TABLE unlogged_default_iceberg;
ROLLBACK TO SAVEPOINT unlogged_change;
COMMIT;
SELECT (SELECT array_agg(id ORDER BY id) FROM unlogged_iceberg) AS table_rows,
       (SELECT array_agg(id ORDER BY id) FROM unlogged_partitioned_iceberg) AS partitioned_rows,
       (SELECT array_agg(id ORDER BY id) FROM unlogged_default_iceberg) AS default_am_rows;
TRUNCATE unlogged_iceberg, unlogged_partitioned_iceberg, unlogged_default_iceberg;
SELECT NOT EXISTS (
    SELECT FROM unlogged_iceberg
    UNION ALL SELECT FROM unlogged_partitioned_iceberg
    UNION ALL SELECT FROM unlogged_default_iceberg
) AS unlogged_truncate_cleared_rows;
SELECT pg_read_file(metadata_location)::jsonb->>'location' AS unlogged_table_root
FROM iceberg.iceberg_metadata WHERE relid = 'unlogged_iceberg'::regclass \gset
SELECT pg_read_file(metadata_location)::jsonb->>'location' AS unlogged_partitioned_root
FROM iceberg.iceberg_metadata WHERE relid = 'unlogged_partitioned_iceberg'::regclass \gset
SELECT pg_read_file(metadata_location)::jsonb->>'location' AS unlogged_default_root
FROM iceberg.iceberg_metadata WHERE relid = 'unlogged_default_iceberg'::regclass \gset
DROP TABLE unlogged_iceberg, unlogged_partitioned_iceberg, unlogged_default_iceberg;
SELECT pg_stat_file(:'unlogged_table_root', true) IS NULL
       AND pg_stat_file(:'unlogged_partitioned_root', true) IS NULL
       AND pg_stat_file(:'unlogged_default_root', true) IS NULL AS storage_removed;

--
-- Section A: Transaction Lifecycle
--
-- Test 0: Create table in sub-transaction and rollback (Abort Cleanup)
--
BEGIN;
SAVEPOINT s1;
CREATE TABLE test_sub_create (id int) USING iceberg;

-- capture path info inside sub-xact so we can check it after rollback
SELECT pg_relation_filepath('test_sub_create'::regclass) || '_iceberg' AS path_sub_1 \gset

-- Verify directory exists inside sub-xact
SELECT (pg_stat_file(:'path_sub_1')).isdir as directory_found_in_sub;

ROLLBACK TO SAVEPOINT s1;
COMMIT;

-- Verify directory is gone after rollback
SELECT (pg_stat_file(:'path_sub_1', true)) is null as directory_missing_after_rollback;


--
-- Test 1: Drop table in sub-transaction and rollback (Commit Cleanup - Cancelled)
--
CREATE TABLE test_sub_drop (id int) USING iceberg;
INSERT INTO test_sub_drop VALUES (10), (20);
-- capture path now because we need to verify it still exists after rollback
SELECT pg_relation_filepath('test_sub_drop'::regclass) || '_iceberg' AS path_sub_2 \gset

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
-- Test 2: Drop table in sub-transaction and commit (Commit Cleanup - Executed)
--
CREATE TABLE test_sub_drop_commit (id int) USING iceberg;
-- capture path now to verify it is gone after commit
SELECT pg_relation_filepath('test_sub_drop_commit'::regclass) || '_iceberg' AS path_sub_3 \gset

BEGIN;
SAVEPOINT s3;
DROP TABLE test_sub_drop_commit;
RELEASE SAVEPOINT s3;
COMMIT;

-- Verify directory is gone
SELECT (pg_stat_file(:'path_sub_3', true)) is null as directory_missing_after_commit;

--
-- Section B: Table Options
--
-- Test custom table options for Iceberg access method.
-- This test verifies that the IcebergTableHook correctly extracts and persists
-- table options defined in the options module.
-- ============================================================================
-- Test 1: Create table with default options (no custom options specified)
-- ============================================================================
CREATE TABLE iceberg_default_test (
    id integer,
    name text
) USING iceberg;

-- Local physical tables derive storage from PG and need no internal option.
SELECT count(*) FILTER (WHERE name IN ('location', 'relfilenumber')) = 0
           AS has_no_storage_option,
       count(*) FILTER (WHERE name <> 'location') = 0
           AS has_no_user_overrides
FROM lagodb.table_option_values
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

-- Verify the option is stored.
SELECT value AS format_version
FROM lagodb.table_option_values
WHERE relid = 'iceberg_format_test'::regclass
  AND name = 'format-version';

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

-- Verify all user options are stored correctly.
SELECT name, value
FROM lagodb.table_option_values
WHERE relid = 'iceberg_multi_opts_test'::regclass
  AND name IN (
      'format-version',
      'write.parquet.compression-codec',
      'write.format.default'
  )
ORDER BY name;

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

-- Verify the compression option.
SELECT value AS compression
FROM lagodb.table_option_values
WHERE relid = 'iceberg_compression_test'::regclass
  AND name = 'write.parquet.compression-codec';

DROP TABLE iceberg_compression_test;

-- ============================================================================
-- Test 5: Create table with enum option
-- ============================================================================
CREATE TABLE iceberg_enum_opt_test (
    id integer
) USING iceberg WITH (
    "write.format.default" = 'parquet'
);

-- Verify the enum option.
SELECT value AS write_format
FROM lagodb.table_option_values
WHERE relid = 'iceberg_enum_opt_test'::regclass
  AND name = 'write.format.default';

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

SELECT name, value
FROM lagodb.table_option_values
WHERE relid = 'iceberg_isolation_opts_test'::regclass
  AND name IN (
      'write.delete.isolation-level',
      'write.update.isolation-level',
      'write.merge.isolation-level'
  )
ORDER BY name;

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

-- Verify the option is stored for the schema-qualified relation.
SELECT relid::regclass::text AS table_name, value AS format_version
FROM lagodb.table_option_values
WHERE relid = 'test_schema.iceberg_schema_test'::regclass
  AND name = 'format-version';

DROP TABLE test_schema.iceberg_schema_test;
DROP SCHEMA test_schema;

-- ============================================================================
-- Test 8: representative rejection of a user-supplied AM-owned option
-- ============================================================================
\set VERBOSITY sqlstate
CREATE TABLE iceberg_user_location_test (id integer)
USING iceberg WITH (location = 'user-owned-location');

\set VERBOSITY default

-- ============================================================================
-- Test 9: rename preserves the physical root and name reuse gets a new root
-- ============================================================================
CREATE TABLE iceberg_identity_test (id integer) USING iceberg;
SELECT pg_relation_filepath('iceberg_identity_test'::regclass) || '_iceberg' AS original_location \gset

ALTER TABLE iceberg_identity_test RENAME TO iceberg_identity_archive;
CREATE TABLE iceberg_identity_test (id integer) USING iceberg;
SELECT pg_relation_filepath('iceberg_identity_test'::regclass) || '_iceberg' AS recreated_location \gset

SELECT (pg_relation_filepath('iceberg_identity_archive'::regclass) || '_iceberg') = :'original_location'
           AS archived_location_unchanged,
       :'recreated_location' <> :'original_location'
           AS recreated_table_has_distinct_root;

DROP TABLE iceberg_identity_archive;
SELECT pg_stat_file(:'original_location', true) IS NULL
           AS renamed_storage_removed,
       (pg_stat_file(:'recreated_location')).isdir
           AS recreated_storage_preserved;
DROP TABLE iceberg_identity_test;

-- Numeric normalization for LIKE sources and inherited columns.
-- Basic LIKE preserves column order and metadata, skipping dropped attributes.
CREATE DOMAIN ddl_positive_numeric AS double precision CHECK (VALUE > 0);
CREATE TYPE ddl_numeric_member AS (amount ddl_positive_numeric, exact numeric(12,2));
CREATE TABLE ddl_source (
    gone integer, amount numeric NOT NULL DEFAULT 7, exact numeric(12,2),
    amounts numeric[], member ddl_numeric_member, label text COLLATE "C"
);
ALTER TABLE ddl_source DROP COLUMN gone;
CREATE TABLE ddl_suffix_source (suffix numeric);
CREATE TABLE ddl_copied (
    head integer, LIKE ddl_source, LIKE ddl_suffix_source, tail boolean
) USING iceberg;

INSERT INTO ddl_copied VALUES (1, 2.5, 3.25, ARRAY[4.5,NULL], '(5.5,6.25)', 'text', 7.5, true);
SELECT * FROM ddl_copied;

SELECT attname,
       CASE WHEN attname = 'member' THEN 'composite'
            ELSE format_type(atttypid, atttypmod) END AS type,
       attnotnull, atthasdef, attcollation = '"C"'::regcollation AS c_collation
FROM pg_attribute
WHERE attrelid = 'ddl_copied'::regclass AND attnum > 0 AND NOT attisdropped
ORDER BY attnum;

-- Compatible composite members retain their declared domain identity.
SELECT pg_typeof((member).amount) AS member_type FROM ddl_copied;

SELECT attname, format_type(atttypid, atttypmod) AS type
FROM pg_attribute
WHERE attrelid = 'ddl_source'::regclass AND attname IN ('amount', 'member')
ORDER BY attnum;

-- A standalone composite is also a valid LIKE source.
CREATE TABLE ddl_copied_type (LIKE ddl_numeric_member) USING iceberg;

INSERT INTO ddl_copied_type VALUES (8.5, 9.25);
SELECT * FROM ddl_copied_type;

-- Unchanged INCLUDING clauses retain PG's deferred default/generated processing.
CREATE TABLE ddl_stable_source (
    base integer NOT NULL DEFAULT 4 CHECK (base > 0),
    doubled integer GENERATED ALWAYS AS (base * 2) STORED
);
COMMENT ON COLUMN ddl_stable_source.base IS 'copied comment';
CREATE TABLE ddl_stable_copy (
    LIKE ddl_stable_source INCLUDING DEFAULTS INCLUDING GENERATED INCLUDING CONSTRAINTS INCLUDING COMMENTS
) USING iceberg;
INSERT INTO ddl_stable_copy DEFAULT VALUES;
SELECT base, doubled, col_description('ddl_stable_copy'::regclass, 1) AS comment
FROM ddl_stable_copy;

-- IF NOT EXISTS must skip both source lookups when the target already exists.
CREATE TABLE IF NOT EXISTS ddl_copied (LIKE missing_like_source)
INHERITS (missing_parent) USING iceberg;

-- A skipped non-Iceberg CREATE has no newly-created OID or relation lock for
-- the Iceberg post-hook to consume.
CREATE TABLE ddl_existing_heap (id integer);
CREATE TABLE IF NOT EXISTS ddl_existing_heap (id bigint) USING heap;
DROP TABLE ddl_existing_heap;

CREATE TABLE ddl_parent_ok (amount double precision);
CREATE TABLE ddl_child_ok (local_amount numeric) INHERITS (ddl_parent_ok) USING iceberg;

INSERT INTO ddl_child_ok VALUES (10.5, 11.5);
SELECT * FROM ONLY ddl_child_ok;

-- Vector types expose element metadata but are not ordinary array codecs.
CREATE TABLE ddl_int2vector (value int2vector) USING iceberg;
CREATE DOMAIN ddl_vector_domain AS oidvector;
CREATE TYPE ddl_vector_record AS (entries ddl_vector_domain[]);
ALTER TABLE ddl_copied ADD COLUMN vector ddl_vector_record;
DROP TYPE ddl_vector_record;
DROP DOMAIN ddl_vector_domain;

-- Reject a violated copied CHECK and unsupported DDL/type combinations.
CREATE TABLE ddl_parent_numeric (amount numeric);
CREATE DOMAIN ddl_unbounded_numeric AS numeric;
CREATE TYPE ddl_incompatible_member AS (amount ddl_unbounded_numeric[]);
CREATE DOMAIN ddl_incompatible_array AS ddl_incompatible_member[];
\set VERBOSITY sqlstate
INSERT INTO ddl_stable_copy(base) VALUES (-1);

CREATE TABLE ddl_rejected_child () INHERITS (ddl_parent_numeric) USING iceberg;

CREATE TABLE ddl_rejected_copy (LIKE ddl_source INCLUDING DEFAULTS) USING iceberg;

-- Composite definitions must already fit Iceberg; DDL never replaces their types.
CREATE TABLE ddl_rejected_composite (member ddl_incompatible_array) USING iceberg;

ALTER TABLE ddl_copied ADD COLUMN rejected ddl_incompatible_member;

-- Type I/O functions alone do not establish a supported storage encoding.
CREATE TABLE ddl_unsupported_type (value money) USING iceberg;

\set VERBOSITY default
SELECT to_regclass('ddl_rejected_child') IS NULL AS no_child,
       to_regclass('ddl_rejected_copy') IS NULL AS no_copy;

DROP TABLE ddl_child_ok, ddl_parent_ok, ddl_parent_numeric,
    ddl_stable_copy, ddl_stable_source, ddl_copied_type, ddl_copied,
    ddl_suffix_source, ddl_source;
DROP TYPE ddl_numeric_member;
DROP DOMAIN ddl_positive_numeric, ddl_incompatible_array;
DROP TYPE ddl_incompatible_member;
DROP DOMAIN ddl_unbounded_numeric;

-- ============================================================================
-- Clean up: Verify no orphaned entries remain
-- ============================================================================
SELECT COUNT(*) AS orphan_count FROM lagodb.table_options;

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

-- Storage relocation remains unsupported; TRUNCATE has explicit AM lifecycle.
\set VERBOSITY sqlstate
ALTER TABLE dml_lifecycle.after_ctas_t SET ACCESS METHOD heap;
ALTER TABLE dml_lifecycle.after_ctas_t SET TABLESPACE pg_default;
TRUNCATE dml_lifecycle.after_ctas_t;
\set VERBOSITY default

SET client_min_messages = warning;
DROP SCHEMA dml_lifecycle CASCADE;
SET client_min_messages = notice;

RESET client_min_messages;
