-- Iceberg FDW read, pushdown, plan identity and no-vending routing coverage.

\set ECHO none
\setenv PGDATABASE :DBNAME

SELECT rest_uri AS regress_rest_uri,
       fallback_rest_uri AS regress_fallback_rest_uri,
       endpoint AS regress_s3_endpoint,
       fallback_bucket AS regress_fallback_bucket,
       fallback_second_bucket AS regress_fallback_second_bucket,
       region AS regress_s3_region,
       access_key_id AS regress_s3_access_key_id,
       secret_access_key AS regress_s3_secret_access_key
FROM lagodb_regress.object_storage_fixture
\gset
\set regress_bucket_a_scope 's3://' :regress_fallback_bucket '/'
\set regress_bucket_a_narrow_scope 's3://' :regress_fallback_bucket '/iceberg-fallback/fdw_reads/'
\set regress_bucket_b_scope 's3://' :regress_fallback_second_bucket '/'

SET client_min_messages = warning;
DROP EXTENSION IF EXISTS lagodb_iceberg CASCADE;
CREATE EXTENSION lagodb_iceberg;
CREATE SCHEMA iceberg_fdw_read;
CREATE SERVER iceberg_read_rest
TYPE 'rest'
FOREIGN DATA WRAPPER lagodb_iceberg
OPTIONS (uri :'regress_rest_uri');
CREATE SERVER iceberg_read_fallback
TYPE 'rest'
FOREIGN DATA WRAPPER lagodb_iceberg
OPTIONS (
    uri :'regress_fallback_rest_uri',
    enable_vended_credentials 'false'
);
CREATE USER MAPPING FOR CURRENT_USER SERVER iceberg_read_rest;
CREATE USER MAPPING FOR CURRENT_USER SERVER iceberg_read_fallback;

CREATE SERVER iceberg_read_storage_broad
TYPE 'storage'
FOREIGN DATA WRAPPER lagodb_iceberg
OPTIONS (
    provider 's3_compatible',
    endpoint :'regress_s3_endpoint',
    region :'regress_s3_region',
    scope :'regress_bucket_a_scope',
    allow_http 'true',
    virtual_hosted_style_request 'false'
);
CREATE SERVER iceberg_read_storage_narrow
TYPE 'storage'
FOREIGN DATA WRAPPER lagodb_iceberg
OPTIONS (
    provider 's3_compatible',
    endpoint :'regress_s3_endpoint',
    region :'regress_s3_region',
    scope :'regress_bucket_a_narrow_scope',
    allow_http 'true',
    virtual_hosted_style_request 'false'
);
CREATE SERVER iceberg_read_storage_b
TYPE 'storage'
FOREIGN DATA WRAPPER lagodb_iceberg
OPTIONS (
    provider 's3_compatible',
    endpoint :'regress_s3_endpoint',
    region :'regress_s3_region',
    scope :'regress_bucket_b_scope',
    allow_http 'true',
    virtual_hosted_style_request 'false'
);
CREATE USER MAPPING FOR CURRENT_USER SERVER iceberg_read_storage_broad
OPTIONS (
    access_key_id :'regress_s3_access_key_id',
    secret_access_key :'regress_s3_secret_access_key'
);
CREATE USER MAPPING FOR CURRENT_USER SERVER iceberg_read_storage_narrow
OPTIONS (
    access_key_id :'regress_s3_access_key_id',
    secret_access_key :'regress_s3_secret_access_key'
);
CREATE USER MAPPING FOR CURRENT_USER SERVER iceberg_read_storage_b
OPTIONS (
    access_key_id :'regress_s3_access_key_id',
    secret_access_key :'regress_s3_secret_access_key'
);
RESET client_min_messages;

\set ECHO all
CREATE FOREIGN TABLE iceberg_fdw_read.filters (
    id integer,
    payload text,
    event_date date
)
SERVER iceberg_read_rest
OPTIONS (
    catalog_name 'regress',
    catalog_namespace 'fdw_reads',
    catalog_table_name 'filter_rows_v2',
    mode 'read_only'
);

-- Projection and zero-column aggregation use the remote schema contract.
SELECT payload FROM iceberg_fdw_read.filters WHERE id IN (1, 4) ORDER BY id;
SELECT count(*) AS all_rows FROM iceberg_fdw_read.filters;

-- Spark creates a format-v3 table, then DELETE and UPDATE produce deletion
-- vectors. Reading it through the FDW verifies REST metadata and delete-aware
-- scan wiring independently from the writable adapter.
CREATE FOREIGN TABLE iceberg_fdw_read.v3_mutations (
    id integer,
    payload text
)
SERVER iceberg_read_rest
OPTIONS (
    catalog_name 'regress',
    catalog_namespace 'fdw_reads',
    catalog_table_name 'deletion_vectors_v3',
    mode 'read_only'
);
SELECT * FROM iceberg_fdw_read.v3_mutations ORDER BY id;

-- Partition specs are discovered from Iceberg metadata; no PostgreSQL table
-- option duplicates the partition columns or transforms. The remote table has
-- files written with two specs, so both predicates exercise per-manifest
-- projection while PostgreSQL still rechecks the row predicate.
CREATE FOREIGN TABLE iceberg_fdw_read.partition_evolution_v2 ()
SERVER iceberg_read_rest
OPTIONS (
    catalog_name 'regress',
    catalog_namespace 'fdw_reads',
    catalog_table_name 'partition_evolution_v2',
    mode 'read_only'
);
SELECT array_agg(id ORDER BY id) AS partitioned_all_rows
FROM iceberg_fdw_read.partition_evolution_v2;
SELECT array_agg(id ORDER BY id) AS partitioned_category_rows
FROM iceberg_fdw_read.partition_evolution_v2
WHERE category = 'alpha';
SELECT array_agg(id ORDER BY id) AS partitioned_id_rows
FROM iceberg_fdw_read.partition_evolution_v2
WHERE id >= 4;

-- Historical partition files remain readable after their source column is dropped.
CREATE FOREIGN TABLE iceberg_fdw_read.dropped_partition_source_v2 ()
SERVER iceberg_read_rest
OPTIONS (
    catalog_name 'regress',
    catalog_namespace 'fdw_reads',
    catalog_table_name 'dropped_partition_source_v2',
    mode 'read_only'
);
SELECT array_agg(id ORDER BY id) AS dropped_source_rows
FROM iceberg_fdw_read.dropped_partition_source_v2;

-- Exact, conservative and unsupported filters must remain distinguishable in
-- both the plan and result.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT payload FROM iceberg_fdw_read.filters WHERE id = 2;
SELECT array_agg(id ORDER BY id) AS exact_rows
FROM iceberg_fdw_read.filters WHERE id = 2;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT id FROM iceberg_fdw_read.filters
WHERE event_date = DATE '2024-01-02';
SELECT array_agg(id ORDER BY id) AS conservative_rows
FROM iceberg_fdw_read.filters
WHERE event_date = DATE '2024-01-02';

EXPLAIN (VERBOSE, COSTS OFF)
SELECT id FROM iceberg_fdw_read.filters WHERE length(payload) = 3;
SELECT array_agg(id ORDER BY id) AS residual_rows
FROM iceberg_fdw_read.filters WHERE length(payload) = 3;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT id FROM iceberg_fdw_read.filters
WHERE payload IS NULL OR (id >= 2 AND NOT id = 4);
SELECT array_agg(id ORDER BY id) AS boolean_and_null_rows
FROM iceberg_fdw_read.filters
WHERE payload IS NULL OR (id >= 2 AND NOT id = 4);

-- Generic parameters and a lateral parameterized path exercise runtime binding
-- and rescan without rebuilding the schema contract per row.
SET plan_cache_mode = force_generic_plan;
PREPARE iceberg_read_by_id(integer) AS
SELECT payload FROM iceberg_fdw_read.filters WHERE id = $1;
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE iceberg_read_by_id(2);
EXECUTE iceberg_read_by_id(1);
EXECUTE iceberg_read_by_id(3);

EXPLAIN (VERBOSE, COSTS OFF)
SELECT wanted.id, matched.payload
FROM (VALUES (1), (4)) AS wanted(id)
CROSS JOIN LATERAL (
    SELECT payload
    FROM iceberg_fdw_read.filters
    WHERE filters.id = wanted.id
    OFFSET 0
) AS matched;
SELECT wanted.id, matched.payload
FROM (VALUES (1), (4)) AS wanted(id)
CROSS JOIN LATERAL (
    SELECT payload
    FROM iceberg_fdw_read.filters
    WHERE filters.id = wanted.id
    OFFSET 0
) AS matched
ORDER BY wanted.id;
RESET plan_cache_mode;

-- Only this testcase owns the replaceable remote UUID. Read fixtures stay intact.
CREATE FOREIGN TABLE iceberg_fdw_read.cached_plan ()
SERVER iceberg_read_rest
OPTIONS (
    catalog_name 'regress',
    catalog_namespace 'fdw_cached_plan',
    catalog_table_name 'filter_rows_v2',
    mode 'read_only'
);
PREPARE iceberg_stale_read AS
SELECT payload FROM iceberg_fdw_read.cached_plan WHERE id = 2;
EXECUTE iceberg_stale_read;
\set ECHO none
\set iceberg_fixture cached-plan
\i include/iceberg_fixture.sql
\set ECHO all
\set VERBOSITY terse
EXECUTE iceberg_stale_read;
\set VERBOSITY default
DEALLOCATE iceberg_stale_read;
DEALLOCATE iceberg_read_by_id;

-- IMPORT ALL/EXCEPT and inferred local columns exercise read-only DDL binding.
CREATE SCHEMA iceberg_fdw_read_import;
IMPORT FOREIGN SCHEMA fdw_reads
EXCEPT (
    deletion_vectors_v3,
    partition_evolution_v2,
    dropped_partition_source_v2
)
FROM SERVER iceberg_read_rest
INTO iceberg_fdw_read_import
OPTIONS (mode 'read_only');
SELECT foreign_table_name
FROM information_schema.foreign_tables
WHERE foreign_table_schema = 'iceberg_fdw_read_import'
ORDER BY foreign_table_name;
SELECT * FROM iceberg_fdw_read_import.import_rows_v2 ORDER BY id;

-- The no-vending path selects the longest normalized scope and supports a
-- second bucket through an independent profile.
CREATE FOREIGN TABLE iceberg_fdw_read.fallback_main ()
SERVER iceberg_read_fallback
OPTIONS (
    catalog_name 'regress',
    catalog_namespace 'fdw_reads',
    catalog_table_name 'rows_v2',
    mode 'read_only'
);
CREATE FOREIGN TABLE iceberg_fdw_read.fallback_second_bucket ()
SERVER iceberg_read_fallback
OPTIONS (
    catalog_name 'regress',
    catalog_namespace 'fdw_reads',
    catalog_table_name 'second_bucket_v2',
    mode 'read_only'
);
SELECT * FROM iceberg_fdw_read.fallback_main ORDER BY id;
SELECT * FROM iceberg_fdw_read.fallback_second_bucket ORDER BY id;

CREATE SERVER iceberg_read_storage_ambiguous
TYPE 'storage'
FOREIGN DATA WRAPPER lagodb_iceberg
OPTIONS (
    provider 's3_compatible',
    endpoint :'regress_s3_endpoint',
    region :'regress_s3_region',
    scope :'regress_bucket_a_narrow_scope',
    allow_http 'true',
    virtual_hosted_style_request 'false'
);
CREATE USER MAPPING FOR CURRENT_USER SERVER iceberg_read_storage_ambiguous
OPTIONS (
    access_key_id :'regress_s3_access_key_id',
    secret_access_key :'regress_s3_secret_access_key'
);
\set VERBOSITY terse
SELECT * FROM iceberg_fdw_read.fallback_main;
\set VERBOSITY default

SET client_min_messages = warning;
DROP SCHEMA iceberg_fdw_read CASCADE;
DROP SCHEMA iceberg_fdw_read_import CASCADE;
DROP SERVER iceberg_read_rest CASCADE;
DROP SERVER iceberg_read_fallback CASCADE;
DROP SERVER iceberg_read_storage_broad CASCADE;
DROP SERVER iceberg_read_storage_narrow CASCADE;
DROP SERVER iceberg_read_storage_b CASCADE;
DROP SERVER iceberg_read_storage_ambiguous CASCADE;
RESET client_min_messages;
