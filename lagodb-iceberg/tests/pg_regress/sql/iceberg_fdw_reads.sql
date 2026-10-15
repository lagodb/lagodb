-- Iceberg FDW read, pushdown, plan identity and no-vending routing coverage.

\setenv PGDATABASE :DBNAME

SELECT rest_uri AS regress_rest_uri,
       fallback_rest_uri AS regress_fallback_rest_uri,
       endpoint AS regress_s3_endpoint,
       format('s3://%s/', bucket) AS regress_vended_scope,
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
RESET client_min_messages;
CREATE EXTENSION lagodb_iceberg;
CREATE SCHEMA iceberg_fdw_read;
CREATE SERVER iceberg_read_rest
TYPE 'rest'
FOREIGN DATA WRAPPER lagodb_iceberg
OPTIONS (uri :'regress_rest_uri', "rest.auth.type" 'none');
CREATE SERVER iceberg_read_fallback
TYPE 'rest'
FOREIGN DATA WRAPPER lagodb_iceberg
OPTIONS (
    uri :'regress_fallback_rest_uri',
    "rest.auth.type" 'none',
    enable_vended_credentials 'false'
);
-- Unauthenticated REST catalogs require no empty user mappings.

-- Complete REST credentials must bypass a local mapping, including its role.
CREATE SERVER iceberg_read_storage_vended
TYPE 'storage'
FOREIGN DATA WRAPPER lagodb_iceberg
OPTIONS (
    scope :'regress_vended_scope', endpoint :'regress_s3_endpoint',
    region :'regress_s3_region', allow_http 'true',
    virtual_hosted_style_request 'false'
);
CREATE USER MAPPING FOR CURRENT_USER SERVER iceberg_read_storage_vended
OPTIONS (
    aws_profile 'unused-regression-profile',
    role_arn 'arn:aws:iam::123456789012:role/unused-regression-role'
);

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
-- The broader fallback route is unused; its GCS identity is invalid for S3.
CREATE USER MAPPING FOR CURRENT_USER SERVER iceberg_read_storage_broad
OPTIONS (service_account_key '{}');
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

CREATE FOREIGN TABLE iceberg_fdw_read.filters (
    id integer,
    payload text,
    event_date date
)
SERVER iceberg_read_rest
OPTIONS (
    table 'fdw_reads.filter_rows_v2'
);

-- Unlike unauthenticated REST, OAuth2 requires catalog credentials.
ALTER SERVER iceberg_read_rest OPTIONS (SET "rest.auth.type" 'oauth2');
\set VERBOSITY terse
CREATE FOREIGN TABLE iceberg_fdw_read.oauth_missing ()
SERVER iceberg_read_rest OPTIONS (table 'fdw_reads.filter_rows_v2');
\set VERBOSITY default
ALTER SERVER iceberg_read_rest OPTIONS (SET "rest.auth.type" 'none');

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
    table 'fdw_reads.deletion_vectors_v3'
);
SELECT * FROM iceberg_fdw_read.v3_mutations ORDER BY id;

-- Historical partition files remain readable after their source column is dropped.
CREATE FOREIGN TABLE iceberg_fdw_read.dropped_partition_source_v2 ()
SERVER iceberg_read_rest
OPTIONS (
    table 'fdw_reads.dropped_partition_source_v2'
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
DEALLOCATE iceberg_read_by_id;
RESET plan_cache_mode;

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

-- Replace both the remote UUID and schema: the stale-plan error takes precedence
-- over binding the now-absent payload column. Other read fixtures stay intact.
CREATE FOREIGN TABLE iceberg_fdw_read.cached_plan ()
SERVER iceberg_read_rest
OPTIONS (
    table 'fdw_cached_plan.filter_rows_v2'
);
PREPARE iceberg_stale_read AS
SELECT payload FROM iceberg_fdw_read.cached_plan WHERE id = 2;
EXECUTE iceberg_stale_read;
\set iceberg_fixture cached-plan
\i include/iceberg_fixture.sql
EXECUTE iceberg_stale_read;
DEALLOCATE iceberg_stale_read;

-- IMPORT ALL/EXCEPT and inferred local columns exercise foreign DDL binding.
CREATE SCHEMA iceberg_fdw_read_import;
IMPORT FOREIGN SCHEMA fdw_reads
EXCEPT (
    deletion_vectors_v3,
    partition_evolution_v2,
    dropped_partition_source_v2
)
FROM SERVER iceberg_read_rest
INTO iceberg_fdw_read_import;
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
    table 'fdw_reads.rows_v2'
);
CREATE FOREIGN TABLE iceberg_fdw_read.fallback_second_bucket ()
SERVER iceberg_read_fallback
OPTIONS (
    table 'fdw_reads.second_bucket_v2'
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
SELECT * FROM iceberg_fdw_read.fallback_main;

-- Nested JSONB schema binding and direct FDW materialization.
SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'off';
SET max_parallel_workers_per_gather = 0;
SET TimeZone = 'America/New_York';
SET DateStyle = 'SQL, DMY';
-- JSON float output must retain precision even when SQL float output rounds.
SET extra_float_digits = 0;

IMPORT FOREIGN SCHEMA fdw_nested LIMIT TO (rows_v2)
FROM SERVER iceberg_read_rest INTO iceberg_fdw_read;

-- Defaults preserve native basic arrays and use JSONB for complex columns.
SELECT attname, format_type(atttypid, atttypmod) AS type
FROM pg_attribute
WHERE attrelid = 'iceberg_fdw_read.rows_v2'::regclass
  AND attnum > 0 AND NOT attisdropped
ORDER BY attnum;

-- Recursive values retain null fields/elements and structured map keys.
SELECT record ->> 'label' AS label,
       record ? 'missing' AND record -> 'missing' = 'null'::jsonb AS field_null,
       items = '[{"n":1,"tags":["x",null]},null,{"n":2,"tags":[]}]'::jsonb AS list_ok,
       labels = '{"a":null,"b":"text"}'::jsonb AS map_ok,
       indexed = '[{"key":7,"value":["x",null]},{"key":8,"value":null}]'::jsonb AS int_keys,
       structured = '[{"key":{"code":9},"value":"nine"}]'::jsonb AS struct_keys,
       numbers IS NOT DISTINCT FROM ARRAY[1,NULL,2] AS array_ok
FROM iceberg_fdw_read.rows_v2 WHERE id = 1;

-- Numeric precision, binary encoding and temporal output are independent
-- of the session's DateStyle and TimeZone.
SELECT record -> 'amount' = '123456789012345678901234567890.12345678'::jsonb AS decimal_ok,
       record -> 'large' = '9223372036854775807'::jsonb AS bigint_ok,
       record ->> 'bytes' = '00ff10' AS binary_ok,
       record ->> 'event_date' = '2024-01-02' AS date_ok,
       record ->> 'event_time' = '1969-12-31T23:59:59.123456' AS timestamp_ok,
       record ->> 'event_tz' = '2024-01-02T03:04:05.123456+00:00' AS utc_ok,
       record -> 'special' = '[1.2345678901234567,"NaN","Infinity","-Infinity","-0.0",0]'::jsonb
       AND record -> 'single' = '[1.25,"-0.0",0]'::jsonb AS floats_ok
FROM iceberg_fdw_read.rows_v2 WHERE id = 1;

-- Empty containers and SQL NULL are distinct.
SELECT items = '[]'::jsonb AND labels = '{}'::jsonb
       AND indexed = '[]'::jsonb AND structured = '[]'::jsonb
       AND numbers = '{}'::integer[] AS empty_ok
FROM iceberg_fdw_read.rows_v2 WHERE id = 2;

SELECT record IS NULL AND items IS NULL AND labels IS NULL
       AND indexed IS NULL AND structured IS NULL AND numbers IS NULL AS null_ok
FROM iceberg_fdw_read.rows_v2 WHERE id = 3;

-- Explicit declarations use JSONB for nested values and native arrays for basic lists.
CREATE FOREIGN TABLE iceberg_fdw_read.explicit_types (
    id integer,
    record jsonb,
    items jsonb,
    labels jsonb,
    indexed jsonb,
    structured jsonb,
    numbers integer[]
)
SERVER iceberg_read_rest
OPTIONS (
    table 'fdw_nested.rows_v2'
);

SELECT id, numbers FROM iceberg_fdw_read.explicit_types ORDER BY id;

CREATE FOREIGN TABLE iceberg_fdw_read.nanos ()
SERVER iceberg_read_rest
OPTIONS (
    table 'fdw_nanos.rows_v3'
);

-- Top-level timestamps floor to PG microseconds; nested values retain nanoseconds.
SELECT array_agg((extract(epoch FROM ts) * 1000000)::bigint ORDER BY id)
           IS NOT DISTINCT FROM ARRAY[-2,-1,-1,-1,0,0,0,1,NULL]::bigint[] AS timestamp_floor,
       array_agg((extract(epoch FROM tz) * 1000000)::bigint ORDER BY id)
           IS NOT DISTINCT FROM ARRAY[-2,-1,-1,-1,0,0,0,1,NULL]::bigint[] AS timestamptz_floor
FROM iceberg_fdw_read.nanos;

SELECT s.id, detail->>'ts' = expected AS timestamp_ok,
       detail->>'tz' = expected || '+00:00' AS timestamptz_ok
FROM iceberg_fdw_read.nanos s
JOIN (VALUES (4, '1969-12-31T23:59:59.999999999'),
             (5, '1970-01-01T00:00:00.000000000'),
             (6, '1970-01-01T00:00:00.000000001')) AS times(id, expected) USING (id)
ORDER BY s.id;

CREATE FOREIGN TABLE iceberg_fdw_read.batches ()
SERVER iceberg_read_rest
OPTIONS (
    table 'fdw_nested.batches_v2'
);

-- Every nested output must belong to its own row, including batch transitions.
SELECT count(*) AS rows,
       bool_and(value = jsonb_build_object(
           'id', id, 'values', jsonb_build_array(id, NULL))) AS values_ok
FROM iceberg_fdw_read.batches;

-- ANALYZE uses its own row context and materializes the same nested columns.
ANALYZE iceberg_fdw_read.batches;

SELECT reltuples::integer AS sampled_rows
FROM pg_class WHERE oid = 'iceberg_fdw_read.batches'::regclass;

-- Invalid Arrow strings must fail at the statement boundary, then permit another scan.
CREATE FOREIGN TABLE iceberg_fdw_read.invalid_string ()
SERVER iceberg_read_rest
OPTIONS (
    table 'fdw_nested.invalid_string_v2'
);

\set VERBOSITY sqlstate
SELECT value FROM iceberg_fdw_read.invalid_string;
\set VERBOSITY default

SELECT count(*) AS rows_after_error
FROM iceberg_fdw_read.batches
WHERE value ->> 'id' = id::text;

RESET TimeZone;
RESET DateStyle;
RESET extra_float_digits;
RESET max_parallel_workers_per_gather;
RESET lagodb.query_offload_mode;
RESET lagodb.customscan_mode;

SET client_min_messages = warning;
DROP SCHEMA iceberg_fdw_read CASCADE;
DROP SCHEMA iceberg_fdw_read_import CASCADE;
DROP SERVER iceberg_read_rest CASCADE;
DROP SERVER iceberg_read_fallback CASCADE;
DROP SERVER iceberg_read_storage_vended CASCADE;
DROP SERVER iceberg_read_storage_broad CASCADE;
DROP SERVER iceberg_read_storage_narrow CASCADE;
DROP SERVER iceberg_read_storage_b CASCADE;
DROP SERVER iceberg_read_storage_ambiguous CASCADE;
RESET client_min_messages;
