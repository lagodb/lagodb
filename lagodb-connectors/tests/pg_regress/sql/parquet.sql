\i include/column_definitions.sql

-- Parquet format coverage: I/O, schema, arrays, projection, filters, and ANALYZE.

SELECT endpoint, bucket, region, access_key_id, secret_access_key
FROM lagodb_regress.object_storage_fixture
\gset storage_

\setenv OBJECT_STORAGE_ENDPOINT :storage_endpoint
\setenv OBJECT_STORAGE_BUCKET :storage_bucket
\setenv OBJECT_STORAGE_REGION :storage_region
\setenv OBJECT_STORAGE_ACCESS_KEY_ID :storage_access_key_id
\setenv OBJECT_STORAGE_SECRET_ACCESS_KEY :storage_secret_access_key

\set parquet_base 's3://' :storage_bucket '/lagodb-connectors/parquet/'
\pset null _null_

-- COPY round trip through an exact object; the suffix selects the format.
\set parquet_exact_path :parquet_base 'exact.parquet'
COPY lagodb_connectors_regress.parquet_source TO :'parquet_exact_path'
WITH (server 'lagodb_connectors_regress_s3');
CREATE TABLE lagodb_connectors_regress.parquet_copy_sink (:parquet_columns);
COPY lagodb_connectors_regress.parquet_copy_sink FROM :'parquet_exact_path'
WITH (server 'lagodb_connectors_regress_s3');
SELECT * FROM lagodb_connectors_regress.parquet_copy_sink ORDER BY id;

-- Parquet COPY FROM reads every object under a prefix.
\set parquet_prefix_path :parquet_base 'prefix/'
\set parquet_part_path :parquet_prefix_path 'part-a.parquet'
COPY (SELECT * FROM lagodb_connectors_regress.parquet_source WHERE id = 1)
TO :'parquet_part_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');
\set parquet_part_path :parquet_prefix_path 'part-b.parquet'
COPY (SELECT * FROM lagodb_connectors_regress.parquet_source WHERE id = 2)
TO :'parquet_part_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');
TRUNCATE lagodb_connectors_regress.parquet_copy_sink;
COPY lagodb_connectors_regress.parquet_copy_sink FROM :'parquet_prefix_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');
SELECT * FROM lagodb_connectors_regress.parquet_copy_sink ORDER BY id;

-- A zstd-compressed object.
\set parquet_zstd_path :parquet_base 'zstd.parquet'
COPY lagodb_connectors_regress.parquet_source TO :'parquet_zstd_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet', compression 'zstd');
TRUNCATE lagodb_connectors_regress.parquet_copy_sink;
COPY lagodb_connectors_regress.parquet_copy_sink FROM :'parquet_zstd_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');
SELECT * FROM lagodb_connectors_regress.parquet_copy_sink ORDER BY id;

-- An empty object and a prefix without objects are valid input.
\set parquet_empty_path :parquet_base 'empty.parquet'
COPY (SELECT * FROM lagodb_connectors_regress.parquet_source WHERE false)
TO :'parquet_empty_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');
TRUNCATE lagodb_connectors_regress.parquet_copy_sink;
COPY lagodb_connectors_regress.parquet_copy_sink FROM :'parquet_empty_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');
SELECT count(*) AS empty_rows FROM lagodb_connectors_regress.parquet_copy_sink;
\set parquet_empty_prefix_path :parquet_base 'empty-prefix/'
COPY lagodb_connectors_regress.parquet_copy_sink FROM :'parquet_empty_prefix_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');
SELECT count(*) AS empty_prefix_rows FROM lagodb_connectors_regress.parquet_copy_sink;

-- Exact and prefix foreign scans restore the values written by PostgreSQL.
CREATE FOREIGN TABLE lagodb_connectors_regress.parquet_exact (:parquet_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'parquet_exact_path');
SELECT * FROM lagodb_connectors_regress.parquet_exact ORDER BY id;
CREATE FOREIGN TABLE lagodb_connectors_regress.parquet_filter (:parquet_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'parquet_prefix_path', format 'parquet');
SELECT * FROM lagodb_connectors_regress.parquet_filter ORDER BY id;

-- Parquet-owned schema inference.
CREATE FOREIGN TABLE lagodb_connectors_regress.parquet_inferred ()
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'parquet_exact_path', format 'parquet');
SELECT attname, format_type(atttypid, atttypmod) AS type
FROM pg_attribute
WHERE attrelid = 'lagodb_connectors_regress.parquet_inferred'::regclass
  AND attnum > 0 AND NOT attisdropped
ORDER BY attnum;
SELECT count(*) AS inferred_rows FROM lagodb_connectors_regress.parquet_inferred;

-- ParquetScanState must restart for parameterized nested-loop rescans.
SELECT outer_rel.id AS outer_id, inner_rel.id AS inner_id
FROM (VALUES (1), (2), (999)) AS outer_rel(id)
LEFT JOIN LATERAL (
    SELECT id
    FROM lagodb_connectors_regress.parquet_filter AS inner_rel
    WHERE inner_rel.id = outer_rel.id
    OFFSET 0
) AS inner_rel ON true
ORDER BY outer_rel.id;

-- Prefix foreign INSERT is the writable Parquet FDW path.
\set parquet_write_path :parquet_base 'write/'
CREATE FOREIGN TABLE lagodb_connectors_regress.parquet_write (:parquet_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'parquet_write_path', format 'parquet');
INSERT INTO lagodb_connectors_regress.parquet_write
SELECT * FROM lagodb_connectors_regress.parquet_source;
SELECT * FROM lagodb_connectors_regress.parquet_write ORDER BY id;

-- ANALYZE scans the complete object set for the population and persists its
-- compressed byte size as relpages. The prefix contains exactly two rows.
ANALYZE lagodb_connectors_regress.parquet_filter;
SELECT reltuples::bigint AS reltuples, relpages > 0 AS has_pages
FROM pg_class
WHERE oid = 'lagodb_connectors_regress.parquet_filter'::regclass;

-- Mirrored and ordinary integer comparisons are pushed without a local Filter.
-- No global object or row order is guaranteed, so PostgreSQL keeps the Sort.
EXPLAIN (COSTS OFF)
SELECT id FROM lagodb_connectors_regress.parquet_filter
WHERE 1 < id AND bigint_col < 0::bigint ORDER BY id;
SELECT id FROM lagodb_connectors_regress.parquet_filter
WHERE 1 < id AND bigint_col < 0::bigint ORDER BY id;
SELECT id FROM lagodb_connectors_regress.parquet_filter
WHERE smallint_col = 7::smallint;

-- Unsupported arithmetic remains a local PostgreSQL Filter.
EXPLAIN (COSTS OFF)
SELECT id FROM lagodb_connectors_regress.parquet_filter WHERE id + 1 = 2;

-- Boolean comparison, NULL tests, AND, OR, and NOT compose into one pushed
-- predicate that keeps PostgreSQL's three-valued logic.
EXPLAIN (COSTS OFF)
SELECT inner_rel.id
FROM (VALUES (true)) AS outer_rel(flag)
CROSS JOIN LATERAL (
    SELECT id FROM lagodb_connectors_regress.parquet_filter
    WHERE (NOT (smallint_col IS NULL) AND bool_col = outer_rel.flag)
       OR (smallint_col IS NULL AND bool_col <> outer_rel.flag)
    OFFSET 0
) AS inner_rel
ORDER BY inner_rel.id;
SELECT inner_rel.id
FROM (VALUES (true)) AS outer_rel(flag)
CROSS JOIN LATERAL (
    SELECT id FROM lagodb_connectors_regress.parquet_filter
    WHERE (NOT (smallint_col IS NULL) AND bool_col = outer_rel.flag)
       OR (smallint_col IS NULL AND bool_col <> outer_rel.flag)
    OFFSET 0
) AS inner_rel
ORDER BY inner_rel.id;

-- Equality is exact for deterministic collations; ordering is pushed only for
-- byte-order C/POSIX collations.
EXPLAIN (COSTS OFF)
SELECT id FROM lagodb_connectors_regress.parquet_filter
WHERE varchar_col = 'varchar-one' OR text_col COLLATE "C" > 'z' COLLATE "C"
ORDER BY id;
SELECT id FROM lagodb_connectors_regress.parquet_filter
WHERE varchar_col = 'varchar-one' OR text_col COLLATE "C" > 'z' COLLATE "C"
ORDER BY id;

-- A NULL runtime parameter is UNKNOWN and returns no rows. Metadata pruning
-- normalizes NOT to leaf operators, and NOT UNKNOWN must remain UNKNOWN.
CREATE TABLE lagodb_connectors_regress.null_parameter_source (id integer);
INSERT INTO lagodb_connectors_regress.null_parameter_source VALUES (NULL);
SELECT inner_rel.id
FROM lagodb_connectors_regress.null_parameter_source AS outer_rel
CROSS JOIN LATERAL (
    SELECT id FROM lagodb_connectors_regress.parquet_filter
    WHERE id = outer_rel.id OFFSET 0
) AS inner_rel;
EXPLAIN (COSTS OFF)
SELECT inner_rel.id
FROM lagodb_connectors_regress.null_parameter_source AS outer_rel
CROSS JOIN LATERAL (
    SELECT id FROM lagodb_connectors_regress.parquet_filter
    WHERE NOT (id = outer_rel.id) OFFSET 0
) AS inner_rel;
SELECT inner_rel.id
FROM lagodb_connectors_regress.null_parameter_source AS outer_rel
CROSS JOIN LATERAL (
    SELECT id FROM lagodb_connectors_regress.parquet_filter
    WHERE NOT (id = outer_rel.id) OFFSET 0
) AS inner_rel;

-- Reject narrowing before a predicate can hide an incompatible column. Scan
-- and COPY bind every scalar, array element, and width through one mapping.
CREATE FOREIGN TABLE lagodb_connectors_regress.parquet_integer_mismatch (
    bigint_col integer
)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'parquet_exact_path', format 'parquet');
\set VERBOSITY sqlstate
SELECT bigint_col FROM lagodb_connectors_regress.parquet_integer_mismatch
WHERE bigint_col = 1;
\set VERBOSITY default

-- A constant-only target list reads no column, so the incompatible column is
-- never bound.
SELECT 1 AS one FROM lagodb_connectors_regress.parquet_integer_mismatch LIMIT 1;

-- Arrow List represents one-dimensional PostgreSQL arrays. A multidimensional
-- value is the representative unsupported-shape boundary.
\set array_multidimensional_path :parquet_base 'arrays/multidimensional/'
CREATE FOREIGN TABLE lagodb_connectors_regress.parquet_multidimensional (
    id integer,
    integer_array integer[]
)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'array_multidimensional_path', format 'parquet');

\set VERBOSITY sqlstate
INSERT INTO lagodb_connectors_regress.parquet_multidimensional
VALUES (1, ARRAY[[1, 2], [3, 4]]);
\set VERBOSITY default
\setenv OBJECT_STORAGE_PREFIX 'lagodb-connectors/parquet/arrays/multidimensional/'
\! sh bin/object_storage_tool assert-prefix-empty

-- Parquet COPY binds target columns by name, so a column list may skip and
-- reorder file columns.
\set parquet_projection_path :parquet_base 'projection.parquet'
COPY (SELECT id, bool_col, varchar_col FROM lagodb_connectors_regress.common_source)
TO :'parquet_projection_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');
CREATE TABLE lagodb_connectors_regress.parquet_projection_sink (
    varchar_col varchar(20),
    id integer
);
COPY lagodb_connectors_regress.parquet_projection_sink (varchar_col, id)
FROM :'parquet_projection_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');
SELECT * FROM lagodb_connectors_regress.parquet_projection_sink ORDER BY id;

-- A whole-row reference reads every column of the foreign table.
CREATE FOREIGN TABLE lagodb_connectors_regress.parquet_projection (
    id integer,
    bool_col boolean,
    varchar_col varchar(20)
)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'parquet_projection_path', format 'parquet');
SELECT p FROM lagodb_connectors_regress.parquet_projection AS p ORDER BY p.id;

-- Prefix objects must share one writer schema.
\set parquet_drift_path :parquet_base 'drift/'
\set parquet_part_path :parquet_drift_path 'part-a.parquet'
COPY (SELECT id, text_col FROM lagodb_connectors_regress.common_source WHERE id = 1)
TO :'parquet_part_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');
\set parquet_part_path :parquet_drift_path 'part-b.parquet'
COPY (SELECT id, integer_col FROM lagodb_connectors_regress.common_source WHERE id = 2)
TO :'parquet_part_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');
CREATE FOREIGN TABLE lagodb_connectors_regress.parquet_drift (id integer, text_col text)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'parquet_drift_path', format 'parquet');
\set VERBOSITY sqlstate
SELECT count(*) FROM lagodb_connectors_regress.parquet_drift;
\set VERBOSITY default
