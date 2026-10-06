\i include/column_definitions.sql

-- Parquet format coverage: I/O, schema, arrays, projection, filters, and ANALYZE.

SELECT bucket AS lagodb_regress_bucket
FROM lagodb_regress.object_storage_fixture
\gset

SELECT format('s3://%s/lagodb-connectors/parquet/exact.parquet',
              :'lagodb_regress_bucket') AS parquet_exact_path,
       format('s3://%s/lagodb-connectors/parquet/prefix/',
              :'lagodb_regress_bucket') AS parquet_filter_path,
       format('s3://%s/lagodb-connectors/parquet/prefix/part-a.parquet',
              :'lagodb_regress_bucket') AS parquet_part_a_path,
       format('s3://%s/lagodb-connectors/parquet/prefix/part-b.parquet',
              :'lagodb_regress_bucket') AS parquet_part_b_path,
       format('s3://%s/lagodb-connectors/parquet/zstd.parquet',
              :'lagodb_regress_bucket') AS parquet_zstd_path,
       format('s3://%s/lagodb-connectors/parquet/empty.parquet',
              :'lagodb_regress_bucket') AS parquet_empty_path,
       format('s3://%s/lagodb-connectors/parquet/empty-prefix/',
              :'lagodb_regress_bucket') AS parquet_empty_prefix_path,
       format('s3://%s/lagodb-connectors/parquet/write/',
              :'lagodb_regress_bucket') AS parquet_write_path
\gset

-- Exact, prefix, compressed, and empty direct-COPY paths.
COPY lagodb_connectors_regress.parquet_source
TO :'parquet_exact_path'
WITH (server 'lagodb_connectors_regress_s3');
COPY (
    SELECT * FROM lagodb_connectors_regress.parquet_source WHERE id = 1
) TO :'parquet_part_a_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');
COPY (
    SELECT * FROM lagodb_connectors_regress.parquet_source WHERE id = 2
) TO :'parquet_part_b_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');
COPY lagodb_connectors_regress.parquet_source
TO :'parquet_zstd_path'
WITH (
    server 'lagodb_connectors_regress_s3',
    format 'parquet',
    compression 'zstd'
);
COPY (
    SELECT * FROM lagodb_connectors_regress.parquet_source WHERE false
) TO :'parquet_empty_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');

-- json and json[] require normalization for equality. Compare both
-- directions with ALL to check every value and duplicate row count.
CREATE TABLE lagodb_connectors_regress.parquet_copy_sink (:parquet_columns);
COPY lagodb_connectors_regress.parquet_copy_sink
FROM :'parquet_exact_path'
WITH (server 'lagodb_connectors_regress_s3');
(SELECT to_jsonb(value) AS row_data FROM lagodb_connectors_regress.parquet_copy_sink AS value
 EXCEPT ALL SELECT to_jsonb(value) FROM lagodb_connectors_regress.parquet_source AS value)
UNION ALL
(SELECT to_jsonb(value) FROM lagodb_connectors_regress.parquet_source AS value
 EXCEPT ALL SELECT to_jsonb(value) FROM lagodb_connectors_regress.parquet_copy_sink AS value);

TRUNCATE lagodb_connectors_regress.parquet_copy_sink;
COPY lagodb_connectors_regress.parquet_copy_sink
FROM :'parquet_filter_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');
(SELECT to_jsonb(value) AS row_data FROM lagodb_connectors_regress.parquet_copy_sink AS value
 EXCEPT ALL SELECT to_jsonb(value) FROM lagodb_connectors_regress.parquet_source AS value)
UNION ALL
(SELECT to_jsonb(value) FROM lagodb_connectors_regress.parquet_source AS value
 EXCEPT ALL SELECT to_jsonb(value) FROM lagodb_connectors_regress.parquet_copy_sink AS value);

TRUNCATE lagodb_connectors_regress.parquet_copy_sink;
COPY lagodb_connectors_regress.parquet_copy_sink
FROM :'parquet_zstd_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');
(SELECT to_jsonb(value) AS row_data FROM lagodb_connectors_regress.parquet_copy_sink AS value
 EXCEPT ALL SELECT to_jsonb(value) FROM lagodb_connectors_regress.parquet_source AS value)
UNION ALL
(SELECT to_jsonb(value) FROM lagodb_connectors_regress.parquet_source AS value
 EXCEPT ALL SELECT to_jsonb(value) FROM lagodb_connectors_regress.parquet_copy_sink AS value);

TRUNCATE lagodb_connectors_regress.parquet_copy_sink;
COPY lagodb_connectors_regress.parquet_copy_sink
FROM :'parquet_empty_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');
SELECT count(*) AS empty_rows FROM lagodb_connectors_regress.parquet_copy_sink;

TRUNCATE lagodb_connectors_regress.parquet_copy_sink;
COPY lagodb_connectors_regress.parquet_copy_sink
FROM :'parquet_empty_prefix_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');
SELECT count(*) AS empty_prefix_rows FROM lagodb_connectors_regress.parquet_copy_sink;

-- Exact/prefix foreign scans and format-owned schema inference.
CREATE FOREIGN TABLE lagodb_connectors_regress.parquet_exact
    (:parquet_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'parquet_exact_path');
CREATE FOREIGN TABLE lagodb_connectors_regress.parquet_filter
    (:parquet_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'parquet_filter_path', format 'parquet');
CREATE FOREIGN TABLE lagodb_connectors_regress.parquet_inferred ()
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'parquet_exact_path', format 'parquet');

(SELECT to_jsonb(value) AS row_data FROM lagodb_connectors_regress.parquet_exact AS value
 EXCEPT ALL SELECT to_jsonb(value) FROM lagodb_connectors_regress.parquet_source AS value)
UNION ALL
(SELECT to_jsonb(value) FROM lagodb_connectors_regress.parquet_source AS value
 EXCEPT ALL SELECT to_jsonb(value) FROM lagodb_connectors_regress.parquet_exact AS value);

(SELECT to_jsonb(value) AS row_data FROM lagodb_connectors_regress.parquet_filter AS value
 EXCEPT ALL SELECT to_jsonb(value) FROM lagodb_connectors_regress.parquet_source AS value)
UNION ALL
(SELECT to_jsonb(value) FROM lagodb_connectors_regress.parquet_source AS value
 EXCEPT ALL SELECT to_jsonb(value) FROM lagodb_connectors_regress.parquet_filter AS value);

SELECT attname, format_type(atttypid, atttypmod) AS type
FROM pg_attribute
WHERE attrelid = 'lagodb_connectors_regress.parquet_inferred'::regclass
  AND attnum > 0 AND NOT attisdropped
ORDER BY attnum;
SELECT count(*) AS inferred_rows
FROM lagodb_connectors_regress.parquet_inferred;

-- Prefix foreign INSERT is the writable Parquet FDW path.
CREATE FOREIGN TABLE lagodb_connectors_regress.parquet_write
    (:parquet_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'parquet_write_path', format 'parquet');
INSERT INTO lagodb_connectors_regress.parquet_write
SELECT * FROM lagodb_connectors_regress.parquet_source;
(SELECT to_jsonb(value) AS row_data FROM lagodb_connectors_regress.parquet_write AS value
 EXCEPT ALL SELECT to_jsonb(value) FROM lagodb_connectors_regress.parquet_source AS value)
UNION ALL
(SELECT to_jsonb(value) FROM lagodb_connectors_regress.parquet_source AS value
 EXCEPT ALL SELECT to_jsonb(value) FROM lagodb_connectors_regress.parquet_write AS value);

CREATE TABLE lagodb_connectors_regress.null_parameter_source (id integer);
INSERT INTO lagodb_connectors_regress.null_parameter_source VALUES (NULL);

-- Mirrored and ordinary integer comparisons are both evaluated by Parquet.
SELECT id
FROM lagodb_connectors_regress.parquet_filter
WHERE 1 < id AND bigint_col < 0::bigint
ORDER BY id;

-- EXPLAIN distinguishes pushed predicates from local residual quals.
EXPLAIN (COSTS OFF)
SELECT id
FROM lagodb_connectors_regress.parquet_filter
WHERE id = 1;

-- Boolean comparison, NULL tests, AND, OR, and NOT retain PostgreSQL's
-- three-valued logic inside the Arrow predicate.
SELECT inner_rel.id
FROM (VALUES (true)) AS outer_rel(flag)
CROSS JOIN LATERAL (
    SELECT id
    FROM lagodb_connectors_regress.parquet_filter
    WHERE (NOT (smallint_col IS NULL) AND bool_col = outer_rel.flag)
       OR (smallint_col IS NULL AND bool_col <> outer_rel.flag)
    OFFSET 0
) AS inner_rel
ORDER BY inner_rel.id;

-- Equality is exact for deterministic collations; ordering is restricted to
-- byte-order C/POSIX collations.
SELECT id
FROM lagodb_connectors_regress.parquet_filter
WHERE varchar_col = 'varchar-one'
   OR text_col COLLATE "C" > 'z' COLLATE "C"
ORDER BY id;

-- NULL runtime parameters retain UNKNOWN and return no rows.
SELECT inner_rel.id
FROM lagodb_connectors_regress.null_parameter_source AS outer_rel
CROSS JOIN LATERAL (
    SELECT id
    FROM lagodb_connectors_regress.parquet_filter
    WHERE id = outer_rel.id
    OFFSET 0
) AS inner_rel
ORDER BY inner_rel.id;

-- Metadata pruning normalizes NOT to leaf operators. NOT UNKNOWN must remain
-- UNKNOWN rather than becoming TRUE when the runtime parameter is NULL.
SELECT inner_rel.id
FROM lagodb_connectors_regress.null_parameter_source AS outer_rel
CROSS JOIN LATERAL (
    SELECT id
    FROM lagodb_connectors_regress.parquet_filter
    WHERE NOT (id = outer_rel.id)
    OFFSET 0
) AS inner_rel
ORDER BY inner_rel.id;

ANALYZE lagodb_connectors_regress.parquet_filter;

-- ANALYZE scans the complete object set for the population and persists its
-- compressed byte size as relpages. The fixture contains exactly two rows.
SELECT reltuples::bigint AS reltuples, relpages > 0 AS has_pages
FROM pg_class
WHERE oid = 'lagodb_connectors_regress.parquet_filter'::regclass;

CREATE FUNCTION lagodb_connectors_regress.parquet_explain_plan(query text)
RETURNS jsonb
LANGUAGE plpgsql
AS $$
DECLARE
    plan text;
BEGIN
    EXECUTE 'EXPLAIN (FORMAT JSON) ' || query INTO plan;
    RETURN plan::jsonb -> 0 -> 'Plan';
END
$$;

-- Inspect the ForeignScan fields: exact predicates have no local Filter.
SELECT (plan #>> '{LagoDB Pushdown,Pushed Filter}') LIKE '%id > 1%'
   AND (plan #>> '{LagoDB Pushdown,Pushed Filter}') LIKE '%bigint_col <%'
   AND (plan #>> '{LagoDB Pushdown,Pushed Filter}') LIKE '%::bigint%'
   AND NOT (plan ? 'Filter')
       AS mirrored_and_pushdown_complete
FROM lagodb_connectors_regress.parquet_explain_plan(
    $$SELECT id
          FROM lagodb_connectors_regress.parquet_filter
          WHERE 1 < id AND bigint_col < 0::bigint$$
) AS explained(plan);

-- Folded boolean Vars remain local; pushed NULL tests must retain the residual Filter.
SELECT ((plan #>> '{LagoDB Pushdown,Pushed Filter}') LIKE '%smallint_col IS NOT NULL%'
        OR (plan #>> '{LagoDB Pushdown,Pushed Filter}') LIKE '%NOT (smallint_col IS NULL)%')
   AND (plan #>> '{LagoDB Pushdown,Pushed Filter}') LIKE '%smallint_col IS NULL%'
   AND (plan ->> 'Filter') LIKE '%bool_col%'
   AND plan ? 'Filter'
       AS boolean_null_logic_pushdown_complete
FROM jsonb_path_query_first(
    lagodb_connectors_regress.parquet_explain_plan(
        $$SELECT inner_rel.id
          FROM (VALUES (true)) AS outer_rel(flag)
          CROSS JOIN LATERAL (
              SELECT id
              FROM lagodb_connectors_regress.parquet_filter
              WHERE (NOT (smallint_col IS NULL) AND bool_col = outer_rel.flag)
                 OR (smallint_col IS NULL AND bool_col <> outer_rel.flag)
              OFFSET 0
          ) AS inner_rel$$
    ),
    '$.** ? (@."Node Type" == "Foreign Scan")'
) AS explained(plan);

SELECT (plan #>> '{LagoDB Pushdown,Pushed Filter}') LIKE '%varchar_col =%'
   AND (plan #>> '{LagoDB Pushdown,Pushed Filter}') LIKE '%text_col >%'
   AND NOT (plan ? 'Filter')
       AS string_collation_pushdown_complete
FROM lagodb_connectors_regress.parquet_explain_plan(
    $$SELECT id
          FROM lagodb_connectors_regress.parquet_filter
          WHERE varchar_col = 'varchar-one'
             OR text_col COLLATE "C" > 'z' COLLATE "C"$$
) AS explained(plan);

-- PARAM_EXEC descriptions are persisted during planning. NULL and NOT
-- UNKNOWN execution cases above share this parameterized EXPLAIN path.
SELECT (plan #>> '{LagoDB Pushdown,Pushed Filter}') LIKE '%id = $1%'
   AND NOT (plan ? 'Filter')
       AS null_parameter_pushdown_complete
FROM jsonb_path_query_first(
    lagodb_connectors_regress.parquet_explain_plan(
        $$SELECT inner_rel.id
          FROM lagodb_connectors_regress.null_parameter_source AS outer_rel
          CROSS JOIN LATERAL (
              SELECT id
              FROM lagodb_connectors_regress.parquet_filter
              WHERE id = outer_rel.id
              OFFSET 0
          ) AS inner_rel$$
    ),
    '$.** ? (@."Node Type" == "Foreign Scan")'
) AS explained(plan);

SELECT (plan #>> '{LagoDB Pushdown,Pushed Filter}') LIKE '%id%'
   AND (plan #>> '{LagoDB Pushdown,Pushed Filter}') LIKE '%$1%'
   AND NOT (plan ? 'Filter')
       AS null_parameter_not_pushdown_complete
FROM jsonb_path_query_first(
    lagodb_connectors_regress.parquet_explain_plan(
        $$SELECT inner_rel.id
          FROM lagodb_connectors_regress.null_parameter_source AS outer_rel
          CROSS JOIN LATERAL (
              SELECT id
              FROM lagodb_connectors_regress.parquet_filter
              WHERE NOT (id = outer_rel.id)
              OFFSET 0
          ) AS inner_rel$$
    ),
    '$.** ? (@."Node Type" == "Foreign Scan")'
) AS explained(plan);

-- Unsupported arithmetic remains a local PostgreSQL Filter.
SELECT (plan #> '{LagoDB Pushdown,Pushed Filter}') IS NULL AND plan ? 'Filter'
       AS unsupported_expression_remains_local
FROM lagodb_connectors_regress.parquet_explain_plan(
    $$SELECT id
          FROM lagodb_connectors_regress.parquet_filter
          WHERE id + 1 = 2$$
) AS explained(plan);

-- ANALYZE statistics drive row/width estimates and startup cost.
SELECT (plan ->> 'Plan Rows')::integer <> 1000
   AND (plan ->> 'Plan Width')::integer <> 32
   AND (plan ->> 'Startup Cost')::double precision > 0
       AS planner_uses_analyze_stats
FROM lagodb_connectors_regress.parquet_explain_plan(
    'SELECT * FROM lagodb_connectors_regress.parquet_filter WHERE id = 1'
) AS explained(plan);

-- No global object/row ordering is guaranteed; PostgreSQL must retain Sort.
SELECT (plan ->> 'Node Type') = 'Sort'
       AS planner_retains_sort
FROM lagodb_connectors_regress.parquet_explain_plan(
    'SELECT id FROM lagodb_connectors_regress.parquet_filter ORDER BY id'
) AS explained(plan);

DROP FUNCTION lagodb_connectors_regress.parquet_explain_plan(text);

-- Parquet array element and shape boundaries.

SELECT endpoint,
       bucket,
       region,
       access_key_id,
       secret_access_key
FROM lagodb_regress.object_storage_fixture
\gset storage_

\setenv OBJECT_STORAGE_ENDPOINT :storage_endpoint
\setenv OBJECT_STORAGE_BUCKET :storage_bucket
\setenv OBJECT_STORAGE_REGION :storage_region
\setenv OBJECT_STORAGE_ACCESS_KEY_ID :storage_access_key_id
\setenv OBJECT_STORAGE_SECRET_ACCESS_KEY :storage_secret_access_key

SELECT format('s3://%s/lagodb-connectors/parquet-arrays/null-elements.parquet',
              :'storage_bucket') AS null_elements_path,
       format('s3://%s/lagodb-connectors/parquet-arrays/multidimensional/',
              :'storage_bucket') AS multidimensional_path,
       'lagodb-connectors/parquet-arrays/multidimensional/' AS multidimensional_key
\gset array_

CREATE TABLE lagodb_connectors_regress.parquet_null_array_source
    (:parquet_columns);
INSERT INTO lagodb_connectors_regress.parquet_null_array_source
SELECT id,
       bool_col,
       smallint_col,
       integer_col,
       bigint_col,
       real_col,
       double_col,
       numeric_col,
       text_col,
       varchar_col,
       char_col,
       name_col,
       bytea_col,
       uuid_col,
       date_col,
       time_col,
       timestamp_col,
       timestamptz_col,
       json_col,
       ARRAY[true, NULL, false]::boolean[],
       ARRAY[1, NULL, 3]::smallint[],
       ARRAY[1, NULL, 3]::integer[],
       ARRAY[1, NULL, 3]::bigint[],
       ARRAY[1.0, NULL, 3.0]::real[],
       ARRAY[1.0, NULL, 3.0]::double precision[],
       ARRAY['left', NULL, 'right']::text[],
       ARRAY['left', NULL, 'right']::varchar(20)[],
       ARRAY['left', NULL, 'right']::character(5)[],
       ARRAY['left', NULL, 'right']::name[],
       ARRAY['{"side":"left"}'::json, NULL, '{"side":"right"}'::json]
FROM lagodb_connectors_regress.parquet_source
WHERE id = 1;

COPY lagodb_connectors_regress.parquet_null_array_source
TO :'array_null_elements_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');

COPY lagodb_connectors_regress.parquet_copy_sink
FROM :'array_null_elements_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');

(SELECT to_jsonb(value) AS row_data FROM lagodb_connectors_regress.parquet_copy_sink AS value
 EXCEPT ALL SELECT to_jsonb(value) FROM lagodb_connectors_regress.parquet_null_array_source AS value)
UNION ALL
(SELECT to_jsonb(value) FROM lagodb_connectors_regress.parquet_null_array_source AS value
 EXCEPT ALL SELECT to_jsonb(value) FROM lagodb_connectors_regress.parquet_copy_sink AS value);

-- Arrow List represents one-dimensional PostgreSQL arrays. A multidimensional
-- value is the representative unsupported-shape boundary.
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
\setenv OBJECT_STORAGE_PREFIX :array_multidimensional_key
\! sh bin/object_storage_tool assert-prefix-empty

-- Native-reader projection and COPY target-column mapping.

SELECT format('s3://%s/lagodb-connectors/scan/reorder.parquet',
              :'lagodb_regress_bucket') AS reorder_path
\gset projection_

-- A three-column object isolates projection and column-order behavior.
COPY (
    SELECT id, bool_col, text_col
    FROM lagodb_connectors_regress.common_source
    ORDER BY id
) TO :'projection_reorder_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');

CREATE TABLE lagodb_connectors_regress.scan_projection_sink (
    id integer,
    text_col text
);
COPY lagodb_connectors_regress.scan_projection_sink (id, text_col)
FROM :'projection_reorder_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');

CREATE TABLE lagodb_connectors_regress.scan_reordered_sink (
    id integer,
    bool_col boolean,
    text_col text
);
COPY lagodb_connectors_regress.scan_reordered_sink (
    text_col,
    id,
    bool_col
)
FROM :'projection_reorder_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');

SELECT id, to_json(text_col) AS text_col
FROM lagodb_connectors_regress.scan_projection_sink ORDER BY id;
SELECT id, bool_col, to_json(text_col) AS text_col
FROM lagodb_connectors_regress.scan_reordered_sink ORDER BY id;

CREATE FOREIGN TABLE lagodb_connectors_regress.scan_projection (
    id integer,
    bool_col boolean,
    text_col text
)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'projection_reorder_path', format 'parquet');

SELECT id, text_col
FROM lagodb_connectors_regress.scan_projection
ORDER BY id;

SELECT text_col, id, bool_col
FROM lagodb_connectors_regress.scan_projection
ORDER BY id;

-- ParquetScanState rescan, schema-drift, and supported-type boundaries.
SET enable_hashjoin = off;
SET enable_mergejoin = off;
SET enable_material = off;
SET enable_nestloop = on;
SELECT outer_rel.id AS outer_id, inner_rel.id AS inner_id
FROM (VALUES (1), (2), (999)) AS outer_rel(id)
LEFT JOIN LATERAL (
    SELECT id
    FROM lagodb_connectors_regress.parquet_filter AS inner_rel
    WHERE inner_rel.id = outer_rel.id
    OFFSET 0
) AS inner_rel ON true
ORDER BY outer_rel.id;
RESET enable_hashjoin;
RESET enable_mergejoin;
RESET enable_material;
RESET enable_nestloop;

SELECT format('s3://%s/lagodb-connectors/parquet/drift/',
              :'lagodb_regress_bucket') AS drift_path,
       format('s3://%s/lagodb-connectors/parquet/drift/part-a.parquet',
              :'lagodb_regress_bucket') AS drift_a_path,
       format('s3://%s/lagodb-connectors/parquet/drift/part-b.parquet',
              :'lagodb_regress_bucket') AS drift_b_path,
       format('s3://%s/lagodb-connectors/parquet/unsupported.parquet',
              :'lagodb_regress_bucket') AS unsupported_path
\gset parquet_boundary_

COPY (
    SELECT id, text_col
    FROM lagodb_connectors_regress.common_source WHERE id = 1
) TO :'parquet_boundary_drift_a_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');
COPY (
    SELECT id, integer_col
    FROM lagodb_connectors_regress.common_source WHERE id = 2
) TO :'parquet_boundary_drift_b_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');
CREATE FOREIGN TABLE lagodb_connectors_regress.parquet_drift (
    id integer,
    text_col text
)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'parquet_boundary_drift_path', format 'parquet');

\set VERBOSITY sqlstate
SELECT count(*) FROM lagodb_connectors_regress.parquet_drift;
COPY lagodb_connectors_regress.json_source
TO :'parquet_boundary_unsupported_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');
\set VERBOSITY default
