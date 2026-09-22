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

CREATE TABLE lagodb_connectors_regress.parquet_copy_exact
    (:parquet_columns);
COPY lagodb_connectors_regress.parquet_copy_exact
FROM :'parquet_exact_path'
WITH (server 'lagodb_connectors_regress_s3');
CREATE TABLE lagodb_connectors_regress.parquet_copy_prefix
    (:parquet_columns);
COPY lagodb_connectors_regress.parquet_copy_prefix
FROM :'parquet_filter_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');
CREATE TABLE lagodb_connectors_regress.parquet_copy_zstd
    (:parquet_columns);
COPY lagodb_connectors_regress.parquet_copy_zstd
FROM :'parquet_zstd_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');
CREATE TABLE lagodb_connectors_regress.parquet_copy_empty
    (:parquet_columns);
COPY lagodb_connectors_regress.parquet_copy_empty
FROM :'parquet_empty_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');

SELECT relation, rows, round_trip
FROM (
    SELECT 'exact' AS relation, count(*) AS rows,
           array_agg(to_jsonb(value) ORDER BY to_jsonb(value)) =
               (SELECT array_agg(to_jsonb(source) ORDER BY to_jsonb(source))
                FROM lagodb_connectors_regress.parquet_source AS source)
               AS round_trip
    FROM lagodb_connectors_regress.parquet_copy_exact AS value
    UNION ALL
    SELECT 'prefix', count(*),
           array_agg(to_jsonb(value) ORDER BY to_jsonb(value)) =
               (SELECT array_agg(to_jsonb(source) ORDER BY to_jsonb(source))
                FROM lagodb_connectors_regress.parquet_source AS source)
    FROM lagodb_connectors_regress.parquet_copy_prefix AS value
    UNION ALL
    SELECT 'zstd', count(*),
           array_agg(to_jsonb(value) ORDER BY to_jsonb(value)) =
               (SELECT array_agg(to_jsonb(source) ORDER BY to_jsonb(source))
                FROM lagodb_connectors_regress.parquet_source AS source)
    FROM lagodb_connectors_regress.parquet_copy_zstd AS value
    UNION ALL
    SELECT 'empty', count(*), count(*) = 0
    FROM lagodb_connectors_regress.parquet_copy_empty
) AS results
ORDER BY relation;

-- Exact/prefix foreign scans and format-owned schema inference.
CREATE FOREIGN TABLE lagodb_connectors_regress.parquet_exact
    (:parquet_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'parquet_exact_path');
CREATE FOREIGN TABLE lagodb_connectors_regress.parquet_prefix
    (:parquet_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'parquet_filter_path', format 'parquet');
CREATE FOREIGN TABLE lagodb_connectors_regress.parquet_inferred ()
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'parquet_exact_path', format 'parquet');

SELECT relation, rows, matches_source
FROM (
    SELECT 'exact' AS relation, count(*) AS rows,
           array_agg(to_jsonb(value) ORDER BY to_jsonb(value)) =
               (SELECT array_agg(to_jsonb(source) ORDER BY to_jsonb(source))
                FROM lagodb_connectors_regress.parquet_source AS source)
               AS matches_source
    FROM lagodb_connectors_regress.parquet_exact AS value
    UNION ALL
    SELECT 'prefix', count(*),
           array_agg(to_jsonb(value) ORDER BY to_jsonb(value)) =
               (SELECT array_agg(to_jsonb(source) ORDER BY to_jsonb(source))
                FROM lagodb_connectors_regress.parquet_source AS source)
    FROM lagodb_connectors_regress.parquet_prefix AS value
) AS results
ORDER BY relation;
SELECT count(*) AS inferred_columns,
       string_agg(format_type(atttypid, atttypmod), ', ' ORDER BY attnum)
           AS inferred_types
FROM pg_attribute
WHERE attrelid = 'lagodb_connectors_regress.parquet_inferred'::regclass
  AND attnum > 0 AND NOT attisdropped;
SELECT count(*) AS inferred_rows
FROM lagodb_connectors_regress.parquet_inferred;

-- Prefix foreign INSERT is the writable Parquet FDW path.
CREATE FOREIGN TABLE lagodb_connectors_regress.parquet_write
    (:parquet_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'parquet_write_path', format 'parquet');
INSERT INTO lagodb_connectors_regress.parquet_write
SELECT * FROM lagodb_connectors_regress.parquet_source;
SELECT count(*) AS written_rows,
       array_agg(to_jsonb(value) ORDER BY to_jsonb(value)) =
           (SELECT array_agg(to_jsonb(source) ORDER BY to_jsonb(source))
            FROM lagodb_connectors_regress.parquet_source AS source)
           AS matches_source
FROM lagodb_connectors_regress.parquet_write AS value;

CREATE FOREIGN TABLE lagodb_connectors_regress.parquet_filter
    (:parquet_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'parquet_filter_path', format 'parquet');

CREATE TABLE lagodb_connectors_regress.null_parameter_source (id integer);
INSERT INTO lagodb_connectors_regress.null_parameter_source VALUES (NULL);

-- Mirrored and ordinary integer comparisons are both evaluated by Parquet.
SELECT coalesce(string_agg(id::text, ',' ORDER BY id), '<none>') AS ids
FROM lagodb_connectors_regress.parquet_filter
WHERE 1 < id AND bigint_col < 0::bigint;

-- The ForeignScan reports provider-accepted predicates separately from local
-- residual quals. This plan assertion verifies that filter pushdown occurred.
EXPLAIN (COSTS OFF)
SELECT id
FROM lagodb_connectors_regress.parquet_filter
WHERE id = 1;

-- Boolean comparison, NULL tests, AND, OR, and NOT retain PostgreSQL's
-- three-valued logic inside the Arrow predicate.
SELECT coalesce(
           string_agg(inner_rel.id::text, ',' ORDER BY inner_rel.id),
           '<none>'
       ) AS ids
FROM (VALUES (true)) AS outer_rel(flag)
CROSS JOIN LATERAL (
    SELECT id
    FROM lagodb_connectors_regress.parquet_filter
    WHERE (NOT (smallint_col IS NULL) AND bool_col = outer_rel.flag)
       OR (smallint_col IS NULL AND bool_col <> outer_rel.flag)
    OFFSET 0
) AS inner_rel;

-- Equality is exact for deterministic collations; ordering is restricted to
-- byte-order C/POSIX collations.
SELECT coalesce(string_agg(id::text, ',' ORDER BY id), '<none>') AS ids
FROM lagodb_connectors_regress.parquet_filter
WHERE varchar_col = 'varchar-one'
   OR text_col COLLATE "C" > 'z' COLLATE "C";

-- A NULL runtime value must remain UNKNOWN rather than becoming a value or a
-- provider error. PostgreSQL WHERE semantics therefore return no rows.
SELECT coalesce(
           string_agg(inner_rel.id::text, ',' ORDER BY inner_rel.id),
           '<none>'
       ) AS ids
FROM lagodb_connectors_regress.null_parameter_source AS outer_rel
CROSS JOIN LATERAL (
    SELECT id
    FROM lagodb_connectors_regress.parquet_filter
    WHERE id = outer_rel.id
    OFFSET 0
) AS inner_rel;

-- Metadata pruning normalizes NOT to leaf operators. NOT UNKNOWN must remain
-- UNKNOWN rather than becoming TRUE when the runtime parameter is NULL.
SELECT coalesce(
           string_agg(inner_rel.id::text, ',' ORDER BY inner_rel.id),
           '<none>'
       ) AS ids
FROM lagodb_connectors_regress.null_parameter_source AS outer_rel
CROSS JOIN LATERAL (
    SELECT id
    FROM lagodb_connectors_regress.parquet_filter
    WHERE NOT (id = outer_rel.id)
    OFFSET 0
) AS inner_rel;

ANALYZE lagodb_connectors_regress.parquet_filter;

-- ANALYZE scans the complete object set for the population and persists its
-- compressed byte size as relpages. The fixture contains exactly two rows.
SELECT reltuples::bigint AS reltuples, relpages > 0 AS has_pages
FROM pg_class
WHERE oid = 'lagodb_connectors_regress.parquet_filter'::regclass;

CREATE FUNCTION lagodb_connectors_regress.explain_json(query text)
RETURNS jsonb
LANGUAGE plpgsql
AS $$
DECLARE
    plan text;
BEGIN
    EXECUTE 'EXPLAIN (FORMAT JSON) ' || query INTO plan;
    RETURN plan::jsonb;
END
$$;

-- Representative plans pin every supported predicate capability exercised
-- above. Exact filters have no local residual; conservative pruning retains
-- the original PostgreSQL Filter by contract.
WITH explained AS (
    SELECT lagodb_connectors_regress.explain_json(
        $$SELECT id
          FROM lagodb_connectors_regress.parquet_filter
          WHERE 1 < id AND bigint_col < 0::bigint$$
    ) AS value
)
SELECT value::text LIKE '%Pushed Filter%'
   AND value::text LIKE '%id > 1%'
   AND value::text LIKE '%bigint_col <%'
   AND value::text LIKE '%::bigint%'
   AND value::text NOT LIKE '%"Filter":%'
       AS mirrored_and_pushdown_complete
FROM explained;

WITH explained AS (
    SELECT lagodb_connectors_regress.explain_json(
        $$SELECT inner_rel.id
          FROM (VALUES (true)) AS outer_rel(flag)
          CROSS JOIN LATERAL (
              SELECT id
              FROM lagodb_connectors_regress.parquet_filter
              WHERE (NOT (smallint_col IS NULL) AND bool_col = outer_rel.flag)
                 OR (smallint_col IS NULL AND bool_col <> outer_rel.flag)
              OFFSET 0
          ) AS inner_rel$$
    ) AS value
)
SELECT value::text LIKE '%Pushed Filter%'
   AND (value::text LIKE '%smallint_col IS NOT NULL%'
        OR value::text LIKE '%NOT (smallint_col IS NULL)%')
   AND value::text LIKE '%smallint_col IS NULL%'
   AND value::text LIKE '%bool_col%'
   AND value::text LIKE '%"Filter":%'
       AS boolean_null_logic_pushdown_complete
FROM explained;

WITH explained AS (
    SELECT lagodb_connectors_regress.explain_json(
        $$SELECT id
          FROM lagodb_connectors_regress.parquet_filter
          WHERE varchar_col = 'varchar-one'
             OR text_col COLLATE "C" > 'z' COLLATE "C"$$
    ) AS value
)
SELECT value::text LIKE '%Pushed Filter%'
   AND value::text LIKE '%varchar_col =%'
   AND value::text LIKE '%text_col >%'
   AND value::text NOT LIKE '%"Filter":%'
       AS string_collation_pushdown_complete
FROM explained;

WITH explained AS (
    SELECT lagodb_connectors_regress.explain_json(
        $$SELECT inner_rel.id
          FROM lagodb_connectors_regress.null_parameter_source AS outer_rel
          CROSS JOIN LATERAL (
              SELECT id
              FROM lagodb_connectors_regress.parquet_filter
              WHERE id = outer_rel.id
              OFFSET 0
          ) AS inner_rel$$
    ) AS value
)
SELECT value::text LIKE '%Pushed Filter%'
   AND value::text LIKE '%id = $1%'
   AND value::text NOT LIKE '%"Filter":%'
       AS null_parameter_pushdown_complete
FROM explained;

WITH explained AS (
    SELECT lagodb_connectors_regress.explain_json(
        $$SELECT inner_rel.id
          FROM lagodb_connectors_regress.null_parameter_source AS outer_rel
          CROSS JOIN LATERAL (
              SELECT id
              FROM lagodb_connectors_regress.parquet_filter
              WHERE NOT (id = outer_rel.id)
              OFFSET 0
          ) AS inner_rel$$
    ) AS value
)
SELECT value::text LIKE '%Pushed Filter%'
   AND value::text LIKE '%id%'
   AND value::text LIKE '%$1%'
   AND value::text NOT LIKE '%"Filter":%'
       AS null_parameter_not_pushdown_complete
FROM explained;

-- Unsupported arithmetic remains solely a PostgreSQL local residual.
WITH explained AS (
    SELECT lagodb_connectors_regress.explain_json(
        $$SELECT id
          FROM lagodb_connectors_regress.parquet_filter
          WHERE id + 1 = 2$$
    ) AS value
)
SELECT value::text NOT LIKE '%Pushed Filter%'
   AND value::text LIKE '%"Filter":%'
       AS unsupported_expression_remains_local
FROM explained;

-- A parameterized ForeignScan must use its persisted predicate description;
-- ExplainForeignScan has no ancestor list for deparsing PARAM_EXEC expressions.
WITH explained AS (
    SELECT lagodb_connectors_regress.explain_json(
        'SELECT inner_rel.id
         FROM generate_series(1, 2) AS outer_rel(id)
         CROSS JOIN LATERAL (
             SELECT id
             FROM lagodb_connectors_regress.parquet_filter
             WHERE id = outer_rel.id
             OFFSET 0
         ) AS inner_rel'
    ) AS value
)
SELECT value::text LIKE '%Pushed Filter%'
   AND value::text LIKE '%$1%'
       AS parameterized_explain_reports_pushdown
FROM explained;

-- The planner consumes persisted stats and charges provider startup/filter
-- work; none of the former fixed 1000/32/zero values remain.
WITH explained AS (
    SELECT lagodb_connectors_regress.explain_json(
        'SELECT * FROM lagodb_connectors_regress.parquet_filter WHERE id = 1'
    ) AS value
), plan AS (
    SELECT value -> 0 -> 'Plan' AS value FROM explained
)
SELECT ((value ->> 'Plan Rows')::integer <> 1000)
   AND ((value ->> 'Plan Width')::integer <> 32)
   AND ((value ->> 'Startup Cost')::double precision > 0)
       AS planner_uses_analyze_stats
FROM plan;

-- Object-key order and per-file row order do not establish a global ordering,
-- so a requested ORDER BY must retain PostgreSQL's Sort node.
WITH explained AS (
    SELECT lagodb_connectors_regress.explain_json(
        'SELECT id FROM lagodb_connectors_regress.parquet_filter ORDER BY id'
    ) AS value
)
SELECT (value -> 0 -> 'Plan' ->> 'Node Type') = 'Sort'
       AS planner_retains_sort
FROM explained;

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

SET client_min_messages = warning;
DROP TABLE IF EXISTS lagodb_connectors_regress.parquet_null_array_source;
RESET client_min_messages;
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

SET client_min_messages = warning;
DROP TABLE IF EXISTS lagodb_connectors_regress.parquet_null_array_sink;
RESET client_min_messages;
CREATE TABLE lagodb_connectors_regress.parquet_null_array_sink
    (:parquet_columns);
COPY lagodb_connectors_regress.parquet_null_array_sink
FROM :'array_null_elements_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');

SELECT bool_array[2] IS NULL AS bool_null,
       smallint_array[2] IS NULL AS smallint_null,
       integer_array[2] IS NULL AS integer_null,
       bigint_array[2] IS NULL AS bigint_null,
       real_array[2] IS NULL AS real_null,
       double_array[2] IS NULL AS double_null,
       text_array[2] IS NULL AS text_null,
       varchar_array[2] IS NULL AS varchar_null,
       bpchar_array[2] IS NULL AS bpchar_null,
       name_array[2] IS NULL AS name_null,
       json_array[2] IS NULL AS json_null
FROM lagodb_connectors_regress.parquet_null_array_sink;

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

-- A small native Parquet object makes projection and column-order failures
-- visible without hiding them in the complete type matrix.
COPY (
    SELECT id, bool_col, text_col
    FROM lagodb_connectors_regress.common_source
    ORDER BY id
) TO :'projection_reorder_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');

SET client_min_messages = warning;
DROP TABLE IF EXISTS lagodb_connectors_regress.scan_projection_sink;
RESET client_min_messages;
CREATE TABLE lagodb_connectors_regress.scan_projection_sink (
    id integer,
    text_col text
);
COPY lagodb_connectors_regress.scan_projection_sink (id, text_col)
FROM :'projection_reorder_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');

SET client_min_messages = warning;
DROP TABLE IF EXISTS lagodb_connectors_regress.scan_reordered_sink;
RESET client_min_messages;
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

SELECT relation, rows, digest
FROM (
    SELECT 'projection' AS relation,
           count(*) AS rows,
           md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id)) AS digest
    FROM lagodb_connectors_regress.scan_projection_sink AS value
    UNION ALL
    SELECT 'reordered', count(*),
           md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id))
    FROM lagodb_connectors_regress.scan_reordered_sink AS value
    UNION ALL
    SELECT 'source', count(*),
           md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id))
    FROM (
        SELECT id, bool_col, text_col
        FROM lagodb_connectors_regress.common_source
        ORDER BY id
    ) AS value
) AS mapping_results
ORDER BY relation;

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
