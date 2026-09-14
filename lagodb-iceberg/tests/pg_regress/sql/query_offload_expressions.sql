-- query_offload_expressions.sql
-- Focused query-offload expression and PostgreSQL-fallback coverage.

DROP EXTENSION IF EXISTS lagodb_iceberg CASCADE;
CREATE EXTENSION IF NOT EXISTS lagodb_iceberg;

SET lagodb.query_batch_rows = 2;
SET timezone = 'UTC';

CREATE TABLE query_offload_expression_basic (
    id integer,
    value_i2 smallint,
    value_i4 integer,
    value_i8 bigint,
    value_f4 real,
    value_f8 double precision,
    value_bool boolean,
    value_text text,
    value_timestamp timestamp
) USING iceberg;

INSERT INTO query_offload_expression_basic VALUES
    (1,  1,  10,  100,  1.0,  10.0, true,  'alpha',
     TIMESTAMP '2024-01-01 10:00:00'),
    (2,  2,  20,  200,  2.0,  20.0, false, 'beta',
     TIMESTAMP '2024-01-02 11:00:00'),
    (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
    (4, -2, -20, -200, -2.0, -20.0, true, 'alpha',
     TIMESTAMP '2023-12-31 09:00:00'),
    (5,  3,  30,  300,  3.0,  30.0, false, 'gamma',
     TIMESTAMP '2024-01-03 12:00:00');

-- ============================================================================
-- Provider pruning and exact PostgreSQL fallback remain independent.
-- ============================================================================

-- Each OR branch contributes its safe integer conjunct to provider pruning;
-- the exact residual retains both PostgreSQL text expressions.
SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'force';
EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(*) AS rows
FROM query_offload_expression_basic
WHERE (id >= 2 AND lower(value_text) = 'alpha')
   OR (id <= 1 AND upper(value_text) = 'ALPHA');

SELECT count(*) AS rows
FROM query_offload_expression_basic
WHERE (id >= 2 AND lower(value_text) = 'alpha')
   OR (id <= 1 AND upper(value_text) = 'ALPHA');

SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'off';
SELECT count(*) AS rows
FROM query_offload_expression_basic
WHERE (id >= 2 AND lower(value_text) = 'alpha')
   OR (id <= 1 AND upper(value_text) = 'ALPHA');

-- A volatile subtree blocks pruning of the complete OR clause. Forced serial
-- query offload still evaluates the complete expression through PostgreSQL.
SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT count(*) AS rows
FROM query_offload_expression_basic
WHERE (id >= 2 AND random() >= 0)
   OR id <= 1;

SELECT count(*) AS rows
FROM query_offload_expression_basic
WHERE (id >= 2 AND random() >= 0)
   OR id <= 1;

SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'off';
SELECT count(*) AS rows
FROM query_offload_expression_basic
WHERE (id >= 2 AND random() >= 0)
   OR id <= 1;

-- A PG-only exact filter remains force-offloadable without a pushed filter.
SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT count(*) AS rows
FROM query_offload_expression_basic
WHERE lower(value_text) = 'alpha';

SELECT count(*) AS rows
FROM query_offload_expression_basic
WHERE lower(value_text) = 'alpha';

SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'off';
SELECT count(*) AS rows
FROM query_offload_expression_basic
WHERE lower(value_text) = 'alpha';

-- Per-row PostgreSQL fallback remains isolated from auto mode until a measured
-- fallback cost model justifies selecting it.
SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'auto';
EXPLAIN (COSTS OFF)
SELECT count(*) AS rows
FROM query_offload_expression_basic
WHERE lower(value_text) = 'alpha';

-- ============================================================================
-- Representative native and PostgreSQL-fallback expression shapes.
-- ============================================================================

-- This compact matrix crosses BooleanTest, searched CASE, COALESCE, NULLIF,
-- native scalar-array comparison, text and numeric scalar functions.
SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT count(*) FILTER (WHERE value_bool IS NOT FALSE) AS boolean_test_rows,
       count(*) FILTER (
           WHERE CASE WHEN id <= 3 THEN true ELSE false END
       ) AS searched_case_rows,
       sum(coalesce(value_i4, 0)) AS coalesce_sum,
       count(*) FILTER (WHERE nullif(id, 3) IS NOT NULL) AS nullif_rows,
       count(*) FILTER (
           WHERE id = ANY (ARRAY[1, 3, NULL]::integer[])
       ) AS any_rows,
       count(*) FILTER (WHERE starts_with(value_text, 'a')) AS prefix_rows,
       count(replace(value_text, 'a', 'A')) AS replaced_rows,
       sum(abs(value_i8)) AS abs_i8_sum,
       sum(abs(value_f8)) AS abs_f8_sum
FROM query_offload_expression_basic;

SELECT count(*) FILTER (WHERE value_bool IS NOT FALSE) AS boolean_test_rows,
       count(*) FILTER (
           WHERE CASE WHEN id <= 3 THEN true ELSE false END
       ) AS searched_case_rows,
       sum(coalesce(value_i4, 0)) AS coalesce_sum,
       count(*) FILTER (WHERE nullif(id, 3) IS NOT NULL) AS nullif_rows,
       count(*) FILTER (
           WHERE id = ANY (ARRAY[1, 3, NULL]::integer[])
       ) AS any_rows,
       count(*) FILTER (WHERE starts_with(value_text, 'a')) AS prefix_rows,
       count(replace(value_text, 'a', 'A')) AS replaced_rows,
       sum(abs(value_i8)) AS abs_i8_sum,
       sum(abs(value_f8)) AS abs_f8_sum
FROM query_offload_expression_basic;

SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'off';
SELECT count(*) FILTER (WHERE value_bool IS NOT FALSE) AS boolean_test_rows,
       count(*) FILTER (
           WHERE CASE WHEN id <= 3 THEN true ELSE false END
       ) AS searched_case_rows,
       sum(coalesce(value_i4, 0)) AS coalesce_sum,
       count(*) FILTER (WHERE nullif(id, 3) IS NOT NULL) AS nullif_rows,
       count(*) FILTER (
           WHERE id = ANY (ARRAY[1, 3, NULL]::integer[])
       ) AS any_rows,
       count(*) FILTER (WHERE starts_with(value_text, 'a')) AS prefix_rows,
       count(replace(value_text, 'a', 'A')) AS replaced_rows,
       sum(abs(value_i8)) AS abs_i8_sum,
       sum(abs(value_f8)) AS abs_f8_sum
FROM query_offload_expression_basic;

-- `> ANY` and simple CASE are complete PostgreSQL fallback expressions.
SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT count(*) FILTER (WHERE id > ANY (ARRAY[2, 5]::integer[])) AS any_gt_rows,
       count(*) FILTER (
           WHERE CASE id WHEN 1 THEN true WHEN 2 THEN false ELSE true END
       ) AS simple_case_rows
FROM query_offload_expression_basic;

SELECT count(*) FILTER (WHERE id > ANY (ARRAY[2, 5]::integer[])) AS any_gt_rows,
       count(*) FILTER (
           WHERE CASE id WHEN 1 THEN true WHEN 2 THEN false ELSE true END
       ) AS simple_case_rows
FROM query_offload_expression_basic;

SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'off';
SELECT count(*) FILTER (WHERE id > ANY (ARRAY[2, 5]::integer[])) AS any_gt_rows,
       count(*) FILTER (
           WHERE CASE id WHEN 1 THEN true WHEN 2 THEN false ELSE true END
       ) AS simple_case_rows
FROM query_offload_expression_basic;

-- Non-selected error branches prove PostgreSQL lazy CASE/COALESCE evaluation.
SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT count(*) FILTER (
           WHERE CASE WHEN id >= 0 THEN true ELSE 1 / (id - id) > 0 END
       ) AS lazy_case_rows,
       sum(coalesce(value_i4, 1 / (id - id))) AS lazy_coalesce_sum
FROM query_offload_expression_basic
WHERE value_i4 IS NOT NULL;

SELECT count(*) FILTER (
           WHERE CASE WHEN id >= 0 THEN true ELSE 1 / (id - id) > 0 END
       ) AS lazy_case_rows,
       sum(coalesce(value_i4, 1 / (id - id))) AS lazy_coalesce_sum
FROM query_offload_expression_basic
WHERE value_i4 IS NOT NULL;

SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'off';
SELECT count(*) FILTER (
           WHERE CASE WHEN id >= 0 THEN true ELSE 1 / (id - id) > 0 END
       ) AS lazy_case_rows,
       sum(coalesce(value_i4, 1 / (id - id))) AS lazy_coalesce_sum
FROM query_offload_expression_basic
WHERE value_i4 IS NOT NULL;

-- One matrix covers every initial fallback converter family, including NULLs.
SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT sum((-value_i2)::smallint) AS udf_i2,
       sum(mod(value_i4, 7)) AS udf_i4,
       sum(mod(value_i8, 7::bigint)) AS udf_i8,
       sum(value_f4 + 0::real) AS udf_f4,
       sum(degrees(value_f8)) AS udf_f8,
       bool_and(value_bool IS NOT FALSE) AS udf_bool,
       string_agg(lower(value_text), ',' ORDER BY id) AS udf_text,
       count(lower(value_text)::varchar) AS udf_varchar,
       count(lower(value_text)::name) AS udf_name
FROM query_offload_expression_basic;

SELECT sum((-value_i2)::smallint) AS udf_i2,
       sum(mod(value_i4, 7)) AS udf_i4,
       sum(mod(value_i8, 7::bigint)) AS udf_i8,
       sum(value_f4 + 0::real) AS udf_f4,
       sum(degrees(value_f8)) AS udf_f8,
       bool_and(value_bool IS NOT FALSE) AS udf_bool,
       string_agg(lower(value_text), ',' ORDER BY id) AS udf_text,
       count(lower(value_text)::varchar) AS udf_varchar,
       count(lower(value_text)::name) AS udf_name
FROM query_offload_expression_basic;

SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'off';
SELECT sum((-value_i2)::smallint) AS udf_i2,
       sum(mod(value_i4, 7)) AS udf_i4,
       sum(mod(value_i8, 7::bigint)) AS udf_i8,
       sum(value_f4 + 0::real) AS udf_f4,
       sum(degrees(value_f8)) AS udf_f8,
       bool_and(value_bool IS NOT FALSE) AS udf_bool,
       string_agg(lower(value_text), ',' ORDER BY id) AS udf_text,
       count(lower(value_text)::varchar) AS udf_varchar,
       count(lower(value_text)::name) AS udf_name
FROM query_offload_expression_basic;

-- ============================================================================
-- Compact capability boundary.
-- ============================================================================

-- A fallback result outside the initial converter matrix declines the complete
-- query-offload path even in force mode.
SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT count(date_trunc('day', value_timestamp))
FROM query_offload_expression_basic;

SELECT count(date_trunc('day', value_timestamp))
FROM query_offload_expression_basic;

SET lagodb.query_offload_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT count(date_trunc('day', value_timestamp))
FROM query_offload_expression_basic;

SELECT count(date_trunc('day', value_timestamp))
FROM query_offload_expression_basic;

-- PostgreSQL integer ABS raises 22003 at the declared int2 minimum. This one
-- case locks the widening adapter without repeating the same negative for all
-- integer widths in SQL regression.
CREATE TABLE query_offload_expression_abs_edge (
    value_i2 smallint
) USING iceberg;

INSERT INTO query_offload_expression_abs_edge VALUES ('-32768'::smallint);

SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT count(*) FILTER (WHERE abs(value_i2) >= 0::smallint)
FROM query_offload_expression_abs_edge;

SELECT count(*) FILTER (WHERE abs(value_i2) >= 0::smallint)
FROM query_offload_expression_abs_edge;

SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'off';
SELECT count(*) FILTER (WHERE abs(value_i2) >= 0::smallint)
FROM query_offload_expression_abs_edge;

DROP TABLE query_offload_expression_abs_edge;
DROP TABLE query_offload_expression_basic;

RESET timezone;
RESET lagodb.query_batch_rows;
RESET lagodb.query_offload_mode;
RESET lagodb.customscan_mode;
