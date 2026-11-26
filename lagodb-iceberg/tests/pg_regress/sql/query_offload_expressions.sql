-- query_offload_expressions.sql
-- Focused query-offload expression and PostgreSQL-fallback coverage.

DROP EXTENSION IF EXISTS lagodb_iceberg CASCADE;
CREATE EXTENSION IF NOT EXISTS lagodb_iceberg;

SET lagodb.query_batch_rows = 2;

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

SET lagodb.query_offload_mode = 'off';
SELECT count(*) AS rows
FROM query_offload_expression_basic
WHERE (id >= 2 AND lower(value_text) = 'alpha')
   OR (id <= 1 AND upper(value_text) = 'ALPHA');

-- A volatile subtree blocks pruning of the complete OR clause. Forced serial
-- query offload still evaluates the complete expression through PostgreSQL.
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

SET lagodb.query_offload_mode = 'off';
SELECT count(*) AS rows
FROM query_offload_expression_basic
WHERE (id >= 2 AND random() >= 0)
   OR id <= 1;

-- A PG-only exact filter remains force-offloadable without a pushed filter.
SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT count(*) AS rows
FROM query_offload_expression_basic
WHERE lower(value_text) = 'alpha';

SELECT count(*) AS rows
FROM query_offload_expression_basic
WHERE lower(value_text) = 'alpha';

SET lagodb.query_offload_mode = 'off';
SELECT count(*) AS rows
FROM query_offload_expression_basic
WHERE lower(value_text) = 'alpha';

-- Per-row PostgreSQL fallback remains isolated from auto mode until a measured
-- fallback cost model justifies selecting it.
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

SET lagodb.query_offload_mode = 'off';
SELECT count(*) FILTER (WHERE id > ANY (ARRAY[2, 5]::integer[])) AS any_gt_rows,
       count(*) FILTER (
           WHERE CASE id WHEN 1 THEN true WHEN 2 THEN false ELSE true END
       ) AS simple_case_rows
FROM query_offload_expression_basic;

-- Native division by zero in non-selected branches proves lazy CASE/COALESCE
-- evaluation, as in PostgreSQL.
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

SET lagodb.query_offload_mode = 'off';
SELECT count(*) FILTER (
           WHERE CASE WHEN id >= 0 THEN true ELSE 1 / (id - id) > 0 END
       ) AS lazy_case_rows,
       sum(coalesce(value_i4, 1 / (id - id))) AS lazy_coalesce_sum
FROM query_offload_expression_basic
WHERE value_i4 IS NOT NULL;

-- One matrix covers every initial fallback converter family, including NULLs.
-- Unary minus keeps the integer and float4 entries in PostgreSQL.
SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT sum((-value_i2)::smallint) AS udf_i2,
       sum(-value_i4) AS udf_i4,
       sum(-value_i8) AS udf_i8,
       sum(-value_f4) AS udf_f4,
       sum(degrees(value_f8)) AS udf_f8,
       bool_and(value_bool IS NOT FALSE) AS udf_bool,
       string_agg(lower(value_text), ',' ORDER BY id) AS udf_text,
       count(lower(value_text)::varchar) AS udf_varchar,
       count(lower(value_text)::name) AS udf_name
FROM query_offload_expression_basic;

SELECT sum((-value_i2)::smallint) AS udf_i2,
       sum(-value_i4) AS udf_i4,
       sum(-value_i8) AS udf_i8,
       sum(-value_f4) AS udf_f4,
       sum(degrees(value_f8)) AS udf_f8,
       bool_and(value_bool IS NOT FALSE) AS udf_bool,
       string_agg(lower(value_text), ',' ORDER BY id) AS udf_text,
       count(lower(value_text)::varchar) AS udf_varchar,
       count(lower(value_text)::name) AS udf_name
FROM query_offload_expression_basic;

SET lagodb.query_offload_mode = 'off';
SELECT sum((-value_i2)::smallint) AS udf_i2,
       sum(-value_i4) AS udf_i4,
       sum(-value_i8) AS udf_i8,
       sum(-value_f4) AS udf_f4,
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

SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT count(*) FILTER (WHERE abs(value_i2) >= 0::smallint)
FROM query_offload_expression_abs_edge;

SELECT count(*) FILTER (WHERE abs(value_i2) >= 0::smallint)
FROM query_offload_expression_abs_edge;

SET lagodb.query_offload_mode = 'off';
SELECT count(*) FILTER (WHERE abs(value_i2) >= 0::smallint)
FROM query_offload_expression_abs_edge;

DROP TABLE query_offload_expression_abs_edge;

-- Integer and floating-point arithmetic runs in the engine in grouping keys,
-- filters and aggregate arguments, so auto mode keeps the offload path. The
-- query crosses widths, constants on either side, NULL operands, mod(), the
-- int4 cast PostgreSQL places over an int2 expression for `%` and COALESCE,
-- and the float casts of integer operands.
SET lagodb.query_offload_mode = 'auto';
SET lagodb.query_offload_setup_cost = 0;
SET lagodb.query_offload_cpu_cost_multiplier = 0;
EXPLAIN (COSTS OFF)
SELECT id % 2 AS parity,
       sum(2::smallint * value_i2 - value_i2 / 2::smallint) AS smallint_ops,
       sum(value_i2 * value_i4 - value_i8 / 3) AS mixed_widths,
       sum((value_i2 + value_i2) % value_i4) AS widened_modulo,
       sum(coalesce(value_i2 + 1::smallint, value_i4)) AS widened_coalesce,
       sum(mod(value_i8, 7) + (100 - value_i4)) AS mod_function,
       sum(value_i4 * value_f8 + value_f4) AS float8_mixed,
       sum(value_f4 / 2::real) AS float4_divide
FROM query_offload_expression_basic
WHERE id * 2 <= 8
GROUP BY id % 2
ORDER BY parity;

SELECT id % 2 AS parity,
       sum(2::smallint * value_i2 - value_i2 / 2::smallint) AS smallint_ops,
       sum(value_i2 * value_i4 - value_i8 / 3) AS mixed_widths,
       sum((value_i2 + value_i2) % value_i4) AS widened_modulo,
       sum(coalesce(value_i2 + 1::smallint, value_i4)) AS widened_coalesce,
       sum(mod(value_i8, 7) + (100 - value_i4)) AS mod_function,
       sum(value_i4 * value_f8 + value_f4) AS float8_mixed,
       sum(value_f4 / 2::real) AS float4_divide
FROM query_offload_expression_basic
WHERE id * 2 <= 8
GROUP BY id % 2
ORDER BY parity;

RESET lagodb.query_offload_setup_cost;
RESET lagodb.query_offload_cpu_cost_multiplier;

SET lagodb.query_offload_mode = 'off';
SELECT id % 2 AS parity,
       sum(2::smallint * value_i2 - value_i2 / 2::smallint) AS smallint_ops,
       sum(value_i2 * value_i4 - value_i8 / 3) AS mixed_widths,
       sum((value_i2 + value_i2) % value_i4) AS widened_modulo,
       sum(coalesce(value_i2 + 1::smallint, value_i4)) AS widened_coalesce,
       sum(mod(value_i8, 7) + (100 - value_i4)) AS mod_function,
       sum(value_i4 * value_f8 + value_f4) AS float8_mixed,
       sum(value_f4 / 2::real) AS float4_divide
FROM query_offload_expression_basic
WHERE id * 2 <= 8
GROUP BY id % 2
ORDER BY parity;

-- PostgreSQL folds a NULL constant operand away, but a NULL parameter of a
-- generic plan reaches the int2 kernel as a NULL scalar on either side.
SET lagodb.query_offload_mode = 'force';
SET plan_cache_mode = force_generic_plan;
PREPARE query_offload_expression_null_operand (smallint) AS
SELECT count(value_i2 + $1) AS right_operand,
       count($1 - value_i2) AS left_operand
FROM query_offload_expression_basic;
EXPLAIN (COSTS OFF) EXECUTE query_offload_expression_null_operand(NULL);
EXECUTE query_offload_expression_null_operand(NULL);
EXECUTE query_offload_expression_null_operand(1);
DEALLOCATE query_offload_expression_null_operand;
RESET plan_cache_mode;

-- Integer errors match PostgreSQL: int2 results are checked in the int2
-- range, MIN / -1 overflows, MIN % -1 is 0, and division by zero raises.
-- Float arithmetic follows IEEE: overflow gives Infinity, underflow 0 and
-- division by zero Infinity, where PostgreSQL raises.
CREATE TABLE query_offload_expression_arithmetic_edge (
    value_i2 smallint,
    value_i4 integer,
    value_i8 bigint,
    value_f8 double precision
) USING iceberg;

INSERT INTO query_offload_expression_arithmetic_edge VALUES
    ('-32768'::smallint, '-2147483648'::integer,
     '-9223372036854775808'::bigint, 1e308);

SET lagodb.query_offload_mode = 'force';
SELECT sum(value_i2 % '-1'::smallint) AS i2_modulo,
       sum(value_i4 % -1) AS i4_modulo,
       sum(value_i8 % -1) AS i8_modulo
FROM query_offload_expression_arithmetic_edge;

SELECT sum(value_i2 * 2::smallint) FROM query_offload_expression_arithmetic_edge;
SELECT sum(value_i2 / '-1'::smallint) FROM query_offload_expression_arithmetic_edge;
SELECT sum(value_i2 / (value_i2 - value_i2))
FROM query_offload_expression_arithmetic_edge;
SELECT sum(value_i4 - 1) FROM query_offload_expression_arithmetic_edge;
SELECT sum(value_i8 / -1) FROM query_offload_expression_arithmetic_edge;
SELECT sum(value_i4 / (value_i4 - value_i4))
FROM query_offload_expression_arithmetic_edge;

SELECT max(value_f8 * 10) AS overflow,
       max(1 / value_f8 / value_f8) AS underflow,
       max(value_f8 / (value_f8 - value_f8)) AS divide_by_zero
FROM query_offload_expression_arithmetic_edge;

DROP TABLE query_offload_expression_arithmetic_edge;

CREATE TABLE query_offload_expression_numeric (
    id integer,
    qty integer,
    price numeric(15, 2),
    discount numeric(15, 2),
    tax numeric(15, 2),
    wide numeric(38, 0)
) USING iceberg;

INSERT INTO query_offload_expression_numeric VALUES
    (1, 2, 1234.56, 0.05, 0.08, 1e37),
    (2, 3, 99.99, 0.10, 0.00, NULL),
    (3, NULL, NULL, NULL, NULL, NULL),
    (4, 1, -10.00, 0.00, 0.04, NULL);

-- +, - and * keep PostgreSQL's result scales. The three-factor product has
-- 47 digits and runs as Decimal256; CASE and COALESCE take the common type of
-- their branches.
SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT sum(price * (1 - discount)) AS revenue,
       sum(price * (1 - discount) * (1 + tax)) AS charge,
       sum(price + qty) AS price_plus_qty,
       sum(CASE WHEN qty >= 2 THEN price ELSE coalesce(discount, 0) END)
           AS conditional
FROM query_offload_expression_numeric;

SELECT sum(price * (1 - discount)) AS revenue,
       sum(price * (1 - discount) * (1 + tax)) AS charge,
       sum(price + qty) AS price_plus_qty,
       sum(CASE WHEN qty >= 2 THEN price ELSE coalesce(discount, 0) END)
           AS conditional
FROM query_offload_expression_numeric;

-- A product that may exceed 76 digits stays in PostgreSQL.
SELECT sum(wide * wide * wide) AS cubed
FROM query_offload_expression_numeric;

-- Comparisons run in the engine: a constant is bound exactly in the other
-- operand's fixed type, and operands of two fixed types compare in their
-- lossless common type.
EXPLAIN (COSTS OFF)
SELECT count(*) FILTER (WHERE price * (1 - discount) >= 1172.83) AS bound,
       count(*) FILTER (WHERE price < wide) AS mixed,
       count(*) FILTER (WHERE price * (1 - discount) > tax * 10000) AS derived
FROM query_offload_expression_numeric;

SELECT count(*) FILTER (WHERE price * (1 - discount) >= 1172.83) AS bound,
       count(*) FILTER (WHERE price < wide) AS mixed,
       count(*) FILTER (WHERE price * (1 - discount) > tax * 10000) AS derived
FROM query_offload_expression_numeric;

-- A constant finer than the other operand's scale stays in PostgreSQL.
EXPLAIN (COSTS OFF)
SELECT count(*) FILTER (WHERE price + tax >= 100.005)
FROM query_offload_expression_numeric;

DROP TABLE query_offload_expression_numeric;
DROP TABLE query_offload_expression_basic;

RESET lagodb.query_batch_rows;
RESET lagodb.query_offload_mode;
RESET lagodb.customscan_mode;
