-- query_offload_aggregates.sql
-- Basic single-relation S2/S3 grouping, aggregate, FILTER, and HAVING coverage.

DROP EXTENSION IF EXISTS lagodb_iceberg CASCADE;
CREATE EXTENSION IF NOT EXISTS lagodb_iceberg;

-- A tiny batch limit makes every positive query consume more than one Arrow
-- batch without requiring a large fixture. Each test case sets both planner
-- modes locally instead of depending on session state from an earlier case.
SET lagodb.query_batch_rows = 2;
SET timezone = 'UTC';

CREATE TABLE query_offload_aggregate_basic (
    id integer,
    group_i4 integer,
    group_i8 bigint,
    value_i2 smallint,
    value_i4 integer,
    value_i8 bigint,
    value_f4 real,
    value_f8 double precision,
    value_numeric numeric(12, 2),
    value_bool boolean,
    value_date date,
    value_time time,
    value_timestamp timestamp,
    value_timestamptz timestamptz,
    value_text text COLLATE "C"
) USING iceberg;

INSERT INTO query_offload_aggregate_basic VALUES
    (1, 1, 10,  1,  10,  100,  1.0,  10.0,  1.25, true,
     DATE '2024-01-01', TIME '10:00:00',
     TIMESTAMP '2024-01-01 10:00:00',
     TIMESTAMPTZ '2024-01-01 10:00:00+00', 'alpha'),
    (2, 1, 10,  2,  20,  200,  2.0,  20.0,  2.50, false,
     DATE '2024-01-02', TIME '11:00:00',
     TIMESTAMP '2024-01-02 11:00:00',
     TIMESTAMPTZ '2024-01-02 11:00:00+00', 'beta'),
    (3, 2, 20, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
     NULL, NULL, NULL, NULL, NULL),
    (4, 2, 20, -2, -20, -200, -2.0, -20.0, -2.50, true,
     DATE '2023-12-31', TIME '09:00:00',
     TIMESTAMP '2023-12-31 09:00:00',
     TIMESTAMPTZ '2023-12-31 09:00:00+00', 'alpha'),
    (5, 2, 30,  3,  30,  300,  3.0,  30.0,  3.75, false,
     DATE '2024-01-03', TIME '12:00:00',
     TIMESTAMP '2024-01-03 12:00:00',
     TIMESTAMPTZ '2024-01-03 12:00:00+00', 'gamma'),
    (6, 3, 30, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
     NULL, NULL, NULL, NULL, NULL);

-- ============================================================================
-- GUC ownership: either path family remains usable while the other is off.
-- ============================================================================

-- Relation CustomScan only.
SET lagodb.customscan_mode = 'force';
SET lagodb.query_offload_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT count(*)
FROM query_offload_aggregate_basic
WHERE id = 1;

-- Query offload only.
SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT count(*)
FROM query_offload_aggregate_basic
WHERE id = 1;

-- ============================================================================
-- S2: unfiltered and filtered grouped aggregates, pure GROUP BY, COUNT, and
-- integer MIN/MAX. The filtered EXPLAIN must show both the exact `Filter`
-- evaluated by DataFusion and the provider-accepted `Pushed Filter` used
-- for Iceberg storage pruning.
-- ============================================================================

-- Query offload plan and result.
SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT group_i4,
       group_i8,
       count(*) AS rows,
       count(value_i4) AS nonnull_i4,
       min(value_i4) AS min_i4,
       max(value_i4) AS max_i4,
       min(value_i8) AS min_i8,
       max(value_i8) AS max_i8
FROM query_offload_aggregate_basic
GROUP BY group_i4, group_i8
ORDER BY group_i4, group_i8;

SELECT group_i4,
       group_i8,
       count(*) AS rows,
       count(value_i4) AS nonnull_i4,
       min(value_i4) AS min_i4,
       max(value_i4) AS max_i4,
       min(value_i8) AS min_i8,
       max(value_i8) AS max_i8
FROM query_offload_aggregate_basic
GROUP BY group_i4, group_i8
ORDER BY group_i4, group_i8;

-- PostgreSQL native result.
SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'off';
SELECT group_i4,
       group_i8,
       count(*) AS rows,
       count(value_i4) AS nonnull_i4,
       min(value_i4) AS min_i4,
       max(value_i4) AS max_i4,
       min(value_i8) AS min_i8,
       max(value_i8) AS max_i8
FROM query_offload_aggregate_basic
GROUP BY group_i4, group_i8
ORDER BY group_i4, group_i8;

-- Query offload filtered plan and result.
SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT group_i4,
       count(*) AS rows,
       min(value_i4) AS min_i4,
       max(value_i4) AS max_i4
FROM query_offload_aggregate_basic
WHERE id >= 2
GROUP BY group_i4
ORDER BY group_i4;

SELECT group_i4,
       count(*) AS rows,
       min(value_i4) AS min_i4,
       max(value_i4) AS max_i4
FROM query_offload_aggregate_basic
WHERE id >= 2
GROUP BY group_i4
ORDER BY group_i4;

-- PostgreSQL native filtered result.
SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'off';
SELECT group_i4,
       count(*) AS rows,
       min(value_i4) AS min_i4,
       max(value_i4) AS max_i4
FROM query_offload_aggregate_basic
WHERE id >= 2
GROUP BY group_i4
ORDER BY group_i4;

-- Query offload plan and result.
SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT group_i4, group_i8
FROM query_offload_aggregate_basic
GROUP BY group_i4, group_i8
ORDER BY group_i4, group_i8;

SELECT group_i4, group_i8
FROM query_offload_aggregate_basic
GROUP BY group_i4, group_i8
ORDER BY group_i4, group_i8;

-- PostgreSQL native result.
SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'off';
SELECT group_i4, group_i8
FROM query_offload_aggregate_basic
GROUP BY group_i4, group_i8
ORDER BY group_i4, group_i8;

-- ============================================================================
-- S3: integer aggregates, aggregate FILTER, and HAVING.
-- Cast AVG results to a fixed scale so the accepted native-Float64 transition
-- contract is compared over values with a stable PostgreSQL representation.
-- ============================================================================

-- Query offload plan and result.
SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT group_i4,
       count(*) FILTER (WHERE id >= 2) AS filtered_rows,
       sum(value_i4) FILTER (WHERE id >= 2) AS filtered_sum_i4,
       sum(value_i2) AS sum_i2,
       sum(value_i4) AS sum_i4,
       sum(value_i8) AS sum_i8,
       avg(value_i2)::numeric(20, 4) AS avg_i2,
       avg(value_i4)::numeric(20, 4) AS avg_i4,
       avg(value_i8)::numeric(20, 4) AS avg_i8
FROM query_offload_aggregate_basic
GROUP BY group_i4
HAVING count(*) >= 2
ORDER BY group_i4;

SELECT group_i4,
       count(*) FILTER (WHERE id >= 2) AS filtered_rows,
       sum(value_i4) FILTER (WHERE id >= 2) AS filtered_sum_i4,
       sum(value_i2) AS sum_i2,
       sum(value_i4) AS sum_i4,
       sum(value_i8) AS sum_i8,
       avg(value_i2)::numeric(20, 4) AS avg_i2,
       avg(value_i4)::numeric(20, 4) AS avg_i4,
       avg(value_i8)::numeric(20, 4) AS avg_i8
FROM query_offload_aggregate_basic
GROUP BY group_i4
HAVING count(*) >= 2
ORDER BY group_i4;

-- PostgreSQL native result.
SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'off';
SELECT group_i4,
       count(*) FILTER (WHERE id >= 2) AS filtered_rows,
       sum(value_i4) FILTER (WHERE id >= 2) AS filtered_sum_i4,
       sum(value_i2) AS sum_i2,
       sum(value_i4) AS sum_i4,
       sum(value_i8) AS sum_i8,
       avg(value_i2)::numeric(20, 4) AS avg_i2,
       avg(value_i4)::numeric(20, 4) AS avg_i4,
       avg(value_i8)::numeric(20, 4) AS avg_i8
FROM query_offload_aggregate_basic
GROUP BY group_i4
HAVING count(*) >= 2
ORDER BY group_i4;

-- HAVING over integer SUM and integer AVG exercises the Int64 and Float64
-- physical result domains used by the query engine.
-- Query offload plan and result.
SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT group_i4,
       sum(value_i4) AS sum_i4,
       avg(value_i4)::numeric(20, 4) AS avg_i4
FROM query_offload_aggregate_basic
GROUP BY group_i4
HAVING sum(value_i4) >= 0 AND avg(value_i4) >= 10
ORDER BY group_i4;

SELECT group_i4,
       sum(value_i4) AS sum_i4,
       avg(value_i4)::numeric(20, 4) AS avg_i4
FROM query_offload_aggregate_basic
GROUP BY group_i4
HAVING sum(value_i4) >= 0 AND avg(value_i4) >= 10
ORDER BY group_i4;

-- PostgreSQL native result.
SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'off';
SELECT group_i4,
       sum(value_i4) AS sum_i4,
       avg(value_i4)::numeric(20, 4) AS avg_i4
FROM query_offload_aggregate_basic
GROUP BY group_i4
HAVING sum(value_i4) >= 0 AND avg(value_i4) >= 10
ORDER BY group_i4;

-- ============================================================================
-- S3: bounded NUMERIC aggregates.
-- ============================================================================

-- Query offload plan and result.
SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT min(value_numeric) AS min_numeric,
       max(value_numeric) AS max_numeric,
       sum(value_numeric)::numeric(20, 2) AS sum_numeric,
       avg(value_numeric)::numeric(20, 4) AS avg_numeric
FROM query_offload_aggregate_basic;

SELECT min(value_numeric) AS min_numeric,
       max(value_numeric) AS max_numeric,
       sum(value_numeric)::numeric(20, 2) AS sum_numeric,
       avg(value_numeric)::numeric(20, 4) AS avg_numeric
FROM query_offload_aggregate_basic;

-- PostgreSQL native result.
SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'off';
SELECT min(value_numeric) AS min_numeric,
       max(value_numeric) AS max_numeric,
       sum(value_numeric)::numeric(20, 2) AS sum_numeric,
       avg(value_numeric)::numeric(20, 4) AS avg_numeric
FROM query_offload_aggregate_basic;

-- ============================================================================
-- S3: float, temporal, boolean, ARRAY_AGG, and STRING_AGG families.
-- Only finite exactly representable float inputs are used by this basic
-- parity test; NaN and signed-zero behavior belongs to the documented
-- capability-boundary suite.
-- ============================================================================

-- Query offload plan and result.
SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT min(value_f4) AS min_f4,
       max(value_f4) AS max_f4,
       sum(value_f4)::numeric(20, 4) AS sum_f4,
       avg(value_f4)::numeric(20, 4) AS avg_f4,
       min(value_f8) AS min_f8,
       max(value_f8) AS max_f8,
       sum(value_f8)::numeric(20, 4) AS sum_f8,
       avg(value_f8)::numeric(20, 4) AS avg_f8,
       min(value_date) AS min_date,
       max(value_date) AS max_date,
       min(value_time) AS min_time,
       max(value_time) AS max_time,
       min(value_timestamp) AS min_timestamp,
       max(value_timestamp) AS max_timestamp,
       min(value_timestamptz) AS min_timestamptz,
       max(value_timestamptz) AS max_timestamptz,
       bool_and(value_bool) AS all_true,
       bool_or(value_bool) AS any_true,
       array_agg(value_i4 ORDER BY id) AS ordered_i4,
       string_agg(value_text, ',' ORDER BY id) AS ordered_text,
       string_agg(value_text, NULL ORDER BY id) AS concatenated_text
FROM query_offload_aggregate_basic;

SELECT min(value_f4) AS min_f4,
       max(value_f4) AS max_f4,
       sum(value_f4)::numeric(20, 4) AS sum_f4,
       avg(value_f4)::numeric(20, 4) AS avg_f4,
       min(value_f8) AS min_f8,
       max(value_f8) AS max_f8,
       sum(value_f8)::numeric(20, 4) AS sum_f8,
       avg(value_f8)::numeric(20, 4) AS avg_f8,
       min(value_date) AS min_date,
       max(value_date) AS max_date,
       min(value_time) AS min_time,
       max(value_time) AS max_time,
       min(value_timestamp) AS min_timestamp,
       max(value_timestamp) AS max_timestamp,
       min(value_timestamptz) AS min_timestamptz,
       max(value_timestamptz) AS max_timestamptz,
       bool_and(value_bool) AS all_true,
       bool_or(value_bool) AS any_true,
       array_agg(value_i4 ORDER BY id) AS ordered_i4,
       string_agg(value_text, ',' ORDER BY id) AS ordered_text,
       string_agg(value_text, NULL ORDER BY id) AS concatenated_text
FROM query_offload_aggregate_basic;

-- PostgreSQL native result.
SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'off';
SELECT min(value_f4) AS min_f4,
       max(value_f4) AS max_f4,
       sum(value_f4)::numeric(20, 4) AS sum_f4,
       avg(value_f4)::numeric(20, 4) AS avg_f4,
       min(value_f8) AS min_f8,
       max(value_f8) AS max_f8,
       sum(value_f8)::numeric(20, 4) AS sum_f8,
       avg(value_f8)::numeric(20, 4) AS avg_f8,
       min(value_date) AS min_date,
       max(value_date) AS max_date,
       min(value_time) AS min_time,
       max(value_time) AS max_time,
       min(value_timestamp) AS min_timestamp,
       max(value_timestamp) AS max_timestamp,
       min(value_timestamptz) AS min_timestamptz,
       max(value_timestamptz) AS max_timestamptz,
       bool_and(value_bool) AS all_true,
       bool_or(value_bool) AS any_true,
       array_agg(value_i4 ORDER BY id) AS ordered_i4,
       string_agg(value_text, ',' ORDER BY id) AS ordered_text,
       string_agg(value_text, NULL ORDER BY id) AS concatenated_text
FROM query_offload_aggregate_basic;

-- ============================================================================
-- S3: variance/stddev output and empty-input aggregate semantics.
-- ============================================================================

-- Query offload plan and result.
SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT round(var_pop(value_i4), 6) AS var_pop_i4,
       round(var_samp(value_i4), 6) AS var_samp_i4,
       round(stddev_pop(value_i4), 6) AS stddev_pop_i4,
       round(stddev_samp(value_i4), 6) AS stddev_samp_i4
FROM query_offload_aggregate_basic
WHERE id <= 2;

SELECT round(var_pop(value_i4), 6) AS var_pop_i4,
       round(var_samp(value_i4), 6) AS var_samp_i4,
       round(stddev_pop(value_i4), 6) AS stddev_pop_i4,
       round(stddev_samp(value_i4), 6) AS stddev_samp_i4
FROM query_offload_aggregate_basic
WHERE id <= 2;

-- PostgreSQL native result.
SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'off';
SELECT round(var_pop(value_i4), 6) AS var_pop_i4,
       round(var_samp(value_i4), 6) AS var_samp_i4,
       round(stddev_pop(value_i4), 6) AS stddev_pop_i4,
       round(stddev_samp(value_i4), 6) AS stddev_samp_i4
FROM query_offload_aggregate_basic
WHERE id <= 2;

-- Query offload plan and result.
SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT count(*) AS rows,
       count(value_i4) AS nonnull_i4,
       min(value_i4) AS min_i4,
       max(value_i4) AS max_i4,
       sum(value_i4) AS sum_i4,
       avg(value_i4)::numeric(20, 4) AS avg_i4
FROM query_offload_aggregate_basic
WHERE id > 100;

SELECT count(*) AS rows,
       count(value_i4) AS nonnull_i4,
       min(value_i4) AS min_i4,
       max(value_i4) AS max_i4,
       sum(value_i4) AS sum_i4,
       avg(value_i4)::numeric(20, 4) AS avg_i4
FROM query_offload_aggregate_basic
WHERE id > 100;

-- PostgreSQL native result.
SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'off';
SELECT count(*) AS rows,
       count(value_i4) AS nonnull_i4,
       min(value_i4) AS min_i4,
       max(value_i4) AS max_i4,
       sum(value_i4) AS sum_i4,
       avg(value_i4)::numeric(20, 4) AS avg_i4
FROM query_offload_aggregate_basic
WHERE id > 100;

-- ============================================================================
-- Exact-expression fallback smoke coverage.
-- Detailed expression-shape, lazy-evaluation, codec, and overflow coverage is
-- kept in query_offload_expressions.sql rather than this basic aggregate suite.
-- ============================================================================

-- The exact filter contains both conjuncts, while Iceberg receives only the
-- independently safe integer conjunct. VERBOSE also locks the shared
-- `Filter` / `Pushed Filter Conservative` EXPLAIN vocabulary.
SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'force';
EXPLAIN (VERBOSE, COSTS OFF)
SELECT group_i4, count(*) AS rows
FROM query_offload_aggregate_basic
WHERE id >= 2 AND lower(value_text) = 'alpha'
GROUP BY group_i4
ORDER BY group_i4;

SELECT group_i4, count(*) AS rows
FROM query_offload_aggregate_basic
WHERE id >= 2 AND lower(value_text) = 'alpha'
GROUP BY group_i4
ORDER BY group_i4;

SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'off';
SELECT group_i4, count(*) AS rows
FROM query_offload_aggregate_basic
WHERE id >= 2 AND lower(value_text) = 'alpha'
GROUP BY group_i4
ORDER BY group_i4;

-- Aggregate FILTER and HAVING use the same exact-expression planner.
SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT group_i4,
       count(*) FILTER (WHERE lower(value_text) = 'alpha') AS alpha_rows
FROM query_offload_aggregate_basic
GROUP BY group_i4
HAVING mod(sum(value_i4), 7) >= 0
ORDER BY group_i4;

SELECT group_i4,
       count(*) FILTER (WHERE lower(value_text) = 'alpha') AS alpha_rows
FROM query_offload_aggregate_basic
GROUP BY group_i4
HAVING mod(sum(value_i4), 7) >= 0
ORDER BY group_i4;

SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'off';
SELECT group_i4,
       count(*) FILTER (WHERE lower(value_text) = 'alpha') AS alpha_rows
FROM query_offload_aggregate_basic
GROUP BY group_i4
HAVING mod(sum(value_i4), 7) >= 0
ORDER BY group_i4;

-- PostgreSQL evaluates FILTER before aggregate arguments. This expression
-- argument must therefore keep the complete query on the native path.
SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT sum(abs(value_i4)) FILTER (WHERE id >= 2)
FROM query_offload_aggregate_basic;

-- ============================================================================
-- PARAM_EXTERN is bound when each query-offload execution begins.
-- Separate prepared statements prevent a cached generic plan from crossing
-- the offload/native comparison boundary.
-- ============================================================================

SET plan_cache_mode = force_generic_plan;

SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'force';
PREPARE query_offload_aggregate_param(integer) AS
SELECT count(*)
FROM query_offload_aggregate_basic
WHERE id >= $1;

EXPLAIN (VERBOSE, COSTS OFF)
EXECUTE query_offload_aggregate_param(2);
EXECUTE query_offload_aggregate_param(2);
EXECUTE query_offload_aggregate_param(100);
DEALLOCATE query_offload_aggregate_param;

SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'off';
PREPARE query_native_aggregate_param(integer) AS
SELECT count(*)
FROM query_offload_aggregate_basic
WHERE id >= $1;

EXECUTE query_native_aggregate_param(2);
EXECUTE query_native_aggregate_param(100);
DEALLOCATE query_native_aggregate_param;

RESET plan_cache_mode;

-- ============================================================================
-- Capability boundary: force must not admit aggregate-over-join before the
-- join stage is implemented. PostgreSQL remains responsible for this query.
-- ============================================================================

-- Query offload is forced, but the unsupported join shape must remain native.
SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT count(*)
FROM query_offload_aggregate_basic AS lake
JOIN (VALUES (1), (2)) AS ids(id) USING (id);

SELECT count(*)
FROM query_offload_aggregate_basic AS lake
JOIN (VALUES (1), (2)) AS ids(id) USING (id);

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP TABLE query_offload_aggregate_basic;

RESET timezone;
RESET lagodb.query_batch_rows;
RESET lagodb.query_offload_mode;
RESET lagodb.customscan_mode;
