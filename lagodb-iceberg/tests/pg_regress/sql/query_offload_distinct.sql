-- query_offload_distinct.sql
-- Basic aggregate-DISTINCT and query-level DISTINCT coverage.

DROP EXTENSION IF EXISTS lagodb_iceberg CASCADE;
CREATE EXTENSION IF NOT EXISTS lagodb_iceberg;

-- Each test case sets both planner modes locally instead of depending on
-- session state from an earlier case.
SET lagodb.query_batch_rows = 2;
SET timezone = 'UTC';

CREATE TABLE query_offload_distinct_basic (
    id integer,
    value_bool boolean,
    value_i4 integer,
    value_f8 double precision,
    value_numeric numeric(12, 2),
    value_text text COLLATE "C",
    value_date date,
    value_time time,
    value_timestamp timestamp,
    value_timestamptz timestamptz,
    value_uuid uuid,
    value_bytea bytea
) USING iceberg;

INSERT INTO query_offload_distinct_basic VALUES
    (1, true, 10, 1.5, 1.25, 'alpha',
     DATE '2024-01-01', TIME '10:00:00',
     TIMESTAMP '2024-01-01 10:00:00',
     TIMESTAMPTZ '2024-01-01 10:00:00+00',
     UUID '00000000-0000-0000-0000-000000000001', '\x01'),
    (2, true, 10, 1.5, 1.25, 'alpha',
     DATE '2024-01-01', TIME '10:00:00',
     TIMESTAMP '2024-01-01 10:00:00',
     TIMESTAMPTZ '2024-01-01 10:00:00+00',
     UUID '00000000-0000-0000-0000-000000000001', '\x01'),
    (3, false, 20, 2.5, 2.50, 'beta',
     DATE '2024-01-02', TIME '11:00:00',
     TIMESTAMP '2024-01-02 11:00:00',
     TIMESTAMPTZ '2024-01-02 11:00:00+00',
     UUID '00000000-0000-0000-0000-000000000002', '\x02'),
    (4, NULL, NULL, NULL, NULL, NULL,
     NULL, NULL, NULL, NULL, NULL, NULL),
    (5, true, NULL, 1.5, 1.25, 'alpha',
     DATE '2024-01-01', TIME '10:00:00',
     TIMESTAMP '2024-01-01 10:00:00',
     TIMESTAMPTZ '2024-01-01 10:00:00+00',
     UUID '00000000-0000-0000-0000-000000000001', '\x01');

-- ============================================================================
-- Aggregate DISTINCT across the current primitive/output families.
-- The values avoid the documented NaN, signed-zero, collation, and large-int
-- semantic differences so this block is an exact native/offload comparison.
-- ============================================================================

-- Query offload plan and result.
SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT count(DISTINCT value_bool) AS bool_values,
       count(DISTINCT value_i4) AS i4_values,
       count(DISTINCT value_f8) AS f8_values,
       count(DISTINCT value_numeric) AS numeric_values,
       count(DISTINCT value_text) AS text_values,
       count(DISTINCT value_date) AS date_values,
       count(DISTINCT value_time) AS time_values,
       count(DISTINCT value_timestamp) AS timestamp_values,
       count(DISTINCT value_timestamptz) AS timestamptz_values,
       count(DISTINCT value_uuid) AS uuid_values,
       count(DISTINCT value_bytea) AS bytea_values,
       min(DISTINCT value_i4) AS min_i4,
       max(DISTINCT value_i4) AS max_i4,
       sum(DISTINCT value_i4) AS sum_i4,
       avg(DISTINCT value_i4)::numeric(20, 4) AS avg_i4,
       round(var_pop(DISTINCT value_i4), 6) AS var_pop_i4,
       bool_and(DISTINCT value_bool) AS all_true,
       bool_or(DISTINCT value_bool) AS any_true,
       array_agg(DISTINCT value_i4 ORDER BY value_i4) AS ordered_i4,
       array_agg(DISTINCT value_i4 ORDER BY value_i4)
           FILTER (WHERE id <= 4) AS filtered_ordered_i4,
       string_agg(DISTINCT value_text, ',' ORDER BY value_text) AS ordered_text
FROM query_offload_distinct_basic;

SELECT count(DISTINCT value_bool) AS bool_values,
       count(DISTINCT value_i4) AS i4_values,
       count(DISTINCT value_f8) AS f8_values,
       count(DISTINCT value_numeric) AS numeric_values,
       count(DISTINCT value_text) AS text_values,
       count(DISTINCT value_date) AS date_values,
       count(DISTINCT value_time) AS time_values,
       count(DISTINCT value_timestamp) AS timestamp_values,
       count(DISTINCT value_timestamptz) AS timestamptz_values,
       count(DISTINCT value_uuid) AS uuid_values,
       count(DISTINCT value_bytea) AS bytea_values,
       min(DISTINCT value_i4) AS min_i4,
       max(DISTINCT value_i4) AS max_i4,
       sum(DISTINCT value_i4) AS sum_i4,
       avg(DISTINCT value_i4)::numeric(20, 4) AS avg_i4,
       round(var_pop(DISTINCT value_i4), 6) AS var_pop_i4,
       bool_and(DISTINCT value_bool) AS all_true,
       bool_or(DISTINCT value_bool) AS any_true,
       array_agg(DISTINCT value_i4 ORDER BY value_i4) AS ordered_i4,
       array_agg(DISTINCT value_i4 ORDER BY value_i4)
           FILTER (WHERE id <= 4) AS filtered_ordered_i4,
       string_agg(DISTINCT value_text, ',' ORDER BY value_text) AS ordered_text
FROM query_offload_distinct_basic;

-- PostgreSQL native result.
SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'off';
SELECT count(DISTINCT value_bool) AS bool_values,
       count(DISTINCT value_i4) AS i4_values,
       count(DISTINCT value_f8) AS f8_values,
       count(DISTINCT value_numeric) AS numeric_values,
       count(DISTINCT value_text) AS text_values,
       count(DISTINCT value_date) AS date_values,
       count(DISTINCT value_time) AS time_values,
       count(DISTINCT value_timestamp) AS timestamp_values,
       count(DISTINCT value_timestamptz) AS timestamptz_values,
       count(DISTINCT value_uuid) AS uuid_values,
       count(DISTINCT value_bytea) AS bytea_values,
       min(DISTINCT value_i4) AS min_i4,
       max(DISTINCT value_i4) AS max_i4,
       sum(DISTINCT value_i4) AS sum_i4,
       avg(DISTINCT value_i4)::numeric(20, 4) AS avg_i4,
       round(var_pop(DISTINCT value_i4), 6) AS var_pop_i4,
       bool_and(DISTINCT value_bool) AS all_true,
       bool_or(DISTINCT value_bool) AS any_true,
       array_agg(DISTINCT value_i4 ORDER BY value_i4) AS ordered_i4,
       array_agg(DISTINCT value_i4 ORDER BY value_i4)
           FILTER (WHERE id <= 4) AS filtered_ordered_i4,
       string_agg(DISTINCT value_text, ',' ORDER BY value_text) AS ordered_text
FROM query_offload_distinct_basic;

-- ============================================================================
-- Query-level DISTINCT over fixed-width, temporal, and varlena keys. The first
-- case is the unfiltered baseline. The final mixed-filter case covers the
-- DISTINCT builder's exact residual and provider-pruning path independently
-- from aggregation.
-- ============================================================================

-- Query offload plan and result.
SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT DISTINCT value_bool,
                value_i4,
                value_f8,
                value_numeric
FROM query_offload_distinct_basic
ORDER BY value_bool NULLS LAST,
         value_i4 NULLS LAST,
         value_f8 NULLS LAST,
         value_numeric NULLS LAST;

SELECT DISTINCT value_bool,
                value_i4,
                value_f8,
                value_numeric
FROM query_offload_distinct_basic
ORDER BY value_bool NULLS LAST,
         value_i4 NULLS LAST,
         value_f8 NULLS LAST,
         value_numeric NULLS LAST;

-- PostgreSQL native result.
SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'off';
SELECT DISTINCT value_bool,
                value_i4,
                value_f8,
                value_numeric
FROM query_offload_distinct_basic
ORDER BY value_bool NULLS LAST,
         value_i4 NULLS LAST,
         value_f8 NULLS LAST,
         value_numeric NULLS LAST;

-- Query offload plan and result.
SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT DISTINCT value_text,
                value_date,
                value_time,
                value_timestamp,
                value_timestamptz,
                value_uuid,
                value_bytea
FROM query_offload_distinct_basic
ORDER BY value_text NULLS LAST,
         value_date NULLS LAST,
         value_time NULLS LAST,
         value_timestamp NULLS LAST,
         value_timestamptz NULLS LAST,
         value_uuid NULLS LAST,
         value_bytea NULLS LAST;

SELECT DISTINCT value_text,
                value_date,
                value_time,
                value_timestamp,
                value_timestamptz,
                value_uuid,
                value_bytea
FROM query_offload_distinct_basic
ORDER BY value_text NULLS LAST,
         value_date NULLS LAST,
         value_time NULLS LAST,
         value_timestamp NULLS LAST,
         value_timestamptz NULLS LAST,
         value_uuid NULLS LAST,
         value_bytea NULLS LAST;

-- PostgreSQL native result.
SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'off';
SELECT DISTINCT value_text,
                value_date,
                value_time,
                value_timestamp,
                value_timestamptz,
                value_uuid,
                value_bytea
FROM query_offload_distinct_basic
ORDER BY value_text NULLS LAST,
         value_date NULLS LAST,
         value_time NULLS LAST,
         value_timestamp NULLS LAST,
         value_timestamptz NULLS LAST,
         value_uuid NULLS LAST,
         value_bytea NULLS LAST;

-- ============================================================================
-- DISTINCT uses the same exact residual and pruning negotiation as aggregate.
-- One mixed predicate is sufficient to verify the DISTINCT-specific wiring.
-- ============================================================================

-- The integer conjunct is available for Iceberg pruning; lower(value_text)
-- remains in the complete DataFusion residual as one PostgreSQL UDF.
SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'force';
EXPLAIN (VERBOSE, COSTS OFF)
SELECT DISTINCT value_i4
FROM query_offload_distinct_basic
WHERE id <= 4 AND lower(value_text) = 'alpha'
ORDER BY value_i4 NULLS LAST;

SELECT DISTINCT value_i4
FROM query_offload_distinct_basic
WHERE id <= 4 AND lower(value_text) = 'alpha'
ORDER BY value_i4 NULLS LAST;

SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'off';
SELECT DISTINCT value_i4
FROM query_offload_distinct_basic
WHERE id <= 4 AND lower(value_text) = 'alpha'
ORDER BY value_i4 NULLS LAST;

-- DISTINCT ON remains outside the query-level DISTINCT capability.
SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT DISTINCT ON (value_i4) value_i4, id
FROM query_offload_distinct_basic
ORDER BY value_i4 NULLS LAST, id;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP TABLE query_offload_distinct_basic;

RESET timezone;
RESET lagodb.query_batch_rows;
RESET lagodb.query_offload_mode;
RESET lagodb.customscan_mode;
