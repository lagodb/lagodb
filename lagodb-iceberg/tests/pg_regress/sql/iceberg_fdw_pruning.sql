-- Physical partition pruning across historical/current remote partition specs.
-- The immutable Spark fixture has write.metadata.metrics.default=none:
-- column bounds and null counts cannot mask a partition-projection regression.

SELECT rest_uri AS regress_rest_uri
FROM lagodb_regress.object_storage_fixture
\gset
SET client_min_messages = warning;
DROP EXTENSION IF EXISTS lagodb_iceberg CASCADE;
RESET client_min_messages;
CREATE EXTENSION lagodb_iceberg;
CREATE SCHEMA iceberg_fdw_pruning;
CREATE SERVER iceberg_pruning_rest
TYPE 'rest'
FOREIGN DATA WRAPPER lagodb_iceberg
OPTIONS (uri :'regress_rest_uri');
CREATE USER MAPPING FOR CURRENT_USER SERVER iceberg_pruning_rest;
CREATE FOREIGN TABLE iceberg_fdw_pruning.source ()
SERVER iceberg_pruning_rest
OPTIONS (
    table 'fdw_reads.partition_evolution_v2'
);
CREATE FOREIGN TABLE iceberg_fdw_pruning.inventory ()
SERVER iceberg_pruning_rest
OPTIONS (
    table 'fdw_reads.partition_pruning_inventory'
);
SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'off';
SET max_parallel_workers_per_gather = 0;


-- As in PostgreSQL's explain.sql, use a short EXECUTE adapter to expose
-- stable EXPLAIN JSON fields. Test assertions remain ordinary SELECT results.
CREATE FUNCTION pg_temp.partition_query_json(query text) RETURNS jsonb
LANGUAGE plpgsql AS $$
DECLARE
    result jsonb;
BEGIN
    EXECUTE query INTO result;
    RETURN result;
END;
$$;

-- Both historical/current specs must be populated and have no column metrics.
SELECT count(*) FILTER (WHERE case_name = 'all') AS populated_specs,
       bool_and(file_count > 0) FILTER (WHERE case_name = 'all') AS nonempty_specs,
       bool_and(metrics_absent IS NOT DISTINCT FROM true) AS metrics_absent
FROM iceberg_fdw_pruning.inventory;

CREATE TEMP TABLE pruning_cases (
    ordinal integer, case_name text, predicate text, pushed_first text, pushed_second text
);
INSERT INTO pruning_cases VALUES
    (1, 'all', 'true', '', ''),
    (2, 'old_day', 'event_date = DATE ''2024-01-01''', 'event_date =', ''),
    (3, 'new_day', 'event_date = DATE ''2024-01-03''', 'event_date =', ''),
    (4, 'new_range', 'event_date >= DATE ''2024-01-03'' AND event_date < DATE ''2024-01-05''', 'event_date >=', 'event_date <'),
    (5, 'cross_range', 'event_date >= DATE ''2024-01-02'' AND event_date < DATE ''2024-01-04''', 'event_date >=', 'event_date <'),
    (6, 'category', 'category = ''alpha''', 'category =', ''),
    (7, 'bucket', 'id = 4', 'id =', ''),
    (8, 'null_day', 'event_date IS NULL', 'event_date IS NULL', '');

-- Old/new day cases isolate each spec; mixed cases preserve truncate/bucket
-- collisions. Expected counts come from Spark's physical files inventory.
CREATE TEMP VIEW pruning_results AS
SELECT ordinal, case_name, scan->>'Node Type' AS scan_type,
       (scan->>'Data Files Selected')::bigint =
           (SELECT sum(file_count) FROM iceberg_fdw_pruning.inventory i
            WHERE i.case_name = c.case_name) AS files_match,
       strpos(pushed, pushed_first) > 0 AND strpos(pushed, pushed_second) > 0 AS filter_bound,
       pg_temp.partition_query_json(
           'SELECT to_jsonb(array_agg(id ORDER BY id)) FROM iceberg_fdw_pruning.source WHERE ' || predicate) AS ids
FROM pruning_cases c
CROSS JOIN LATERAL pg_temp.partition_query_json(
    'EXPLAIN (ANALYZE, VERBOSE, COSTS OFF, TIMING OFF, SUMMARY OFF, FORMAT JSON)
     SELECT count(*) FROM iceberg_fdw_pruning.source WHERE ' || predicate) AS explained(plan)
CROSS JOIN LATERAL jsonb_path_query_first(plan,
    '$[0].Plan.** ? (@."Node Type" == "Table Scan" || @."Node Type" == "Foreign Scan")') AS selected(scan)
CROSS JOIN LATERAL concat_ws(' ', scan->>'Pushed Filter Exact', scan->>'Pushed Filter Conservative',
                                scan #>> '{LagoDB Pushdown,Pushed Filter Exact}',
                                scan #>> '{LagoDB Pushdown,Pushed Filter Conservative}') AS filters(pushed);

-- Finite DATE literals now execute through query offload as well.
-- Both routes must bind and physically prune the same predicates.
SET lagodb.query_offload_mode = 'force';
SELECT case_name, scan_type, files_match, filter_bound, ids
FROM pruning_results ORDER BY ordinal;

-- Native FDW must independently bind and prune the same predicates.
SET lagodb.query_offload_mode = 'off';
SELECT case_name, scan_type, files_match, filter_bound, ids
FROM pruning_results ORDER BY ordinal;

DROP VIEW pruning_results;
DROP TABLE pruning_cases;

-- A range over the bucket source also retains the complete row truth set.
SELECT array_agg(id ORDER BY id) AS partitioned_id_rows
FROM iceberg_fdw_pruning.source
WHERE id >= 4;

-- Nanosecond storage comparisons preserve PostgreSQL microsecond predicates.
CREATE FOREIGN TABLE iceberg_fdw_pruning.nanos ()
SERVER iceberg_pruning_rest
OPTIONS (
    table 'fdw_nanos.rows_v3'
);

SET TimeZone = 'UTC';
SET DateStyle = 'ISO, MDY';
CREATE TEMP TABLE nanos_pg AS SELECT id, ts, tz FROM iceberg_fdw_pruning.nanos;

-- These three distinct Arrow nanoseconds decode to the same PG microsecond.
SELECT id, ts, tz FROM iceberg_fdw_pruning.nanos WHERE id >= 5 AND id <= 7 ORDER BY id;

-- Six supported comparisons, both time types, and both sides of the epoch.
-- Every case has excluded rows in the same file. Zero rows removed by the PG
-- residual proves that storage filtering ran, not just that planning accepted it.
CREATE TEMP TABLE nanos_cases AS
SELECT col, op, ordinal,
       format('%I %s %s %L', col, op, typ, instant) AS predicate
FROM (VALUES ('ts', 'TIMESTAMP'), ('tz', 'TIMESTAMPTZ')) AS types(col, typ)
CROSS JOIN (VALUES (1, '='), (2, '<'), (3, '<='), (4, '>'), (5, '>='), (6, '<>')) AS ops(ordinal, op)
CROSS JOIN (VALUES ('1970-01-01 00:00:00'),
                   ('1969-12-31 23:59:59.999999')) AS instants(instant);

SELECT col, op,
       bool_and(pg_temp.partition_query_json(
           'SELECT to_jsonb(array_agg(id ORDER BY id)) FROM iceberg_fdw_pruning.nanos WHERE ' || predicate)
           IS NOT DISTINCT FROM pg_temp.partition_query_json(
           'SELECT to_jsonb(array_agg(id ORDER BY id)) FROM nanos_pg WHERE ' || predicate)) AS same_rows,
       bool_and((scan->>'Node Type' = 'Foreign Scan'
           AND (scan->'LagoDB Pushdown') ? 'Pushed Filter Conservative' AND scan ? 'Filter'
           AND (scan->>'Rows Removed by Filter')::integer = 0) IS TRUE) AS storage_filtered
FROM nanos_cases
CROSS JOIN LATERAL pg_temp.partition_query_json(
    'EXPLAIN (ANALYZE, VERBOSE, COSTS OFF, TIMING OFF, SUMMARY OFF, FORMAT JSON)
     SELECT id FROM iceberg_fdw_pruning.nanos WHERE ' || predicate) AS explained(plan)
CROSS JOIN LATERAL (SELECT plan->0->'Plan') AS selected(scan)
GROUP BY col, op, ordinal ORDER BY col, ordinal;

-- Reversed operands and Boolean composition retain the PG truth set.
SELECT array_agg(id ORDER BY id) = ARRAY[2,3,4,8] AS composed
FROM iceberg_fdw_pruning.nanos
WHERE (TIMESTAMP '1970-01-01' > ts AND ts >= TIMESTAMP '1969-12-31 23:59:59.999999')
   OR tz >= TIMESTAMPTZ '1970-01-01 00:00:00.000001+00';

-- Generic cached plans must retain source precision through plan serialization.
SET plan_cache_mode = force_generic_plan;
PREPARE nanos_lookup(timestamp, timestamptz) AS
SELECT coalesce(array_agg(id ORDER BY id), '{}'::integer[]) AS ids
FROM iceberg_fdw_pruning.nanos WHERE ts = $1 AND tz = $2;
EXECUTE nanos_lookup('1970-01-01', '1970-01-01 00:00:00+00');
EXECUTE nanos_lookup('1969-12-31 23:59:59.999999', '1969-12-31 23:59:59.999999+00');
EXECUTE nanos_lookup(NULL, NULL);
SELECT jsonb_path_exists(pg_temp.partition_query_json(
    'EXPLAIN (ANALYZE, VERBOSE, COSTS OFF, TIMING OFF, SUMMARY OFF, FORMAT JSON)
     EXECUTE nanos_lookup(''1970-01-01'', ''1970-01-01 00:00:00+00'')'),
    '$[0].Plan.** ? (@."Node Type" == "Foreign Scan" && exists(@."LagoDB Pushdown"."Pushed Filter Conservative") && @."Rows Removed by Filter" == 0)') AS parameter_filtered;

-- Parameterized rescans must rebind each outer timestamp value.
SELECT wanted.label, array_agg(matched.id ORDER BY matched.id) AS ids
FROM (VALUES ('negative', TIMESTAMP '1969-12-31 23:59:59.999999'),
             ('zero', TIMESTAMP '1970-01-01')) AS wanted(label, ts)
CROSS JOIN LATERAL (
    SELECT id FROM iceberg_fdw_pruning.nanos WHERE nanos.ts = wanted.ts OFFSET 0
) AS matched
GROUP BY wanted.label ORDER BY wanted.label;
DEALLOCATE nanos_lookup;
RESET plan_cache_mode;

-- Out-of-range constants and infinity retain PG residual evaluation.
SELECT array_agg(id ORDER BY id) = ARRAY[1,2,3,4,5,6,7,8] AS rows_ok
FROM iceberg_fdw_pruning.nanos WHERE ts < TIMESTAMP '2500-01-01';

SELECT array_agg(id ORDER BY id) = ARRAY[1,2,3,4,5,6,7,8] AS rows_ok
FROM iceberg_fdw_pruning.nanos WHERE tz > TIMESTAMPTZ '1600-01-01 00:00:00+00';

SELECT array_agg(id ORDER BY id) = ARRAY[1,2,3,4,5,6,7,8] AS rows_ok
FROM iceberg_fdw_pruning.nanos WHERE ts > '-infinity'::timestamp AND tz < 'infinity'::timestamptz;

-- Scalar operators: exercise each truth set once on matching, nonmatching and
-- NULL rows. UUID, fixed binary and time retain their actual Iceberg types.
CREATE FOREIGN TABLE iceberg_fdw_pruning.scalars ()
SERVER iceberg_pruning_rest
OPTIONS (table 'fdw_predicates.rows_v2');

EXPLAIN (COSTS OFF)
SELECT id FROM iceberg_fdw_pruning.scalars WHERE flag IS NOT TRUE;

EXPLAIN (COSTS OFF)
SELECT id FROM iceberg_fdw_pruning.scalars WHERE payload < '\x8000'::bytea;

EXPLAIN (COSTS OFF)
SELECT id FROM iceberg_fdw_pruning.scalars WHERE key >= '80000000-0000-0000-0000-000000000000'::uuid;

SELECT id FROM iceberg_fdw_pruning.scalars WHERE flag ORDER BY id;

SELECT id FROM iceberg_fdw_pruning.scalars WHERE NOT flag ORDER BY id;

SELECT id FROM iceberg_fdw_pruning.scalars WHERE flag IS TRUE ORDER BY id;

SELECT id FROM iceberg_fdw_pruning.scalars WHERE flag IS NOT TRUE ORDER BY id;

SELECT id FROM iceberg_fdw_pruning.scalars WHERE flag IS FALSE ORDER BY id;

SELECT id FROM iceberg_fdw_pruning.scalars WHERE flag IS NOT FALSE ORDER BY id;

SELECT id FROM iceberg_fdw_pruning.scalars WHERE flag IS UNKNOWN ORDER BY id;

SELECT id FROM iceberg_fdw_pruning.scalars WHERE flag IS NOT UNKNOWN ORDER BY id;

SELECT id FROM iceberg_fdw_pruning.scalars WHERE payload = '\x8000'::bytea ORDER BY id;

SELECT id FROM iceberg_fdw_pruning.scalars WHERE payload <> '\x8000'::bytea ORDER BY id;

SELECT id FROM iceberg_fdw_pruning.scalars WHERE payload < '\x8000'::bytea ORDER BY id;

SELECT id FROM iceberg_fdw_pruning.scalars WHERE payload <= '\x8000'::bytea ORDER BY id;

SELECT id FROM iceberg_fdw_pruning.scalars WHERE payload > '\x8000'::bytea ORDER BY id;

SELECT id FROM iceberg_fdw_pruning.scalars WHERE payload >= '\x8000'::bytea ORDER BY id;

SELECT id FROM iceberg_fdw_pruning.scalars WHERE payload IS NULL ORDER BY id;

SELECT id FROM iceberg_fdw_pruning.scalars WHERE payload IS NOT NULL ORDER BY id;

SELECT id FROM iceberg_fdw_pruning.scalars WHERE key = '80000000-0000-0000-0000-000000000000'::uuid ORDER BY id;

SELECT id FROM iceberg_fdw_pruning.scalars WHERE key <> '80000000-0000-0000-0000-000000000000'::uuid ORDER BY id;

SELECT id FROM iceberg_fdw_pruning.scalars WHERE key < '80000000-0000-0000-0000-000000000000'::uuid ORDER BY id;

SELECT id FROM iceberg_fdw_pruning.scalars WHERE key <= '80000000-0000-0000-0000-000000000000'::uuid ORDER BY id;

SELECT id FROM iceberg_fdw_pruning.scalars WHERE key > '80000000-0000-0000-0000-000000000000'::uuid ORDER BY id;

SELECT id FROM iceberg_fdw_pruning.scalars WHERE key >= '80000000-0000-0000-0000-000000000000'::uuid ORDER BY id;

SELECT id FROM iceberg_fdw_pruning.scalars WHERE key IS NULL ORDER BY id;

SELECT id FROM iceberg_fdw_pruning.scalars WHERE key IS NOT NULL ORDER BY id;

SELECT id FROM iceberg_fdw_pruning.scalars WHERE day <> DATE '2000-01-01' ORDER BY id;

SELECT id FROM iceberg_fdw_pruning.scalars WHERE stamp <> TIMESTAMP '2000-01-01' ORDER BY id;

SELECT id FROM iceberg_fdw_pruning.scalars WHERE instant <> TIMESTAMPTZ '2000-01-01+00' ORDER BY id;

SELECT id FROM iceberg_fdw_pruning.scalars WHERE clock IS NULL ORDER BY id;

SELECT id FROM iceberg_fdw_pruning.scalars WHERE clock IS NOT NULL ORDER BY id;

-- Fixed binary equality uses the source width, not UUID semantics.
SELECT id FROM iceberg_fdw_pruning.scalars WHERE fixed_payload = '\x8000'::bytea ORDER BY id;

-- A newly added optional boolean is NULL in every older data file.
SELECT id FROM iceberg_fdw_pruning.scalars WHERE later_flag IS TRUE ORDER BY id;

SELECT id FROM iceberg_fdw_pruning.scalars WHERE later_flag IS NOT TRUE ORDER BY id;

-- CustomScan shares predicate planning with FDW; check its exact and
-- conservative execution boundaries without repeating the operator matrix.
CREATE TABLE iceberg_fdw_pruning.scalar_managed (
    id integer, flag boolean, stamp timestamp(6), small smallint, wide bigint
) USING iceberg;
INSERT INTO iceberg_fdw_pruning.scalar_managed
SELECT id, flag, stamp, small, wide FROM iceberg_fdw_pruning.scalars;
SET lagodb.customscan_mode = 'force';

EXPLAIN (COSTS OFF)
SELECT id FROM iceberg_fdw_pruning.scalar_managed WHERE flag IS NOT TRUE;

SELECT id FROM iceberg_fdw_pruning.scalar_managed WHERE flag IS NOT TRUE ORDER BY id;

EXPLAIN (COSTS OFF)
SELECT id FROM iceberg_fdw_pruning.scalar_managed WHERE stamp <> TIMESTAMP '2000-01-01';

SELECT id FROM iceberg_fdw_pruning.scalar_managed WHERE stamp <> TIMESTAMP '2000-01-01' ORDER BY id;

-- Cross-width operators and an int8 boundary outside Iceberg int range.
SELECT id FROM iceberg_fdw_pruning.scalar_managed WHERE small = 2::bigint ORDER BY id;

SELECT id FROM iceberg_fdw_pruning.scalar_managed WHERE wide > 2::smallint ORDER BY id;

SELECT id FROM iceberg_fdw_pruning.scalar_managed WHERE small < 2147483648::bigint ORDER BY id;

DROP TABLE iceberg_fdw_pruning.scalar_managed;
DROP FOREIGN TABLE iceberg_fdw_pruning.scalars;

DROP TABLE nanos_cases, nanos_pg;
RESET TimeZone;
RESET DateStyle;

DROP FUNCTION pg_temp.partition_query_json(text);
SET client_min_messages = warning;
DROP SCHEMA iceberg_fdw_pruning CASCADE;
DROP SERVER iceberg_pruning_rest CASCADE;
RESET client_min_messages;
RESET max_parallel_workers_per_gather;
RESET lagodb.query_offload_mode;
RESET lagodb.customscan_mode;
