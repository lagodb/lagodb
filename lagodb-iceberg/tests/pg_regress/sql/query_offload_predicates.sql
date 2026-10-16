-- Runtime predicate adapters and Arrow representation admission.
-- Native operator truth sets live in iceberg_fdw_pruning; each adapter below
-- gets a representative aggregate so that query offload is actually eligible.

SELECT rest_uri AS regress_rest_uri
FROM lagodb_regress.object_storage_fixture
\gset
SET client_min_messages = warning;
DROP EXTENSION IF EXISTS lagodb_iceberg CASCADE;
RESET client_min_messages;
CREATE EXTENSION lagodb_iceberg;
CREATE SCHEMA query_predicates;
CREATE SERVER query_predicates_rest
TYPE 'rest'
FOREIGN DATA WRAPPER lagodb_iceberg
OPTIONS (uri :'regress_rest_uri');
CREATE USER MAPPING FOR CURRENT_USER SERVER query_predicates_rest;
CREATE FOREIGN TABLE query_predicates.scalars ()
SERVER query_predicates_rest
OPTIONS (table 'fdw_predicates.rows_v2');
CREATE FOREIGN TABLE query_predicates.nanos ()
SERVER query_predicates_rest
OPTIONS (table 'fdw_nanos.rows_v3');
SET max_parallel_workers_per_gather = 0;
SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'off';

-- The managed source preserves a PG int2 over Iceberg's int32 storage.
CREATE TABLE query_predicates.managed (id integer, small smallint) USING iceberg;
INSERT INTO query_predicates.managed SELECT id, small FROM query_predicates.scalars;

-- As in PG explain.sql, expose stable JSON fields without timings or costs.
-- Also reuse this adapter for scalar query results in the named cases below.
CREATE FUNCTION pg_temp.predicate_query_json(query text) RETURNS jsonb
LANGUAGE plpgsql AS $$
DECLARE
    result jsonb;
BEGIN
    EXECUTE query INTO result;
    RETURN result;
END;
$$;

SET lagodb.query_offload_mode = 'force';

-- Source nanosecond precision permits validity tests. Projection is covered
-- by the nanos join in query_offload_foreign_tables, including NULL output.
EXPLAIN (COSTS OFF)
SELECT count(*) FROM query_predicates.nanos WHERE ts IS NULL;

SELECT count(*) FROM query_predicates.nanos WHERE ts IS NULL;

-- Input rows are measured before DataFusion residual evaluation. Each file
-- contains excluded rows, so results alone cannot prove reader filtering.
-- The different-width binary literal deliberately retains all four input rows.
CREATE TEMP TABLE runtime_cases(name text, query text);
INSERT INTO runtime_cases VALUES
    ('boolean',
     'SELECT count(*) FROM query_predicates.scalars WHERE flag IS NOT TRUE'),
    ('binary',
     'SELECT count(*) FROM query_predicates.scalars WHERE payload < ''\x8000''::bytea'),
    ('uuid',
     'SELECT count(*) FROM query_predicates.scalars WHERE key >= ''80000000-0000-0000-0000-000000000000''::uuid'),
    ('date',
     'SELECT count(*) FROM query_predicates.scalars WHERE day <> DATE ''2000-01-01'''),
    ('timestamp',
     'SELECT count(*) FROM query_predicates.scalars WHERE stamp <> TIMESTAMP ''2000-01-01'''),
    ('timestamptz',
     'SELECT count(*) FROM query_predicates.scalars WHERE instant <> TIMESTAMPTZ ''2000-01-01+00'''),
    ('time_validity',
     'SELECT count(*) FROM query_predicates.scalars WHERE clock IS NULL'),
    ('fixed_binary',
     'SELECT count(*) FROM query_predicates.scalars WHERE fixed_payload = ''\x8000''::bytea'),
    ('fixed16_binary',
     'SELECT count(*) FROM query_predicates.scalars WHERE fixed16_payload = ''\x80000000000000000000000000000000''::bytea'),
    ('missing_boolean',
     'SELECT count(*) FROM query_predicates.scalars WHERE later_flag IS TRUE'),
    ('managed_integer',
     'SELECT count(*) FROM query_predicates.managed WHERE small = 2::bigint'),
    ('binary_width_residual',
     'SELECT count(*) FROM query_predicates.scalars WHERE fixed_payload < ''\x80''::bytea');

SELECT name, pg_temp.predicate_query_json(query) AS matched_rows,
       scan->>'Node Type' AS scan_type, scan->'Actual Input Rows' AS input_rows
FROM runtime_cases
CROSS JOIN LATERAL pg_temp.predicate_query_json(
    'EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, FORMAT JSON) ' || query) AS explained(plan)
CROSS JOIN LATERAL jsonb_path_query_first(plan,
    '$[0].Plan.** ? (@."Node Type" == "Table Scan")') AS selected(scan)
ORDER BY name;

DROP TABLE runtime_cases;

-- Compare native results and report the actual route for each representation
-- boundary. Literal and managed-integer cases are already covered above.
-- LargeBinary/fixed mixtures and nanos comparisons/promotions retain PG
-- evaluation; same-representation expressions still offload.
CREATE TEMP TABLE representation_cases(name text, query text);
INSERT INTO representation_cases VALUES
    ('mixed_bytea',
     'SELECT count(*) FROM query_predicates.scalars WHERE payload = fixed_payload'),
    ('mixed_bytea_join',
     'SELECT count(*) FROM query_predicates.scalars a JOIN query_predicates.scalars b ON a.id = b.id WHERE a.payload = b.fixed_payload'),
    ('fixed_bytea_join',
     'SELECT count(*) FROM query_predicates.scalars a JOIN query_predicates.scalars b ON a.id = b.id WHERE a.fixed_payload = b.fixed_payload'),
    ('fixed_columns',
     'SELECT count(*) FROM query_predicates.scalars WHERE fixed_payload = fixed_payload'),
    ('binary_coalesce',
     'SELECT count(*) FROM query_predicates.scalars WHERE coalesce(payload, fixed_payload) IS NOT NULL'),
    ('binary_case',
     'SELECT count(*) FROM query_predicates.scalars WHERE (CASE WHEN flag THEN payload ELSE fixed_payload END) IS NOT NULL'),
    ('nanos_compare',
     'SELECT count(*) FROM query_predicates.nanos WHERE ts = TIMESTAMP ''1970-01-01'''),
    ('nanos_aggregate_filter',
     'SELECT count(*) FILTER (WHERE ts = TIMESTAMP ''1970-01-01'') FROM query_predicates.nanos'),
    ('nanos_coalesce',
     'SELECT count(*) FROM query_predicates.nanos WHERE coalesce(ts, TIMESTAMP ''1600-01-01'') IS NOT NULL'),
    ('nanos_case',
     'SELECT count(*) FROM query_predicates.nanos WHERE (CASE WHEN id = 6 THEN ts ELSE TIMESTAMP ''1600-01-01'' END) IS NOT NULL'),
    ('nanos_same_type',
     'SELECT count(*) FROM query_predicates.nanos WHERE coalesce(ts, ts) IS NULL');

SET lagodb.query_offload_mode = 'off';
CREATE TEMP TABLE representation_results AS
SELECT name, pg_temp.predicate_query_json(query) AS result
FROM representation_cases;

SET lagodb.query_offload_mode = 'force';
SELECT name, result AS native_result,
       pg_temp.predicate_query_json(query) IS NOT DISTINCT FROM result AS same_results,
       jsonb_path_exists(pg_temp.predicate_query_json(
           'EXPLAIN (COSTS OFF, FORMAT JSON) ' || query),
           '$[0].Plan.** ? (@."Custom Plan Provider" == "LagoDB Query Offload")') AS offloaded
FROM representation_cases JOIN representation_results USING (name)
ORDER BY name;

DROP TABLE representation_cases, representation_results;
DROP FUNCTION pg_temp.predicate_query_json(text);
DROP TABLE query_predicates.managed;
SET client_min_messages = warning;
DROP SCHEMA query_predicates CASCADE;
DROP SERVER query_predicates_rest CASCADE;
RESET client_min_messages;
RESET max_parallel_workers_per_gather;
RESET lagodb.query_offload_mode;
RESET lagodb.customscan_mode;
