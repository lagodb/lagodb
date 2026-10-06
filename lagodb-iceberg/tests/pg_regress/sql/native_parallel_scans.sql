-- native_parallel_scans.sql
-- PostgreSQL-native parallel relation scans for managed and foreign Iceberg
-- tables. Query-offload execution has a separate lifecycle and test suite.

\setenv PGDATABASE :DBNAME

SELECT rest_uri AS regress_rest_uri
FROM lagodb_regress.object_storage_fixture
\gset

SET client_min_messages = warning;
DROP EXTENSION IF EXISTS lagodb_iceberg CASCADE;
RESET client_min_messages;
CREATE EXTENSION lagodb_iceberg;

SET lagodb.query_offload_mode = 'off';
SET lagodb.customscan_mode = 'force';
SET max_parallel_workers_per_gather = 2;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET parallel_leader_participation = off;

CREATE TABLE native_parallel_managed (
    id integer,
    payload text
) USING iceberg;

-- Require a two-worker partial path independently of this deliberately small
-- lifecycle fixture.
ALTER TABLE native_parallel_managed SET (parallel_workers = 2);

INSERT INTO native_parallel_managed VALUES (1, 'one');
INSERT INTO native_parallel_managed VALUES (2, 'two');
INSERT INTO native_parallel_managed VALUES (3, 'three');

-- This projected read has no pushed predicate. With leader participation
-- disabled, a launched worker must attach DSM and execute the CustomScan.
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT id, payload
FROM native_parallel_managed
ORDER BY id;

SELECT id, payload
FROM native_parallel_managed
ORDER BY id;

SET max_parallel_workers_per_gather = 0;
SELECT id, payload
FROM native_parallel_managed
ORDER BY id;
SET max_parallel_workers_per_gather = 2;

-- Managed roots use their own partial path and DSM task inventory. PG
-- forbids native parallel_workers storage parameters on partition roots.
-- Small Snappy files with distinct payloads provide physical page estimates;
-- 63 selected files also form multiple groups at the default 4 MiB open cost.
SET min_parallel_table_scan_size = 0;
CREATE TABLE native_parallel_root (
    id integer,
    region integer,
    payload text
) PARTITION BY LIST (region) USING iceberg
WITH ("write.parquet.compression-codec" = 'snappy');
INSERT INTO native_parallel_root
SELECT id, id, (SELECT string_agg(md5(id::text || ':' || part::text), '')
               FROM generate_series(1, 32) AS part)
FROM generate_series(1, 64) AS id;
ANALYZE native_parallel_root;

-- Capture EXPLAIN once and compare only stable properties, as in PG explain.sql.
CREATE FUNCTION pg_temp.native_parallel_plan(query text) RETURNS jsonb
LANGUAGE plpgsql AS $$
DECLARE
    plan jsonb;
BEGIN
    EXECUTE query INTO plan;
    RETURN plan;
END;
$$;
BEGIN;
SELECT coalesce((gather->>'Workers Launched')::integer, 0) >= 1 AS worker_launched,
       (gather->>'Actual Rows')::bigint IS NOT DISTINCT FROM 63 AS gather_rows_ok,
       (scan->>'Parallel Aware')::boolean IS NOT DISTINCT FROM true AS parallel_aware,
       coalesce(jsonb_path_exists(scan, '$.Workers[*] ? (@."Actual Rows" > 0)'), false) AS worker_scanned_rows,
       (scan->>'Data Files Selected')::bigint = 63 AS files_selected_ok,
       coalesce(strpos(scan #>> '{LagoDB Pushdown,Pushed Filter Exact}', 'region >= 2'), 0) > 0 AS filter_pushed
FROM pg_temp.native_parallel_plan(
    'EXPLAIN (ANALYZE, VERBOSE, COSTS OFF, TIMING OFF, SUMMARY OFF, FORMAT JSON)
     SELECT id FROM native_parallel_root WHERE region >= 2 ORDER BY id'
) AS document(plan)
CROSS JOIN LATERAL jsonb_path_query_first(plan,
    '$[0].Plan.** ? (@."Node Type" == "Gather" || @."Node Type" == "Gather Merge")') AS gathers(gather)
CROSS JOIN LATERAL jsonb_path_query_first(plan,
    '$[0].Plan.** ? (@."Node Type" == "Custom Scan" && @."Custom Plan Provider" == "lagodb-iceberg")') AS scans(scan);
SELECT array_agg(id ORDER BY id) AS parallel_rows
FROM native_parallel_root WHERE region >= 2;
SET LOCAL max_parallel_workers_per_gather = 0;
SELECT array_agg(id ORDER BY id) AS serial_rows
FROM native_parallel_root WHERE region >= 2;
COMMIT;
DROP FUNCTION pg_temp.native_parallel_plan(text);
DROP TABLE native_parallel_root;

-- A read-only foreign relation reconstructs its remote Iceberg reader in the
-- launched worker. This query also protects pushed-filter transfer.

CREATE SCHEMA native_parallel_fdw;
CREATE SERVER native_parallel_rest
TYPE 'rest'
FOREIGN DATA WRAPPER lagodb_iceberg
OPTIONS (uri :'regress_rest_uri');
CREATE USER MAPPING FOR CURRENT_USER SERVER native_parallel_rest;

CREATE FOREIGN TABLE native_parallel_fdw.filters (
    id integer,
    payload text,
    event_date date
)
SERVER native_parallel_rest
OPTIONS (
    catalog_name 'regress',
    catalog_namespace 'fdw_reads',
    catalog_table_name 'filter_rows_v2',
    mode 'read_only'
);

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT id, payload
FROM native_parallel_fdw.filters
WHERE id >= 2
ORDER BY id;

SELECT id, payload
FROM native_parallel_fdw.filters
WHERE id >= 2
ORDER BY id;

SET max_parallel_workers_per_gather = 0;
SELECT id, payload
FROM native_parallel_fdw.filters
WHERE id >= 2
ORDER BY id;
SET max_parallel_workers_per_gather = 2;

-- A PostgreSQL-only expression remains a residual Filter on the parallel
-- ForeignScan; no storage predicate is invented or lost in the worker.
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT id, payload
FROM native_parallel_fdw.filters
WHERE length(payload) > 0
ORDER BY id;

SELECT id, payload
FROM native_parallel_fdw.filters
WHERE length(payload) > 0
ORDER BY id;

SET max_parallel_workers_per_gather = 0;
SELECT id, payload
FROM native_parallel_fdw.filters
WHERE length(payload) > 0
ORDER BY id;

SET client_min_messages = warning;
DROP SCHEMA native_parallel_fdw CASCADE;
DROP SERVER native_parallel_rest CASCADE;
RESET client_min_messages;

DROP TABLE native_parallel_managed;

RESET parallel_leader_participation;
RESET parallel_tuple_cost;
RESET parallel_setup_cost;
RESET min_parallel_table_scan_size;
RESET max_parallel_workers_per_gather;
RESET lagodb.customscan_mode;
RESET lagodb.query_offload_mode;
