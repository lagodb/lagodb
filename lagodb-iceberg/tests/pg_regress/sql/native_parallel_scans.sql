-- native_parallel_scans.sql
-- PostgreSQL-native parallel relation scans for managed and foreign Iceberg
-- tables. Query-offload execution has a separate lifecycle and test suite.

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

-- Use JSON for runtime statistics whose text includes variable worker
-- row splits and memory sizes. Check stable plan shapes with plain EXPLAIN.
CREATE FUNCTION pg_temp.native_parallel_analyze(query text) RETURNS jsonb
LANGUAGE plpgsql AS $$
DECLARE
    plan jsonb;
BEGIN
    EXECUTE 'EXPLAIN (ANALYZE, VERBOSE, COSTS OFF, TIMING OFF, SUMMARY OFF, FORMAT JSON) '
            || query INTO plan;
    RETURN plan;
END;
$$;

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
EXPLAIN (COSTS OFF)
SELECT id, payload
FROM native_parallel_managed
ORDER BY id;
SELECT (gather->>'Workers Launched')::integer > 0 AS worker_launched,
       (gather->>'Actual Rows')::bigint = 3 AS gather_rows_ok,
       jsonb_path_exists(scan, '$.Workers[*] ? (@."Actual Rows" > 0)') AS worker_scanned_rows
FROM pg_temp.native_parallel_analyze(
    'SELECT id, payload
     FROM native_parallel_managed
     ORDER BY id') AS document(plan)
CROSS JOIN LATERAL jsonb_path_query_first(plan,
    '$[0].Plan.** ? (@."Node Type" == "Gather" || @."Node Type" == "Gather Merge")') AS gathers(gather)
CROSS JOIN LATERAL jsonb_path_query_first(plan,
    '$[0].Plan.** ? (@."Node Type" == "Custom Scan")') AS scans(scan);

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

-- Check the root's parallel and exact-filter contracts in the same runtime
-- result; its planned worker count follows physical page estimates.
SELECT coalesce((gather->>'Workers Launched')::integer, 0) >= 1 AS worker_launched,
       (gather->>'Actual Rows')::bigint IS NOT DISTINCT FROM 63 AS gather_rows_ok,
       (scan->>'Parallel Aware')::boolean IS NOT DISTINCT FROM true AS parallel_aware,
       coalesce(jsonb_path_exists(scan, '$.Workers[*] ? (@."Actual Rows" > 0)'), false) AS worker_scanned_rows,
       (scan->>'Data Files Selected')::bigint = 63 AS files_selected_ok,
       coalesce(strpos(scan #>> '{LagoDB Pushdown,Pushed Filter Exact}', 'region >= 2'), 0) > 0 AS filter_pushed
FROM pg_temp.native_parallel_analyze(
    'SELECT id FROM native_parallel_root WHERE region >= 2 ORDER BY id'
) AS document(plan)
CROSS JOIN LATERAL jsonb_path_query_first(plan,
    '$[0].Plan.** ? (@."Node Type" == "Gather" || @."Node Type" == "Gather Merge")') AS gathers(gather)
CROSS JOIN LATERAL jsonb_path_query_first(plan,
    '$[0].Plan.** ? (@."Node Type" == "Custom Scan" && @."Custom Plan Provider" == "lagodb-iceberg")') AS scans(scan);
SELECT array_agg(id ORDER BY id) AS parallel_rows
FROM native_parallel_root WHERE region >= 2;
SET max_parallel_workers_per_gather = 0;
SELECT array_agg(id ORDER BY id) AS serial_rows
FROM native_parallel_root WHERE region >= 2;
SET max_parallel_workers_per_gather = 2;
DROP TABLE native_parallel_root;

-- A foreign relation reconstructs its remote Iceberg reader in the
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
    table 'fdw_reads.filter_rows_v2'
);

EXPLAIN (COSTS OFF)
SELECT id, payload
FROM native_parallel_fdw.filters
WHERE id >= 2
ORDER BY id;
SELECT (gather->>'Workers Launched')::integer > 0 AS worker_launched,
       (gather->>'Actual Rows')::bigint = 3 AS gather_rows_ok,
       jsonb_path_exists(scan, '$.Workers[*] ? (@."Actual Rows" > 0)') AS worker_scanned_rows
FROM pg_temp.native_parallel_analyze(
    'SELECT id, payload
     FROM native_parallel_fdw.filters
     WHERE id >= 2
     ORDER BY id') AS document(plan)
CROSS JOIN LATERAL jsonb_path_query_first(plan,
    '$[0].Plan.** ? (@."Node Type" == "Gather" || @."Node Type" == "Gather Merge")') AS gathers(gather)
CROSS JOIN LATERAL jsonb_path_query_first(plan,
    '$[0].Plan.** ? (@."Node Type" == "Foreign Scan")') AS scans(scan);

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
EXPLAIN (COSTS OFF)
SELECT id, payload
FROM native_parallel_fdw.filters
WHERE length(payload) > 0
ORDER BY id;
SELECT (gather->>'Workers Launched')::integer > 0 AS worker_launched,
       (gather->>'Actual Rows')::bigint = 3 AS gather_rows_ok,
       jsonb_path_exists(scan, '$.Workers[*] ? (@."Actual Rows" > 0)') AS worker_scanned_rows
FROM pg_temp.native_parallel_analyze(
    'SELECT id, payload
     FROM native_parallel_fdw.filters
     WHERE length(payload) > 0
     ORDER BY id') AS document(plan)
CROSS JOIN LATERAL jsonb_path_query_first(plan,
    '$[0].Plan.** ? (@."Node Type" == "Gather" || @."Node Type" == "Gather Merge")') AS gathers(gather)
CROSS JOIN LATERAL jsonb_path_query_first(plan,
    '$[0].Plan.** ? (@."Node Type" == "Foreign Scan")') AS scans(scan);

SELECT id, payload
FROM native_parallel_fdw.filters
WHERE length(payload) > 0
ORDER BY id;

SET max_parallel_workers_per_gather = 0;
SELECT id, payload
FROM native_parallel_fdw.filters
WHERE length(payload) > 0
ORDER BY id;

-- Keep historical-schema worker coverage separate from the default-schema
-- cases above. Schema 1 and the current schema share id/body names; this
-- local subset reverses the definition order without changing the scan schema.
CREATE FOREIGN TABLE native_parallel_fdw.historical (
    body text,
    id integer,
    removed integer
)
SERVER native_parallel_rest
OPTIONS (table 'fdw_schema.evolving', schema_id '1');

SET max_parallel_workers_per_gather = 2;
-- Confirm that the worker, rather than a serial fallback, binds schema_id.
SELECT (gather->>'Workers Launched')::integer > 0 AS worker_launched,
       jsonb_path_exists(scan, '$.Workers[*] ? (@."Actual Rows" > 0)') AS worker_scanned_rows
FROM pg_temp.native_parallel_analyze(
    'SELECT id, body FROM native_parallel_fdw.historical
     WHERE body >= ''one'' ORDER BY id') AS document(plan)
CROSS JOIN LATERAL jsonb_path_query_first(plan,
    '$[0].Plan.** ? (@."Node Type" == "Gather" || @."Node Type" == "Gather Merge")') AS gathers(gather)
CROSS JOIN LATERAL jsonb_path_query_first(plan,
    '$[0].Plan.** ? (@."Node Type" == "Foreign Scan")') AS scans(scan);

-- Read both file versions in parallel without binding the unused removed column.
SELECT id, body FROM native_parallel_fdw.historical
WHERE body >= 'one' ORDER BY id;

-- Cache partial plans before writing. The second query follows PG's
-- select_parallel.sql rescan pattern; disabling Material forces DSM reuse.
CREATE FOREIGN TABLE native_parallel_fdw.filters_alias ()
SERVER native_parallel_rest
OPTIONS (table 'fdw_reads.filter_rows_v2', schema_id '0');
SET enable_material = off;
PREPARE native_cached_gather AS
SELECT count(*), sum(id) FROM native_parallel_fdw.filters WHERE id >= 2;
PREPARE native_cached_merge AS
SELECT v.n, s.id
FROM (SELECT id FROM native_parallel_fdw.filters
      WHERE id >= 2 ORDER BY id) s
RIGHT JOIN (VALUES (1), (2)) v(n) ON true
ORDER BY v.n, s.id;

EXPLAIN (COSTS OFF) EXECUTE native_cached_gather;
EXPLAIN (COSTS OFF) EXECUTE native_cached_merge;

BEGIN;
SAVEPOINT before_foreign_write;
-- The schema_id alias writes the same identity observed by the cached scans.
INSERT INTO native_parallel_fdw.filters_alias VALUES (5, 'five', DATE '2024-01-05');

-- New plans exclude workers for both aliases, but not for an untouched table.
EXPLAIN (COSTS OFF) SELECT id FROM native_parallel_fdw.filters;
EXPLAIN (COSTS OFF) SELECT id FROM native_parallel_fdw.filters_alias;
EXPLAIN (COSTS OFF) SELECT id FROM native_parallel_fdw.historical;

-- Cached plans retain Gather, but run on the leader with participation off.
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
EXECUTE native_cached_gather;
EXECUTE native_cached_gather;

-- The inner Gather Merge is initialized, shut down, and reinitialized.
SELECT (gather->>'Workers Launched')::integer IS NOT DISTINCT FROM 0 AS zero_workers,
       (gather->>'Actual Loops')::integer IS NOT DISTINCT FROM 2 AS rescanned
FROM pg_temp.native_parallel_analyze(
    'EXECUTE native_cached_merge') AS document(plan)
CROSS JOIN LATERAL jsonb_path_query_first(plan,
    '$[0].Plan.** ? (@."Node Type" == "Gather Merge")') AS nodes(gather);
EXECUTE native_cached_merge;

ROLLBACK TO SAVEPOINT before_foreign_write;
-- Eligibility comes from the restored log, not a sticky modified flag.
EXPLAIN (COSTS OFF) SELECT id FROM native_parallel_fdw.filters_alias;
EXECUTE native_cached_gather;
ROLLBACK;
DEALLOCATE native_cached_gather;
DEALLOCATE native_cached_merge;
RESET enable_material;

SET client_min_messages = warning;
DROP SCHEMA native_parallel_fdw CASCADE;
DROP SERVER native_parallel_rest CASCADE;
RESET client_min_messages;

DROP TABLE native_parallel_managed;
DROP FUNCTION pg_temp.native_parallel_analyze(text);

RESET parallel_leader_participation;
RESET parallel_tuple_cost;
RESET parallel_setup_cost;
RESET min_parallel_table_scan_size;
RESET max_parallel_workers_per_gather;
RESET lagodb.customscan_mode;
RESET lagodb.query_offload_mode;
