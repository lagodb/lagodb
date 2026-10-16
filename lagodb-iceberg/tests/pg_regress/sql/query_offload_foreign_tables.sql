-- query_offload_foreign_tables.sql
-- Foreign scan binding, transaction visibility, worker reconstruction and
-- nested output contracts. Operator matrices live in the focused managed-table
-- suites; this file keeps representative foreign-provider integration cases.

\setenv PGDATABASE :DBNAME
\set iceberg_fixture query-offload-writes
\i include/iceberg_fixture.sql
SELECT rest_uri AS regress_rest_uri
FROM lagodb_regress.object_storage_fixture
\gset

SET client_min_messages = warning;
DROP EXTENSION IF EXISTS lagodb_iceberg CASCADE;
RESET client_min_messages;
CREATE EXTENSION lagodb_iceberg;
CREATE SCHEMA query_offload_foreign;
CREATE SERVER query_offload_foreign_rest
TYPE 'rest'
FOREIGN DATA WRAPPER lagodb_iceberg
OPTIONS (uri :'regress_rest_uri');
CREATE USER MAPPING FOR CURRENT_USER SERVER query_offload_foreign_rest;

CREATE FOREIGN TABLE query_offload_foreign.left_source (
    id integer,
    group_key integer,
    measure integer,
    payload text
)
SERVER query_offload_foreign_rest
OPTIONS (
    table 'query_offload_reads.left_source'
);

CREATE FOREIGN TABLE query_offload_foreign.writable_source (
    id integer,
    group_key integer,
    measure integer,
    payload text
)
SERVER query_offload_foreign_rest
OPTIONS (
    table 'query_offload_writes.left_source'
);

CREATE FOREIGN TABLE query_offload_foreign.parallel_source (
    id integer,
    group_key integer,
    measure integer,
    file_group integer
)
SERVER query_offload_foreign_rest
OPTIONS (
    table 'query_offload_writes.parallel_source'
);

-- Filter runtime EXPLAIN to the execution properties each case protects.
CREATE FUNCTION pg_temp.foreign_analyze(query text) RETURNS jsonb
LANGUAGE plpgsql AS $$
DECLARE
    result jsonb;
BEGIN
    EXECUTE 'EXPLAIN (ANALYZE, FORMAT JSON, COSTS OFF, TIMING OFF, SUMMARY OFF) '
        || query INTO result;
    RETURN result;
END;
$$;

SET lagodb.query_batch_rows = 2;
SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'force';
SET max_parallel_workers_per_gather = 0;

-- Decimal conversion must fall back before offloaded filtering or aggregation.
CREATE FOREIGN TABLE query_offload_foreign.decimals (id integer, amount numeric(5,2))
SERVER query_offload_foreign_rest OPTIONS (table 'fdw_schema.decimals', schema_id '0');
SELECT sum(amount) FROM query_offload_foreign.decimals WHERE id < 3 AND amount > 10;
\set VERBOSITY default
SELECT sum(amount) FROM query_offload_foreign.decimals WHERE id = 3;
\set VERBOSITY terse
DROP FOREIGN TABLE query_offload_foreign.decimals;

-- Different selected schemas of the same remote table share its read view.
-- The historical definition includes an unused removed column; scans use current.
CREATE FOREIGN TABLE query_offload_foreign.historical (body text, id integer, removed integer)
SERVER query_offload_foreign_rest
OPTIONS (table 'fdw_schema.evolving', schema_id '1');
CREATE FOREIGN TABLE query_offload_foreign.current_definition (id integer, body text)
SERVER query_offload_foreign_rest OPTIONS (table 'fdw_schema.evolving');
SELECT h.id, h.body
FROM query_offload_foreign.historical h
JOIN query_offload_foreign.current_definition c ON h.id = c.id AND h.body = c.body
WHERE h.body >= 'one'
ORDER BY h.id;

-- A column-free aggregate exercises the foreign CountRows scan projection.
EXPLAIN (COSTS OFF)
SELECT count(*) AS rows
FROM query_offload_foreign.left_source;

SELECT count(*) AS rows
FROM query_offload_foreign.left_source;

-- A generic parameter is retained as a runtime binding and can execute with
-- different values against the same planned foreign source.
SET plan_cache_mode = force_generic_plan;
PREPARE query_offload_foreign_by_id(integer) AS
SELECT count(*) AS rows, sum(measure) AS total
FROM query_offload_foreign.left_source
WHERE id >= $1;

EXPLAIN (COSTS OFF) EXECUTE query_offload_foreign_by_id(3);
EXECUTE query_offload_foreign_by_id(3);
EXECUTE query_offload_foreign_by_id(5);
DEALLOCATE query_offload_foreign_by_id;

RESET plan_cache_mode;

-- One projected aggregate covers foreign grouping, scalar expressions and
-- a scan-local filter; detailed aggregate semantics live in the managed suite.
EXPLAIN (COSTS OFF)
SELECT group_key, count(*) AS rows, sum(abs(measure)) AS absolute_total
FROM query_offload_foreign.left_source
WHERE id >= 2
GROUP BY group_key
ORDER BY group_key NULLS LAST;

SELECT group_key, count(*) AS rows, sum(abs(measure)) AS absolute_total
FROM query_offload_foreign.left_source
WHERE id >= 2
GROUP BY group_key
ORDER BY group_key NULLS LAST;

-- DISTINCT owns the foreign scan; PostgreSQL may retain sorting and pagination
-- above that offloaded fragment according to its normal upper-path rules.
EXPLAIN (COSTS OFF)
SELECT DISTINCT payload
FROM query_offload_foreign.left_source
ORDER BY payload NULLS LAST
LIMIT 3 OFFSET 1;

SELECT DISTINCT payload
FROM query_offload_foreign.left_source
ORDER BY payload NULLS LAST
LIMIT 3 OFFSET 1;

-- A foreign source and a managed source must resolve independently rather
-- than coupling either adapter to the other.
CREATE TABLE query_offload_foreign_managed (
    group_key integer,
    label text
) USING iceberg;
INSERT INTO query_offload_foreign_managed VALUES
    (1, 'managed-one'),
    (2, 'managed-two'),
    (4, 'managed-four');
EXPLAIN (COSTS OFF)
SELECT f.id, m.label
FROM query_offload_foreign.left_source AS f
JOIN query_offload_foreign_managed AS m USING (group_key)
ORDER BY f.id;

SELECT f.id, m.label
FROM query_offload_foreign.left_source AS f
JOIN query_offload_foreign_managed AS m USING (group_key)
ORDER BY f.id;

-- Multiple scans share a remote source within one execution. Reusing the
-- parameter-free generic plan after a commit must bind the new remote version.
PREPARE foreign_read_view AS
SELECT count(*) AS matches
FROM query_offload_foreign.writable_source a
JOIN query_offload_foreign.writable_source b USING (id)
WHERE a.id = 9001;
EXECUTE foreign_read_view;
INSERT INTO query_offload_foreign.writable_source
VALUES (9001, 9, 90, 'read-view');
EXECUTE foreign_read_view;
DEALLOCATE foreign_read_view;
DELETE FROM query_offload_foreign.writable_source WHERE id = 9001;

-- A writable source binds through the transaction-local Iceberg view. The
-- offloaded scan must observe the staged data file before PostgreSQL commits,
-- and the native FDW path must expose the same statement-visible rows.
BEGIN;
INSERT INTO query_offload_foreign.writable_source
VALUES (6, 4, 60, 'staged');
EXPLAIN (COSTS OFF)
SELECT count(*) AS rows, sum(measure) AS total
FROM query_offload_foreign.writable_source
WHERE id >= 5;

SELECT count(*) AS rows, sum(measure) AS total
FROM query_offload_foreign.writable_source
WHERE id >= 5;

SET lagodb.query_offload_mode = 'off';
SELECT count(*) AS rows, sum(measure) AS total
FROM query_offload_foreign.writable_source
WHERE id >= 5;
ROLLBACK;

-- The writable source has enough independently written files for at least two
-- Iceberg work groups. Worker reconstruction must preserve its remote table
-- identity while executing every work group and reporting complete metrics.
SET max_parallel_workers_per_gather = 2;
SET lagodb.query_offload_parallel_min_scan_rows = 0;

SELECT offload->>'Execution Mode' = 'Parallel' AS parallel,
       (offload->>'Maximum Launched Workers')::integer > 0 AS worker_launched,
       (offload->>'Parallel Runs')::integer = 1 AS one_run,
       (offload->>'Worker Metric Reports')::integer > 0
           AND offload->'Worker Metric Reports' = offload->'Expected Worker Metric Reports' AS metrics_complete,
       (offload->>'Actual Rows')::bigint = 4 AS output_rows_ok,
       (scan->>'Actual Input Rows')::bigint = 33 AS input_rows_ok,
       (scan->>'Scan Tasks')::bigint = 33
           AND (scan->>'Data Files Selected')::bigint = 33 AS inventory_complete,
       (scan->>'Selected File Bytes')::bigint > 0 AS file_bytes_reported,
       (offload->>'Maximum Worker Fragment Peak Memory Bytes')::bigint > 0
           AND (offload->>'Maximum Worker Participant Peak Memory Bytes')::bigint > 0
           AND (offload->>'Local Engine Peak Memory Bytes')::bigint > 0 AS memory_reported,
       strpos(scan->>'Pushed Filter', 'id >= 1') > 0 AS filter_pushed
FROM pg_temp.foreign_analyze(
    'SELECT group_key, count(*) AS rows, sum(measure) AS total
     FROM query_offload_foreign.parallel_source
     WHERE id >= 1 GROUP BY group_key ORDER BY group_key') AS document(plan)
CROSS JOIN LATERAL jsonb_path_query_first(plan,
    '$[0].Plan.** ? (@."Custom Plan Provider" == "LagoDB Query Offload")') AS nodes(offload)
CROSS JOIN LATERAL jsonb_path_query_first(plan,
    '$[0].Plan.** ? (@."Node Type" == "Table Scan")') AS scans(scan);

SELECT group_key, count(*) AS rows, sum(measure) AS total
FROM query_offload_foreign.parallel_source
WHERE id >= 1
GROUP BY group_key
ORDER BY group_key;

SET lagodb.query_offload_mode = 'off';
SELECT group_key, count(*) AS rows, sum(measure) AS total
FROM query_offload_foreign.parallel_source
WHERE id >= 1
GROUP BY group_key
ORDER BY group_key;

-- Nested Arrow outputs use the same JSONB conversion as direct FDW reads.
SET max_parallel_workers_per_gather = 0;
SET TimeZone = 'America/New_York';
SET DateStyle = 'SQL, DMY';
-- JSON float output must retain precision even when SQL float output rounds.
SET extra_float_digits = 0;
IMPORT FOREIGN SCHEMA fdw_nested LIMIT TO (rows_v2)
FROM SERVER query_offload_foreign_rest INTO query_offload_foreign;

CREATE FOREIGN TABLE query_offload_foreign.explicit_jsonb (
    id integer,
    record jsonb,
    items jsonb,
    labels jsonb,
    indexed jsonb,
    structured jsonb,
    numbers integer[]
)
SERVER query_offload_foreign_rest
OPTIONS (
    table 'fdw_nested.rows_v2'
);

CREATE FOREIGN TABLE query_offload_foreign.batches ()
SERVER query_offload_foreign_rest
OPTIONS (
    table 'fdw_nested.batches_v2'
);

CREATE FOREIGN TABLE query_offload_foreign.invalid_string ()
SERVER query_offload_foreign_rest
OPTIONS (
    table 'fdw_nested.invalid_string_v2'
);

CREATE FOREIGN TABLE query_offload_foreign.nanos ()
SERVER query_offload_foreign_rest
OPTIONS (
    table 'fdw_nanos.rows_v3'
);

-- Preserve FDW results before exercising the same mappings through offload.
CREATE TEMP TABLE nested_fdw_rows AS
SELECT *, numbers AS imported_numbers FROM query_offload_foreign.explicit_jsonb;

CREATE TEMP TABLE nanos_pg AS SELECT id, detail, ts, tz FROM query_offload_foreign.nanos;

SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT a.id, a.record, a.items, a.labels, a.indexed, a.structured, b.numbers,
       a.numbers AS imported_numbers
FROM query_offload_foreign.rows_v2 a
JOIN query_offload_foreign.explicit_jsonb b ON a.id = b.id;

-- CTAS retains nested outputs from both sides and native arrays from
-- imported and explicitly declared foreign columns in the same query.
-- JSONB comparisons run afterwards on local materialized results.
CREATE TEMP TABLE nested_join_rows AS
SELECT a.id, a.record, a.items, a.labels, a.indexed, a.structured, b.numbers,
       a.numbers AS imported_numbers
FROM query_offload_foreign.rows_v2 a
JOIN query_offload_foreign.explicit_jsonb b ON a.id = b.id;
-- Compare every nested field, including decimal, timestamps and map keys.
SELECT count(*) = 3 AND bool_and(a IS NOT DISTINCT FROM b) AS same_values
FROM nested_fdw_rows a FULL JOIN nested_join_rows b USING (id);
-- Write nested values, then read the committed Parquet output through offload.
CREATE FOREIGN TABLE query_offload_foreign.nested_writable (
    id integer, record jsonb, items jsonb, labels jsonb,
    indexed jsonb, structured jsonb, numbers integer[]
)
SERVER query_offload_foreign_rest
OPTIONS (
    table 'query_offload_writes.nested_rows_v2'
);
INSERT INTO query_offload_foreign.nested_writable
SELECT id + 10, record, items, labels, indexed, structured, numbers FROM nested_fdw_rows;
EXPLAIN (COSTS OFF)
SELECT a.id, a.record, a.items, a.labels, a.indexed, a.structured, b.numbers
FROM query_offload_foreign.nested_writable a
JOIN query_offload_foreign.nested_writable b ON a.id = b.id WHERE a.id > 10;
CREATE TEMP TABLE nested_written_rows AS
SELECT a.id, a.record, a.items, a.labels, a.indexed, a.structured, b.numbers
FROM query_offload_foreign.nested_writable a
JOIN query_offload_foreign.nested_writable b ON a.id = b.id WHERE a.id > 10;
SELECT count(*) = 3 AND bool_and(
           ROW(a.record, a.items, a.labels, a.indexed, a.structured, a.numbers)
           IS NOT DISTINCT FROM
           ROW(b.record, b.items, b.labels, b.indexed, b.structured, b.numbers)) AS write_roundtrip
FROM nested_written_rows a JOIN nested_fdw_rows b ON a.id = b.id + 10;
DROP TABLE nested_written_rows;

-- Reading nested values does not imply that different physical containers
-- can be combined inside DataFusion. PG evaluates these JSONB expressions.
EXPLAIN (COSTS OFF)
SELECT a.id,
       CASE WHEN a.id = 1 THEN a.record ELSE b.labels END AS chosen,
       coalesce(a.record, b.labels) AS first_value
FROM query_offload_foreign.rows_v2 a
JOIN query_offload_foreign.rows_v2 b ON a.id = b.id;

CREATE TEMP TABLE nested_branch_rows AS
SELECT a.id,
       CASE WHEN a.id = 1 THEN a.record ELSE b.labels END AS chosen,
       coalesce(a.record, b.labels) AS first_value
FROM query_offload_foreign.rows_v2 a
JOIN query_offload_foreign.rows_v2 b ON a.id = b.id;
SELECT count(*) = 3 AND bool_and(
    r.id IS NOT DISTINCT FROM f.id
    AND r.chosen IS NOT DISTINCT FROM CASE WHEN f.id = 1 THEN f.record ELSE f.labels END
    AND r.first_value IS NOT DISTINCT FROM coalesce(f.record, f.labels)) AS branches_ok
FROM nested_branch_rows r FULL JOIN nested_fdw_rows f USING (id);
DROP TABLE nested_branch_rows;
DROP TABLE nested_fdw_rows;
DROP TABLE nested_join_rows;

-- Nested values can also be consumed internally without JSONB output.
EXPLAIN (COSTS OFF)
SELECT count(*) FROM query_offload_foreign.rows_v2 a
JOIN query_offload_foreign.rows_v2 b ON a.id = b.id WHERE a.record IS NULL;

SELECT count(*) AS null_records
FROM query_offload_foreign.rows_v2 a
JOIN query_offload_foreign.rows_v2 b ON a.id = b.id
WHERE a.record IS NULL;
-- Reordered aliases, repeated columns and outer-join null extension retain
-- the source field contract. Only id=1 has a matching right-side row.
EXPLAIN (COSTS OFF)
SELECT b.items AS right_items, a.id, a.items AS left_items, a.items AS repeated
FROM query_offload_foreign.rows_v2 a
LEFT JOIN query_offload_foreign.rows_v2 b ON a.id = b.id AND b.id = 1;
CREATE TEMP TABLE nested_outer_rows AS
SELECT b.items AS right_items, a.id, a.items AS left_items, a.items AS repeated
FROM query_offload_foreign.rows_v2 a
LEFT JOIN query_offload_foreign.rows_v2 b ON a.id = b.id AND b.id = 1;
SELECT id, left_items IS NOT DISTINCT FROM repeated AS alias_ok,
       CASE WHEN id = 1 THEN right_items = left_items
            ELSE right_items IS NULL END AS outer_ok
FROM nested_outer_rows ORDER BY id;
DROP TABLE nested_outer_rows;

-- Materialize the complete nested result across batches through query offload.
SELECT jsonb_path_exists(pg_temp.foreign_analyze(
    'SELECT a.id, b.value FROM query_offload_foreign.batches a
     JOIN query_offload_foreign.batches b ON a.id = b.id'),
    '$[0].Plan.** ? (@."Custom Plan Provider" == "LagoDB Query Offload" && @."Execution Mode" == "Serial")') AS serial_offloaded;
CREATE TEMP TABLE nested_serial_rows AS
SELECT a.id, b.value FROM query_offload_foreign.batches a
JOIN query_offload_foreign.batches b ON a.id = b.id;
SELECT count(*) AS rows,
       bool_and(value = jsonb_build_object(
           'id', id, 'values', jsonb_build_array(id, NULL))) AS values_ok
FROM nested_serial_rows;
-- The same projected schema crosses worker reconstruction and Arrow IPC.
SET max_parallel_workers_per_gather = 2;
SELECT jsonb_path_exists(pg_temp.foreign_analyze(
    'SELECT a.id, b.value FROM query_offload_foreign.batches a
     JOIN query_offload_foreign.batches b ON a.id = b.id'),
    '$[0].Plan.** ? (@."Custom Plan Provider" == "LagoDB Query Offload" && @."Execution Mode" == "Parallel" && @."Maximum Launched Workers" > 0)') AS parallel_offloaded;
CREATE TEMP TABLE nested_parallel_rows AS
SELECT a.id, b.value FROM query_offload_foreign.batches a
JOIN query_offload_foreign.batches b ON a.id = b.id;
SELECT count(*) = 4097 AND bool_and(a IS NOT DISTINCT FROM b) AS same_values
FROM nested_serial_rows a FULL JOIN nested_parallel_rows b USING (id);
DROP TABLE nested_parallel_rows;
DROP TABLE nested_serial_rows;

-- Confirm schema-option binding in an actual parallel offload execution.
-- Integer predicates avoid PostgreSQL expression fallback, which executes serially.
SELECT offload->>'Execution Mode' = 'Parallel' AS parallel,
       (offload->>'Maximum Launched Workers')::integer > 0 AS worker_launched
FROM pg_temp.foreign_analyze(
    'SELECT h.id, h.body
     FROM query_offload_foreign.historical h
     JOIN query_offload_foreign.current_definition c ON h.id = c.id
     WHERE h.id >= 1 ORDER BY h.id') AS document(plan)
CROSS JOIN LATERAL jsonb_path_query_first(plan,
    '$[0].Plan.** ? (@."Custom Plan Provider" == "LagoDB Query Offload")') AS nodes(offload);

SELECT h.id, h.body
FROM query_offload_foreign.historical h
JOIN query_offload_foreign.current_definition c ON h.id = c.id AND h.body = c.body
WHERE h.body >= 'one'
ORDER BY h.id;

SET max_parallel_workers_per_gather = 0;

-- Nanosecond leaf formatting and top-level projection survive query output binding.
EXPLAIN (COSTS OFF)
SELECT a.id, a.detail, a.ts, a.tz FROM query_offload_foreign.nanos a
JOIN query_offload_foreign.nanos b ON a.id = b.id;
CREATE TEMP TABLE nanos_offload AS
SELECT a.id, a.detail, a.ts, a.tz FROM query_offload_foreign.nanos a
JOIN query_offload_foreign.nanos b ON a.id = b.id;
-- Whole-row equality includes id, including the all-NULL payload at id=9.
SELECT count(*) = 9 AND bool_and(a IS NOT DISTINCT FROM b) AS same_rows
FROM nanos_pg a FULL JOIN nanos_offload b USING (id);

-- Nanosecond equality stays in PG even when some source values are aligned.
-- Include values that decode to the same microsecond and the NULL row.
SELECT jsonb_path_exists(pg_temp.foreign_analyze(
    'SELECT count(DISTINCT ts), count(DISTINCT tz)
     FROM query_offload_foreign.nanos'),
    '$[0].Plan.** ? (@."Custom Plan Provider" == "LagoDB Query Offload")') AS count_distinct_offloaded;

SELECT count(DISTINCT ts) AS ts_count, count(DISTINCT tz) AS tz_count
FROM query_offload_foreign.nanos;

SELECT jsonb_path_exists(pg_temp.foreign_analyze(
    'SELECT DISTINCT ts FROM query_offload_foreign.nanos'),
    '$[0].Plan.** ? (@."Custom Plan Provider" == "LagoDB Query Offload")') AS distinct_offloaded;

SELECT count(*) AS distinct_rows
FROM (SELECT DISTINCT ts FROM query_offload_foreign.nanos) AS timestamps;

DROP TABLE nanos_offload, nanos_pg;

-- Require the JOIN's output plan to be offloaded before checking its error.
-- Plain EXPLAIN does not read the invalid string or construct its JSONB value.
EXPLAIN (COSTS OFF)
SELECT a.value FROM query_offload_foreign.invalid_string a
JOIN query_offload_foreign.rows_v2 b ON a.id = b.id;

\set VERBOSITY sqlstate
SELECT a.value FROM query_offload_foreign.invalid_string a
JOIN query_offload_foreign.rows_v2 b ON a.id = b.id;
\set VERBOSITY default

-- A normal offloaded scan must still execute after the output conversion error.
EXPLAIN (COSTS OFF)
SELECT count(*) AS rows_after_error
FROM query_offload_foreign.batches
WHERE value IS NOT NULL;

SELECT count(*) AS rows_after_error
FROM query_offload_foreign.batches
WHERE value IS NOT NULL;

DROP FUNCTION pg_temp.foreign_analyze(text);
RESET TimeZone;
RESET DateStyle;
RESET extra_float_digits;

DROP TABLE query_offload_foreign_managed;
SET client_min_messages = warning;
DROP SCHEMA query_offload_foreign CASCADE;
DROP SERVER query_offload_foreign_rest CASCADE;
RESET client_min_messages;

RESET max_parallel_workers_per_gather;
RESET lagodb.query_offload_parallel_min_scan_rows;
RESET lagodb.query_offload_mode;
RESET lagodb.customscan_mode;
RESET lagodb.query_batch_rows;
