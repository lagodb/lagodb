-- query_offload_parallel.sql
-- Leader-owned PostgreSQL-worker execution for query-offload plans. Operator
-- semantics remain in the focused aggregate/join/composition suites; this file
-- protects parallel admission, representative query shapes, and observability.

DROP EXTENSION IF EXISTS lagodb_iceberg CASCADE;
CREATE EXTENSION IF NOT EXISTS lagodb_iceberg;

SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'force';
SET lagodb.query_batch_rows = 2;
SET lagodb.query_offload_parallel_min_scan_rows = 0;
SET max_parallel_workers_per_gather = 2;

SELECT current_setting('lagodb.query_offload_parallel_min_scan_rows')::integer = 0
           AS size_gate_disabled;

CREATE TABLE query_offload_parallel_left (
    id integer,
    key integer,
    value integer
) USING iceberg;

CREATE TABLE query_offload_parallel_right (
    id integer,
    key integer,
    value integer
) USING iceberg;

CREATE TABLE query_offload_parallel_small (
    id integer,
    key integer,
    value integer
) USING iceberg;

-- Iceberg's default split weighting charges one 4 MiB open cost per data file
-- and targets 128 MiB groups. Thirty-three separately written files therefore
-- form at least two source work groups without creating a large data fixture.
-- \gexec preserves those statement/file boundaries; one INSERT ... SELECT
-- would put all rows through a single DML session.
\set ECHO none
SELECT format(
    'INSERT INTO query_offload_parallel_left VALUES (%s, %s, %s)',
    row_id,
    row_id % 4,
    CASE
        WHEN row_id % 5 = 0 THEN 'NULL'
        ELSE (row_id * 10)::text
    END
)
FROM generate_series(1, 33) AS row_id
\gexec
\set ECHO all

INSERT INTO query_offload_parallel_right
SELECT 100 + row_id, row_id % 4, row_id * 100
FROM generate_series(1, 33) AS row_id;

INSERT INTO query_offload_parallel_small VALUES (1, 1, 10);

-- A one-file source has no distributable stage and negotiates serial execution
-- before any worker starts.
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT key, count(*)
FROM query_offload_parallel_small
GROUP BY key;

-- The planner-row threshold is an admission gate, not an execution fallback.
SET lagodb.query_offload_parallel_min_scan_rows = 2147483647;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT key, count(*)
FROM query_offload_parallel_left
GROUP BY key;
SET lagodb.query_offload_parallel_min_scan_rows = 0;

-- PostgreSQL worker budget below two producers also negotiates serial mode.
SET max_parallel_workers_per_gather = 0;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT key, count(*)
FROM query_offload_parallel_left
GROUP BY key;
SET max_parallel_workers_per_gather = 2;

-- A filtered grouped aggregate consumes every source work group exactly
-- once. ANALYZE metrics prove that a parallel run was launched and completely
-- reported, while the result is compared with PostgreSQL's native execution.
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT key,
       count(*) AS rows,
       count(value) AS nonnull_values,
       sum(value) AS total
FROM query_offload_parallel_left
WHERE id >= 1
GROUP BY key
ORDER BY key;

SELECT key,
       count(*) AS rows,
       count(value) AS nonnull_values,
       sum(value) AS total
FROM query_offload_parallel_left
WHERE id >= 1
GROUP BY key
ORDER BY key;

SET lagodb.query_offload_mode = 'off';
SELECT key,
       count(*) AS rows,
       count(value) AS nonnull_values,
       sum(value) AS total
FROM query_offload_parallel_left
WHERE id >= 1
GROUP BY key
ORDER BY key;

-- A limited ordered inner join exercises parallel join distribution and
-- early output termination without routing through an aggregate.
SET lagodb.query_offload_mode = 'force';
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT l.id AS left_id, r.id AS right_id, l.key
FROM query_offload_parallel_left AS l
JOIN query_offload_parallel_right AS r USING (key)
ORDER BY left_id, right_id
LIMIT 8;

SELECT l.id AS left_id, r.id AS right_id, l.key
FROM query_offload_parallel_left AS l
JOIN query_offload_parallel_right AS r USING (key)
ORDER BY left_id, right_id
LIMIT 8;

SET lagodb.query_offload_mode = 'off';
SELECT l.id AS left_id, r.id AS right_id, l.key
FROM query_offload_parallel_left AS l
JOIN query_offload_parallel_right AS r USING (key)
ORDER BY left_id, right_id
LIMIT 8;

-- Aggregate-over-inner-join reuses the same parallel execution framework
-- and preserves duplicate fanout, NULL aggregate inputs, FILTER, and HAVING.
SET lagodb.query_offload_mode = 'force';
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT l.key,
       count(*) AS joined_rows,
       count(l.value) AS nonnull_left,
       sum(r.value) FILTER (WHERE r.id >= 120) AS filtered_total
FROM query_offload_parallel_left AS l
JOIN query_offload_parallel_right AS r USING (key)
GROUP BY l.key
HAVING count(*) > 60
ORDER BY l.key;

SELECT l.key,
       count(*) AS joined_rows,
       count(l.value) AS nonnull_left,
       sum(r.value) FILTER (WHERE r.id >= 120) AS filtered_total
FROM query_offload_parallel_left AS l
JOIN query_offload_parallel_right AS r USING (key)
GROUP BY l.key
HAVING count(*) > 60
ORDER BY l.key;

SET lagodb.query_offload_mode = 'off';
SELECT l.key,
       count(*) AS joined_rows,
       count(l.value) AS nonnull_left,
       sum(r.value) FILTER (WHERE r.id >= 120) AS filtered_total
FROM query_offload_parallel_left AS l
JOIN query_offload_parallel_right AS r USING (key)
GROUP BY l.key
HAVING count(*) > 60
ORDER BY l.key;

DROP TABLE query_offload_parallel_small;
DROP TABLE query_offload_parallel_right;
DROP TABLE query_offload_parallel_left;

RESET max_parallel_workers_per_gather;
RESET lagodb.query_offload_parallel_min_scan_rows;
RESET lagodb.query_batch_rows;
RESET lagodb.query_offload_mode;
RESET lagodb.customscan_mode;
