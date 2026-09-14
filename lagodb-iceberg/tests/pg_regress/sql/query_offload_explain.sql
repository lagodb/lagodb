-- query_offload_explain.sql
-- Stable user-facing and structured EXPLAIN contract.

DROP EXTENSION IF EXISTS lagodb_iceberg CASCADE;
CREATE EXTENSION IF NOT EXISTS lagodb_iceberg;

SET lagodb.query_batch_rows = 2;
SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'force';

CREATE TABLE query_offload_explain_left (
    id integer,
    key integer,
    value integer
) USING iceberg;

CREATE TABLE query_offload_explain_right (
    key integer,
    value integer
) USING iceberg;

INSERT INTO query_offload_explain_left VALUES
    (1, 1, 10),
    (2, 2, 20),
    (3, 2, 30);

INSERT INTO query_offload_explain_right VALUES
    (1, 100),
    (2, 200);

CREATE FUNCTION query_offload_explain_json(options text, query text)
RETURNS jsonb
LANGUAGE plpgsql AS $$
DECLARE
    plan jsonb;
BEGIN
    EXECUTE format('EXPLAIN (%s, FORMAT JSON) %s', options, query) INTO plan;
    RETURN plan;
END;
$$;

-- Default output is one logical tree without configuration, internal IDs, or
-- projection details. COSTS OFF removes every provider estimate.
EXPLAIN (COSTS OFF)
SELECT count(*)
FROM query_offload_explain_left
WHERE id = 1;

-- Provider estimates are attached to the corresponding scan only when costs
-- are requested. Provider cost components remain VERBOSE diagnostics.
EXPLAIN (COSTS ON)
SELECT count(*)
FROM query_offload_explain_left
WHERE id = 1;

EXPLAIN (VERBOSE, COSTS ON)
SELECT count(*)
FROM query_offload_explain_left
WHERE id = 1;

-- VERBOSE may expose stable runtime-binding slots, but never the IR's
-- `$valueN` spelling or `#N` output identities.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT id, count(*)
FROM query_offload_explain_left
GROUP BY id
ORDER BY id
LIMIT 1 OFFSET 1;

-- Join children and their relation aliases occupy one PostgreSQL Plans tree.
EXPLAIN (COSTS OFF)
SELECT l.key, count(*)
FROM query_offload_explain_left AS l
JOIN query_offload_explain_right AS r USING (key)
GROUP BY l.key
ORDER BY l.key;

-- Text headings and expressions use PostgreSQL identifier quoting rather than
-- concatenating raw relation aliases.
EXPLAIN (COSTS OFF)
SELECT count(*)
FROM query_offload_explain_left AS "Left Side"
JOIN query_offload_explain_right AS "Right Side"
  ON "Left Side".key = "Right Side".key;

-- Actual metrics survive COSTS OFF, are associated with their scan leaves,
-- and exclude timing when PostgreSQL requests TIMING OFF.
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT l.key, count(*)
FROM query_offload_explain_left AS l
JOIN query_offload_explain_right AS r USING (key)
GROUP BY l.key
ORDER BY l.key;

-- Engine physical diagnostics are restricted to ANALYZE VERBOSE.
EXPLAIN (ANALYZE, VERBOSE, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT l.key, count(*)
FROM query_offload_explain_left AS l
JOIN query_offload_explain_right AS r USING (key)
GROUP BY l.key
ORDER BY l.key;

-- JSON protects real Plans nesting and native numeric property types without
-- snapshotting the complete structured document into the expected file.
WITH document AS (
    SELECT query_offload_explain_json(
        'VERBOSE, COSTS ON',
        'SELECT l.key, count(*)
         FROM query_offload_explain_left AS l
         JOIN query_offload_explain_right AS r USING (key)
         GROUP BY l.key'
    ) AS plan
)
SELECT (plan #> '{0,Plan}') ? 'Plans' AS has_main_plans,
       NOT ((plan #> '{0,Plan}') ?| ARRAY['Relation Tree', 'Table Scans'])
           AS has_no_competing_tree,
       jsonb_path_exists(
           plan #> '{0,Plan,Plans}',
           '$.** ? (@."Node Type" == "Table Scan")'
       ) AS scan_is_in_main_tree,
       jsonb_typeof(jsonb_path_query_first(
           plan,
           '$.**."Estimated Rows Read"'
       )) = 'number' AS estimate_is_number,
       jsonb_typeof(jsonb_path_query_first(
           plan,
           '$.**."Scan ID"'
       )) = 'number' AS scan_id_is_number
FROM document;

-- A NULL-aware anti join contributes a custom boolean property. COSTS OFF
-- removes estimates from structured output as well as text output.
WITH document AS (
    SELECT query_offload_explain_json(
        'COSTS OFF',
        'SELECT l.id
         FROM query_offload_explain_left AS l
         WHERE l.key NOT IN (
             SELECT r.key FROM query_offload_explain_right AS r
         )'
    ) AS plan
)
SELECT jsonb_typeof(jsonb_path_query_first(
           plan,
           '$.**."Null Aware"'
       )) = 'boolean' AS null_aware_is_boolean,
       NOT jsonb_path_exists(plan, '$.**."Estimated Rows Read"')
           AS costs_off_hides_estimates
FROM document;

-- TIMING controls engine duration metrics independently of actual counters.
-- Numeric predicates avoid unstable wall-clock values in regression output.
WITH documents AS (
    SELECT query_offload_explain_json(
               'ANALYZE, VERBOSE, COSTS OFF, TIMING ON, SUMMARY OFF',
               'SELECT count(*) FROM query_offload_explain_left'
           ) AS timed,
           query_offload_explain_json(
               'ANALYZE, VERBOSE, COSTS OFF, TIMING OFF, SUMMARY OFF',
               'SELECT count(*) FROM query_offload_explain_left'
           ) AS untimed
)
SELECT jsonb_typeof(jsonb_path_query_first(
           timed,
           '$.**."Metric: elapsed_compute"'
       )) = 'number' AS timing_metric_is_number,
       NOT jsonb_path_exists(untimed, '$.**."Metric: elapsed_compute"')
           AS timing_off_hides_engine_time
FROM documents;

-- Zero fallback counts stay hidden; a real PostgreSQL evaluator boundary is
-- visible without restoring the old aggregate/join summary inventory.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(*) FILTER (WHERE random() >= 0)
FROM query_offload_explain_left;

DROP FUNCTION query_offload_explain_json(text, text);
DROP TABLE query_offload_explain_right;
DROP TABLE query_offload_explain_left;

RESET lagodb.query_batch_rows;
RESET lagodb.query_offload_mode;
RESET lagodb.customscan_mode;
