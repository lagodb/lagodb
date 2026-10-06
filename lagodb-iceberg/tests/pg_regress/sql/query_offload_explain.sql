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
FROM query_offload_explain_json(
    'VERBOSE, COSTS ON',
    'SELECT l.key, count(*)
     FROM query_offload_explain_left AS l
     JOIN query_offload_explain_right AS r USING (key)
     GROUP BY l.key'
) AS document(plan);

-- A NULL-aware anti join contributes a custom boolean property. COSTS OFF
-- removes estimates from structured output as well as text output.
SELECT jsonb_typeof(jsonb_path_query_first(
           plan,
           '$.**."Null Aware"'
       )) = 'boolean' AS null_aware_is_boolean,
       NOT jsonb_path_exists(plan, '$.**."Estimated Rows Read"')
           AS costs_off_hides_estimates
FROM query_offload_explain_json(
    'COSTS OFF',
    'SELECT l.id
     FROM query_offload_explain_left AS l
     WHERE l.key NOT IN (
         SELECT r.key FROM query_offload_explain_right AS r
     )'
) AS document(plan);

-- TIMING controls engine duration metrics independently of actual counters.
-- Numeric predicates avoid unstable wall-clock values in regression output.
SELECT jsonb_typeof(jsonb_path_query_first(
           timed,
           '$.**."Metric: elapsed_compute"'
       )) = 'number' AS timing_metric_is_number,
       NOT jsonb_path_exists(untimed, '$.**."Metric: elapsed_compute"')
           AS timing_off_hides_engine_time
FROM query_offload_explain_json(
    'ANALYZE, VERBOSE, COSTS OFF, TIMING ON, SUMMARY OFF',
    'SELECT count(*) FROM query_offload_explain_left'
) AS timed_plan(timed)
CROSS JOIN query_offload_explain_json(
    'ANALYZE, VERBOSE, COSTS OFF, TIMING OFF, SUMMARY OFF',
    'SELECT count(*) FROM query_offload_explain_left'
) AS untimed_plan(untimed);

-- Zero fallback counts stay hidden; a real PostgreSQL evaluator boundary is
-- visible without restoring the old aggregate/join summary inventory.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(*) FILTER (WHERE random() >= 0)
FROM query_offload_explain_left;

-- Runtime filter diagnostics must use complete binding expressions and the
-- scan's PlannerInfo scope. Reuse the JSON adapter and join fixtures above.

-- varchar -> text is a binary-compatible RelabelType around the generic
-- Param. Both the display binding and execution retain that boundary.
CREATE TABLE query_offload_explain_labels (label text COLLATE "C") USING iceberg;
INSERT INTO query_offload_explain_labels VALUES ('east'), ('west'), ('east');
SET plan_cache_mode = force_generic_plan;
PREPARE query_offload_explain_relabel (varchar) AS
SELECT count(*) FROM query_offload_explain_labels WHERE label = $1;
SELECT plan #>> '{0,Plan,Custom Plan Provider}' AS provider,
       jsonb_path_query_first(plan,
           '$[0].Plan.** ? (@."Node Type" == "Table Scan")')->>'Filter'
           LIKE '%$1%' AS has_binding
FROM query_offload_explain_json(
    'VERBOSE, COSTS OFF',
    'EXECUTE query_offload_explain_relabel(''east'')'
) AS document(plan);
EXECUTE query_offload_explain_relabel('east');
EXECUTE query_offload_explain_relabel(NULL);
DEALLOCATE query_offload_explain_relabel;
RESET plan_cache_mode;
DROP TABLE query_offload_explain_labels;

-- Identical PARAM_EXTERN expressions in the main query and lifted NOT IN
-- SubPlan belong to separate PlannerInfo scopes and binding slots. Both
-- displays must use their own slot rather than the first structural match.
SET plan_cache_mode = force_generic_plan;
PREPARE query_offload_explain_scoped (int) AS
SELECT l.id FROM query_offload_explain_left AS l
WHERE l.id >= $1 AND l.id NOT IN (
    SELECT scoped.id FROM query_offload_explain_left AS scoped
    WHERE scoped.id > $1
) ORDER BY l.id;
SELECT (scan->>'Filter') LIKE '%id > $2%' AS scoped_filter,
       (scan->>'Pushed Filter Conservative') LIKE '%id > $2%' AS scoped_pruning
FROM query_offload_explain_json(
    'VERBOSE, COSTS OFF', 'EXECUTE query_offload_explain_scoped(2)'
) AS document(plan)
CROSS JOIN LATERAL jsonb_path_query_first(plan,
    '$[0].Plan.** ? (@."Node Type" == "Table Scan" && @."Alias" == "scoped")') AS scan;
EXECUTE query_offload_explain_scoped(2);
EXECUTE query_offload_explain_scoped(1);
DEALLOCATE query_offload_explain_scoped;
RESET plan_cache_mode;

-- PostgreSQL pulls up this LATERAL subquery. The unparameterized offload
-- becomes a Nested Loop inner and must restart its null-aware hash join on
-- each rescan. Its lifted SubPlan's local column stays a column in EXPLAIN.
CREATE TEMP TABLE query_offload_explain_outer (id integer);
INSERT INTO query_offload_explain_outer VALUES (1), (2);
SET enable_hashjoin = off;
SET enable_mergejoin = off;
SET enable_material = off;
SELECT (scan->>'Filter') LIKE '%id > 2%'
           AND (scan->>'Filter') NOT LIKE '%$%' AS scoped_filter,
       (scan->>'Pushed Filter Conservative') LIKE '%id > 2%'
           AND (scan->>'Pushed Filter Conservative') NOT LIKE '%$%' AS scoped_pruning,
       (jsonb_path_query_first(plan,
           '$[0].Plan.** ? (@."Custom Plan Provider" == "LagoDB Query Offload")')
           ->>'Actual Loops')::int > 1 AS rescanned,
       (SELECT sum((node->>'Metric: planned_files')::int)
        FROM jsonb_path_query(plan,
            'strict $[0].Plan.** ? (@."Node Type" == "ExternalTableScanExec")') AS node)
       = (SELECT sum((node->>'Data Files Selected')::int)
          FROM jsonb_path_query(plan,
              'strict $[0].Plan.** ? (@."Node Type" == "Table Scan")') AS node)
           AS scan_metrics_match
FROM query_offload_explain_json(
    'ANALYZE, VERBOSE, COSTS OFF, TIMING OFF, SUMMARY OFF',
    'SELECT nested.id FROM query_offload_explain_outer AS o
     CROSS JOIN LATERAL (
         SELECT l.id FROM query_offload_explain_left AS l
         JOIN query_offload_explain_right AS r ON l.key = r.key
         WHERE l.id = o.id AND l.id NOT IN (
             SELECT scoped.id FROM query_offload_explain_left AS scoped
             WHERE scoped.id > 2
         )
     ) AS nested ORDER BY nested.id'
) AS document(plan)
CROSS JOIN LATERAL jsonb_path_query_first(plan,
    '$[0].Plan.** ? (@."Node Type" == "Table Scan" && @."Alias" == "scoped")') AS scan;
SELECT nested.id FROM query_offload_explain_outer AS o
CROSS JOIN LATERAL (
    SELECT l.id FROM query_offload_explain_left AS l
    JOIN query_offload_explain_right AS r ON l.key = r.key
    WHERE l.id = o.id AND l.id NOT IN (
        SELECT scoped.id FROM query_offload_explain_left AS scoped
        WHERE scoped.id > 2
    )
) AS nested ORDER BY nested.id;
RESET enable_hashjoin;
RESET enable_mergejoin;
RESET enable_material;
DROP TABLE query_offload_explain_outer;

DROP FUNCTION query_offload_explain_json(text, text);
DROP TABLE query_offload_explain_right;
DROP TABLE query_offload_explain_left;

RESET lagodb.query_batch_rows;
RESET lagodb.query_offload_mode;
RESET lagodb.customscan_mode;
