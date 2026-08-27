\i include/column_definitions.sql

-- Parquet RowFilter and ANALYZE coverage. Each query deliberately selects a
-- different expression shape accepted by the format filter policy.

SET client_min_messages = warning;

SELECT bucket AS lagodb_regress_bucket
FROM lagodb_regress.object_storage_fixture
\gset

SELECT format('s3://%s/lagodb-connectors/seed/parquet-prefix/',
              :'lagodb_regress_bucket') AS parquet_filter_path
\gset

CREATE FOREIGN TABLE lagodb_connectors_regress.parquet_filter
    (:parquet_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'parquet_filter_path', format 'parquet');

CREATE TABLE lagodb_connectors_regress.null_parameter_source (id integer);
INSERT INTO lagodb_connectors_regress.null_parameter_source VALUES (NULL);

-- Mirrored and ordinary integer comparisons are both evaluated by Parquet.
SELECT coalesce(string_agg(id::text, ',' ORDER BY id), '<none>') AS ids
FROM lagodb_connectors_regress.parquet_filter
WHERE 1 < id AND bigint_col < 0::bigint;

-- The ForeignScan reports provider-accepted predicates separately from local
-- residual quals. This plan assertion verifies that filter pushdown occurred.
EXPLAIN (COSTS OFF)
SELECT id
FROM lagodb_connectors_regress.parquet_filter
WHERE id = 1;

-- Boolean comparison, NULL tests, AND, OR, and NOT retain PostgreSQL's
-- three-valued logic inside the Arrow predicate.
SELECT coalesce(
           string_agg(inner_rel.id::text, ',' ORDER BY inner_rel.id),
           '<none>'
       ) AS ids
FROM (VALUES (true)) AS outer_rel(flag)
CROSS JOIN LATERAL (
    SELECT id
    FROM lagodb_connectors_regress.parquet_filter
    WHERE (NOT (smallint_col IS NULL) AND bool_col = outer_rel.flag)
       OR (smallint_col IS NULL AND bool_col <> outer_rel.flag)
    OFFSET 0
) AS inner_rel;

-- Equality is exact for deterministic collations; ordering is restricted to
-- byte-order C/POSIX collations.
SELECT coalesce(string_agg(id::text, ',' ORDER BY id), '<none>') AS ids
FROM lagodb_connectors_regress.parquet_filter
WHERE varchar_col = 'varchar-one'
   OR text_col COLLATE "C" > 'z' COLLATE "C";

-- A NULL runtime value must remain UNKNOWN rather than becoming a value or a
-- provider error. PostgreSQL WHERE semantics therefore return no rows.
SELECT coalesce(
           string_agg(inner_rel.id::text, ',' ORDER BY inner_rel.id),
           '<none>'
       ) AS ids
FROM lagodb_connectors_regress.null_parameter_source AS outer_rel
CROSS JOIN LATERAL (
    SELECT id
    FROM lagodb_connectors_regress.parquet_filter
    WHERE id = outer_rel.id
    OFFSET 0
) AS inner_rel;

-- Metadata pruning normalizes NOT to leaf operators. NOT UNKNOWN must remain
-- UNKNOWN rather than becoming TRUE when the runtime parameter is NULL.
SELECT coalesce(
           string_agg(inner_rel.id::text, ',' ORDER BY inner_rel.id),
           '<none>'
       ) AS ids
FROM lagodb_connectors_regress.null_parameter_source AS outer_rel
CROSS JOIN LATERAL (
    SELECT id
    FROM lagodb_connectors_regress.parquet_filter
    WHERE NOT (id = outer_rel.id)
    OFFSET 0
) AS inner_rel;

ANALYZE lagodb_connectors_regress.parquet_filter;

-- ANALYZE scans the complete object set for the population and persists its
-- compressed byte size as relpages. The fixture contains exactly two rows.
SELECT reltuples::bigint AS reltuples, relpages > 0 AS has_pages
FROM pg_class
WHERE oid = 'lagodb_connectors_regress.parquet_filter'::regclass;

CREATE FUNCTION lagodb_connectors_regress.explain_json(query text)
RETURNS jsonb
LANGUAGE plpgsql
AS $$
DECLARE
    plan text;
BEGIN
    EXECUTE 'EXPLAIN (FORMAT JSON) ' || query INTO plan;
    RETURN plan::jsonb;
END
$$;

-- Representative plans pin every supported predicate capability exercised
-- above. Exact filters have no local residual; conservative pruning retains
-- the original PostgreSQL Filter by contract.
WITH explained AS (
    SELECT lagodb_connectors_regress.explain_json(
        $$SELECT id
          FROM lagodb_connectors_regress.parquet_filter
          WHERE 1 < id AND bigint_col < 0::bigint$$
    ) AS value
)
SELECT value::text LIKE '%Pushed Filter%'
   AND value::text LIKE '%id > 1%'
   AND value::text LIKE '%bigint_col <%'
   AND value::text LIKE '%::bigint%'
   AND value::text NOT LIKE '%"Filter":%'
       AS mirrored_and_pushdown_complete
FROM explained;

WITH explained AS (
    SELECT lagodb_connectors_regress.explain_json(
        $$SELECT inner_rel.id
          FROM (VALUES (true)) AS outer_rel(flag)
          CROSS JOIN LATERAL (
              SELECT id
              FROM lagodb_connectors_regress.parquet_filter
              WHERE (NOT (smallint_col IS NULL) AND bool_col = outer_rel.flag)
                 OR (smallint_col IS NULL AND bool_col <> outer_rel.flag)
              OFFSET 0
          ) AS inner_rel$$
    ) AS value
)
SELECT value::text LIKE '%Pushed Filter%'
   AND (value::text LIKE '%smallint_col IS NOT NULL%'
        OR value::text LIKE '%NOT (smallint_col IS NULL)%')
   AND value::text LIKE '%smallint_col IS NULL%'
   AND value::text LIKE '%bool_col%'
   AND value::text LIKE '%"Filter":%'
       AS boolean_null_logic_pushdown_complete
FROM explained;

WITH explained AS (
    SELECT lagodb_connectors_regress.explain_json(
        $$SELECT id
          FROM lagodb_connectors_regress.parquet_filter
          WHERE varchar_col = 'varchar-one'
             OR text_col COLLATE "C" > 'z' COLLATE "C"$$
    ) AS value
)
SELECT value::text LIKE '%Pushed Filter%'
   AND value::text LIKE '%varchar_col =%'
   AND value::text LIKE '%text_col >%'
   AND value::text NOT LIKE '%"Filter":%'
       AS string_collation_pushdown_complete
FROM explained;

WITH explained AS (
    SELECT lagodb_connectors_regress.explain_json(
        $$SELECT inner_rel.id
          FROM lagodb_connectors_regress.null_parameter_source AS outer_rel
          CROSS JOIN LATERAL (
              SELECT id
              FROM lagodb_connectors_regress.parquet_filter
              WHERE id = outer_rel.id
              OFFSET 0
          ) AS inner_rel$$
    ) AS value
)
SELECT value::text LIKE '%Pushed Filter%'
   AND value::text LIKE '%id = $1%'
   AND value::text NOT LIKE '%"Filter":%'
       AS null_parameter_pushdown_complete
FROM explained;

WITH explained AS (
    SELECT lagodb_connectors_regress.explain_json(
        $$SELECT inner_rel.id
          FROM lagodb_connectors_regress.null_parameter_source AS outer_rel
          CROSS JOIN LATERAL (
              SELECT id
              FROM lagodb_connectors_regress.parquet_filter
              WHERE NOT (id = outer_rel.id)
              OFFSET 0
          ) AS inner_rel$$
    ) AS value
)
SELECT value::text LIKE '%Pushed Filter%'
   AND value::text LIKE '%id%'
   AND value::text LIKE '%$1%'
   AND value::text NOT LIKE '%"Filter":%'
       AS null_parameter_not_pushdown_complete
FROM explained;

-- Unsupported arithmetic remains solely a PostgreSQL local residual.
WITH explained AS (
    SELECT lagodb_connectors_regress.explain_json(
        $$SELECT id
          FROM lagodb_connectors_regress.parquet_filter
          WHERE id + 1 = 2$$
    ) AS value
)
SELECT value::text NOT LIKE '%Pushed Filter%'
   AND value::text LIKE '%"Filter":%'
       AS unsupported_expression_remains_local
FROM explained;

-- A parameterized ForeignScan must use its persisted predicate description;
-- ExplainForeignScan has no ancestor list for deparsing PARAM_EXEC expressions.
WITH explained AS (
    SELECT lagodb_connectors_regress.explain_json(
        'SELECT inner_rel.id
         FROM generate_series(1, 2) AS outer_rel(id)
         CROSS JOIN LATERAL (
             SELECT id
             FROM lagodb_connectors_regress.parquet_filter
             WHERE id = outer_rel.id
             OFFSET 0
         ) AS inner_rel'
    ) AS value
)
SELECT value::text LIKE '%Pushed Filter%'
   AND value::text LIKE '%$1%'
       AS parameterized_explain_reports_pushdown
FROM explained;

-- The planner consumes persisted stats and charges provider startup/filter
-- work; none of the former fixed 1000/32/zero values remain.
WITH explained AS (
    SELECT lagodb_connectors_regress.explain_json(
        'SELECT * FROM lagodb_connectors_regress.parquet_filter WHERE id = 1'
    ) AS value
), plan AS (
    SELECT value -> 0 -> 'Plan' AS value FROM explained
)
SELECT ((value ->> 'Plan Rows')::integer <> 1000)
   AND ((value ->> 'Plan Width')::integer <> 32)
   AND ((value ->> 'Startup Cost')::double precision > 0)
       AS planner_uses_analyze_stats
FROM plan;

-- Object-key order and per-file row order do not establish a global ordering,
-- so a requested ORDER BY must retain PostgreSQL's Sort node.
WITH explained AS (
    SELECT lagodb_connectors_regress.explain_json(
        'SELECT id FROM lagodb_connectors_regress.parquet_filter ORDER BY id'
    ) AS value
)
SELECT (value -> 0 -> 'Plan' ->> 'Node Type') = 'Sort'
       AS planner_retains_sort
FROM explained;

RESET client_min_messages;
