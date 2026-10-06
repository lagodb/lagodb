-- Managed partition reads, typed pruning/pushdown, rescans, and native planning.
-- Managed file counts include both partition and column-metrics pruning.
-- The external FDW fixture separately disables column metrics.
DROP EXTENSION IF EXISTS lagodb_iceberg CASCADE;
CREATE EXTENSION lagodb_iceberg;
SET lagodb.query_offload_mode = 'off';
SET lagodb.customscan_mode = 'force';
SET max_parallel_workers_per_gather = 0;
CREATE SCHEMA partitioned_reads;

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

CREATE TEMP TABLE partition_read_cases (
    case_name text,
    sql_type text,
    first_value text,
    second_value text,
    partition_key text,
    comparison_pushes boolean
);
-- UUID identity partitions are rejected at DDL time; partitioned_table.sql
-- covers that boundary. Enable their read/write cases after upstream manifest
-- schema and literal encoding support Iceberg's fixed(16) UUID representation.
INSERT INTO partition_read_cases VALUES
    ('identity_bool', 'boolean', 'true', 'false', 'LIST (part_key)', false),
    ('identity_int2', 'smallint', '-7', '9', 'LIST (part_key)', true),
    ('identity_int4', 'integer', '-7', '9', 'LIST (part_key)', true),
    ('identity_int8', 'bigint', '9000000000', '9000000001', 'LIST (part_key)', true),
    ('identity_decimal', 'numeric(10,2)', '-12.34', '56.78', 'LIST (part_key)', true),
    ('identity_text', 'text COLLATE "C"', 'east', 'west', 'LIST (part_key)', true),
    ('identity_varchar', 'varchar(12) COLLATE "C"', 'east', 'west', 'LIST (part_key)', true),
    ('identity_date', 'date', '1969-12-31', '2024-01-02', 'LIST (part_key)', true),
    ('identity_time', 'time', '00:00:00.123456', '23:59:59.999999', 'LIST (part_key)', false),
    ('identity_timestamp', 'timestamp', '1969-12-31 23:59:59.123456', '2024-01-02 12:00:00', 'LIST (part_key)', true),
    ('identity_timestamptz', 'timestamptz', '2024-01-01 01:00:00+08', '2024-01-02 12:00:00-05', 'LIST (part_key)', true),
    ('identity_binary', 'bytea', '\x0001', '\x00ff', 'LIST (part_key)', false),
    ('range_date', 'date', '1969-12-31', '2024-01-02', 'RANGE (part_key)', true),
    ('timestamp_year', 'timestamp', '1969-12-31 23:59:59.123456', '2024-01-02 12:00:00', 'RANGE ((date_trunc(''year'', part_key)))', true),
    ('timestamp_month', 'timestamp', '1969-12-31 23:59:59.123456', '2024-01-02 12:00:00', 'RANGE ((date_trunc(''month'', part_key)))', true),
    ('timestamp_day', 'timestamp', '1969-12-31 23:59:59.123456', '2024-01-02 12:00:00', 'RANGE ((date_trunc(''day'', part_key)))', true),
    ('timestamp_hour', 'timestamp', '1969-12-31 23:59:59.123456', '2024-01-02 12:00:00', 'RANGE ((date_trunc(''hour'', part_key)))', true),
    ('timestamptz_year', 'timestamptz', '1969-12-31 23:30:00-02', '2024-01-02 12:00:00+08', 'RANGE ((date_trunc(''year'', part_key, ''UTC'')))', true),
    ('timestamptz_month', 'timestamptz', '1969-12-31 23:30:00-02', '2024-01-02 12:00:00+08', 'RANGE ((date_trunc(''month'', part_key, ''UTC'')))', true),
    ('timestamptz_day', 'timestamptz', '1969-12-31 23:30:00-02', '2024-01-02 12:00:00+08', 'RANGE ((date_trunc(''day'', part_key, ''UTC'')))', true),
    ('timestamptz_hour', 'timestamptz', '1969-12-31 23:30:00-02', '2024-01-02 12:00:00+08', 'RANGE ((date_trunc(''hour'', part_key, ''UTC'')))', true);

-- One unsorted fanout INSERT per table, including a repeated key and NULL.
-- The heap relation supplies the PostgreSQL type-conversion baseline.
SELECT format(
    'CREATE TABLE partitioned_reads.%1$I (id integer, part_key %2$s, payload text)
     PARTITION BY %3$s USING iceberg;
CREATE TABLE partitioned_reads.%4$I (id integer, part_key %2$s, payload text) USING heap;
INSERT INTO partitioned_reads.%1$I VALUES
    (1, %5$L::%2$s, ''first''), (2, %6$L::%2$s, ''keep2''),
    (0, %6$L::%2$s, ''keep0''), (3, %5$L::%2$s, ''keep3''),
    (5, %6$L::%2$s, ''drop''), (4, NULL, ''empty'');
INSERT INTO partitioned_reads.%4$I VALUES
    (1, %5$L::%2$s, ''first''), (2, %6$L::%2$s, ''keep2''),
    (0, %6$L::%2$s, ''keep0''), (3, %5$L::%2$s, ''keep3''),
    (5, %6$L::%2$s, ''drop''), (4, NULL, ''empty'');',
    case_name, sql_type, partition_key, case_name || '_heap', second_value, first_value)
FROM partition_read_cases ORDER BY case_name
\gexec

-- Display the actual transform/source identity, file count, and heap equality.
SELECT case_name,
       metadata->'partition-specs'->0->'fields'->0->>'transform' AS transform,
       metadata->'partition-specs'->0->'fields'->0->>'source-id' AS source_id,
       jsonb_path_query_first(plan,
           '$[0].Plan.** ? (@."Node Type" == "Custom Scan" && @."Custom Plan Provider" == "lagodb-iceberg")')
           ->>'Data Files Selected' AS files,
       rows IS NOT DISTINCT FROM heap_rows AS heap_match
FROM partition_read_cases
CROSS JOIN LATERAL pg_temp.partition_query_json(format(
    'EXPLAIN (ANALYZE, VERBOSE, COSTS OFF, TIMING OFF, SUMMARY OFF, FORMAT JSON)
     SELECT id FROM partitioned_reads.%I', case_name)) AS explained(plan)
CROSS JOIN LATERAL pg_temp.partition_query_json(format(
    'SELECT jsonb_agg(to_jsonb(t) ORDER BY id) FROM partitioned_reads.%I t', case_name)) AS results(rows)
CROSS JOIN LATERAL pg_temp.partition_query_json(format(
    'SELECT jsonb_agg(to_jsonb(t) ORDER BY id) FROM partitioned_reads.%I t', case_name || '_heap')) AS heap_results(heap_rows)
JOIN iceberg.iceberg_metadata ON relid = format('partitioned_reads.%I', case_name)::regclass
CROSS JOIN LATERAL (SELECT pg_read_file(metadata_location)::jsonb AS metadata) AS file
ORDER BY case_name;

-- Partition-key equality, an ordinary pushed column, and a PG residual compose.
-- In the selected partition's single file, id=0 passes the residual but fails
-- id >= 2; id=5 passes id >= 2 but fails the residual; only id=2 survives.
-- id=3 in the other partition passes both non-partition predicates.
-- As in PG17 gin.sql, LATERAL queries expose plan fields and actual results.
SELECT case_name, scan->>'Data Files Selected' AS files,
       strpos(pushed, 'part_key') > 0 AND strpos(pushed, ' = ') > 0 AS partition_pushed,
       strpos(pushed, 'id >= 2') > 0 AS id_pushed,
       coalesce(strpos(scan->>'Filter', 'length'), 0) > 0 AS pg_residual,
       ids
FROM partition_read_cases
CROSS JOIN LATERAL pg_temp.partition_query_json(format(
    'EXPLAIN (ANALYZE, VERBOSE, COSTS OFF, TIMING OFF, SUMMARY OFF, FORMAT JSON)
     SELECT id FROM partitioned_reads.%I
     WHERE part_key = %L::%s AND id >= 2 AND length(payload) = 5',
    case_name, first_value, sql_type)) AS explained(plan)
CROSS JOIN LATERAL jsonb_path_query_first(plan,
    '$[0].Plan.** ? (@."Node Type" == "Custom Scan" && @."Custom Plan Provider" == "lagodb-iceberg")') AS selected(scan)
CROSS JOIN LATERAL concat_ws(' ', scan #>> '{LagoDB Pushdown,Pushed Filter Exact}',
                                scan #>> '{LagoDB Pushdown,Pushed Filter Conservative}') AS filters(pushed)
CROSS JOIN LATERAL pg_temp.partition_query_json(format(
    'SELECT jsonb_agg(id ORDER BY id) FROM partitioned_reads.%I
     WHERE part_key = %L::%s AND id >= 2 AND length(payload) = 5',
    case_name, first_value, sql_type)) AS results(ids)
ORDER BY case_name;

-- Each admitted comparison and NULL test has its own visible expected row.
-- Temporal <> stays in PG (3 files); other <> retains the NULL file (2 files).
-- The other pushed comparisons select 1 file. IS NOT NULL selects 2 files.
WITH predicates AS (
    SELECT c.case_name, o.ordinal, o.operator,
           format('part_key %s %L::%s', o.operator, o.value, c.sql_type) AS predicate
    FROM partition_read_cases c
    CROSS JOIN LATERAL (VALUES
        (1, '=', first_value), (2, '<', second_value), (3, '<=', first_value),
        (4, '>', first_value), (5, '>=', second_value), (6, '<>', first_value)
    ) AS o(ordinal, operator, value)
    WHERE comparison_pushes
    UNION ALL
    SELECT c.case_name, o.ordinal, o.operator, 'part_key ' || o.operator
    FROM partition_read_cases c
    CROSS JOIN (VALUES (7, 'IS NULL'), (8, 'IS NOT NULL')) AS o(ordinal, operator)
)
SELECT case_name, operator, scan->>'Data Files Selected' AS files,
       strpos(pushed, 'part_key') > 0 AS partition_pushed,
       strpos(pushed, ' ' || operator || ' ') > 0
           OR (operator IN ('IS NULL', 'IS NOT NULL') AND strpos(pushed, operator) > 0) AS operator_pushed,
       ids, ids IS NOT DISTINCT FROM heap_ids AS heap_match
FROM predicates
CROSS JOIN LATERAL pg_temp.partition_query_json(format(
    'EXPLAIN (ANALYZE, VERBOSE, COSTS OFF, TIMING OFF, SUMMARY OFF, FORMAT JSON)
     SELECT id FROM partitioned_reads.%I WHERE %s', case_name, predicate)) AS explained(plan)
CROSS JOIN LATERAL jsonb_path_query_first(plan,
    '$[0].Plan.** ? (@."Node Type" == "Custom Scan" && @."Custom Plan Provider" == "lagodb-iceberg")') AS selected(scan)
CROSS JOIN LATERAL concat_ws(' ', scan #>> '{LagoDB Pushdown,Pushed Filter Exact}',
                                scan #>> '{LagoDB Pushdown,Pushed Filter Conservative}') AS filters(pushed)
CROSS JOIN LATERAL pg_temp.partition_query_json(format(
    'SELECT jsonb_agg(id ORDER BY id) FROM partitioned_reads.%I WHERE %s',
    case_name, predicate)) AS results(ids)
CROSS JOIN LATERAL pg_temp.partition_query_json(format(
    'SELECT jsonb_agg(id ORDER BY id) FROM partitioned_reads.%I WHERE %s',
    case_name || '_heap', predicate)) AS heap_results(heap_ids)
ORDER BY case_name, ordinal;

-- Generic plans must rebind exact integer and conservative date filters.
SET plan_cache_mode = force_generic_plan;
PREPARE partition_read_int(integer) AS
SELECT jsonb_agg(id ORDER BY id) FROM partitioned_reads.identity_int4 WHERE part_key = $1;
SELECT jsonb_path_exists(pg_temp.partition_query_json(
    'EXPLAIN (VERBOSE, COSTS OFF, FORMAT JSON) EXECUTE partition_read_int(-7)'),
    '$[0].Plan.** ? (@."Node Type" == "Custom Scan" && @."Custom Plan Provider" == "lagodb-iceberg")') AS generic_custom_scan;

EXECUTE partition_read_int(-7);

EXECUTE partition_read_int(9);

EXECUTE partition_read_int(NULL);

DEALLOCATE partition_read_int;
PREPARE partition_read_date(date) AS
SELECT jsonb_agg(id ORDER BY id) FROM partitioned_reads.range_date WHERE part_key = $1;
SELECT jsonb_path_exists(pg_temp.partition_query_json(
    'EXPLAIN (VERBOSE, COSTS OFF, FORMAT JSON) EXECUTE partition_read_date(DATE ''1969-12-31'')'),
    '$[0].Plan.** ? (@."Node Type" == "Custom Scan" && @."Custom Plan Provider" == "lagodb-iceberg")') AS generic_custom_scan;

EXECUTE partition_read_date(DATE '1969-12-31');

EXECUTE partition_read_date(DATE '2024-01-02');

EXECUTE partition_read_date(NULL);

DEALLOCATE partition_read_date;
RESET plan_cache_mode;
-- Repeated outer keys and NULL exercise parameterized LATERAL rescans.
SELECT wanted.ordinal, matched.id
FROM (VALUES (1, 9), (2, -7), (3, 9), (4, NULL)) AS wanted(ordinal, part_key)
CROSS JOIN LATERAL (
    SELECT id FROM partitioned_reads.identity_int4 AS t
    WHERE t.part_key = wanted.part_key OFFSET 0
) AS matched
ORDER BY wanted.ordinal, matched.id;

-- Owned roots still require the storage scan when optional CustomScan is off.
SET lagodb.customscan_mode = 'off';
SELECT jsonb_path_query_first(pg_temp.partition_query_json(
    'EXPLAIN (ANALYZE, VERBOSE, COSTS OFF, TIMING OFF, SUMMARY OFF, FORMAT JSON)
     SELECT id FROM partitioned_reads.identity_int4 WHERE part_key = -7'),
    '$[0].Plan.** ? (@."Node Type" == "Custom Scan" && @."Custom Plan Provider" == "lagodb-iceberg")')
    ->>'Data Files Selected' AS files;

SELECT id FROM partitioned_reads.identity_int4 WHERE part_key = -7 ORDER BY id;

DROP TABLE partition_read_cases;
SET client_min_messages = warning;
DROP SCHEMA partitioned_reads CASCADE;
RESET client_min_messages;
RESET max_parallel_workers_per_gather;
RESET lagodb.customscan_mode;

-- Provider-owned partitioned-table planning and native join costing.

CREATE SCHEMA partitioned_planning;

CREATE TABLE partitioned_planning.part_t (
    id integer,
    region text,
    label text
) PARTITION BY LIST (region) USING iceberg;
INSERT INTO partitioned_planning.part_t
VALUES (10, 'east', 'a'), (20, 'west', 'b'), (40, 'west', 'd');

-- Constraint exclusion returns no rows and leaves UPDATE/DELETE unchanged.
SELECT id FROM partitioned_planning.part_t WHERE FALSE;
UPDATE partitioned_planning.part_t SET label = 'unexpected' WHERE FALSE RETURNING id;

DELETE FROM partitioned_planning.part_t WHERE FALSE RETURNING id;

SELECT id, label FROM partitioned_planning.part_t ORDER BY id;

-- A native index's parameterized cost must not depend on whether the managed
-- partitioned table is sized before or after it in range-table order.
CREATE TABLE partitioned_planning.native_lookup (id integer PRIMARY KEY, payload text);
INSERT INTO partitioned_planning.native_lookup
SELECT id, 'native-' || id FROM generate_series(1, 10000) AS id;
ANALYZE partitioned_planning.native_lookup;
BEGIN;
SET LOCAL enable_seqscan = off;
SET LOCAL enable_bitmapscan = off;
SET LOCAL enable_indexonlyscan = off;
SET LOCAL enable_hashjoin = off;
SET LOCAL enable_mergejoin = off;

-- Compare original JSON costs without putting unstable estimates in expected.
SELECT native_index->>'Index Cond' IS NOT NULL AS native_first_parameterized,
       partitioned_index->>'Index Cond' IS NOT NULL AS partitioned_first_parameterized,
       native_index->'Total Cost' = partitioned_index->'Total Cost' AS costs_match
FROM pg_temp.partition_query_json(
    'EXPLAIN (FORMAT JSON) SELECT n.payload
     FROM partitioned_planning.native_lookup n JOIN partitioned_planning.part_t r
     ON n.id = r.id') AS native_first(plan)
CROSS JOIN pg_temp.partition_query_json(
    'EXPLAIN (FORMAT JSON) SELECT n.payload
     FROM partitioned_planning.part_t r JOIN partitioned_planning.native_lookup n
     ON n.id = r.id') AS partitioned_first(plan)
CROSS JOIN LATERAL jsonb_path_query_first(native_first.plan,
    '$[0].Plan.** ? (@."Node Type" == "Index Scan" && @."Alias" == "n")') AS native_scan(native_index)
CROSS JOIN LATERAL jsonb_path_query_first(partitioned_first.plan,
    '$[0].Plan.** ? (@."Node Type" == "Index Scan" && @."Alias" == "n")') AS partitioned_scan(partitioned_index);

ROLLBACK;
DROP TABLE partitioned_planning.native_lookup;

SET client_min_messages = warning;
DROP SCHEMA partitioned_planning CASCADE;
RESET client_min_messages;
RESET lagodb.query_offload_mode;

DROP FUNCTION pg_temp.partition_query_json(text);
