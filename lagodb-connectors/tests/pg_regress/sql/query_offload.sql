\i include/column_definitions.sql

-- Parquet query offload: declared-type projection, footer COUNT, predicate
-- pushdown, generic-plan parameters, parallel workers and FDW fallback.

SELECT bucket FROM lagodb_regress.object_storage_fixture \gset storage_
\set offload_base 's3://' :storage_bucket '/lagodb-connectors/query-offload/'
\pset null _null_

-- Two objects under one prefix; the declared table covers every scalar and
-- array type the Parquet contract admits.
\set offload_prefix :offload_base 'types/'
\set offload_part :offload_prefix 'part-a.parquet'
COPY (SELECT * FROM lagodb_connectors_regress.parquet_source WHERE id = 1)
TO :'offload_part'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');
\set offload_part :offload_prefix 'part-b.parquet'
COPY (SELECT * FROM lagodb_connectors_regress.parquet_source WHERE id <> 1)
TO :'offload_part'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');
CREATE FOREIGN TABLE lagodb_connectors_regress.offload_types (:parquet_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'offload_prefix', format 'parquet');

-- Thirty-three objects of 100 rows. Each object weighs the 4 MiB open cost
-- and a split group targets 128 MiB, so parallel execution has two groups.
\set offload_wide :offload_base 'wide/'
\set ECHO none
SELECT format(
    'COPY (SELECT g AS id, g %% 7 AS group_key, g * 10 AS measure, '
    '%L || g AS payload FROM generate_series(%s, %s) AS g) '
    'TO %L WITH (server %L, format %L)',
    'row-', part * 100 - 99, part * 100,
    :'offload_wide' || format('part-%s.parquet', lpad(part::text, 2, '0')),
    'lagodb_connectors_regress_s3', 'parquet')
FROM generate_series(1, 33) AS part
\gexec
\set ECHO all
CREATE FOREIGN TABLE lagodb_connectors_regress.offload_wide (
    id integer, group_key integer, measure integer, payload text COLLATE "C"
)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'offload_wide', format 'parquet');

SET lagodb.query_offload_mode = 'force';
SET lagodb.query_batch_rows = 2;

-- Offload starts at a join or an aggregate. Through a self-join every
-- declared column crosses the engine as its canonical type and back.
EXPLAIN (COSTS OFF)
SELECT t.*
FROM lagodb_connectors_regress.offload_types AS t
JOIN lagodb_connectors_regress.offload_types AS u USING (id)
ORDER BY id;
SELECT t.*
FROM lagodb_connectors_regress.offload_types AS t
JOIN lagodb_connectors_regress.offload_types AS u USING (id)
ORDER BY id;

-- A column-free aggregate reads footer row counts.
EXPLAIN (COSTS OFF)
SELECT count(*) AS rows FROM lagodb_connectors_regress.offload_wide;
SELECT count(*) AS rows FROM lagodb_connectors_regress.offload_wide;

-- Pushed predicates over each comparable type family.
EXPLAIN (COSTS OFF)
SELECT array_agg(id ORDER BY id) AS ids
FROM lagodb_connectors_regress.offload_types
WHERE date_col >= DATE '2024-01-01'
  AND timestamptz_col < TIMESTAMPTZ '2030-01-01 00:00:00+00';
SELECT array_agg(id ORDER BY id) AS ids
FROM lagodb_connectors_regress.offload_types
WHERE date_col >= DATE '2024-01-01'
  AND timestamptz_col < TIMESTAMPTZ '2030-01-01 00:00:00+00';
SELECT array_agg(id ORDER BY id) AS ids
FROM lagodb_connectors_regress.offload_types
WHERE text_col >= 'a' COLLATE "C";
SELECT array_agg(id ORDER BY id) AS ids
FROM lagodb_connectors_regress.offload_types
WHERE char_col = 'abc';
SELECT array_agg(id ORDER BY id) AS ids
FROM lagodb_connectors_regress.offload_types
WHERE smallint_col IS NULL OR bool_col IS NOT TRUE;
SELECT array_agg(id ORDER BY id) AS ids
FROM lagodb_connectors_regress.offload_types
WHERE numeric_col > 0 AND uuid_col = '00000000-0000-0000-0000-000000000001';
SELECT count(*) AS rows, sum(measure) AS total
FROM lagodb_connectors_regress.offload_wide
WHERE id BETWEEN 95 AND 105 AND payload >= 'row-100';

-- A generic parameter is bound per execution of one planned source.
SET plan_cache_mode = force_generic_plan;
PREPARE offload_by_id(integer) AS
SELECT count(*) AS rows, sum(measure) AS total
FROM lagodb_connectors_regress.offload_wide
WHERE id >= $1;
EXPLAIN (COSTS OFF) EXECUTE offload_by_id(3290);
EXECUTE offload_by_id(3290);
EXECUTE offload_by_id(1);
DEALLOCATE offload_by_id;
RESET plan_cache_mode;

-- Grouping and ordering run in one offloaded fragment.
EXPLAIN (COSTS OFF)
SELECT group_key, count(*) AS rows, sum(measure) AS total
FROM lagodb_connectors_regress.offload_wide
WHERE id > 100
GROUP BY group_key
ORDER BY group_key;
SELECT group_key, count(*) AS rows, sum(measure) AS total
FROM lagodb_connectors_regress.offload_wide
WHERE id > 100
GROUP BY group_key
ORDER BY group_key;

-- Parallel workers lock the relation, rebind the leader's listing and read
-- their split groups. The execution mode, worker count, and connector file
-- pruning count are stable in ANALYZE output.
CREATE FUNCTION pg_temp.offload_parallelism(query text) RETURNS SETOF text
LANGUAGE plpgsql AS $$
DECLARE
    ln text;
BEGIN
    FOR ln IN
        EXECUTE 'EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) '
            || query
    LOOP
        IF ln ~ 'Execution Mode|Launched Workers|Data Files Pruned' THEN
            RETURN NEXT btrim(ln);
        END IF;
    END LOOP;
END;
$$;
SET lagodb.query_offload_parallel_min_scan_rows = 0;
SELECT pg_temp.offload_parallelism($$
SELECT group_key, count(*), sum(measure)
FROM lagodb_connectors_regress.offload_wide
WHERE id > 10
GROUP BY group_key
$$);
SELECT group_key, count(*) AS rows, sum(measure) AS total
FROM lagodb_connectors_regress.offload_wide
WHERE id > 10
GROUP BY group_key
ORDER BY group_key;
RESET lagodb.query_offload_parallel_min_scan_rows;
DROP FUNCTION pg_temp.offload_parallelism(text);

-- A declaration without a columnar representation keeps the foreign scan.
CREATE FOREIGN TABLE lagodb_connectors_regress.offload_unconstrained (
    id integer, numeric_col numeric
)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'offload_prefix', format 'parquet');
EXPLAIN (COSTS OFF)
SELECT count(numeric_col) FROM lagodb_connectors_regress.offload_unconstrained;
DROP FOREIGN TABLE lagodb_connectors_regress.offload_unconstrained;

RESET lagodb.query_batch_rows;
RESET lagodb.query_offload_mode;
DROP FOREIGN TABLE lagodb_connectors_regress.offload_wide;
DROP FOREIGN TABLE lagodb_connectors_regress.offload_types;
