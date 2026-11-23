-- query_offload_spill.sql
-- Query offload operators that outgrow work_mem spill to PostgreSQL temporary
-- files and return the rows PostgreSQL returns, as in aggregates.sql.

DROP EXTENSION IF EXISTS lagodb_iceberg CASCADE;
CREATE EXTENSION IF NOT EXISTS lagodb_iceberg;

CREATE TABLE query_offload_spill_fact (
    id integer,
    key integer,
    payload text
) USING iceberg;

CREATE TABLE query_offload_spill_dim (
    key integer,
    label text
) USING iceberg;

CREATE TABLE query_offload_spill_skew (
    key integer,
    note text
) USING iceberg;

CREATE TABLE query_offload_spill_resplit (
    key integer,
    payload text
) USING iceberg;

-- Thirty-three data files form several source work groups, so the same table
-- also serves the parallel case (see query_offload_parallel.sql).
\set ECHO none
SELECT format(
    'INSERT INTO query_offload_spill_fact '
    'SELECT g, g %% 20000, repeat(''x'', 40) || g '
    'FROM generate_series(%s, %s) AS g',
    part * 1200 + 1, (part + 1) * 1200)
FROM generate_series(0, 32) AS part
\gexec
\set ECHO all

-- Keys 20000 and up have no fact rows.
INSERT INTO query_offload_spill_dim
SELECT g, 'dim-' || g
FROM generate_series(0, 24999) AS g;

-- One hot key, then ten rows whose key is NULL.
INSERT INTO query_offload_spill_skew
SELECT CASE WHEN g <= 12000 THEN 7 END,
       CASE WHEN g % 2 = 0 THEN 'even' ELSE 'odd' END
FROM generate_series(1, 12010) AS g;

-- 256 keys of one 42496-byte row each. At a 128kB hash_mem one key's row
-- fits a hash table and two keys' rows do not.
INSERT INTO query_offload_spill_resplit
SELECT g, repeat(md5(g::text), 1328)
FROM generate_series(1, 256) AS g;

-- PostgreSQL still owns the Sort above the offloaded join, so its plan must
-- not depend on whether autovacuum has analyzed these tables yet.
ANALYZE query_offload_spill_fact;
ANALYZE query_offload_spill_dim;
ANALYZE query_offload_spill_skew;
ANALYZE query_offload_spill_resplit;

-- As in join_hash.sql, EXPLAIN JSON keeps each metric on its own plan node.
CREATE FUNCTION pg_temp.explain_json(query text) RETURNS jsonb
LANGUAGE plpgsql AS $$
DECLARE
    plan jsonb;
BEGIN
    EXECUTE 'EXPLAIN (ANALYZE, VERBOSE, COSTS OFF, TIMING OFF, SUMMARY OFF, FORMAT JSON) '
        || query INTO plan;
    RETURN plan;
END;
$$;

-- PostgreSQL's results are the reference.
CREATE TEMP TABLE spill_agg_pg AS
SELECT id, count(*) AS n, sum(key) AS total
FROM query_offload_spill_fact
GROUP BY id;

CREATE TEMP TABLE spill_join_pg AS
SELECT d.key, d.label, f.id
FROM query_offload_spill_dim AS d
LEFT JOIN query_offload_spill_fact AS f ON f.key = d.key;

CREATE TEMP TABLE spill_parallel_pg AS
SELECT f.key, f.id, d.label
FROM query_offload_spill_fact AS f
JOIN query_offload_spill_dim AS d ON d.key = f.key;

CREATE TEMP TABLE spill_resplit_pg AS
SELECT a.key, length(a.payload) + length(b.payload) AS width
FROM query_offload_spill_resplit AS a
JOIN query_offload_spill_resplit AS b ON b.key = a.key;

SET lagodb.query_offload_mode = 'force';
SET work_mem = '64kB';

-- A Sort whose input batch alone exceeds its share spills runs instead of
-- failing. The ORDER BY Sort is the one above the join.
SELECT jsonb_path_exists(pg_temp.explain_json($$
SELECT f.id, f.key, f.payload
FROM query_offload_spill_fact AS f
JOIN query_offload_spill_dim AS d ON d.key = f.key
ORDER BY f.key DESC, f.id
OFFSET 39595
$$), '$.** ? (@."Node Type" == "SortExec"
             && exists(@.Plans.** ? (@."Node Type" == "AdaptiveHashJoinExec"))
             && @."Metric: spill_count" > 0)')
       AS sort_spilled;

SELECT f.id, f.key, f.payload
FROM query_offload_spill_fact AS f
JOIN query_offload_spill_dim AS d ON d.key = f.key
ORDER BY f.key DESC, f.id
OFFSET 39595;

-- A hash join whose build side exceeds hash_mem continues as a hybrid hash
-- join: both inputs are split into spilled bucket runs and joined one group of
-- buckets at a time, keeping the null-extended rows of an outer join.
SELECT jsonb_path_exists(pg_temp.explain_json($$
SELECT d.key, d.label, f.id
FROM query_offload_spill_dim AS d
LEFT JOIN query_offload_spill_fact AS f ON f.key = d.key
$$), '$.** ? (@."Node Type" == "AdaptiveHashJoinExec"
             && @."Metric: fallback_partitions" > 0
             && @."Metric: spill_count" > 0
             && @."Metric: sort_merge_fallback_groups" == 0)')
       AS hybrid_join;

CREATE TEMP TABLE spill_join_offload AS
SELECT d.key, d.label, f.id
FROM query_offload_spill_dim AS d
LEFT JOIN query_offload_spill_fact AS f ON f.key = d.key;

-- Semi, anti and mark joins over the same overflowing inputs join group by
-- group too.
SELECT count(*), sum(d.key)
FROM query_offload_spill_dim AS d
WHERE EXISTS (SELECT 1 FROM query_offload_spill_fact AS f WHERE f.key = d.key);

SELECT count(*), sum(d.key)
FROM query_offload_spill_dim AS d
WHERE NOT EXISTS (SELECT 1 FROM query_offload_spill_fact AS f WHERE f.key = d.key);

SELECT count(*), sum(d.key)
FROM query_offload_spill_dim AS d
WHERE d.key IS NULL OR d.key IN (SELECT f.key FROM query_offload_spill_fact AS f);

-- NOT IN hashes its subquery, as PostgreSQL's hashed SubPlan does, so the
-- overflowing subquery is the input the join splits.
SELECT jsonb_path_exists(pg_temp.explain_json($$
SELECT d.key
FROM query_offload_spill_dim AS d
WHERE d.key NOT IN (SELECT f.key FROM query_offload_spill_fact AS f)
$$), '$.** ? (@."Node Type" == "NullSensitiveAntiJoinExec"
             && exists(@.Plans.** ? (@."Node Type" == "AdaptiveHashJoinExec"
                                     && @."Metric: fallback_partitions" > 0)))')
       AS not_in_spilled;

SELECT count(*), sum(d.key)
FROM query_offload_spill_dim AS d
WHERE d.key NOT IN (SELECT f.key FROM query_offload_spill_fact AS f);

-- An outer side within work_mem never spills: the groups after the first read
-- its rows from memory, so only the subquery's rows are written.
SELECT jsonb_path_exists(pg_temp.explain_json($$
SELECT d.key
FROM query_offload_spill_dim AS d
WHERE d.key >= 24900
  AND d.key NOT IN (SELECT f.key FROM query_offload_spill_fact AS f)
$$), '$.** ? (@."Node Type" == "AdaptiveHashJoinExec"
             && @."Metric: fallback_partitions" > 0
             && @."Metric: spilled_rows" == 39600)')
       AS outer_kept_in_memory;

SELECT count(*), sum(d.key)
FROM query_offload_spill_dim AS d
WHERE d.key >= 24900
  AND d.key NOT IN (SELECT f.key FROM query_offload_spill_fact AS f);

-- x IS NULL OR x NOT IN keeps only the outer rows whose key is NULL when the
-- subquery has a NULL key, and adds them to the unmatched rows when it has none.
SELECT count(*)
FROM query_offload_spill_skew AS s
WHERE s.key IS NULL
   OR s.key NOT IN (SELECT other.key FROM query_offload_spill_skew AS other);

SELECT count(*)
FROM query_offload_spill_skew AS s
WHERE s.key IS NULL
   OR s.key NOT IN (SELECT d.key FROM query_offload_spill_dim AS d WHERE d.key <> 7);

-- A LIMIT stops reading the join before every group has joined.
SELECT count(*) FROM (
    SELECT f.id
    FROM query_offload_spill_fact AS f
    JOIN query_offload_spill_dim AS d ON d.key = f.key
    LIMIT 100
) AS limited;

-- One hot key heavier than hash_mem, which no further split of its bucket
-- separates, joins by sort-merge while the other buckets keep hashing. The
-- FULL JOIN filter and the NULL keys leave rows of both sides unmatched.
SELECT jsonb_path_exists(pg_temp.explain_json($$
SELECT s.key, s.note, d.label
FROM query_offload_spill_skew AS s
FULL JOIN query_offload_spill_dim AS d ON d.key = s.key AND s.note = 'even'
$$), '$.** ? (@."Node Type" == "AdaptiveHashJoinExec"
             && @."Metric: sort_merge_fallback_groups" > 0)')
       AS skew_sort_merged;

SELECT count(*) AS joined_rows, count(s.key) AS skew_rows, count(d.key) AS dim_rows
FROM query_offload_spill_skew AS s
FULL JOIN query_offload_spill_dim AS d ON d.key = s.key AND s.note = 'even';

-- Two keys whose rows share a bucket only together outgrow hash_mem, so the
-- bucket is split again by the next hash bits and every group still hashes.
-- 256 keys over 4096 buckets leave no bucket with two keys about once in 3000
-- bucket hashes, and a split that leaves two keys together about once in 500;
-- resplit_buckets and sort_merge_fallback_groups tell those apart from a
-- wrong join.
SET work_mem = '128kB';
SET hash_mem_multiplier = 1.0;
SELECT jsonb_path_exists(pg_temp.explain_json($$
SELECT a.key, length(a.payload) + length(b.payload) AS width
FROM query_offload_spill_resplit AS a
JOIN query_offload_spill_resplit AS b ON b.key = a.key
$$), '$.** ? (@."Node Type" == "AdaptiveHashJoinExec"
             && @."Metric: resplit_buckets" > 0
             && @."Metric: sort_merge_fallback_groups" == 0)')
       AS resplit_join;

CREATE TEMP TABLE spill_resplit_offload AS
SELECT a.key, length(a.payload) + length(b.payload) AS width
FROM query_offload_spill_resplit AS a
JOIN query_offload_spill_resplit AS b ON b.key = a.key;
RESET hash_mem_multiplier;
SET work_mem = '64kB';

-- With the hybrid join off, the same overflow sorts both inputs into a
-- sort-merge join, NOT IN's included.
SET lagodb.query_offload_batched_hash_join = off;
SELECT jsonb_path_exists(pg_temp.explain_json($$
SELECT d.key, d.label, f.id
FROM query_offload_spill_dim AS d
LEFT JOIN query_offload_spill_fact AS f ON f.key = d.key
$$), '$.** ? (@."Node Type" == "AdaptiveHashJoinExec"
             && @."Metric: fallback_partitions" > 0
             && !(exists(@."Metric: sort_merge_fallback_groups")))')
       AS sort_merge_join;

SELECT count(*) AS joined_rows, count(f.id) AS fact_rows, sum(f.id) AS fact_id_sum
FROM query_offload_spill_dim AS d
LEFT JOIN query_offload_spill_fact AS f ON f.key = d.key;
SELECT count(*), sum(d.key)
FROM query_offload_spill_dim AS d
WHERE d.key NOT IN (SELECT f.key FROM query_offload_spill_fact AS f);
RESET lagodb.query_offload_batched_hash_join;

-- Parallel execution spills under the same work_mem and returns the
-- PostgreSQL rows. Worker metrics are merged into the reported plan.
SET lagodb.query_offload_parallel_min_scan_rows = 0;
SELECT plan #>> '{0,Plan,Execution Mode}' AS execution_mode,
       jsonb_path_exists(plan, '$.** ? (@."Metric: spill_count" > 0)') AS spilled
FROM pg_temp.explain_json($$
SELECT f.key, f.id, d.label
FROM query_offload_spill_fact AS f
JOIN query_offload_spill_dim AS d ON d.key = f.key
$$) AS document(plan);

CREATE TEMP TABLE spill_parallel_offload AS
SELECT f.key, f.id, d.label
FROM query_offload_spill_fact AS f
JOIN query_offload_spill_dim AS d ON d.key = f.key;
-- The NOT IN join reads every probe partition as one stream, so no
-- round-robin repartition splits its probe below it.
SELECT plan #>> '{0,Plan,Execution Mode}' AS execution_mode,
       jsonb_path_exists(plan, '$.** ? (@."Metric: spill_count" > 0)') AS spilled,
       NOT jsonb_path_exists(plan, '$.** ? (@."Node Type" == "NullSensitiveAntiJoinExec")
           .** ? (@."Node Type" == "HashJoinExec").Plans[1]
           .** ? (@."Details" like_regex "RoundRobinBatch")') AS probe_unsplit
FROM pg_temp.explain_json($$
SELECT d.key
FROM query_offload_spill_dim AS d
WHERE d.key NOT IN (SELECT f.key FROM query_offload_spill_fact AS f)
$$) AS document(plan);
SELECT count(*), sum(d.key)
FROM query_offload_spill_dim AS d
WHERE d.key NOT IN (SELECT f.key FROM query_offload_spill_fact AS f);
RESET lagodb.query_offload_parallel_min_scan_rows;

-- Known limitation (apache/datafusion#25851): an aggregate writes its spill
-- runs in batches of rows, not bytes. Each run here holds fewer groups than
-- one batch, so it is a single batch about the size of hash_mem, and reading
-- it back needs more than hash_mem. The query fails with out of memory where
-- PostgreSQL would continue past its soft work_mem.
\set VERBOSITY sqlstate
SELECT count(*), sum(n), sum(total) FROM (
    SELECT key, count(*) AS n, sum(id) AS total
    FROM query_offload_spill_fact
    GROUP BY key
) AS grouped;
\set VERBOSITY default

-- With spilling off, the same aggregate keeps everything in memory.
SET lagodb.query_offload_spill = off;
SELECT NOT jsonb_path_exists(pg_temp.explain_json($$
SELECT key, count(*), sum(id)
FROM query_offload_spill_fact
GROUP BY key
$$), '$.** ? (@."Metric: spill_count" > 0)')
       AS not_spilled;
RESET lagodb.query_offload_spill;

-- Spill files are PostgreSQL temporary files, so temp_file_limit bounds them.
-- PostgreSQL 18 quotes the setting name in this error, so only its SQLSTATE
-- is shown.
SET temp_file_limit = '64kB';
\set VERBOSITY sqlstate
SELECT count(*) FROM (
    SELECT key, count(*), sum(id)
    FROM query_offload_spill_fact
    GROUP BY key
) AS grouped;
\set VERBOSITY default
RESET temp_file_limit;

-- A grouped aggregate larger than hash_mem spills and reads its runs back.
-- At this work_mem each run holds more groups than one batch of rows, so its
-- batches stay well within hash_mem. The session is still usable after the
-- failed spill above.
SET work_mem = '512kB';
SELECT count(*) FROM (
    SELECT key, count(*), sum(id)
    FROM query_offload_spill_fact
    GROUP BY key
) AS grouped;

SELECT jsonb_path_exists(pg_temp.explain_json($$
SELECT id, count(*), sum(key)
FROM query_offload_spill_fact
GROUP BY id
$$), '$.** ? (@."Node Type" == "AggregateExec" && @."Metric: spill_count" > 0)')
       AS aggregate_spilled;

CREATE TEMP TABLE spill_agg_offload AS
SELECT id, count(*) AS n, sum(key) AS total
FROM query_offload_spill_fact
GROUP BY id;

RESET work_mem;
RESET lagodb.query_offload_mode;

-- Compare offload results to PostgreSQL results.
(SELECT * FROM spill_agg_offload EXCEPT SELECT * FROM spill_agg_pg)
  UNION ALL
(SELECT * FROM spill_agg_pg EXCEPT SELECT * FROM spill_agg_offload);

(SELECT * FROM spill_join_offload EXCEPT SELECT * FROM spill_join_pg)
  UNION ALL
(SELECT * FROM spill_join_pg EXCEPT SELECT * FROM spill_join_offload);

(SELECT * FROM spill_parallel_offload EXCEPT SELECT * FROM spill_parallel_pg)
  UNION ALL
(SELECT * FROM spill_parallel_pg EXCEPT SELECT * FROM spill_parallel_offload);

(SELECT * FROM spill_resplit_offload EXCEPT SELECT * FROM spill_resplit_pg)
  UNION ALL
(SELECT * FROM spill_resplit_pg EXCEPT SELECT * FROM spill_resplit_offload);

DROP FUNCTION pg_temp.explain_json(text);
DROP TABLE query_offload_spill_dim;
DROP TABLE query_offload_spill_skew;
DROP TABLE query_offload_spill_resplit;
DROP TABLE query_offload_spill_fact;
