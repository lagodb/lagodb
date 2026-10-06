-- Parameterized joins, parameter binding, and nested-loop rescans.

-- Ordinary parameterized joins.

DROP EXTENSION IF EXISTS lagodb_iceberg CASCADE;
CREATE EXTENSION IF NOT EXISTS lagodb_iceberg;

-- Disable upper query offload to isolate relation-level CustomScan behavior.
SET lagodb.query_offload_mode = 'off';

-- A 5000-row lake and a small heap outer favor a parameterized inner CustomScan.
-- Matching integer types keep the join key free of coercions.
CREATE TABLE customscan_ord_lake (
    k integer,
    payload text
) USING iceberg;

-- File 1: k in [1, 2500]
INSERT INTO customscan_ord_lake
SELECT g, 'lake_' || g
FROM generate_series(1, 2500) AS g;

-- File 2: k in [10000, 12499]
INSERT INTO customscan_ord_lake
SELECT g, 'lake_' || g
FROM generate_series(10000, 12499) AS g;

SELECT COUNT(*) AS lake_total_rows FROM customscan_ord_lake;

-- Outer keys cover matches, a missing key, and NULL.
CREATE TABLE customscan_ord_outer (
    id integer,
    label text
);
INSERT INTO customscan_ord_outer
VALUES (1, 'one'), (2500, 'last'), (999999, 'no_match'), (NULL, 'null_row');

-- Disable competing joins and Materialize so each outer row rescans the lake.
SET enable_hashjoin = off;
SET enable_mergejoin = off;
SET enable_material = off;
SET enable_nestloop = on;

-- Check that the inner CustomScan pushes the join equality instead of leaving a
-- residual.
SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT o.id, o.label, l.k, l.payload
FROM customscan_ord_outer o
JOIN customscan_ord_lake l ON l.k = o.id
ORDER BY o.id, l.payload;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT o.id, o.label, l.k, l.payload
FROM customscan_ord_outer o
JOIN customscan_ord_lake l ON l.k = o.id
ORDER BY o.id, l.payload;

-- Each outer key must bind independently; missing and NULL keys produce no rows.

SET lagodb.customscan_mode = 'force';
SELECT o.id, o.label, l.k, l.payload
FROM customscan_ord_outer o
JOIN customscan_ord_lake l ON l.k = o.id
ORDER BY o.id, l.payload;

SET lagodb.customscan_mode = 'off';
SELECT o.id, o.label, l.k, l.payload
FROM customscan_ord_outer o
JOIN customscan_ord_lake l ON l.k = o.id
ORDER BY o.id, l.payload;

RESET enable_hashjoin;
RESET enable_mergejoin;
RESET enable_material;
RESET enable_nestloop;
RESET lagodb.customscan_mode;

DROP TABLE customscan_ord_lake;
DROP TABLE customscan_ord_outer;
-- Join parameters from multiple outer relations.


-- The lake has k2 = k. Two small outer tables supply independent integer keys to the
-- inner scan.
CREATE TABLE customscan_mo_lake (
    k integer,
    k2 integer,
    payload text
) USING iceberg;

-- File 1: k in [1, 2500], k2 = k
INSERT INTO customscan_mo_lake
SELECT g, g, 'lake_' || g
FROM generate_series(1, 2500) AS g;

-- File 2: k in [10000, 12499], k2 = k
INSERT INTO customscan_mo_lake
SELECT g, g, 'lake_' || g
FROM generate_series(10000, 12499) AS g;

SELECT COUNT(*) AS lake_total_rows FROM customscan_mo_lake;

-- Join the outer tables by grp so their lake keys remain in separate equivalence
-- classes.
CREATE TABLE customscan_mo_o1 (
    id integer,
    grp text,
    label text
);
INSERT INTO customscan_mo_o1
VALUES (1, 'A', 'o1_one'), (2500, 'A', 'o1_last'),
       (999999, 'B', 'o1_none'), (NULL, 'C', 'o1_null');

-- The second outer relation supplies the independent k2 parameter.
CREATE TABLE customscan_mo_o2 (
    id2 integer,
    grp text,
    tag text
);
INSERT INTO customscan_mo_o2
VALUES (1, 'A', 'o2_one'), (2500, 'A', 'o2_last'), (7777, 'B', 'o2_none');

-- Keep the lake on the rescanned inner side of a nested loop.
SET enable_hashjoin = off;
SET enable_mergejoin = off;
SET enable_material = off;
SET enable_nestloop = on;

-- Check that a join key reaches the inner pushed filter; the other remains a join
-- filter.
SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT o1.id, o2.id2, l.k, l.k2, l.payload
FROM customscan_mo_o1 o1
JOIN customscan_mo_o2 o2 ON o1.grp = o2.grp
JOIN customscan_mo_lake l ON l.k = o1.id AND l.k2 = o2.id2
ORDER BY o1.id, o2.id2, l.k;

-- The grp join produces a 2x2 key cross product. Only diagonal pairs match k2 = k,
-- detecting bindings taken from the wrong outer relation.
SELECT o1.id, o2.id2, l.k, l.k2, l.payload
FROM customscan_mo_o1 o1
JOIN customscan_mo_o2 o2 ON o1.grp = o2.grp
JOIN customscan_mo_lake l ON l.k = o1.id AND l.k2 = o2.id2
ORDER BY o1.id, o2.id2, l.k;

SET lagodb.customscan_mode = 'off';
SELECT o1.id, o2.id2, l.k, l.k2, l.payload
FROM customscan_mo_o1 o1
JOIN customscan_mo_o2 o2 ON o1.grp = o2.grp
JOIN customscan_mo_lake l ON l.k = o1.id AND l.k2 = o2.id2
ORDER BY o1.id, o2.id2, l.k;

RESET enable_hashjoin;
RESET enable_mergejoin;
RESET enable_material;
RESET enable_nestloop;
RESET lagodb.customscan_mode;

DROP TABLE customscan_mo_lake;
DROP TABLE customscan_mo_o1;
DROP TABLE customscan_mo_o2;
-- Plain and parameterized path selection.


CREATE TABLE customscan_var_lake (
    k integer,
    payload text
) USING iceberg;

-- File 1: k ∈ [1, 10]
INSERT INTO customscan_var_lake
SELECT g, 'lake_' || g
FROM generate_series(1, 10) AS g;

-- File 2: k ∈ [100, 110]
INSERT INTO customscan_var_lake
SELECT g, 'lake_' || g
FROM generate_series(100, 110) AS g;

SELECT COUNT(*) AS lake_total_rows FROM customscan_var_lake;

-- A small heap outer drives the parameterized inner lake scan.
CREATE TABLE customscan_var_outer (
    id integer,
    label text
);
INSERT INTO customscan_var_outer VALUES (1, 'one'), (5, 'five'), (105, 'oneoh5');

-- Use a nested loop without Materialize to exercise rescans.
SET enable_hashjoin = off;
SET enable_mergejoin = off;
SET enable_material = off;
SET enable_nestloop = on;

-- The expression join key cannot be pushed; only k >= 0 is pushed. OFFSET 0 preserves
-- the lateral clause as an inner residual.

-- Check the pushed base restriction and the residual join expression.
SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT o.id, o.label, l.k, l.payload
FROM customscan_var_outer o
CROSS JOIN LATERAL (
    SELECT k, payload
    FROM customscan_var_lake l
    WHERE l.k >= 0
      AND (l.k + 1) = o.id
    OFFSET 0
) l
ORDER BY o.id, l.k, l.payload;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT o.id, o.label, l.k, l.payload
FROM customscan_var_outer o
CROSS JOIN LATERAL (
    SELECT k, payload
    FROM customscan_var_lake l
    WHERE l.k >= 0
      AND (l.k + 1) = o.id
    OFFSET 0
) l
ORDER BY o.id, l.k, l.payload;

-- Outer keys 5 and 105 match lake keys 4 and 104; outer key 1 has no match.
SET lagodb.customscan_mode = 'force';
SELECT o.id, o.label, l.k, l.payload
FROM customscan_var_outer o
CROSS JOIN LATERAL (
    SELECT k, payload
    FROM customscan_var_lake l
    WHERE l.k >= 0
      AND (l.k + 1) = o.id
    OFFSET 0
) l
ORDER BY o.id, l.k, l.payload;

SET lagodb.customscan_mode = 'off';
SELECT o.id, o.label, l.k, l.payload
FROM customscan_var_outer o
CROSS JOIN LATERAL (
    SELECT k, payload
    FROM customscan_var_lake l
    WHERE l.k >= 0
      AND (l.k + 1) = o.id
    OFFSET 0
) l
ORDER BY o.id, l.k, l.payload;

-- An ordinary equijoin exercises recovery of a join equality from its equivalence
-- class.


-- Use a separate 5000-row lake to favor the parameterized inner path without changing
-- the expression-join fixture.
CREATE TABLE customscan_var_join_lake (
    k integer,
    payload text
) USING iceberg;

-- File 1: k ∈ [1, 2500]
INSERT INTO customscan_var_join_lake
SELECT g, 'lake_' || g
FROM generate_series(1, 2500) AS g;

-- File 2: k ∈ [10000, 12499]
INSERT INTO customscan_var_join_lake
SELECT g, 'lake_' || g
FROM generate_series(10000, 12499) AS g;

SELECT COUNT(*) AS join_lake_total_rows FROM customscan_var_join_lake;

-- Check that the ordinary join equality is pushed on the inner CustomScan.
SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT o.id, o.label, l.k, l.payload
FROM customscan_var_outer o
JOIN customscan_var_join_lake l ON l.k = o.id
ORDER BY o.id, l.payload;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT o.id, o.label, l.k, l.payload
FROM customscan_var_outer o
JOIN customscan_var_join_lake l ON l.k = o.id
ORDER BY o.id, l.payload;

-- Changing outer keys must rebuild the bound predicate instead of reusing the previous
-- key.
SET lagodb.customscan_mode = 'force';
SELECT o.id, o.label, l.k, l.payload
FROM customscan_var_outer o
JOIN customscan_var_join_lake l ON l.k = o.id
ORDER BY o.id, l.payload;

SET lagodb.customscan_mode = 'off';
SELECT o.id, o.label, l.k, l.payload
FROM customscan_var_outer o
JOIN customscan_var_join_lake l ON l.k = o.id
ORDER BY o.id, l.payload;

RESET enable_hashjoin;
RESET enable_mergejoin;
RESET enable_material;
RESET enable_nestloop;
RESET lagodb.customscan_mode;

DROP TABLE customscan_var_lake;
DROP TABLE customscan_var_join_lake;
DROP TABLE customscan_var_outer;

-- External and execution parameter binding.

DROP EXTENSION IF EXISTS lagodb_iceberg CASCADE;
CREATE EXTENSION IF NOT EXISTS lagodb_iceberg;

-- Use disjoint data files, including NULL ids, to expose stale parameter bindings and
-- incorrect pruning.
CREATE TABLE customscan_rescan_lake (
    id integer,
    payload text
) USING iceberg;

-- File 1: id ∈ [1, 50]
INSERT INTO customscan_rescan_lake
SELECT g, 'a_' || g
FROM generate_series(1, 50) AS g;

-- File 2: id ∈ [100, 150] with three NULLs interleaved (g ∈ {102, 119, 136}).
INSERT INTO customscan_rescan_lake
SELECT
    CASE WHEN g % 17 = 0 THEN NULL ELSE g END,
    'b_' || g
FROM generate_series(100, 150) AS g;

-- File 3: id ∈ [1000, 1050]
INSERT INTO customscan_rescan_lake
SELECT g, 'c_' || g
FROM generate_series(1000, 1050) AS g;

SELECT COUNT(*) AS lake_total_rows FROM customscan_rescan_lake;
SELECT COUNT(*) AS lake_null_rows
FROM customscan_rescan_lake WHERE id IS NULL;

-- Use separate prepared statements for force and off to keep each cached plan tied to
-- its mode.

PREPARE customscan_rescan_p1 (int) AS
SELECT id, payload
FROM customscan_rescan_lake
WHERE id = $1
ORDER BY id, payload;

PREPARE customscan_rescan_p1_baseline (int) AS
SELECT id, payload
FROM customscan_rescan_lake
WHERE id = $1
ORDER BY id, payload;

-- Compare prepared-statement plans with CustomScan enabled and disabled.
SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF) EXECUTE customscan_rescan_p1(25);

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF) EXECUTE customscan_rescan_p1_baseline(25);

-- Bound value lives in file 1.
SET lagodb.customscan_mode = 'force';
EXECUTE customscan_rescan_p1(25);
SET lagodb.customscan_mode = 'off';
EXECUTE customscan_rescan_p1_baseline(25);

-- Key 105 matches a non-NULL row in the NULL-bearing file.
SET lagodb.customscan_mode = 'force';
EXECUTE customscan_rescan_p1(105);
SET lagodb.customscan_mode = 'off';
EXECUTE customscan_rescan_p1_baseline(105);

-- Generated row 119 has a NULL id, so neither mode returns it for key 119.
SET lagodb.customscan_mode = 'force';
EXECUTE customscan_rescan_p1(119);
SET lagodb.customscan_mode = 'off';
EXECUTE customscan_rescan_p1_baseline(119);

-- Bound value lives in file 3.
SET lagodb.customscan_mode = 'force';
EXECUTE customscan_rescan_p1(1025);
SET lagodb.customscan_mode = 'off';
EXECUTE customscan_rescan_p1_baseline(1025);

-- Bound value matches NO row (gap between files 1 and 2).
SET lagodb.customscan_mode = 'force';
EXECUTE customscan_rescan_p1(75);
SET lagodb.customscan_mode = 'off';
EXECUTE customscan_rescan_p1_baseline(75);

-- Repeated executions let PostgreSQL consider a generic plan; results must still match
-- the baseline.
SET lagodb.customscan_mode = 'force';
EXECUTE customscan_rescan_p1(1);
EXECUTE customscan_rescan_p1(50);
EXECUTE customscan_rescan_p1(100);
EXECUTE customscan_rescan_p1(150);
EXECUTE customscan_rescan_p1(1000);
EXECUTE customscan_rescan_p1(1050);

-- Check the plan after repeated executions.
EXPLAIN (COSTS OFF) EXECUTE customscan_rescan_p1(1);

-- Compare results after repeated executions.
EXECUTE customscan_rescan_p1(1);
SET lagodb.customscan_mode = 'off';
EXECUTE customscan_rescan_p1_baseline(1);

SET lagodb.customscan_mode = 'force';
EXECUTE customscan_rescan_p1(1050);
SET lagodb.customscan_mode = 'off';
EXECUTE customscan_rescan_p1_baseline(1050);

DEALLOCATE customscan_rescan_p1;
DEALLOCATE customscan_rescan_p1_baseline;

-- LATERAL with OFFSET 0 keeps the lake on the rescanned inner side. Changing outer ids
-- must rebind PARAM_EXEC and rebuild pruning.


SET enable_hashjoin = off;
SET enable_mergejoin = off;
SET enable_material = off;
SET enable_nestloop = on;
-- Disable hash/merge joins and Materialize so every outer row reaches the inner rescan.

-- Compare the parameterized inner scan plans.
SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT outer_rel.id AS outer_id, lake.id AS lake_id, lake.payload
FROM (VALUES (1), (25), (100), (1000), (1050)) AS outer_rel(id)
CROSS JOIN LATERAL (
    SELECT id, payload
    FROM customscan_rescan_lake lake
    WHERE lake.id = outer_rel.id
    OFFSET 0
) lake
ORDER BY outer_rel.id;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT outer_rel.id AS outer_id, lake.id AS lake_id, lake.payload
FROM (VALUES (1), (25), (100), (1000), (1050)) AS outer_rel(id)
CROSS JOIN LATERAL (
    SELECT id, payload
    FROM customscan_rescan_lake lake
    WHERE lake.id = outer_rel.id
    OFFSET 0
) lake
ORDER BY outer_rel.id;

-- Every outer key matches one lake row.
SET lagodb.customscan_mode = 'force';
SELECT outer_rel.id AS outer_id, lake.id AS lake_id, lake.payload
FROM (VALUES (1), (25), (100), (1000), (1050)) AS outer_rel(id)
CROSS JOIN LATERAL (
    SELECT id, payload
    FROM customscan_rescan_lake lake
    WHERE lake.id = outer_rel.id
    OFFSET 0
) lake
ORDER BY outer_rel.id;

SET lagodb.customscan_mode = 'off';
SELECT outer_rel.id AS outer_id, lake.id AS lake_id, lake.payload
FROM (VALUES (1), (25), (100), (1000), (1050)) AS outer_rel(id)
CROSS JOIN LATERAL (
    SELECT id, payload
    FROM customscan_rescan_lake lake
    WHERE lake.id = outer_rel.id
    OFFSET 0
) lake
ORDER BY outer_rel.id;

-- A missing outer key must not reuse the preceding bound value.
SET lagodb.customscan_mode = 'force';
SELECT outer_rel.id AS outer_id, lake.id AS lake_id, lake.payload
FROM (VALUES (25), (75), (100)) AS outer_rel(id)
CROSS JOIN LATERAL (
    SELECT id, payload
    FROM customscan_rescan_lake lake
    WHERE lake.id = outer_rel.id
    OFFSET 0
) lake
ORDER BY outer_rel.id;

SET lagodb.customscan_mode = 'off';
SELECT outer_rel.id AS outer_id, lake.id AS lake_id, lake.payload
FROM (VALUES (25), (75), (100)) AS outer_rel(id)
CROSS JOIN LATERAL (
    SELECT id, payload
    FROM customscan_rescan_lake lake
    WHERE lake.id = outer_rel.id
    OFFSET 0
) lake
ORDER BY outer_rel.id;

-- Repeated outer keys still rescan the inner scan and must produce repeated results.

SET lagodb.customscan_mode = 'force';
SELECT outer_rel.id AS outer_id, lake.id AS lake_id, lake.payload
FROM (VALUES (1), (1), (25), (25), (1000), (1000)) AS outer_rel(id)
CROSS JOIN LATERAL (
    SELECT id, payload
    FROM customscan_rescan_lake lake
    WHERE lake.id = outer_rel.id
    OFFSET 0
) lake
ORDER BY outer_rel.id, lake.payload;

SET lagodb.customscan_mode = 'off';
SELECT outer_rel.id AS outer_id, lake.id AS lake_id, lake.payload
FROM (VALUES (1), (1), (25), (25), (1000), (1000)) AS outer_rel(id)
CROSS JOIN LATERAL (
    SELECT id, payload
    FROM customscan_rescan_lake lake
    WHERE lake.id = outer_rel.id
    OFFSET 0
) lake
ORDER BY outer_rel.id, lake.payload;

-- The pushed id = 25 predicate is constant; only the residual depends on the outer row.
-- Rescans must return correct rows without rebinding the pushed filter.

SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT outer_rel.unrelated, lake.id, lake.payload
FROM (VALUES (10), (20), (30)) AS outer_rel(unrelated)
CROSS JOIN LATERAL (
    SELECT id, payload
    FROM customscan_rescan_lake lake
    WHERE lake.id = 25
      AND (lake.id + outer_rel.unrelated) > 0
    OFFSET 0
) lake
ORDER BY outer_rel.unrelated, lake.id, lake.payload;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT outer_rel.unrelated, lake.id, lake.payload
FROM (VALUES (10), (20), (30)) AS outer_rel(unrelated)
CROSS JOIN LATERAL (
    SELECT id, payload
    FROM customscan_rescan_lake lake
    WHERE lake.id = 25
      AND (lake.id + outer_rel.unrelated) > 0
    OFFSET 0
) lake
ORDER BY outer_rel.unrelated, lake.id, lake.payload;

SET lagodb.customscan_mode = 'force';
SELECT outer_rel.unrelated, lake.id, lake.payload
FROM (VALUES (10), (20), (30)) AS outer_rel(unrelated)
CROSS JOIN LATERAL (
    SELECT id, payload
    FROM customscan_rescan_lake lake
    WHERE lake.id = 25
      AND (lake.id + outer_rel.unrelated) > 0
    OFFSET 0
) lake
ORDER BY outer_rel.unrelated, lake.id, lake.payload;

SET lagodb.customscan_mode = 'off';
SELECT outer_rel.unrelated, lake.id, lake.payload
FROM (VALUES (10), (20), (30)) AS outer_rel(unrelated)
CROSS JOIN LATERAL (
    SELECT id, payload
    FROM customscan_rescan_lake lake
    WHERE lake.id = 25
      AND (lake.id + outer_rel.unrelated) > 0
    OFFSET 0
) lake
ORDER BY outer_rel.unrelated, lake.id, lake.payload;

-- When a conservative parameter becomes unrepresentable, clear the previous storage
-- filter and let the residual evaluate the OR. A stale filter would hide the marker=2
-- row.
CREATE TABLE customscan_rescan_date_lake (
    d date,
    marker integer,
    payload text
) USING iceberg;
INSERT INTO customscan_rescan_date_lake VALUES
    (DATE '2024-01-01', 1, 'finite_1'),
    (DATE '2024-01-02', 2, 'finite_2'),
    (DATE '2024-01-03', 3, 'finite_3');
-- Fix DateStyle for stable date::text output.
SET DateStyle = 'ISO, MDY';

SET lagodb.customscan_mode = 'force';
SELECT outer_rel.marker AS outer_marker,
       outer_rel.d::text AS outer_d,
       lake.marker AS lake_marker,
       lake.d::text AS lake_d,
       lake.payload
FROM (VALUES (DATE '2024-01-01', 1), (DATE 'infinity', 2)) AS outer_rel(d, marker)
CROSS JOIN LATERAL (
    SELECT d, marker, payload
    FROM customscan_rescan_date_lake lake
    WHERE lake.d = outer_rel.d
       OR lake.marker = outer_rel.marker
    OFFSET 0
) lake
ORDER BY outer_marker, lake_marker, lake.payload;

SET lagodb.customscan_mode = 'off';
SELECT outer_rel.marker AS outer_marker,
       outer_rel.d::text AS outer_d,
       lake.marker AS lake_marker,
       lake.d::text AS lake_d,
       lake.payload
FROM (VALUES (DATE '2024-01-01', 1), (DATE 'infinity', 2)) AS outer_rel(d, marker)
CROSS JOIN LATERAL (
    SELECT d, marker, payload
    FROM customscan_rescan_date_lake lake
    WHERE lake.d = outer_rel.d
       OR lake.marker = outer_rel.marker
    OFFSET 0
) lake
ORDER BY outer_marker, lake_marker, lake.payload;

DROP TABLE customscan_rescan_date_lake;

RESET enable_hashjoin;
RESET enable_mergejoin;
RESET enable_material;
RESET enable_nestloop;
RESET lagodb.customscan_mode;

DROP TABLE customscan_rescan_lake;
-- Keep PARAM_EXTERN and PARAM_EXEC bindings independent even when their numeric IDs
-- collide.


-- The prepared tag parameter and correlated sel parameter must each use their own kind
-- and value.

CREATE TABLE customscan_collide_lake (
    sel integer,
    tag integer,
    payload text
) USING iceberg;

-- File 1: sel ∈ [1, 10]. `tag` deterministically derived from `sel`.
INSERT INTO customscan_collide_lake
SELECT g, (g % 3), 'a_' || g
FROM generate_series(1, 10) AS g;

-- Use a different tag distribution in the second file to expose incorrect parameter
-- binding.
INSERT INTO customscan_collide_lake
SELECT g, (g % 2), 'b_' || g
FROM generate_series(100, 110) AS g;

SELECT COUNT(*) AS lake_total_rows FROM customscan_collide_lake;

-- The heap outer supplies sel as PARAM_EXEC.
CREATE TABLE customscan_collide_outer (
    sel integer,
    label text
);
INSERT INTO customscan_collide_outer
VALUES (1, 'o1'), (5, 'o5'), (105, 'o105'), (999, 'gap');

-- Use a nested loop without Materialize to rescan the lake for each outer key.
SET enable_hashjoin = off;
SET enable_mergejoin = off;
SET enable_material = off;
SET enable_nestloop = on;

-- Check that the inner pushed filter contains both external and execution parameters.
PREPARE customscan_collide_plan (int) AS
SELECT o.sel AS outer_sel, l.sel AS lake_sel, l.tag, l.payload
FROM customscan_collide_outer o
CROSS JOIN LATERAL (
    SELECT sel, tag, payload
    FROM customscan_collide_lake l
    WHERE l.sel = o.sel
      AND l.tag = $1
    OFFSET 0
) l
ORDER BY o.sel, l.sel, l.payload;

SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF) EXECUTE customscan_collide_plan(1);

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF) EXECUTE customscan_collide_plan(1);

DEALLOCATE customscan_collide_plan;

-- Run the join as a prepared statement to combine PARAM_EXTERN with per-row PARAM_EXEC
-- binding.

PREPARE customscan_collide_q (int) AS
SELECT o.sel AS outer_sel, l.sel AS lake_sel, l.tag, l.payload
FROM customscan_collide_outer o
CROSS JOIN LATERAL (
    SELECT sel, tag, payload
    FROM customscan_collide_lake l
    WHERE l.sel = o.sel
      AND l.tag = $1
    OFFSET 0
) l
ORDER BY o.sel, l.sel, l.payload;

-- External tag = 1 with correlated sel.
SET lagodb.customscan_mode = 'force';
EXECUTE customscan_collide_q(1);

SET lagodb.customscan_mode = 'off';
EXECUTE customscan_collide_q(1);

-- Change only the external tag parameter.
SET lagodb.customscan_mode = 'force';
EXECUTE customscan_collide_q(0);

SET lagodb.customscan_mode = 'off';
EXECUTE customscan_collide_q(0);

-- Tag = 2 matches rows in the first file but not the second.
SET lagodb.customscan_mode = 'force';
EXECUTE customscan_collide_q(2);

SET lagodb.customscan_mode = 'off';
EXECUTE customscan_collide_q(2);

DEALLOCATE customscan_collide_q;

-- Hold tag fixed while sel changes across files, repeats, and misses. Each rescan must
-- preserve the external parameter and bind the current execution parameter.
PREPARE customscan_collide_rescan (int) AS
SELECT o.sel AS outer_sel, l.sel AS lake_sel, l.tag, l.payload
FROM (VALUES (5), (5), (105), (999), (1)) AS o(sel)
CROSS JOIN LATERAL (
    SELECT sel, tag, payload
    FROM customscan_collide_lake l
    WHERE l.sel = o.sel
      AND l.tag = $1
    OFFSET 0
) l
ORDER BY o.sel, l.sel, l.payload;

-- Compare rescans with external tag = 0.
SET lagodb.customscan_mode = 'force';
EXECUTE customscan_collide_rescan(0);

SET lagodb.customscan_mode = 'off';
EXECUTE customscan_collide_rescan(0);

-- Repeat the rescan sequence with external tag = 1.
SET lagodb.customscan_mode = 'force';
EXECUTE customscan_collide_rescan(1);

SET lagodb.customscan_mode = 'off';
EXECUTE customscan_collide_rescan(1);

DEALLOCATE customscan_collide_rescan;

RESET enable_hashjoin;
RESET enable_mergejoin;
RESET enable_material;
RESET enable_nestloop;
RESET lagodb.customscan_mode;

DROP TABLE customscan_collide_lake;
DROP TABLE customscan_collide_outer;
-- NULL parameters in exact strict comparisons.


CREATE TABLE customscan_null_param_lake (
    id integer,
    payload text
) USING iceberg;

INSERT INTO customscan_null_param_lake
SELECT g, 'lake_' || g
FROM generate_series(1, 10) AS g;

SELECT COUNT(*) AS lake_total_rows FROM customscan_null_param_lake;

-- The heap outer supplies both a matching key and NULL to the inner execution
-- parameter.
CREATE TABLE customscan_null_param_outer (
    id integer,
    label text
);
INSERT INTO customscan_null_param_outer VALUES (1, 'one'), (NULL, 'null_row');

-- Use force_generic_plan to keep NULL as a runtime PARAM_EXTERN rather than a folded
-- constant.
SET plan_cache_mode = force_generic_plan;

PREPARE customscan_null_param_p1 (int) AS
SELECT id, payload FROM customscan_null_param_lake WHERE id = $1 ORDER BY id, payload;

PREPARE customscan_null_param_p1_baseline (int) AS
SELECT id, payload FROM customscan_null_param_lake WHERE id = $1 ORDER BY id, payload;

-- Check the generic plan with a non-NULL parameter.
SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF) EXECUTE customscan_null_param_p1(1);

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF) EXECUTE customscan_null_param_p1_baseline(1);

-- A strict equality with NULL returns no rows and must not raise an error.
SET lagodb.customscan_mode = 'force';
EXECUTE customscan_null_param_p1(NULL);

SET lagodb.customscan_mode = 'off';
EXECUTE customscan_null_param_p1_baseline(NULL);

DEALLOCATE customscan_null_param_p1;
DEALLOCATE customscan_null_param_p1_baseline;
RESET plan_cache_mode;

-- An ordinary join supplies NULL through PARAM_EXEC on the rescanned inner scan.

-- The ordinary equijoin exercises equivalence-class recovery of the parameterized path.

-- A large lake and small heap outer favor the rescanned inner path. Disable hash/merge
-- joins and Materialize to keep rescans visible.
CREATE TABLE customscan_null_param_join_lake (
    id integer,
    payload text
) USING iceberg;

-- File 1: id ∈ [1, 2500]
INSERT INTO customscan_null_param_join_lake
SELECT g, 'lake_' || g
FROM generate_series(1, 2500) AS g;

-- File 2: id ∈ [10000, 12499]
INSERT INTO customscan_null_param_join_lake
SELECT g, 'lake_' || g
FROM generate_series(10000, 12499) AS g;

SELECT COUNT(*) AS join_lake_total_rows FROM customscan_null_param_join_lake;

SET enable_hashjoin = off;
SET enable_mergejoin = off;
SET enable_material = off;
SET enable_nestloop = on;

-- Check that the ordinary join selects a parameterized inner CustomScan.
SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT o.id, o.label, l.id AS lake_id, l.payload
FROM customscan_null_param_outer o
JOIN customscan_null_param_join_lake l ON l.id = o.id
ORDER BY o.id, l.payload;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT o.id, o.label, l.id AS lake_id, l.payload
FROM customscan_null_param_outer o
JOIN customscan_null_param_join_lake l ON l.id = o.id
ORDER BY o.id, l.payload;

-- Only the non-NULL outer key matches; the NULL key contributes no rows.
SET lagodb.customscan_mode = 'force';
SELECT o.id, o.label, l.id AS lake_id, l.payload
FROM customscan_null_param_outer o
JOIN customscan_null_param_join_lake l ON l.id = o.id
ORDER BY o.id, l.payload;

SET lagodb.customscan_mode = 'off';
SELECT o.id, o.label, l.id AS lake_id, l.payload
FROM customscan_null_param_outer o
JOIN customscan_null_param_join_lake l ON l.id = o.id
ORDER BY o.id, l.payload;

RESET enable_hashjoin;
RESET enable_mergejoin;
RESET enable_material;
RESET enable_nestloop;
RESET lagodb.customscan_mode;

DROP TABLE customscan_null_param_join_lake;

-- A NULL equality combined with amount > 0 returns no rows. Keep a generic plan so the
-- NULL parameter is evaluated at runtime.
CREATE TABLE customscan_null_param_mixed (
    id integer,
    amount integer,
    payload text
) USING iceberg;

INSERT INTO customscan_null_param_mixed
SELECT g, g * 10, 'mixed_' || g
FROM generate_series(1, 10) AS g;

SET plan_cache_mode = force_generic_plan;

PREPARE customscan_null_param_mixed_p (int) AS
SELECT id, amount, payload
FROM customscan_null_param_mixed
WHERE id = $1 AND amount > 0
ORDER BY id, payload;

PREPARE customscan_null_param_mixed_p_baseline (int) AS
SELECT id, amount, payload
FROM customscan_null_param_mixed
WHERE id = $1 AND amount > 0
ORDER BY id, payload;

-- Compare the empty result for a NULL parameter.
SET lagodb.customscan_mode = 'force';
EXECUTE customscan_null_param_mixed_p(NULL);

SET lagodb.customscan_mode = 'off';
EXECUTE customscan_null_param_mixed_p_baseline(NULL);

DEALLOCATE customscan_null_param_mixed_p;
DEALLOCATE customscan_null_param_mixed_p_baseline;
RESET plan_cache_mode;
RESET lagodb.customscan_mode;

DROP TABLE customscan_null_param_mixed;

-- The same generic equality plan must still push and match non-NULL parameters.
SET plan_cache_mode = force_generic_plan;

PREPARE customscan_null_param_p_nonnull (int) AS
SELECT id, payload FROM customscan_null_param_lake WHERE id = $1 ORDER BY id, payload;

PREPARE customscan_null_param_p_nonnull_baseline (int) AS
SELECT id, payload FROM customscan_null_param_lake WHERE id = $1 ORDER BY id, payload;

-- Parameter 5 must match exactly row 5 in both modes.
SET lagodb.customscan_mode = 'force';
EXECUTE customscan_null_param_p_nonnull(5);

SET lagodb.customscan_mode = 'off';
EXECUTE customscan_null_param_p_nonnull_baseline(5);

DEALLOCATE customscan_null_param_p_nonnull;
DEALLOCATE customscan_null_param_p_nonnull_baseline;
RESET plan_cache_mode;
RESET lagodb.customscan_mode;
RESET lagodb.query_offload_mode;

-- InitPlan parameters must be evaluated before the scan starts.
SET lagodb.query_offload_mode = 'off';
SET lagodb.customscan_mode = 'force';
SET max_parallel_workers_per_gather = 0;

-- Expose stable EXPLAIN JSON fields as ordinary regression results.
CREATE FUNCTION pg_temp.customscan_parameter_plan(query text) RETURNS jsonb
LANGUAGE plpgsql AS $$
DECLARE
    plan jsonb;
BEGIN
    EXECUTE query INTO plan;
    RETURN plan;
END;
$$;

SELECT plan #>> '{0,Plan,Custom Plan Provider}' AS provider,
       jsonb_path_exists(plan,
           '$[0].Plan.** ? (@."Parent Relationship" == "InitPlan")') AS has_initplan
FROM pg_temp.customscan_parameter_plan(
    'EXPLAIN (COSTS OFF, FORMAT JSON)
     SELECT id FROM customscan_null_param_lake
     WHERE id = (SELECT max(value) FROM (VALUES (2), (3)) AS input(value))'
) AS document(plan);

SELECT id FROM customscan_null_param_lake
WHERE id = (SELECT max(value) FROM (VALUES (2), (3)) AS input(value));
SELECT count(*) AS null_initplan_rows FROM customscan_null_param_lake
WHERE id = (SELECT max(value) FROM (VALUES (NULL::integer)) AS input(value));

-- The mutation cursor must also wait for the InitPlan parameter before opening.
UPDATE customscan_null_param_lake SET payload = 'updated'
WHERE id = (SELECT max(value) FROM (VALUES (2), (3)) AS input(value))
RETURNING id, payload;

-- An empty outer initializes and ends the inner scan without starting it.
TRUNCATE customscan_null_param_outer;
SELECT scan->>'Custom Plan Provider' AS provider,
       (scan->>'Actual Loops')::integer AS inner_loops
FROM pg_temp.customscan_parameter_plan(
    'EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, FORMAT JSON)
     SELECT inner_rel.id FROM customscan_null_param_outer AS outer_rel
     CROSS JOIN LATERAL (
         SELECT id FROM customscan_null_param_lake
         WHERE id = outer_rel.id OFFSET 0
     ) AS inner_rel'
) AS document(plan)
CROSS JOIN LATERAL jsonb_path_query_first(plan,
    '$[0].Plan.** ? (@."Custom Plan Provider" == "lagodb-iceberg")') AS scan;

DROP FUNCTION pg_temp.customscan_parameter_plan(text);
RESET max_parallel_workers_per_gather;
RESET lagodb.customscan_mode;
RESET lagodb.query_offload_mode;

DROP TABLE customscan_null_param_lake;
DROP TABLE customscan_null_param_outer;
