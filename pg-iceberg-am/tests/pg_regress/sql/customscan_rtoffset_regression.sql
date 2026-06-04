-- customscan_rtoffset_regression.sql
-- RTI remapping after setrefs does not break Var resolution.

DROP EXTENSION IF EXISTS pg_iceberg_am CASCADE;
CREATE EXTENSION IF NOT EXISTS pg_iceberg_am;

-- ============================================================================
-- Setup: an Iceberg `lake` table with `int4` `k` and `payload`
-- columns. The classifier promotes `k = <int4 literal>` (opno 96,
-- int4eq) to `Exact` via the `EXACT_ALLOWLIST` in
-- `pg-iceberg-am/src/customscan/classifier.rs`.
-- Multi-file layout: each INSERT opens a fresh DML session and
-- finalizes one Parquet file with bounded `lower_bounds[k]` /
-- `upper_bounds[k]` statistics. Two files give us two scan-relation
-- Var resolution shapes (one per file boundary) and ensure the
-- pushed predicate actually drives manifest-level pruning even when
-- the lake is wrapped in a subplan with non-zero `rtoffset`.
-- ============================================================================
CREATE TABLE customscan_rto_lake (
    k integer,
    payload text
) USING iceberg;

-- File 1: k ∈ [1, 5]
INSERT INTO customscan_rto_lake
SELECT g, 'lake_' || g
FROM generate_series(1, 5) AS g;

-- File 2: k ∈ [100, 105]
INSERT INTO customscan_rto_lake
SELECT g, 'lake_' || g
FROM generate_series(100, 105) AS g;

SELECT COUNT(*) AS lake_total_rows FROM customscan_rto_lake;

-- ============================================================================
-- Block 0: baseline — `WHERE k = N` against the bare lake.
-- This is the row set the wrapped-query blocks below MUST also
-- produce (modulo subquery-introduced columns). Pinning it here
-- makes the diagnostic value of each wrapper concrete: a regression
-- where the runtime walker misidentified scan-relation Vars after
-- `rtoffset` shifted them would produce a different row set in the
-- wrapped blocks while the bare baseline below still passes.
-- ============================================================================

-- 0.1 plan guard.
SET pg_lakebase.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT k, payload FROM customscan_rto_lake WHERE k = 3 ORDER BY k, payload;

SET pg_lakebase.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT k, payload FROM customscan_rto_lake WHERE k = 3 ORDER BY k, payload;

-- 0.2 result-set parity.
SET pg_lakebase.customscan_mode = 'force';
SELECT k, payload FROM customscan_rto_lake WHERE k = 3 ORDER BY k, payload;

SET pg_lakebase.customscan_mode = 'off';
SELECT k, payload FROM customscan_rto_lake WHERE k = 3 ORDER BY k, payload;

-- ============================================================================
-- Block A: subquery wrapper.
--   SELECT ... FROM (SELECT k, payload FROM lake WHERE k = N) sub;
-- The subquery is NOT flattenable — it has its own `targetlist` that
-- the parent SELECT projects on top of, so PG plans a `SubqueryScan`
-- above the lake's CustomScan. The lake's RTI inside the subplan's
-- private range table is `1` (or whatever the planner chooses);
-- after `set_plan_references` runs on the parent, the subplan is
-- recursed with a non-zero `rtoffset` and the lake's CustomScan
-- ends up with `cscan->scan.scanrelid > 1` after rtoffset adjustment
-- (verifiable indirectly: if the framework had cached the
-- pre-`rtoffset` RTI, the runtime translator would now resolve
-- `Var.varno = pre_rtoffset_rti = 1` to "is scan column" but the
-- post-`rtoffset` `cscan->scan.scanrelid = 2` would mean every
-- runtime check would say "no scan-relation Var" and the predicate
-- would be empty, which would break parity below).
-- Wrapping in a subquery does NOT introduce an outer-relation Var
-- inside the lake's pushed predicate, so `column_refs[]` is still
-- `[Var(k)]`. The Block 0 baseline is the row set this block must
-- match.
-- We disable subquery-flattening via `OFFSET 0` only when needed to
-- block flattening; a plain trivial-subquery `SELECT k, payload
-- FROM lake WHERE k = 3` is already non-flattenable enough for
-- PG (the parent has its own targetlist that aliases `sub`).
-- However, PG MAY still inline a single-rel trivial subquery via
-- `pull_up_simple_subquery`. To guarantee an opaque subquery —
-- and thus a non-zero `rtoffset` on the inner — we add an
-- `OFFSET 0` clause inside (matches
-- `customscan_security_movability.sql` Block C and
-- `customscan_variant_selection.sql` Block A patterns).
-- ============================================================================

-- A.1 plan guard.
SET pg_lakebase.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT sub.k, sub.payload
FROM (
    SELECT k, payload
    FROM customscan_rto_lake
    WHERE k = 3
    OFFSET 0
) sub
ORDER BY sub.k, sub.payload;

SET pg_lakebase.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT sub.k, sub.payload
FROM (
    SELECT k, payload
    FROM customscan_rto_lake
    WHERE k = 3
    OFFSET 0
) sub
ORDER BY sub.k, sub.payload;

-- A.2 result-set parity. Must equal Block 0's row set.
SET pg_lakebase.customscan_mode = 'force';
SELECT sub.k, sub.payload
FROM (
    SELECT k, payload
    FROM customscan_rto_lake
    WHERE k = 3
    OFFSET 0
) sub
ORDER BY sub.k, sub.payload;

SET pg_lakebase.customscan_mode = 'off';
SELECT sub.k, sub.payload
FROM (
    SELECT k, payload
    FROM customscan_rto_lake
    WHERE k = 3
    OFFSET 0
) sub
ORDER BY sub.k, sub.payload;

-- A.3 cross-file parity: same wrapper, value in the other data
-- file. Validates that pruning + Var resolution survive
-- `rtoffset` regardless of which file the matching row lives in.
SET pg_lakebase.customscan_mode = 'force';
SELECT sub.k, sub.payload
FROM (
    SELECT k, payload
    FROM customscan_rto_lake
    WHERE k = 102
    OFFSET 0
) sub
ORDER BY sub.k, sub.payload;

SET pg_lakebase.customscan_mode = 'off';
SELECT sub.k, sub.payload
FROM (
    SELECT k, payload
    FROM customscan_rto_lake
    WHERE k = 102
    OFFSET 0
) sub
ORDER BY sub.k, sub.payload;

-- ============================================================================
-- Block B: CTE wrapper (`WITH ... AS MATERIALIZED`).
--   WITH lake_cte AS MATERIALIZED (SELECT ... FROM lake WHERE k = N)
--   SELECT ... FROM lake_cte;
-- The `MATERIALIZED` keyword (PG12+) opts the CTE out of inlining
-- via `inline_cte_walker` / `pull_up_simple_subquery`, so the CTE
-- is planned as a separate `InitPlan` / `CteScan` subplan whose
-- private range table is offset from the parent's. After
-- `set_plan_references_recurse` runs on the CTE subplan, the
-- lake's `CustomScan.scan.scanrelid` reflects the post-`rtoffset`
-- value, and the runtime walker MUST identify scan-relation Vars
-- from that post-rtoffset value.
-- Like Block A, the CTE does not introduce an outer-relation Var
-- into the lake's pushed predicate — it just flows the lake's
-- output up through a `CteScan` node. The result-set parity
-- against Block 0 is the assertion.
-- ============================================================================

-- B.1 plan guard.
SET pg_lakebase.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
WITH lake_cte AS MATERIALIZED (
    SELECT k, payload FROM customscan_rto_lake WHERE k = 3
)
SELECT k, payload FROM lake_cte ORDER BY k, payload;

SET pg_lakebase.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
WITH lake_cte AS MATERIALIZED (
    SELECT k, payload FROM customscan_rto_lake WHERE k = 3
)
SELECT k, payload FROM lake_cte ORDER BY k, payload;

-- B.2 result-set parity. Must equal Block 0's row set.
SET pg_lakebase.customscan_mode = 'force';
WITH lake_cte AS MATERIALIZED (
    SELECT k, payload FROM customscan_rto_lake WHERE k = 3
)
SELECT k, payload FROM lake_cte ORDER BY k, payload;

SET pg_lakebase.customscan_mode = 'off';
WITH lake_cte AS MATERIALIZED (
    SELECT k, payload FROM customscan_rto_lake WHERE k = 3
)
SELECT k, payload FROM lake_cte ORDER BY k, payload;

-- B.3 cross-file parity from the CTE wrapper.
SET pg_lakebase.customscan_mode = 'force';
WITH lake_cte AS MATERIALIZED (
    SELECT k, payload FROM customscan_rto_lake WHERE k = 102
)
SELECT k, payload FROM lake_cte ORDER BY k, payload;

SET pg_lakebase.customscan_mode = 'off';
WITH lake_cte AS MATERIALIZED (
    SELECT k, payload FROM customscan_rto_lake WHERE k = 102
)
SELECT k, payload FROM lake_cte ORDER BY k, payload;

-- ============================================================================
-- Block C: LATERAL derived table wrapper, with an outer-relation
-- reference inside the LATERAL.
--   SELECT o.id, sub.k, sub.payload
--   FROM customscan_rto_outer o,
--   LATERAL (
--       SELECT k, payload FROM lake WHERE k = o.id
--       OFFSET 0
--   ) sub;
-- This is the most aggressive of the three wrappers because:
--   1. The LATERAL subquery has its own range table that PG offsets
--      via `rtoffset` when flattening it into the parent plan tree.
--      The lake's `CustomScan.scan.scanrelid` post-rtoffset
--      reflects the parent's flattened range-table layout.
--   2. `replace_nestloop_params` rewrites the outer-relation `o.id`
--      Var inside the lake's pushed predicate into a `PARAM_EXEC`
--      `Param` node. Runtime translation MUST identify
--      the remaining `Var(k)` as a scan-relation Var via
--      `Var.varno == cscan->scan.scanrelid` (the post-`rtoffset`
--      value) — not via any cached RTI in `custom_private`.
--   3. The pushed predicate `Var(k) = Param(o.id)` walks once at
--      plan time and once at runtime; the runtime
--      `(expr_index = 0, attno = 1)` lookup MUST resolve to
--      `column_refs[(0, 1)] = { rel_oid = lake oid, attno = 1,
--      atttypid = int4, attcollation = 0 }` on both plan and runtime
--      sides. A regression that uses a cached pre-rtoffset RTI or
--      treats the rewritten Param as a column would break the rebuilt
--      predicate, surfacing as wrong rows below.
-- We pin planner GUCs (matches `customscan_security_movability.sql`
-- Block C and `customscan_variant_selection.sql` Block B) to force
-- a deterministic nestloop topology where the LATERAL subplan is
-- the inner side of a `Nested Loop`. These GUCs apply equally to
-- `force` and `off`, so the executor topology is identical on
-- both sides; the only variable across the parity assertion is
-- whether the inner plan node is a CustomScan or a Seq Scan.
-- ============================================================================

-- A small heap outer relation. Three rows so the LATERAL drives
-- three nestloop rescans of the lake CustomScan. Two of `o.id`
-- (3 and 102) live in the lake; one (50) does not — exercising
-- both "match" and "no-match" rescan branches.
CREATE TABLE customscan_rto_outer (id integer);
INSERT INTO customscan_rto_outer VALUES (3), (50), (102);

SET enable_hashjoin = off;
SET enable_mergejoin = off;
SET enable_material = off;
SET enable_nestloop = on;

-- C.1 plan guard.
SET pg_lakebase.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT o.id, sub.k, sub.payload
FROM customscan_rto_outer o,
LATERAL (
    SELECT k, payload
    FROM customscan_rto_lake
    WHERE k = o.id
    OFFSET 0
) sub
ORDER BY o.id, sub.k, sub.payload;

SET pg_lakebase.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT o.id, sub.k, sub.payload
FROM customscan_rto_outer o,
LATERAL (
    SELECT k, payload
    FROM customscan_rto_lake
    WHERE k = o.id
    OFFSET 0
) sub
ORDER BY o.id, sub.k, sub.payload;

-- C.2 result-set parity. With outer.id ∈ {3, 50, 102}:
--   - o.id = 3   → lake.k = 3   → (3, 'lake_3')
--   - o.id = 50  → no match     → (no row)
--   - o.id = 102 → lake.k = 102 → (102, 'lake_102')
-- The "modulo subquery-introduced Vars" disclaimer in the task
-- brief is concrete here: the wrapper introduces `o.id` into the
-- result rows alongside `sub.k` / `sub.payload`. Strip `o.id` and
-- the lake-side rows (`sub.k`, `sub.payload`) exactly equal the
-- union of Block 0 (k = 3) and Block A.3 / B.3 (k = 102) — which
-- is the sense in which Block C's row set "matches the same query
-- without the enclosing subquery (modulo subquery-introduced
-- Vars)".
SET pg_lakebase.customscan_mode = 'force';
SELECT o.id, sub.k, sub.payload
FROM customscan_rto_outer o,
LATERAL (
    SELECT k, payload
    FROM customscan_rto_lake
    WHERE k = o.id
    OFFSET 0
) sub
ORDER BY o.id, sub.k, sub.payload;

SET pg_lakebase.customscan_mode = 'off';
SELECT o.id, sub.k, sub.payload
FROM customscan_rto_outer o,
LATERAL (
    SELECT k, payload
    FROM customscan_rto_lake
    WHERE k = o.id
    OFFSET 0
) sub
ORDER BY o.id, sub.k, sub.payload;

-- ============================================================================
-- Block D: nested wrapper — subquery containing a CTE containing the
-- lake. Two `rtoffset` shifts compose, exercising the runtime
-- walker's resilience under deeper plan-tree rewrites.
--   SELECT outer_sub.k FROM (
--       WITH inner_cte AS MATERIALIZED (
--           SELECT k, payload FROM lake WHERE k = N
--       )
--       SELECT k, payload FROM inner_cte OFFSET 0
--   ) outer_sub;
-- Both wrappers are non-inlinable (the `MATERIALIZED` CTE blocks
-- `inline_cte_walker`; the `OFFSET 0` outer subquery blocks
-- `pull_up_simple_subquery`). The lake's CustomScan ends up
-- nested two layers deep, so its `scan.scanrelid` is offset
-- twice. If the framework cached any RTI in `custom_private`, it
-- would diverge from the post-rtoffset `cscan->scan.scanrelid`
-- by the cumulative offset, and the runtime walker would
-- misidentify every scan-relation `Var`.
-- ============================================================================

-- D.1 plan guard.
SET pg_lakebase.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT outer_sub.k, outer_sub.payload
FROM (
    WITH inner_cte AS MATERIALIZED (
        SELECT k, payload FROM customscan_rto_lake WHERE k = 3
    )
    SELECT k, payload FROM inner_cte
    OFFSET 0
) outer_sub
ORDER BY outer_sub.k, outer_sub.payload;

SET pg_lakebase.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT outer_sub.k, outer_sub.payload
FROM (
    WITH inner_cte AS MATERIALIZED (
        SELECT k, payload FROM customscan_rto_lake WHERE k = 3
    )
    SELECT k, payload FROM inner_cte
    OFFSET 0
) outer_sub
ORDER BY outer_sub.k, outer_sub.payload;

-- D.2 result-set parity.
SET pg_lakebase.customscan_mode = 'force';
SELECT outer_sub.k, outer_sub.payload
FROM (
    WITH inner_cte AS MATERIALIZED (
        SELECT k, payload FROM customscan_rto_lake WHERE k = 3
    )
    SELECT k, payload FROM inner_cte
    OFFSET 0
) outer_sub
ORDER BY outer_sub.k, outer_sub.payload;

SET pg_lakebase.customscan_mode = 'off';
SELECT outer_sub.k, outer_sub.payload
FROM (
    WITH inner_cte AS MATERIALIZED (
        SELECT k, payload FROM customscan_rto_lake WHERE k = 3
    )
    SELECT k, payload FROM inner_cte
    OFFSET 0
) outer_sub
ORDER BY outer_sub.k, outer_sub.payload;

-- ============================================================================
-- Cleanup
-- ============================================================================
RESET enable_hashjoin;
RESET enable_mergejoin;
RESET enable_material;
RESET enable_nestloop;
RESET pg_lakebase.customscan_mode;

DROP TABLE customscan_rto_outer;
DROP TABLE customscan_rto_lake;
