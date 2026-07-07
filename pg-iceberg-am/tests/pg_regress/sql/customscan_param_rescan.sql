-- customscan_rescan_param.sql
-- Nestloop rescan with extern/exec params and chgParam rebuild.

DROP EXTENSION IF EXISTS pg_iceberg_am CASCADE;
CREATE EXTENSION IF NOT EXISTS pg_iceberg_am;

-- ============================================================================
-- Setup: an Iceberg `lake` table with an `int4` `id` column. The
-- classifier promotes `id = <int4 literal>` (opno 96, int4eq) to
-- `Exact` (`EXACT_ALLOWLIST` in
-- `pg-iceberg-am/src/customscan/classifier.rs`), so a `WHERE id = $1`
-- clause whose `$1` is `PARAM_EXTERN` is BOTH pushed for pruning AND
-- recheck-bound at `BeginCustomScan`.
-- Multi-file layout: each INSERT opens a fresh DML session and
-- finalizes one Parquet file with bounded `lower_bounds[id]` /
-- `upper_bounds[id]` statistics, so the iceberg-lite
-- `InclusiveMetricsEvaluator` has something to prune at scan time.
-- The relevant property is rescan/param correctness — pruning depth and
-- ConservativePruning residual quals are covered by
-- `customscan_conservative_pruning.sql`.
-- ============================================================================
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

-- ============================================================================
-- Block A: PARAM_EXTERN via PREPARE / EXECUTE
-- A `PREPARE p AS SELECT ... WHERE id = $1` plan goes through
-- `PlanCustomPath` once (custom-plan path) or once per execution
-- (generic-plan path; PG decides at runtime). Either way, every
-- `EXECUTE p(N)` arrives at `BeginCustomScan` with a fresh
-- `paramInfo->params[$1]`, and the framework's
-- `RuntimeParamResolver::resolve` must mirror `ExecEvalParamExtern`
--:
--   - Call `paramInfo->paramFetch(speculative=false)` when set.
--   - Validate `OidIsValid(prm->ptype)` and
--     `prm->ptype == op->d.param.paramtype`.
--   - Raise `ERRCODE_DATATYPE_MISMATCH` / "no value found for
--     parameter %d" mirroring PG's wording on failure.
-- The framework also computes `cached_pushed_param_ids` at
-- `BeginCustomScan` so that subsequent rescans
-- (e.g. when this same prepared statement runs inside a larger plan
-- that's rescanned) honor the chgParam gate. For a top-level
-- `EXECUTE p(N)` invocation the executor calls `ExecutorRun` once per
-- `EXECUTE`, so each `EXECUTE p(N)` typically runs through
-- `BeginCustomScan` afresh — but the gate logic is the same code
-- path.
-- We prepare two statements with identical SQL — one used under
-- `mode=force` (CustomScan) and one used under `mode=off` (SeqScan
-- baseline). For each bound value we EXECUTE both and let pg_regress
-- compare the two row blocks line by line.
-- Note on `customscan_mode` and PREPARE: PG's `PREPARE` only parses
-- (`PrepareQuery` → `parse_analyze_*`); planning happens lazily at
-- `EXECUTE` time — custom plans are re-planned each EXECUTE under the
-- current GUC, and the generic plan (after PG's `plancache.c`
-- switches) is planned once under whatever GUC was in effect at the
-- switch. So the GUC value at PREPARE time is irrelevant to plan
-- flavor; the GUC at EXECUTE time is what matters. We use two
-- separate prepared statements (rather than one) so each maintains
-- its own plansource and its custom/generic plans are consistently
-- planned under its intended mode, even after the generic-plan
-- switch in A.5 below.
-- ============================================================================

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

-- Plan guards: the parameterized prepared statement should plan a
-- CustomScan when CustomScan is enabled, and an ordinary seqscan when
-- it is disabled.
SET pg_lakebase.customscan_mode = 'force';
EXPLAIN (COSTS OFF) EXECUTE customscan_rescan_p1(25);

SET pg_lakebase.customscan_mode = 'off';
EXPLAIN (COSTS OFF) EXECUTE customscan_rescan_p1_baseline(25);

-- A.1: bound value lives in file 1.
SET pg_lakebase.customscan_mode = 'force';
EXECUTE customscan_rescan_p1(25);
SET pg_lakebase.customscan_mode = 'off';
EXECUTE customscan_rescan_p1_baseline(25);

-- A.2: bound value lives in file 2 (NULL-bearing file). 105 is NOT
-- one of the NULL ids (105 % 17 = 3 ≠ 0), so the lake row is real;
-- the NULL rows must drop on both sides because `NULL = 105` is NULL
-- and PG WHERE treats NULL as FALSE.
SET pg_lakebase.customscan_mode = 'force';
EXECUTE customscan_rescan_p1(105);
SET pg_lakebase.customscan_mode = 'off';
EXECUTE customscan_rescan_p1_baseline(105);

-- A.2b: bound value matches one of the NULL ids of file 2 (119 IS one
-- of {102, 119, 136}). The literal `id = 119` returns zero rows (NULL
-- never compares equal). The pushdown path's manifest pruning uses
-- file 2's bounds [100, 150] (file 2 IS scanned), but residual
-- filtering drops every NULL row, so the result is empty on both
-- sides.
SET pg_lakebase.customscan_mode = 'force';
EXECUTE customscan_rescan_p1(119);
SET pg_lakebase.customscan_mode = 'off';
EXECUTE customscan_rescan_p1_baseline(119);

-- A.3: bound value lives in file 3.
SET pg_lakebase.customscan_mode = 'force';
EXECUTE customscan_rescan_p1(1025);
SET pg_lakebase.customscan_mode = 'off';
EXECUTE customscan_rescan_p1_baseline(1025);

-- A.4: bound value matches NO row (gap between files 1 and 2).
SET pg_lakebase.customscan_mode = 'force';
EXECUTE customscan_rescan_p1(75);
SET pg_lakebase.customscan_mode = 'off';
EXECUTE customscan_rescan_p1_baseline(75);

-- A.5: PG generic-plan path — PG normally switches to the generic
-- plan after five custom-plan executions of the same prepared
-- statement (`plancache.c` heuristic). Run the same prepared
-- statement enough times that PG considers using the generic plan,
-- so both the custom-plan and generic-plan code paths through
-- `BeginCustomScan` / `RuntimeParamResolver::resolve` are exercised. After the
-- switch, EXECUTE one more time on each side and let pg_regress diff
-- the two row blocks: the row set returned by `EXECUTE` against any
-- bound value must still match the baseline regardless of which plan
-- flavor PG chose internally.
SET pg_lakebase.customscan_mode = 'force';
EXECUTE customscan_rescan_p1(1);
EXECUTE customscan_rescan_p1(50);
EXECUTE customscan_rescan_p1(100);
EXECUTE customscan_rescan_p1(150);
EXECUTE customscan_rescan_p1(1000);
EXECUTE customscan_rescan_p1(1050);

-- Post-pump plan guard: after enough custom-plan executions, PG may
-- have switched to a generic plan. EXPLAIN under `force` once more
-- to verify the post-switch plan is STILL a CustomScan with the
-- same pushed-predicate layout — default TEXT carries the
-- `Pushed Filter:` line for the pushed (remote) predicate, with local
-- residual (if any) shown only by PG's standard `Filter:` line
--.
-- pg_regress diffs the EXPLAIN block byte-for-byte; a regression
-- where the generic plan silently falls back to Seq Scan would show
-- up here as well as in the parity row blocks below.
SET pg_lakebase.customscan_mode = 'force';
EXPLAIN (COSTS OFF) EXECUTE customscan_rescan_p1(1);

-- Post-generic-plan parity: re-EXECUTE both sides with the same
-- bound values and let pg_regress compare the row blocks.
SET pg_lakebase.customscan_mode = 'force';
EXECUTE customscan_rescan_p1(1);
SET pg_lakebase.customscan_mode = 'off';
EXECUTE customscan_rescan_p1_baseline(1);

SET pg_lakebase.customscan_mode = 'force';
EXECUTE customscan_rescan_p1(1050);
SET pg_lakebase.customscan_mode = 'off';
EXECUTE customscan_rescan_p1_baseline(1050);

DEALLOCATE customscan_rescan_p1;
DEALLOCATE customscan_rescan_p1_baseline;

-- ============================================================================
-- Block B: nestloop join with `lake.id = outer.id` on the inner side
-- An outer relation drives a nestloop whose inner side is the lake
-- relation parameterized on `outer.id`. After
-- `replace_nestloop_params` runs over the inner CustomScan plan, the
-- outer-relation Var becomes a `PARAM_EXEC` Param node whose paramId
-- is recorded in the outer plan's `nestParams`. PG nestloop sets
-- `innerPlan->chgParam` from those paramIds and unconditionally
-- invokes `ExecReScan(innerPlan)` for each new outer tuple
-- (`nodeNestloop.c`).
-- The framework's `ReScanCustomScan` then:
--   - intersects `node->ss.ps.chgParam` with the cached
--     `cached_pushed_param_ids`,
--   - if the intersection is non-empty:
--     re-resolve params (mirroring `ExecEvalParamExec`,

--     `ExecSetParamPlanMulti` materialization for SubPlan-backed
--     params), rebuild the native predicate, redo file/row-group
--     pruning, reopen the cursor;
--   - otherwise: only reopen the cursor — no
--     re-translation, no re-pruning.
-- The outer here is a literal-driven `VALUES` list; PG's nestloop
-- pushes each outer row's `id` into the inner side's `PARAM_EXEC`
-- slot, so every iteration carries a different bound value and the
-- chgParam intersection is non-empty. Correctness (the inner side
-- finds the right rows for each outer tuple) is what we assert.
-- We force a nestloop because PG's planner might otherwise pick a
-- hash join for an unindexed inner; the nestloop is what exercises
-- `ReScanCustomScan` per outer tuple, which is the contract under
-- test.
-- The LATERAL subquery uses `OFFSET 0` to keep PG from flattening the
-- subquery and reversing the join order. That makes the `VALUES`
-- relation the outer side and the lake CustomScan the rescanned inner
-- side, which is the executor shape this block is meant to cover.
-- ============================================================================

SET enable_hashjoin = off;
SET enable_mergejoin = off;
SET enable_material = off;
SET enable_nestloop = on;
-- Why these planner GUCs are necessary (and orthogonal to
-- `customscan_mode`):
--   - `enable_nestloop = on` and `enable_hashjoin/mergejoin = off`
--     force PG to plan a nestloop for this small-outer × small-inner
--     join. Without this, PG's cost model would normally pick a
--     Hash Join (build a hashtable on the 152-row inner, probe with
--     the 5-row outer), which scans the inner exactly ONCE. That
--     would silently bypass `ReScanCustomScan` entirely and the
--     contract under test would never
--     execute.
--   - `enable_material = off` removes any Material node above the
--     inner CustomScan that would cache its output across rescans
--     and defeat the per-tuple Param resolution we're verifying
--     (
--     tuple).
-- These GUCs apply equally to both `mode='force'` and `mode='off'`
-- so the executor topology is identical on both sides; the only
-- variable across the parity assertion is whether the inner plan
-- node is a CustomScan or a Seq Scan, which is exactly what
-- `customscan_mode` controls. They are NOT a substitute for
-- `customscan_mode` — they're how we make the rescan codepath
-- actually fire.

-- Plan guards: the inner lake relation should be a parameterized
-- CustomScan when CustomScan is enabled, and an ordinary Seq Scan
-- (rescanned per outer tuple) when it is disabled. Print both shapes
-- so the .out file documents the executor topology under each GUC.
SET pg_lakebase.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
WITH outer_rel(id) AS (
    VALUES (1), (25), (100), (1000), (1050)
)
SELECT outer_rel.id AS outer_id, lake.id AS lake_id, lake.payload
FROM outer_rel
CROSS JOIN LATERAL (
    SELECT id, payload
    FROM customscan_rescan_lake lake
    WHERE lake.id = outer_rel.id
    OFFSET 0
) lake
ORDER BY outer_rel.id;

SET pg_lakebase.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
WITH outer_rel(id) AS (
    VALUES (1), (25), (100), (1000), (1050)
)
SELECT outer_rel.id AS outer_id, lake.id AS lake_id, lake.payload
FROM outer_rel
CROSS JOIN LATERAL (
    SELECT id, payload
    FROM customscan_rescan_lake lake
    WHERE lake.id = outer_rel.id
    OFFSET 0
) lake
ORDER BY outer_rel.id;

-- B.1: 5-row outer, every outer.id matches a unique inner row.
SET pg_lakebase.customscan_mode = 'force';
WITH outer_rel(id) AS (
    VALUES (1), (25), (100), (1000), (1050)
)
SELECT outer_rel.id AS outer_id, lake.id AS lake_id, lake.payload
FROM outer_rel
CROSS JOIN LATERAL (
    SELECT id, payload
    FROM customscan_rescan_lake lake
    WHERE lake.id = outer_rel.id
    OFFSET 0
) lake
ORDER BY outer_rel.id;

SET pg_lakebase.customscan_mode = 'off';
WITH outer_rel(id) AS (
    VALUES (1), (25), (100), (1000), (1050)
)
SELECT outer_rel.id AS outer_id, lake.id AS lake_id, lake.payload
FROM outer_rel
CROSS JOIN LATERAL (
    SELECT id, payload
    FROM customscan_rescan_lake lake
    WHERE lake.id = outer_rel.id
    OFFSET 0
) lake
ORDER BY outer_rel.id;

-- B.2: outer tuple whose id has NO matching lake row — the inner
-- rescan must return zero rows for that outer tuple (which means
-- the framework correctly re-resolved the new param value rather
-- than reusing a stale predicate from the previous outer tuple).
SET pg_lakebase.customscan_mode = 'force';
WITH outer_rel(id) AS (
    VALUES (25), (75), (100)
)
SELECT outer_rel.id AS outer_id, lake.id AS lake_id, lake.payload
FROM outer_rel
CROSS JOIN LATERAL (
    SELECT id, payload
    FROM customscan_rescan_lake lake
    WHERE lake.id = outer_rel.id
    OFFSET 0
) lake
ORDER BY outer_rel.id;

SET pg_lakebase.customscan_mode = 'off';
WITH outer_rel(id) AS (
    VALUES (25), (75), (100)
)
SELECT outer_rel.id AS outer_id, lake.id AS lake_id, lake.payload
FROM outer_rel
CROSS JOIN LATERAL (
    SELECT id, payload
    FROM customscan_rescan_lake lake
    WHERE lake.id = outer_rel.id
    OFFSET 0
) lake
ORDER BY outer_rel.id;

-- B.3: outer with repeated values — exercises rescan-with-same-value.
-- Even when the outer emits the same id twice in a row, PG's nestloop
-- still calls `ExecReScan(innerPlan)` for every outer tuple. The
-- inner CustomScan must produce identical results for both
-- iterations. This is what the framework's `cached_pushed_param_ids`
-- bitmap and `chgParam` intersection logic guarantee per

-- state, and the framework never compares Datum values for equality
-- to short-circuit (
-- correctness-bearing trigger).
SET pg_lakebase.customscan_mode = 'force';
WITH outer_rel(id) AS (
    VALUES (1), (1), (25), (25), (1000), (1000)
)
SELECT outer_rel.id AS outer_id, lake.id AS lake_id, lake.payload
FROM outer_rel
CROSS JOIN LATERAL (
    SELECT id, payload
    FROM customscan_rescan_lake lake
    WHERE lake.id = outer_rel.id
    OFFSET 0
) lake
ORDER BY outer_rel.id, lake.payload;

SET pg_lakebase.customscan_mode = 'off';
WITH outer_rel(id) AS (
    VALUES (1), (1), (25), (25), (1000), (1000)
)
SELECT outer_rel.id AS outer_id, lake.id AS lake_id, lake.payload
FROM outer_rel
CROSS JOIN LATERAL (
    SELECT id, payload
    FROM customscan_rescan_lake lake
    WHERE lake.id = outer_rel.id
    OFFSET 0
) lake
ORDER BY outer_rel.id, lake.payload;

-- B.4: nestloop where the pushed inner predicate is a CONSTANT. The
-- extra outer-dependent residual predicate makes the inner CustomScan
-- a rescanned parameterized path, but the pushed clause itself has no
-- `Param` references. Therefore `cached_pushed_param_ids` is empty
-- and `bms_overlap(chgParam, cached_pushed_param_ids)` is FALSE on
-- every rescan.3, the framework must NOT
-- re-translate or re-prune — it must only reopen the cursor. We can't
-- observe "didn't re-translate" from SQL, but we CAN assert the
-- result-set correctness still matches the baseline 
-- 11.4 — the chgParam gate is correctness-preserving regardless of
-- which path it picks).
-- This case complements B.1–B.3 (where the gate is always
-- "overlapping") with the dual case (where the gate is always "not
-- overlapping"), so the test exercises both branches of

-- the correctness invariant.
-- B.4 plan guards: the predicate shape here differs from B.1–B.3
-- (constant `lake.id = 25` is classified Exact and pushed; the
-- outer-dependent `(lake.id + outer.unrelated) > 0` is the local
-- residual). So the
-- Block-B plan guard above does NOT certify this classifier outcome — we
-- print fresh EXPLAIN blocks for both modes to verify:
--   - `force`: `Custom Scan` whose default TEXT carries
--     `Pushed Filter: (id = 25)` (the pushed/remote predicate) with the
--     outer-dependent conjunct shown only as the standard residual
--     `Filter:` line, inside a `Nested Loop` (so rescan still fires).
--   - `off`: `Seq Scan` with the full conjunction in `Filter:`,
--     also inside a `Nested Loop` (matching topology).
SET pg_lakebase.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
WITH outer_rel(unrelated) AS (
    VALUES (10), (20), (30)
)
SELECT outer_rel.unrelated, lake.id, lake.payload
FROM outer_rel
CROSS JOIN LATERAL (
    SELECT id, payload
    FROM customscan_rescan_lake lake
    WHERE lake.id = 25
      AND (lake.id + outer_rel.unrelated) > 0
    OFFSET 0
) lake
ORDER BY outer_rel.unrelated, lake.id, lake.payload;

SET pg_lakebase.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
WITH outer_rel(unrelated) AS (
    VALUES (10), (20), (30)
)
SELECT outer_rel.unrelated, lake.id, lake.payload
FROM outer_rel
CROSS JOIN LATERAL (
    SELECT id, payload
    FROM customscan_rescan_lake lake
    WHERE lake.id = 25
      AND (lake.id + outer_rel.unrelated) > 0
    OFFSET 0
) lake
ORDER BY outer_rel.unrelated, lake.id, lake.payload;

SET pg_lakebase.customscan_mode = 'force';
WITH outer_rel(unrelated) AS (
    VALUES (10), (20), (30)
)
SELECT outer_rel.unrelated, lake.id, lake.payload
FROM outer_rel
CROSS JOIN LATERAL (
    SELECT id, payload
    FROM customscan_rescan_lake lake
    WHERE lake.id = 25
      AND (lake.id + outer_rel.unrelated) > 0
    OFFSET 0
) lake
ORDER BY outer_rel.unrelated, lake.id, lake.payload;

SET pg_lakebase.customscan_mode = 'off';
WITH outer_rel(unrelated) AS (
    VALUES (10), (20), (30)
)
SELECT outer_rel.unrelated, lake.id, lake.payload
FROM outer_rel
CROSS JOIN LATERAL (
    SELECT id, payload
    FROM customscan_rescan_lake lake
    WHERE lake.id = 25
      AND (lake.id + outer_rel.unrelated) > 0
    OFFSET 0
) lake
ORDER BY outer_rel.unrelated, lake.id, lake.payload;

-- B.5: PARAM_EXEC changes from a representable ConservativePruning value to
-- a value that cannot be encoded as an Iceberg Datum. The pushed predicate is
-- a ConservativePruning OR, so translation failure must widen by clearing the
-- Iceberg row_filter and relying on the residual qual. Keeping the previous
-- row_filter would reuse stale PARAM_EXEC values and filter out the marker=2
-- row before PostgreSQL can evaluate the residual OR.
CREATE TABLE customscan_rescan_date_lake (
    d date,
    marker integer,
    payload text
) USING iceberg;
INSERT INTO customscan_rescan_date_lake VALUES
    (DATE '2024-01-01', 1, 'finite_1'),
    (DATE '2024-01-02', 2, 'finite_2'),
    (DATE '2024-01-03', 3, 'finite_3');
-- Pin DateStyle so the date::text rendering below is independent of the
-- environment's default, matching the DateStyle-independence pattern used by
-- type_conversion.
SET DateStyle = 'ISO, MDY';

SET pg_lakebase.customscan_mode = 'force';
WITH outer_rel(d, marker) AS (
    VALUES (DATE '2024-01-01', 1), (DATE 'infinity', 2)
)
SELECT outer_rel.marker AS outer_marker,
       outer_rel.d::text AS outer_d,
       lake.marker AS lake_marker,
       lake.d::text AS lake_d,
       lake.payload
FROM outer_rel
CROSS JOIN LATERAL (
    SELECT d, marker, payload
    FROM customscan_rescan_date_lake lake
    WHERE lake.d = outer_rel.d
       OR lake.marker = outer_rel.marker
    OFFSET 0
) lake
ORDER BY outer_marker, lake_marker, lake.payload;

SET pg_lakebase.customscan_mode = 'off';
WITH outer_rel(d, marker) AS (
    VALUES (DATE '2024-01-01', 1), (DATE 'infinity', 2)
)
SELECT outer_rel.marker AS outer_marker,
       outer_rel.d::text AS outer_d,
       lake.marker AS lake_marker,
       lake.d::text AS lake_d,
       lake.payload
FROM outer_rel
CROSS JOIN LATERAL (
    SELECT d, marker, payload
    FROM customscan_rescan_date_lake lake
    WHERE lake.d = outer_rel.d
       OR lake.marker = outer_rel.marker
    OFFSET 0
) lake
ORDER BY outer_marker, lake_marker, lake.payload;

DROP TABLE customscan_rescan_date_lake;

-- Restore planner GUCs so test isolation is preserved.
RESET enable_hashjoin;
RESET enable_mergejoin;
RESET enable_material;
RESET enable_nestloop;
RESET pg_lakebase.customscan_mode;

-- ============================================================================
-- Cleanup
-- ============================================================================
DROP TABLE customscan_rescan_lake;
-- customscan_prepared_nestloop_rescan.sql
-- Prepared nestloop rescan with parameterized inner scan.


-- ============================================================================
-- Setup: an Iceberg `lake` table with an `int4` `k`/`id` column. The
-- classifier promotes `k = <int4 literal>` (opno 96, int4eq) to
-- `Exact` (`EXACT_ALLOWLIST` in
-- `pg-iceberg-am/src/customscan/classifier.rs`), so a `WHERE k = $1`
-- clause whose `$1` is `PARAM_EXTERN` (or `PARAM_EXEC` after
-- `replace_nestloop_params` runs in the JOIN case) is BOTH pushed
-- for pruning AND recheck-bound at `BeginCustomScan`
--.
-- The matching outer relation `customscan_prep_outer` uses the same
-- column type (`int4`) so PG's nestloop join condition resolves
-- without coercion, which keeps the inner pushed predicate a clean
-- `Var(scan_relid, k) = Param` shape that the runtime translator
-- maps to an Iceberg equality predicate.
-- ============================================================================
CREATE TABLE customscan_prep_lake (
    k integer,
    payload text
) USING iceberg;

-- File 1: k ∈ [1, 10]
INSERT INTO customscan_prep_lake
SELECT g, 'lake_' || g
FROM generate_series(1, 10) AS g;

-- File 2: k ∈ [100, 110]
INSERT INTO customscan_prep_lake
SELECT g, 'lake_' || g
FROM generate_series(100, 110) AS g;

SELECT COUNT(*) AS lake_total_rows FROM customscan_prep_lake;

-- The outer relation for Block B. A heap table so the planner picks a
-- nestloop with the lake CustomScan on the inner side once we disable
-- the alternatives below. Three rows is enough to drive several
-- rescans through the framework's `chgParam`-gated re-translation
-- path without flooding the .out file.
CREATE TABLE customscan_prep_outer (
    id integer,
    label text
);
INSERT INTO customscan_prep_outer VALUES (1, 'one'), (5, 'five'), (105, 'oneoh5');

-- ============================================================================
-- Block A: PREPARE / EXECUTE
-- ============================================================================
--   PREPARE p AS SELECT * FROM lake WHERE k = $1;
--   EXECUTE p(1);
--   EXECUTE p(2);
-- We `PREPARE` two statements with identical text — one used under
-- `force` (CustomScan), one under `off` (SeqScan baseline) — because
-- PG's `plancache.c` caches the plan per plansource, and re-planning
-- a single prepared statement under a different GUC after the
-- generic-plan switch is not deterministic. Two prepared statements
-- give us two independent plansources that each consistently plan
-- under their intended mode.
-- A.1 plan guard: the `force` plan must be a CustomScan whose default
-- TEXT output carries a single `Pushed Filter: (k = $1)` line (Exact
-- equality on `k`, stripped from `plan.qual` so no local residual
-- `Filter:` line); the `off` plan must be a Seq Scan with
-- `Filter: (k = $1)`.

PREPARE customscan_prep_p1 (int) AS
SELECT k, payload FROM customscan_prep_lake WHERE k = $1 ORDER BY k, payload;

PREPARE customscan_prep_p1_baseline (int) AS
SELECT k, payload FROM customscan_prep_lake WHERE k = $1 ORDER BY k, payload;

SET pg_lakebase.customscan_mode = 'force';
EXPLAIN (COSTS OFF) EXECUTE customscan_prep_p1(1);

SET pg_lakebase.customscan_mode = 'off';
EXPLAIN (COSTS OFF) EXECUTE customscan_prep_p1_baseline(1);

-- A.2 EXECUTE p(1): the row set must equal the SeqScan baseline.
SET pg_lakebase.customscan_mode = 'force';
EXECUTE customscan_prep_p1(1);

SET pg_lakebase.customscan_mode = 'off';
EXECUTE customscan_prep_p1_baseline(1);

-- A.3 EXECUTE p(2): same prepared statement, different bound value.
-- The framework's per-EXECUTE `BeginCustomScan` re-resolves the
-- `PARAM_EXTERN` value via `paramInfo->paramFetch`/`params[$1]`
-- and rebuilds the native predicate with the
-- new bound value. The row set must still match the SeqScan
-- baseline — if `BeginCustomScan` had stuck with the value bound
-- on EXECUTE p(1), the result here would be wrong.
SET pg_lakebase.customscan_mode = 'force';
EXECUTE customscan_prep_p1(2);

SET pg_lakebase.customscan_mode = 'off';
EXECUTE customscan_prep_p1_baseline(2);

-- A.4 EXECUTE p(N) for a non-existent N: the row set must be empty
-- on both sides. This pins the "no false positives" half of
-- correctness — the previous EXECUTE's bound value must not leak
-- into this one.
SET pg_lakebase.customscan_mode = 'force';
EXECUTE customscan_prep_p1(999);

SET pg_lakebase.customscan_mode = 'off';
EXECUTE customscan_prep_p1_baseline(999);

DEALLOCATE customscan_prep_p1;
DEALLOCATE customscan_prep_p1_baseline;

-- ============================================================================
-- Block B: nestloop with ordinary `outer JOIN lake ON lake.k = outer.id`
-- ============================================================================
-- Task brief: "`SELECT * FROM outer JOIN lake ON lake.id = outer.id`:
-- nestloop picks the parameterized variant; EXPLAIN shows scan-level
-- filter on `id`; rescan re-translates only when `chgParam`
-- overlaps."
-- We name the lake column `k` (matching Block A's PREPARE shape) and
-- join `lake.k = outer.id`. The semantics are identical.

-- This block uses the ORDINARY join shape
-- calls out — `... JOIN lake l ON l.k = o.id` — rather than a
-- `CROSS JOIN LATERAL (... OFFSET 0)` subquery. The distinction is
-- not cosmetic. A bare mergejoinable equality `l.k = o.id` is
-- absorbed by PostgreSQL into an EquivalenceClass during planning
-- (`process_equivalence`) and REMOVED from `lake->joininfo`; the
-- `OFFSET 0` lateral shape instead keeps the equality as a lateral
-- join clause that survives in `joininfo`. Because the two shapes
-- reach the framework's `enumerate_param_path_groups` through
-- DIFFERENT paths, only the ordinary-join shape exercises the
-- EquivalenceClass-recovery pass (`enumerate_param_path_groups`
-- pass (b), which calls `generate_implied_equalities_for_column` to
-- re-derive the absorbed `l.k = o.id` clause). The lateral shape
-- walks `joininfo` directly (pass (a)) and never touches that
-- recovery path, so it cannot guard it. This block is the focused
-- Layer-2 guard for the EC-recovery enumeration; the broader
-- capability is pinned by `customscan_ordinary_join_pushdown.sql`
-- (. The lateral OFFSET-0 parameter path stays covered by
-- `customscan_rescan_param.sql` (.

-- Unlike the lateral subquery shape (which pins the lake on the
-- inner side via a one-directional lateral dependency), an ordinary
-- `JOIN ... ON ...` lets the planner choose join order freely. With
-- a small lake the planner reverses the order and drives from the
-- lake with a `Join Filter:`, defeating the inner-side pushdown
-- under test. We therefore use a dedicated large lake
-- (`customscan_prep_join_lake`, 5000 rows) and keep the tiny 3-row
-- `customscan_prep_outer`: driving from the large lake (a full scan
-- per outer row) is expensive, while driving from the tiny outer
-- with the parameterized inner lake CustomScan — whose cost
-- `customscan_mode = 'force'` biases to a small constant — is cheap.
-- So under `force` the planner selects the
-- `outer`-drives-`lake`-inner Nested Loop with the lake CustomScan
-- parameterized on `o.id`. (Block A keeps its own small
-- `customscan_prep_lake`, untouched, for the PREPARE/EXECUTE case.)
-- Hash / merge joins scan the inner relation once and never drive
-- `ReScanCustomScan`; a Materialize node caches the inner result
-- across outer rows. Disabling hash, merge, and material while
-- keeping nestloop on forces the Nested Loop the rescan contract
-- needs. These GUCs and the table sizes
-- apply equally to both `mode='force'` and `mode='off'`, so the
-- executor topology is identical on both sides; the only variable
-- across the parity assertion is whether the inner plan node is a
-- CustomScan or a Seq Scan, which is what `customscan_mode`
-- controls.

CREATE TABLE customscan_prep_join_lake (
    k integer,
    payload text
) USING iceberg;

-- File 1: k ∈ [1, 2500]
INSERT INTO customscan_prep_join_lake
SELECT g, 'lake_' || g
FROM generate_series(1, 2500) AS g;

-- File 2: k ∈ [10000, 12499]
INSERT INTO customscan_prep_join_lake
SELECT g, 'lake_' || g
FROM generate_series(10000, 12499) AS g;

SELECT COUNT(*) AS join_lake_total_rows FROM customscan_prep_join_lake;

SET enable_hashjoin = off;
SET enable_mergejoin = off;
SET enable_material = off;
SET enable_nestloop = on;

-- B.1 plan guard: nestloop with parameterized CustomScan on inner.
-- Under `force`, EXPLAIN must show:
--   - A `Nested Loop` plan node.
--   - Inner: `Custom Scan (pg-iceberg-am) on customscan_prep_join_lake`.
--   - On the inner, in default TEXT, a `Pushed Filter: (k = o.id)` line
--     — i.e. the parameterized `lake.k = outer.id` predicate (recovered
--     from the EquivalenceClass by enumeration pass (b)) is the pushed
--     (remote) predicate realized at the CustomScan node level (the
--     "scan-level filter on `id`". It is Exact
--     and stripped from `plan.qual`, so it does NOT appear as a residual
--     `Filter:` line above the scan, which is the structural evidence
--     that the framework picked the parameterized variant
-- and that the runtime translator pushed the
--     inner-side equality successfully. There is no
--     old-style diagnostic title, no default-mode provider-identity
--     line, and no count lines.
-- Under `off`, EXPLAIN must show:
--   - A `Nested Loop` plan node with the same topology.
--   - The join equality evaluated by ordinary PG join machinery
--     (a `Join Filter:` on the Nested Loop), not by Iceberg
--     pushdown.
SET pg_lakebase.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT o.id, o.label, l.k, l.payload
FROM customscan_prep_outer o
JOIN customscan_prep_join_lake l ON l.k = o.id
ORDER BY o.id, l.payload;

SET pg_lakebase.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT o.id, o.label, l.k, l.payload
FROM customscan_prep_outer o
JOIN customscan_prep_join_lake l ON l.k = o.id
ORDER BY o.id, l.payload;

-- B.2 result-set parity. PG's nestloop unconditionally calls
-- `ExecReScan(innerPlan)` for each new outer tuple
-- (`nodeNestloop.c`). The framework's `ReScanCustomScan` then
-- intersects `node->ss.ps.chgParam` with the cached
-- `cached_pushed_param_ids`:
--   - For each outer tuple, the join's nestParam id IS in the
--     pushed param ids (the pushed predicate is `l.k = $param`),
--     so `bms_overlap` is non-empty: the framework re-resolves
--     params, rebuilds the native predicate, and reopens the
--     cursor.
--   - If the framework had ignored the `chgParam` gate and reused
--     the previous outer tuple's predicate, the inner result would
--     be stale; the parity assertion below would fail.
--   - If the framework had failed to re-resolve `PARAM_EXEC`
--     (
--     `ExecSetParamPlanMulti` materialization for SubPlan-backed
--     params), the inner result would also diverge from the
--     baseline.
-- Outer values: 1 → matches lake row k=1; 5 → matches k=5; 105 →
-- matches k=105. Each outer tuple drives one inner rescan with a
-- different bound value, so the test exercises the "non-empty
-- `chgParam ∩ cached_pushed_param_ids`" branch of
-- across multiple iterations.
SET pg_lakebase.customscan_mode = 'force';
SELECT o.id, o.label, l.k, l.payload
FROM customscan_prep_outer o
JOIN customscan_prep_join_lake l ON l.k = o.id
ORDER BY o.id, l.payload;

SET pg_lakebase.customscan_mode = 'off';
SELECT o.id, o.label, l.k, l.payload
FROM customscan_prep_outer o
JOIN customscan_prep_join_lake l ON l.k = o.id
ORDER BY o.id, l.payload;

-- Restore planner GUCs so test isolation is preserved.
RESET enable_hashjoin;
RESET enable_mergejoin;
RESET enable_material;
RESET enable_nestloop;
RESET pg_lakebase.customscan_mode;

-- ============================================================================
-- Cleanup
-- ============================================================================
DROP TABLE customscan_prep_lake;
DROP TABLE customscan_prep_join_lake;
DROP TABLE customscan_prep_outer;
-- customscan_colliding_param_kinds.sql
-- EXTERN $1 and EXEC slot 1 resolve independently.


-- ============================================================================
-- Setup
-- An Iceberg `lake` table with two `int4` columns:
--   - `sel`  — matched against the correlated outer column (becomes the
--               `PARAM_EXEC` side of the collision after
--               `replace_nestloop_params`).
--   - `tag`  — matched against the prepared-statement `$1` (the
--               `PARAM_EXTERN` side).
-- Both `int4 =` comparisons (opno 96, int4eq) are promoted to `Exact`
-- by the classifier's `EXACT_ALLOWLIST`, so each conjunct is BOTH pushed
-- for pruning AND recheck-bound at `BeginCustomScan` and stripped from
-- `plan.qual`. Under Exact pushdown the native
-- predicate alone determines membership — exactly the regime

-- Multi-file layout: each INSERT opens a fresh DML session and finalizes
-- one Parquet file with bounded `lower_bounds` / `upper_bounds`
-- statistics, so the iceberg-lite metrics evaluator has files to prune.
-- The relevant property here is correctness, not pruning depth.
-- ============================================================================
CREATE TABLE customscan_collide_lake (
    sel integer,
    tag integer,
    payload text
) USING iceberg;

-- File 1: sel ∈ [1, 10]. `tag` deterministically derived from `sel`.
INSERT INTO customscan_collide_lake
SELECT g, (g % 3), 'a_' || g
FROM generate_series(1, 10) AS g;

-- File 2: sel ∈ [100, 110]. Different `tag` distribution so a wrong
-- (kind-blind) resolution would visibly change which rows match.
INSERT INTO customscan_collide_lake
SELECT g, (g % 2), 'b_' || g
FROM generate_series(100, 110) AS g;

SELECT COUNT(*) AS lake_total_rows FROM customscan_collide_lake;

-- The outer relation. A small heap table drives the nestloop; its `sel`
-- column is what becomes the inner CustomScan's `PARAM_EXEC` reference.
CREATE TABLE customscan_collide_outer (
    sel integer,
    label text
);
INSERT INTO customscan_collide_outer
VALUES (1, 'o1'), (5, 'o5'), (105, 'o105'), (999, 'gap');

-- Pin planner GUCs so the join is a nestloop with the lake CustomScan
-- on the rescanned inner side (a Hash/Merge join would scan the inner
-- once and never exercise `ReScanCustomScan`). These apply equally to
-- both GUC modes, so the executor topology is identical on both sides;
-- the only variable across the parity assertion is whether the inner
-- node is a CustomScan or a Seq Scan. See `customscan_rescan_param.sql`
-- for the full rationale.
SET enable_hashjoin = off;
SET enable_mergejoin = off;
SET enable_material = off;
SET enable_nestloop = on;

-- ============================================================================
-- Block A: plan guards — the inner lake scan carries BOTH a PARAM_EXTERN
-- ($1) and a PARAM_EXEC (outer.sel) reference in its pushed predicate.
-- Under `force`, EXPLAIN must show a `Nested Loop` whose inner node is a
-- `Custom Scan (pg-iceberg-am) on customscan_collide_lake`. In default
-- TEXT the inner node carries a single `Pushed Filter:` line joining
-- both pushed (remote) predicates — the `sel = outer.sel` EXEC conjunct
-- AND the `tag = $1` EXTERN conjunct (both Exact) — with ` AND `. Both
-- colliding-kind predicates are Exact and stripped from `plan.qual`, so
-- there is no local residual `Filter:` line above the scan — both are
-- realized at the scan node, not as a residual filter. There is no
-- old-style diagnostic title block, no default-mode provider-identity
-- line, and no numeric count lines.
-- Under `off`, EXPLAIN must show the same nestloop topology with a
-- `Seq Scan` inner node whose `Filter:` carries the full conjunction.
-- ============================================================================
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

SET pg_lakebase.customscan_mode = 'force';
EXPLAIN (COSTS OFF) EXECUTE customscan_collide_plan(1);

SET pg_lakebase.customscan_mode = 'off';
EXPLAIN (COSTS OFF) EXECUTE customscan_collide_plan(1);

DEALLOCATE customscan_collide_plan;

-- ============================================================================
-- Block B: row-set parity for the colliding-kind scan
-- `$1 = 1` selects lake rows whose `tag = 1`; the correlated
-- `l.sel = o.sel` selects the row whose `sel` equals each outer tuple's
-- `sel`. The two predicates reference parameters of DIFFERENT kinds
-- (EXTERN `$1` and the EXEC `outer.sel`) on the SAME scan. A kind-blind
-- resolver that collapsed the colliding ids would push a wrong Iceberg
-- predicate (e.g. comparing `tag` against the EXEC value or `sel`
-- against `$1`), changing the row set. The force/off parity below is
-- exactly that regression guard.
-- We run the whole join under a prepared statement so `$1` arrives as a
-- genuine `PARAM_EXTERN` at `BeginCustomScan` (
-- path), while each outer tuple drives a `ReScanCustomScan` that
-- re-resolves the `PARAM_EXEC` value (;

-- ============================================================================
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

-- B.1 $1 = 1: tag = 1 rows that also match a correlated outer.sel.
SET pg_lakebase.customscan_mode = 'force';
EXECUTE customscan_collide_q(1);

SET pg_lakebase.customscan_mode = 'off';
EXECUTE customscan_collide_q(1);

-- B.2 $1 = 0: a DIFFERENT EXTERN value selects a different `tag` class.
-- If the EXTERN value had been collapsed onto the EXEC slot (or vice
-- versa), B.1 and B.2 would not differ in the way the baseline says
-- they should. Running both EXTERN values against the same EXEC-driven
-- correlation pins the independence of the two ParamKeys.
SET pg_lakebase.customscan_mode = 'force';
EXECUTE customscan_collide_q(0);

SET pg_lakebase.customscan_mode = 'off';
EXECUTE customscan_collide_q(0);

-- B.3 $1 = 2: third `tag` class, exercising file 1 (tag = g % 3) where
-- tag = 2 occurs and file 2 (tag = g % 2) where it does not.
SET pg_lakebase.customscan_mode = 'force';
EXECUTE customscan_collide_q(2);

SET pg_lakebase.customscan_mode = 'off';
EXECUTE customscan_collide_q(2);

DEALLOCATE customscan_collide_q;

-- ============================================================================
-- Block C: ReScan after the PARAM_EXEC value changes
-- The nestloop above already rescans the inner CustomScan once per outer
-- tuple, so Block B exercises ReScan implicitly. Block C makes the
-- "changed EXEC value re-resolves by ParamKey" assertion explicit and
-- isolated: a multi-row outer whose `sel` values step through both data
-- files, with `$1` held fixed. Each outer tuple changes ONLY the EXEC
-- parameter; the EXTERN `$1` is constant. The row set for each outer
-- tuple must equal the SeqScan baseline for that tuple's EXEC value,
-- with the EXTERN `$1` predicate unchanged across rescans.
-- Repeated and out-of-range outer `sel` values (5, 5, 105, 999) also
-- pin that re-resolution never reuses a stale EXEC value from the prior
-- iteration and never leaks the EXTERN value into the EXEC slot.
-- ============================================================================
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

-- C.1 $1 = 0: across the rescans, only rows whose tag = 0 AND whose sel
-- matches the current outer tuple survive.
SET pg_lakebase.customscan_mode = 'force';
EXECUTE customscan_collide_rescan(0);

SET pg_lakebase.customscan_mode = 'off';
EXECUTE customscan_collide_rescan(0);

-- C.2 $1 = 1: same rescan sequence, different EXTERN value. Demonstrates
-- the EXTERN `$1` predicate re-applies unchanged on every rescan while
-- the EXEC value steps per outer tuple.
SET pg_lakebase.customscan_mode = 'force';
EXECUTE customscan_collide_rescan(1);

SET pg_lakebase.customscan_mode = 'off';
EXECUTE customscan_collide_rescan(1);

DEALLOCATE customscan_collide_rescan;

-- Restore planner GUCs so test isolation is preserved.
RESET enable_hashjoin;
RESET enable_mergejoin;
RESET enable_material;
RESET enable_nestloop;
RESET pg_lakebase.customscan_mode;

-- ============================================================================
-- Cleanup
-- ============================================================================
DROP TABLE customscan_collide_lake;
DROP TABLE customscan_collide_outer;
-- customscan_null_param_pushdown.sql
-- NULL param on Exact strict compare yields zero rows, not ERROR.


-- ============================================================================
-- Setup: an Iceberg `lake` table with an `int4` `id` column. The classifier
-- promotes `id = <int4 literal/param>` (opno 96, int4eq) to `Exact`
-- (`EXACT_ALLOWLIST` in `pg-iceberg-am/src/customscan/classifier.rs`), so a
-- `WHERE id = $1` clause whose `$1` is `PARAM_EXTERN` (Block A) or
-- `PARAM_EXEC` after `replace_nestloop_params` (Block B) is pushed for
-- pruning AND recheck-bound at `BeginCustomScan`.
-- ============================================================================
CREATE TABLE customscan_null_param_lake (
    id integer,
    payload text
) USING iceberg;

INSERT INTO customscan_null_param_lake
SELECT g, 'lake_' || g
FROM generate_series(1, 10) AS g;

SELECT COUNT(*) AS lake_total_rows FROM customscan_null_param_lake;

-- The outer relation for Block B. Row (1, 'one') supplies a non-NULL match;
-- row (NULL, 'null_row') supplies a NULL into the inner `PARAM_EXEC` slot,
-- which is the bug trigger. A tiny heap table so the
-- planner drives the nestloop from `outer` with the (large) lake CustomScan
-- on the rescanned inner side once the join alternatives are disabled in
-- Block B.
CREATE TABLE customscan_null_param_outer (
    id integer,
    label text
);
INSERT INTO customscan_null_param_outer VALUES (1, 'one'), (NULL, 'null_row');

-- ============================================================================
-- Block A: PREPARE / EXECUTE p(NULL)
-- ============================================================================
-- Two prepared statements with identical text — one used under `force`
-- (CustomScan), one under `off` (SeqScan baseline) — so each maintains its
-- own plansource and plans consistently under its intended mode. PREPARE
-- only parses; planning is lazy at the first EXECUTE/EXPLAIN under the
-- current GUC (force_generic_plan makes that plan generic).
SET plan_cache_mode = force_generic_plan;

PREPARE customscan_null_param_p1 (int) AS
SELECT id, payload FROM customscan_null_param_lake WHERE id = $1 ORDER BY id, payload;

PREPARE customscan_null_param_p1_baseline (int) AS
SELECT id, payload FROM customscan_null_param_lake WHERE id = $1 ORDER BY id, payload;

-- A.1 plan guard: the `force` plan must be a CustomScan with Exact pushdown
-- on `id`; the `off` plan must be a Seq Scan with `Filter: (id = $1)`. A
-- non-NULL bound value is used here; EXPLAIN does not execute, so no NULL is
-- resolved and the plan shape is value-independent (it is identical before
-- and after the fix).
SET pg_lakebase.customscan_mode = 'force';
EXPLAIN (COSTS OFF) EXECUTE customscan_null_param_p1(1);

SET pg_lakebase.customscan_mode = 'off';
EXPLAIN (COSTS OFF) EXECUTE customscan_null_param_p1_baseline(1);

-- A.2 EXECUTE p(NULL): a strict `id = NULL` comparison is UNKNOWN, which a
-- WHERE context filters out, so the correct result is 0 rows. The expected
-- `.out` encodes 0 rows. On UNFIXED code, the `force` block raises `ERROR`
-- (NullLiteral escalated to ereport(ERROR)) — the bug-confirming diff.
SET pg_lakebase.customscan_mode = 'force';
EXECUTE customscan_null_param_p1(NULL);

SET pg_lakebase.customscan_mode = 'off';
EXECUTE customscan_null_param_p1_baseline(NULL);

DEALLOCATE customscan_null_param_p1;
DEALLOCATE customscan_null_param_p1_baseline;
RESET plan_cache_mode;

-- ============================================================================
-- Block B: nestloop whose outer side supplies NULL to a PARAM_EXEC param
-- ============================================================================
-- An ORDINARY `JOIN lake l ON l.id = o.id` with hash/merge/material
-- disabled forces the outer relation to drive a nestloop with the lake
-- CustomScan on the rescanned inner side. After
-- `replace_nestloop_params`, `l.id = o.id` becomes `l.id = $param`
-- (`PARAM_EXEC`); the NULL outer row pushes NULL into that slot at
-- rescan time.

-- A bare mergejoinable equality `l.id = o.id` is absorbed into an
-- EquivalenceClass during planning and removed from `lake->joininfo`,
-- so the inner parameterized CustomScan is enumerated only via
-- `enumerate_param_path_groups` pass (b)
-- (`generate_implied_equalities_for_column`, which recovers the
-- absorbed equality). The lateral OFFSET-0 shape would instead keep
-- the clause in `joininfo` (pass (a)) and never exercise the recovery
-- path. Using the ordinary join here makes the NULL-folding behavior
-- exercise the same EC-recovery enumeration a
-- real `JOIN ... ON ...` query takes.

-- An ordinary join lets the planner choose join order freely; with a
-- small lake it would reverse the order and drive from the lake with a
-- `Join Filter:`, defeating the inner-side pushdown. We use a
-- dedicated large lake (`customscan_null_param_join_lake`, 5000 rows)
-- and keep `customscan_null_param_outer` tiny so that — under `force`
-- — driving from the tiny outer with the parameterized inner lake
-- CustomScan is the cheaper plan. (Block A keeps its own small
-- `customscan_null_param_lake`, untouched.) Hash / merge joins scan
-- the inner once and never drive `ReScanCustomScan`; a Materialize
-- node caches the inner result across outer rows. These GUCs and
-- table sizes apply equally to both `force` and `off`.
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

-- B.1 plan guard: `force` => Nested Loop with a parameterized CustomScan on
-- the inner side (Exact pushdown realized at the scan node, recovered from
-- the EquivalenceClass by enumeration pass (b)); `off` => Nested Loop whose
-- join predicate is a `Join Filter:`.
SET pg_lakebase.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT o.id, o.label, l.id AS lake_id, l.payload
FROM customscan_null_param_outer o
JOIN customscan_null_param_join_lake l ON l.id = o.id
ORDER BY o.id, l.payload;

SET pg_lakebase.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT o.id, o.label, l.id AS lake_id, l.payload
FROM customscan_null_param_outer o
JOIN customscan_null_param_join_lake l ON l.id = o.id
ORDER BY o.id, l.payload;

-- B.2 result parity: the outer NULL row contributes 0 rows (its inner
-- `l.id = NULL` rescan is UNKNOWN), and the outer id=1 row matches lake_1,
-- so the correct result is exactly one row. The expected `.out` encodes
-- that single row. On UNFIXED code, the `force` block raises `ERROR` when
-- the NULL outer row's rescan reaches the translator — the bug-confirming
-- diff.
SET pg_lakebase.customscan_mode = 'force';
SELECT o.id, o.label, l.id AS lake_id, l.payload
FROM customscan_null_param_outer o
JOIN customscan_null_param_join_lake l ON l.id = o.id
ORDER BY o.id, l.payload;

SET pg_lakebase.customscan_mode = 'off';
SELECT o.id, o.label, l.id AS lake_id, l.payload
FROM customscan_null_param_outer o
JOIN customscan_null_param_join_lake l ON l.id = o.id
ORDER BY o.id, l.payload;

-- Restore planner GUCs so test isolation is preserved.
RESET enable_hashjoin;
RESET enable_mergejoin;
RESET enable_material;
RESET enable_nestloop;
RESET pg_lakebase.customscan_mode;

DROP TABLE customscan_null_param_join_lake;

-- ============================================================================
-- Block C: mixed conjunction `id = $1 AND amount > 0` with $1 NULL
-- — added by
-- ============================================================================
-- A conjunction mixing a NULL strict comparison (`id = $1`, with `$1`
-- resolving to NULL) and a non-NULL pushed comparison (`amount > 0`). The
-- NULL comparison folds to `Predicate::AlwaysFalse`, and iceberg-lite's
-- `Predicate::and` short-circuit (`AlwaysFalse AND x -> AlwaysFalse`) drags
-- the whole conjunction to `AlwaysFalse` — so the query returns 0 rows with
-- no error (. On UNFIXED code the
-- `force` block raised `ERROR` (the NULL operand was rejected at scalar
-- decode and escalated as an `Exact`-clause failure).
-- This case needs an `amount` column, so it uses a DEDICATED table
-- (`customscan_null_param_mixed`) to stay independent of the Block A/B
-- `customscan_null_param_lake` schema. The table is created and dropped
-- entirely within this self-contained block.
-- As in Block A, `plan_cache_mode = force_generic_plan` keeps `$1` a runtime
-- `PARAM_EXTERN` (otherwise a custom plan would inline NULL as a `Const` and
-- `eval_const_expressions` would const-fold the clause at plan time, never
-- reaching the CustomScan). Two prepared statements (force / off) keep each
-- plansource consistent under its intended mode.
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

-- C.1 EXECUTE with `$1` NULL: `id = NULL` is UNKNOWN, so the `AlwaysFalse`
-- fold drags the conjunction to false → 0 rows. Byte-identical between
-- `force` (CustomScan) and `off` (SeqScan baseline).
SET pg_lakebase.customscan_mode = 'force';
EXECUTE customscan_null_param_mixed_p(NULL);

SET pg_lakebase.customscan_mode = 'off';
EXECUTE customscan_null_param_mixed_p_baseline(NULL);

DEALLOCATE customscan_null_param_mixed_p;
DEALLOCATE customscan_null_param_mixed_p_baseline;
RESET plan_cache_mode;
RESET pg_lakebase.customscan_mode;

DROP TABLE customscan_null_param_mixed;

-- ============================================================================
-- Block D: non-NULL parity regression guard — added by
-- ============================================================================
-- Preservation guard: the SAME prepared `WHERE id = $1` shape as Block A, but
-- with a NON-NULL `$1`, must CONTINUE to translate into an Iceberg
-- `Predicate` and prune. The fix only inserts a `Null`
-- early-return ahead of the unchanged non-NULL orientation path, so a
-- non-NULL param must still match the expected row(s) and be byte-identical
-- between `force` (CustomScan pushdown) and `off` (SeqScan baseline) — proving
-- the non-NULL pruning behavior was preserved across the fix.
-- Reuses the Block A table `customscan_null_param_lake` (id + payload, rows
-- 1..10), so this block is placed BEFORE the final Cleanup `DROP TABLE`.
-- `plan_cache_mode = force_generic_plan` keeps `$1` a runtime `PARAM_EXTERN`
-- (the same shape Block A exercises with NULL), confirming the non-NULL path
-- is preserved end-to-end.
SET plan_cache_mode = force_generic_plan;

PREPARE customscan_null_param_p_nonnull (int) AS
SELECT id, payload FROM customscan_null_param_lake WHERE id = $1 ORDER BY id, payload;

PREPARE customscan_null_param_p_nonnull_baseline (int) AS
SELECT id, payload FROM customscan_null_param_lake WHERE id = $1 ORDER BY id, payload;

-- D.1 EXECUTE p(5): `id = 5` is a non-NULL strict comparison, pushed as an
-- `Exact` predicate and used for pruning; it matches exactly row 5. The result
-- is byte-identical between `force` (CustomScan pushdown) and `off` (SeqScan
-- baseline) — the non-NULL pruning is preserved.
SET pg_lakebase.customscan_mode = 'force';
EXECUTE customscan_null_param_p_nonnull(5);

SET pg_lakebase.customscan_mode = 'off';
EXECUTE customscan_null_param_p_nonnull_baseline(5);

DEALLOCATE customscan_null_param_p_nonnull;
DEALLOCATE customscan_null_param_p_nonnull_baseline;
RESET plan_cache_mode;
RESET pg_lakebase.customscan_mode;

-- ============================================================================
-- Cleanup
-- ============================================================================
DROP TABLE customscan_null_param_lake;
DROP TABLE customscan_null_param_outer;
