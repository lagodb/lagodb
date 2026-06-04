-- customscan_ordinary_join_pushdown.sql
-- Ordinary equijoin parameterized pushdown and EXPLAIN shape.

DROP EXTENSION IF EXISTS pg_iceberg_am CASCADE;
CREATE EXTENSION IF NOT EXISTS pg_iceberg_am;

-- ============================================================================
-- Setup: an Iceberg `lake` table with an `int4` `k` column. The
-- classifier promotes `k = <int4>` (opno 96, int4eq) to `Exact`
-- (`EXACT_ALLOWLIST` in `pg-iceberg-am/src/customscan/classifier.rs`),
-- so the join predicate `l.k = o.id` — after `replace_nestloop_params`
-- rewrites the outer `Var` into a `PARAM_EXEC` `Param` — is BOTH pushed
-- for pruning AND recheck-bound at `BeginCustomScan`.
-- The outer relation `customscan_ord_outer` uses the same column type
-- (`int4`) so PG's nestloop join condition resolves without coercion,
-- which keeps the inner pushed predicate a clean
-- `Var(scan_relid, k) = Param` shape that the runtime translator maps to
-- an Iceberg equality predicate.
-- `lake` is deliberately large (5000 rows across two files) so that,
-- for an ordinary JOIN with a tiny 4-row `outer`, driving from `outer`
-- with the parameterized inner lake CustomScan is the cheaper plan
-- under `force` (see the header rationale on join-order selection).
-- ============================================================================
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

-- The outer relation. A tiny heap table so the planner drives the
-- nestloop from `outer` with the lake CustomScan on the parameterized
-- inner side once the alternatives below are disabled. Four rows cover
-- every case.4 call out:
--   - (1,     'one')      -> matches lake k=1     (matching key)
--   - (2500,  'last')     -> matches lake k=2500  (matching key)
--   - (999999,'no_match') -> no lake row          (non-matching key)
--   - (NULL,  'null_row') -> NULL join key        (-> 0 rows, 3VL)
CREATE TABLE customscan_ord_outer (
    id integer,
    label text
);
INSERT INTO customscan_ord_outer
VALUES (1, 'one'), (2500, 'last'), (999999, 'no_match'), (NULL, 'null_row');

-- Force a Nested Loop with the lake CustomScan on the rescanned inner
-- side (see header rationale).
SET enable_hashjoin = off;
SET enable_mergejoin = off;
SET enable_material = off;
SET enable_nestloop = on;

-- ============================================================================
-- A. Plan guard
-- ============================================================================
-- Under `force`, EXPLAIN must show:
--   - A `Nested Loop` plan node.
--   - Inner: `Custom Scan (pg-iceberg-am) on customscan_ord_lake`
--     (
--     exercised).
--   - On the inner, in default TEXT, a `Pushed Filter: (k = o.id)` line
--     — the ordinary join predicate `l.k = o.id` is the pushed (remote)
--     predicate realized at the CustomScan node (
--     8.1). It is Exact and stripped from `plan.qual`, so it does NOT
--     appear as a residual `Filter:` line above the scan
--, which is the structural evidence that the
--     classifier accepted the outer `Var` and the runtime translator
--     pushed the inner-side equality successfully. There is no
--     old-style diagnostic title, no default-mode provider-identity
--     line, and no count lines.
-- Under `off`, EXPLAIN shows the same nestloop topology with a `Seq Scan
-- on customscan_ord_lake` inner whose join predicate is an ordinary
-- `Filter:` line — the baseline against which parity is asserted.
SET pg_lakebase.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT o.id, o.label, l.k, l.payload
FROM customscan_ord_outer o
JOIN customscan_ord_lake l ON l.k = o.id
ORDER BY o.id, l.payload;

SET pg_lakebase.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT o.id, o.label, l.k, l.payload
FROM customscan_ord_outer o
JOIN customscan_ord_lake l ON l.k = o.id
ORDER BY o.id, l.payload;

-- ============================================================================
-- B. Result parity
-- ============================================================================
-- The same query under `force` then `off`. PG's nestloop calls
-- `ExecReScan(innerPlan)` for each outer tuple; the framework's
-- `ReScanCustomScan` re-resolves the pushed `PARAM_EXEC` to the current
-- outer row's `id` and rebuilds the native predicate when `chgParam`
-- overlaps the cached pushed param ids.
-- Expected rows (inner join, total order `o.id, l.payload`):
--   - id=1      -> lake_1         (matching key)
--   - id=2500   -> lake_2500      (matching key)
--   - id=999999 -> 0 rows         (non-matching key
--   - NULL      -> 0 rows         (NULL key folds to AlwaysFalse,

-- The two row blocks must be byte-identical: enabling pushdown must not
-- change which rows come back.
SET pg_lakebase.customscan_mode = 'force';
SELECT o.id, o.label, l.k, l.payload
FROM customscan_ord_outer o
JOIN customscan_ord_lake l ON l.k = o.id
ORDER BY o.id, l.payload;

SET pg_lakebase.customscan_mode = 'off';
SELECT o.id, o.label, l.k, l.payload
FROM customscan_ord_outer o
JOIN customscan_ord_lake l ON l.k = o.id
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
DROP TABLE customscan_ord_lake;
DROP TABLE customscan_ord_outer;
-- customscan_multi_outer_join_pushdown.sql
-- Multiple outer rels in join-parameterized pushdown.


-- ============================================================================
-- Setup: an Iceberg `lake` table with two `int4` key columns `k` and
-- `k2`. The classifier promotes `k = <int4>` / `k2 = <int4>` (opno 96,
-- int4eq) to `Exact` (`EXACT_ALLOWLIST` in
-- `pg-iceberg-am/src/customscan/classifier.rs`), so each leg of the
-- join predicate `l.k = o1.id AND l.k2 = o2.id2` — after
-- `replace_nestloop_params` rewrites the outer `Var`s into `PARAM_EXEC`
-- `Param`s — is a clean `Var(scan_relid, ...) = Param` shape.
-- Each lake row has `k2 = k`, so a lake row `k` matches the join only
-- when `o1.id = o2.id2 = k`. The two outer relations use the same
-- column type (`int4`) so PG's nestloop join conditions resolve without
-- coercion.
-- `lake` is deliberately large (5000 rows across two files) so the
-- planner drives from the tiny outer relations with the parameterized
-- inner lake CustomScan under `force` (see header rationale).
-- ============================================================================
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

-- First outer relation. `grp` bridges to `o2` (a NON-key column, so the
-- lake keys stay in independent ECs — see header). Four rows cover a
-- matching key, a second matching key, a non-matching key, and a NULL
-- key:
--   - (1,      'A', 'o1_one')  -> pairs with o2 grp 'A' (matches lake k=1)
--   - (2500,   'A', 'o1_last') -> pairs with o2 grp 'A' (matches lake k=2500)
--   - (999999, 'B', 'o1_none') -> grp 'B' (no lake row) (non-matching)
--   - (NULL,   'C', 'o1_null') -> NULL join key (-> 0 rows, 3VL)
CREATE TABLE customscan_mo_o1 (
    id integer,
    grp text,
    label text
);
INSERT INTO customscan_mo_o1
VALUES (1, 'A', 'o1_one'), (2500, 'A', 'o1_last'),
       (999999, 'B', 'o1_none'), (NULL, 'C', 'o1_null');

-- Second outer relation. The lake predicate's second leg
-- `l.k2 = o2.id2` references this relation, so the lake scan's join is
-- parameterized on a key from a SECOND distinct outer relation. `grp`
-- bridges to `o1`:
--   - (1,    'A', 'o2_one')  -> grp 'A'
--   - (2500, 'A', 'o2_last') -> grp 'A'
--   - (7777, 'B', 'o2_none') -> grp 'B' (no lake row)
CREATE TABLE customscan_mo_o2 (
    id2 integer,
    grp text,
    tag text
);
INSERT INTO customscan_mo_o2
VALUES (1, 'A', 'o2_one'), (2500, 'A', 'o2_last'), (7777, 'B', 'o2_none');

-- Force a Nested Loop with the lake CustomScan on the rescanned inner
-- side (see header rationale).
SET enable_hashjoin = off;
SET enable_mergejoin = off;
SET enable_material = off;
SET enable_nestloop = on;

-- ============================================================================
-- A. Plan guard
-- ============================================================================
-- Under `force`, EXPLAIN must show the inner `lake` relation as a
-- `Custom Scan (pg-iceberg-am)` that, in default TEXT, carries a
-- `Pushed Filter:` line with at least one pushed (remote) predicate —
-- confirming a join key from one of the two outer relations is actually
-- pushed into the inner scan (the other key remains an upper `Join
-- Filter`). This is what makes the result-parity check below a real
-- test of multi-outer pushdown rather than a tautology (see header
-- rationale).
SET pg_lakebase.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT o1.id, o2.id2, l.k, l.k2, l.payload
FROM customscan_mo_o1 o1
JOIN customscan_mo_o2 o2 ON o1.grp = o2.grp
JOIN customscan_mo_lake l ON l.k = o1.id AND l.k2 = o2.id2
ORDER BY o1.id, o2.id2, l.k;

-- ============================================================================
-- B. Result parity
-- ============================================================================
-- `o1 JOIN o2 ON o1.grp = o2.grp` pairs every grp-'A' o1 row with every
-- grp-'A' o2 row, giving the 2x2 cross product
--   (o1.id, o2.id2) in {(1,1), (1,2500), (2500,1), (2500,2500)}
-- (grp 'B'/'C' rows find no lake match / are NULL). The lake predicate
-- `l.k = o1.id AND l.k2 = o2.id2` then keeps only the rows where BOTH
-- keys hit a lake row (lake has k2 = k, so only the diagonal matches):
--   - (1,    1)    -> lake k=1, k2=1       -> lake_1
--   - (1,    2500) -> no lake row (k=1, k2=2500)     -> dropped
--   - (2500, 1)    -> no lake row (k=2500, k2=1)     -> dropped
--   - (2500, 2500) -> lake k=2500, k2=2500  -> lake_2500
-- Expected rows (total order `o1.id, o2.id2, l.k`):
--   - 1    | 1    | 1    | 1    | lake_1
--   - 2500 | 2500 | 2500 | 2500 | lake_2500
-- The off-diagonal pairs are the precise guard for
-- pushed key (k = o1.id) and the residual key (k2 = o2.id2) must each
-- resolve to the value bound for THEIR own outer relation on the current
-- row. A spurious accept that pushed a key bound for the wrong outer
-- relation would admit an off-diagonal pair and diverge from the
-- baseline. The two row blocks (force then off) must be byte-identical
--.
SET pg_lakebase.customscan_mode = 'force';
SELECT o1.id, o2.id2, l.k, l.k2, l.payload
FROM customscan_mo_o1 o1
JOIN customscan_mo_o2 o2 ON o1.grp = o2.grp
JOIN customscan_mo_lake l ON l.k = o1.id AND l.k2 = o2.id2
ORDER BY o1.id, o2.id2, l.k;

SET pg_lakebase.customscan_mode = 'off';
SELECT o1.id, o2.id2, l.k, l.k2, l.payload
FROM customscan_mo_o1 o1
JOIN customscan_mo_o2 o2 ON o1.grp = o2.grp
JOIN customscan_mo_lake l ON l.k = o1.id AND l.k2 = o2.id2
ORDER BY o1.id, o2.id2, l.k;

-- Restore planner GUCs so test isolation is preserved.
RESET enable_hashjoin;
RESET enable_mergejoin;
RESET enable_material;
RESET enable_nestloop;
RESET pg_lakebase.customscan_mode;

-- ============================================================================
-- Cleanup
-- ============================================================================
DROP TABLE customscan_mo_lake;
DROP TABLE customscan_mo_o1;
DROP TABLE customscan_mo_o2;
-- customscan_variant_selection.sql
-- Plain vs join-parameterized CustomPath selection under force.


-- ============================================================================
-- Setup: an Iceberg `lake` table with `int4` `k` and `payload`
-- columns. The classifier promotes `k = <int4 literal/param>`
-- (opno 96, int4eq) and `k >= <int4 literal>` (opno 525, int4ge) to
-- `Exact` via the `EXACT_ALLOWLIST` in
-- `pg-iceberg-am/src/customscan/classifier.rs`. The matching outer
-- relation `customscan_var_outer` uses the same `int4` column type
-- so PG's nestloop join condition resolves without coercion,
-- keeping the inner pushed predicate a clean
-- `Var(scan_relid, k) op Param` shape that the runtime translator
-- maps to an Iceberg predicate.
-- ============================================================================
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

-- The outer relation. A small heap table so the planner picks a
-- nestloop with the lake CustomScan on the inner side once we
-- disable the alternatives below.
CREATE TABLE customscan_var_outer (
    id integer,
    label text
);
INSERT INTO customscan_var_outer VALUES (1, 'one'), (5, 'five'), (105, 'oneoh5');

-- Pin planner GUCs so the executor topology is deterministic and the
-- nestloop drives a per-outer rescan. See header comment for
-- rationale.
SET enable_hashjoin = off;
SET enable_mergejoin = off;
SET enable_material = off;
SET enable_nestloop = on;

-- ============================================================================
-- Block A: NON-pushable join clause → unparameterized (Plain) variant wins
-- ============================================================================
-- The lateral subquery
--   CROSS JOIN LATERAL (
--       SELECT k, payload FROM customscan_var_lake l
--       WHERE l.k >= 0 AND (l.k + 1) = o.id
--       OFFSET 0
--   ) l
-- attaches `l.k >= 0` as a baserestrictinfo of the lake (no outer
-- reference) and `(l.k + 1) = o.id` as a join clause in the lake's
-- `joininfo` (it has an outer reference; PG marks the lake rel
-- lateral on `o`).
-- The Iceberg classifier:
--   - `l.k >= 0` → Exact (operator triple `(525, 0, 0)` is in
--     `EXACT_ALLOWLIST`, shape is `column op literal`).
--   - `(l.k + 1) = o.id` → Unsupported. The LHS is a
--     `FuncExpr(int4pl, l.k, 1)`, not a bare scan-relation `Var`.
--     The strict `column op literal/param` shape filter in
--     `classify_op` collapses any non-`Var` operand to
--     `LeafKind::Other` and rejects the leaf.
-- Path-stage enumeration:
--   - `baserestrict_split.pushed = [l.k >= 0]` (Exact entry).
--   - `enumerate_param_path_groups` enumerates the JoinParameterized
--     variant's `outer_relids = {customscan_var_outer}` from
--     `joininfo`. `resolve_and_split_ppi_clauses` walks
--     `ppi_clauses` (which contains only the Unsupported join
--     clause) and produces `ppi_split.pushed = []`.
--   - `merged_split.pushed = baserestrict_split.pushed ++
--     ppi_split.pushed = [l.k >= 0]` — same as the Plain variant.
-- The JoinParameterized variant offers NO additional pushable
-- clause beyond Plain. Per the cost model in
-- the parameterized variant's cost is lower than the
-- rescan-amortized seqscan only when pushdown wins; here it
-- doesn't, so the Plain variant — whose cost is plain
-- (non-rescan) seqscan reduced by `[l.k >= 0]` pushdown —
-- dominates. The planner picks the Plain CustomPath; the join
-- clause stays as a residual `Filter:` on the inner scan
-- (PG attaches it to the inner via the lateral parameterization
-- chain, but the framework's plan-stage classifier rejects it
-- and routes it to `plan.qual`).

-- A.1 plan guard.
-- Under `force`, EXPLAIN must show:
--   - A `Nested Loop` plan node.
--   - Inner: `Custom Scan (pg-iceberg-am) on customscan_var_lake l`.
--   - On the inner, in default TEXT, a `Pushed Filter: (k >= 0)` line
--     — the baserestrictinfo `l.k >= 0` is the only pushed (remote)
--     entry (Exact); the join clause `(l.k + 1) = o.id` is Unsupported
--     and stays as the local residual, surfacing only as the inner
--     standard `Filter:` line above the `Pushed Filter:` line. This is
--     the structural evidence that the framework picked the
--     unparameterized (Plain) variant rather than a JoinParameterized
--     one — the parameterized variant
--     would have moved this clause into the pushed slice or kept the
--     same shape (which it can't because the clause is structurally
--     Unsupported). There is no old-style diagnostic title, no
--     default-mode provider-identity line, and no count lines.
-- Under `off`, EXPLAIN must show:
--   - A `Nested Loop` with the same shape.
--   - Inner: `Seq Scan on customscan_var_lake l` with
--     `Filter: ((k >= 0) AND ((k + 1) = o.id))` — the conjunction
--     evaluated by ordinary PG scan-qual machinery, not by Iceberg
--     pushdown.
SET pg_lakebase.customscan_mode = 'force';
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

SET pg_lakebase.customscan_mode = 'off';
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

-- A.2 result-set parity. With outer.id ∈ {1, 5, 105} and the join
-- predicate `l.k + 1 = o.id`:
--   - o.id = 1   → l.k = 0   (no match — gap before file 1)
--   - o.id = 5   → l.k = 4   → (4, 'lake_4')
--   - o.id = 105 → l.k = 104 → (104, 'lake_104')
-- Both `l.k = 4` and `l.k = 104` satisfy `l.k >= 0`, so they
-- survive the baserestrict. Both paths must return the same row
-- set. A regression where the framework pushed the bogus join
-- clause as Exact would silently drop rows here.
SET pg_lakebase.customscan_mode = 'force';
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

SET pg_lakebase.customscan_mode = 'off';
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

-- ============================================================================
-- Block B: PUSHABLE join clause → parameterized (JoinParameterized) variant wins
-- ============================================================================
-- An ordinary
--   JOIN customscan_var_join_lake l ON l.k = o.id
-- attaches the join key `l.k = o.id` as a mergejoinable equality.
-- PostgreSQL absorbs it into an EquivalenceClass during planning and
-- removes it from `lake->joininfo`; the framework's
-- `enumerate_param_path_groups` recovers it via pass (b)
-- (`generate_implied_equalities_for_column`) and emits a
-- JoinParameterized variant on `lake` parameterized on `o.id`.

-- Block A's clause `(l.k + 1) = o.id` is NOT a bare mergejoinable
-- equality (the LHS is a `FuncExpr`), so it is never absorbed into an
-- EquivalenceClass and only survives as a join clause inside a LATERAL
-- subquery — Block A must keep the LATERAL shape to present that
-- clause at all. Block B's clause IS a bare equality, so the ordinary
-- `JOIN ... ON ...` shape is both the realistic query shape and the
-- one that exercises the EC-recovery enumeration path (pass (b)) that
-- a lateral OFFSET-0 shape would bypass.
-- The Iceberg classifier promotes `l.k = o.id` to `Exact`:
--   - The operator triple `(96, 0, 0)` (int4eq, no collation) is in
--     `EXACT_ALLOWLIST`.
--   - At runtime — after `replace_nestloop_params` rewrites the
--     outer Var into a `PARAM_EXEC` Param — the shape is
--     `Var(l.k) = Param`, which the classifier accepts as
--     `column op param`.
-- Path-stage enumeration:
--   - `baserestrict_split.pushed = []` (no baserestrictinfo).
--   - `enumerate_param_path_groups` pass (b) recovers the absorbed
--     equality and enumerates the JoinParameterized variant's
--     `outer_relids = {customscan_var_outer}`. The framework's
--     classifier walks `ppi_clauses` and accepts the join clause as
--     Exact, producing `ppi_split.pushed = [l.k = o.id]`.
--   - `merged_split.pushed = [l.k = o.id]` — non-empty, so the
--     provider's `create_path` accepts the JoinParameterized
--     variant.
-- Under nestloop's rescan-amortized cost, the
-- JoinParameterized variant — whose `path.rows = ppi_rows < parent->rows`
-- reflects the per-outer selectivity — dominates
-- the Plain variant: the planner picks it; the equality predicate
-- is realized at the inner CustomScan node level, NOT as a residual
-- `Filter:` above the scan.

-- An ordinary join lets the planner choose join order freely; with a
-- small lake it would reverse the order and drive from the lake. We
-- use a dedicated large lake (`customscan_var_join_lake`, 5000 rows)
-- so that — under `force` — driving from the tiny 3-row
-- `customscan_var_outer` with the parameterized inner lake CustomScan
-- is the cheaper plan. (Block A keeps its own small
-- `customscan_var_lake`, untouched, because its `(l.k + 1) = o.id`
-- clause requires the LATERAL shape.) The hash/merge/material GUCs
-- pinned above apply equally to both `force` and `off`.
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

-- B.1 plan guard.
-- Under `force`, EXPLAIN must show:
--   - A `Nested Loop` plan node WITHOUT a standard `Filter:` line on
--     the inner scan (the equality is realized at the inner CustomScan
--     node level, shown as the inner `Pushed Filter:` line).
--   - Inner: `Custom Scan (pg-iceberg-am) on customscan_var_join_lake l`.
--   - On the inner, in default TEXT, a `Pushed Filter: (k = o.id)` line
--     — the parameterized `l.k = o.id` predicate is the pushed (remote)
--     predicate realized at the CustomScan node level; it is Exact and
--     stripped from `plan.qual`, so there is no local residual `Filter:`
--     line. This is the structural evidence that the framework picked
--     the JoinParameterized variant. There is
--     no old-style diagnostic title, no default-mode provider-identity
--     line, and no count lines.
-- Under `off`, EXPLAIN must show:
--   - A `Nested Loop` plan node with the same topology.
--   - The join equality evaluated by ordinary PG join machinery
--     (a `Join Filter:` on the Nested Loop), not by Iceberg pushdown.
SET pg_lakebase.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT o.id, o.label, l.k, l.payload
FROM customscan_var_outer o
JOIN customscan_var_join_lake l ON l.k = o.id
ORDER BY o.id, l.payload;

SET pg_lakebase.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT o.id, o.label, l.k, l.payload
FROM customscan_var_outer o
JOIN customscan_var_join_lake l ON l.k = o.id
ORDER BY o.id, l.payload;

-- B.2 result-set parity. Each outer.id matches exactly one lake
-- row (1 → lake_1, 5 → lake_5, 105 → lake_105). Three outer
-- tuples drive three nestloop iterations through the inner
-- parameterized CustomScan. The framework's
-- `cached_pushed_param_ids` bitmap contains the join's nestParam
-- id, so each rescan triggers the "non-empty `chgParam ∩
-- cached_pushed_param_ids`" branch:
-- re-resolve the param, rebuild the predicate, redo
-- manifest/file pruning, reopen the cursor. If the framework had
-- stuck with the previous outer tuple's bound value, the result
-- set would diverge from the SeqScan baseline.
SET pg_lakebase.customscan_mode = 'force';
SELECT o.id, o.label, l.k, l.payload
FROM customscan_var_outer o
JOIN customscan_var_join_lake l ON l.k = o.id
ORDER BY o.id, l.payload;

SET pg_lakebase.customscan_mode = 'off';
SELECT o.id, o.label, l.k, l.payload
FROM customscan_var_outer o
JOIN customscan_var_join_lake l ON l.k = o.id
ORDER BY o.id, l.payload;

-- ============================================================================
-- Cleanup
-- ============================================================================
RESET enable_hashjoin;
RESET enable_mergejoin;
RESET enable_material;
RESET enable_nestloop;
RESET pg_lakebase.customscan_mode;

DROP TABLE customscan_var_lake;
DROP TABLE customscan_var_join_lake;
DROP TABLE customscan_var_outer;
