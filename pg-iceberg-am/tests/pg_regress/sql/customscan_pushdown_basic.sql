-- customscan_where_eq_pushdown.sql
-- EXPLAIN and results for simple `WHERE a = 1` CustomScan pushdown.

DROP EXTENSION IF EXISTS pg_iceberg_am CASCADE;
CREATE EXTENSION IF NOT EXISTS pg_iceberg_am;

-- ============================================================================
-- Setup: an Iceberg table whose schema (`a integer, b text`) matches the
-- Each INSERT opens a fresh DML session and finalizes one
-- Parquet data file. We populate three rows so:
--   - `WHERE a = 1` matches exactly one row, exercising the
--     "single-row hit" shape that.
--   - The other rows give us non-trivial result-set parity to verify
--     against the SeqScan baseline.
-- ============================================================================
CREATE TABLE customscan_where_eq_t (
    a integer,
    b text
) USING iceberg;

INSERT INTO customscan_where_eq_t VALUES (1, 'one');
INSERT INTO customscan_where_eq_t VALUES (2, 'two');
INSERT INTO customscan_where_eq_t VALUES (3, 'three');

SELECT COUNT(*) AS total_rows FROM customscan_where_eq_t;

-- ============================================================================
-- Test 1: EXPLAIN parity
-- ============================================================================

-- CustomScan path. Expected EXPLAIN shape (default TEXT):
--   Custom Scan (pg-iceberg-am) on customscan_where_eq_t
--     Pushed Filter: (a = 1)
-- (no old-style diagnostic title, no default-mode provider-identity
-- line, no count lines; `a = 1` is Exact and stripped from
-- `plan.qual`, so there is no local residual `Filter:` line either)
SET pg_lakebase.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT a, b FROM customscan_where_eq_t WHERE a = 1;

-- SeqScan baseline. Expected EXPLAIN shape:
--   Seq Scan on customscan_where_eq_t
--     Filter: (a = 1)
SET pg_lakebase.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT a, b FROM customscan_where_eq_t WHERE a = 1;

-- ============================================================================
-- Test 2: Result-set parity
-- ============================================================================
-- Both modes must return the exact same row set. `ORDER BY` makes the
-- output deterministic so pg_regress diffs are stable.

SET pg_lakebase.customscan_mode = 'force';
SELECT a, b FROM customscan_where_eq_t WHERE a = 1 ORDER BY a, b;

SET pg_lakebase.customscan_mode = 'off';
SELECT a, b FROM customscan_where_eq_t WHERE a = 1 ORDER BY a, b;

-- ============================================================================
-- Test 3: EXPLAIN VERBOSE carries the provider identity and the
-- classified labeled predicate lines.
-- Under `force` and VERBOSE, the output carries `Scan Purpose: Query` and a `Provider:` line
-- plus the deparsed predicate text on labeled lines `Pushed Filter
-- Exact:` and `Recheck:` (both non-empty for this plan). The empty
-- classes (`Pushed Filter Conservative Pruning:`, and any local residual) are
-- omitted — only non-empty classes print labeled predicate lines
--. No numeric count lines appear.
-- ============================================================================

SET pg_lakebase.customscan_mode = 'force';
EXPLAIN (VERBOSE, COSTS OFF)
SELECT a, b FROM customscan_where_eq_t WHERE a = 1;

-- ============================================================================
-- Cleanup
-- ============================================================================
RESET pg_lakebase.customscan_mode;
DROP TABLE customscan_where_eq_t;
-- customscan_exact_pushdown.sql
-- Exact-pushdown results and EXPLAIN match SeqScan (force vs off) for integer ops.


-- ============================================================================
-- Setup: create two parallel Iceberg tables, one with `int4` `id` and one
-- with `int8` `id`, each populated across multiple data files so file-level
-- pruning has something to do. Each INSERT statement opens a fresh DML
-- session and produces one Parquet file with bounded `lower_bounds[id]` /
-- `upper_bounds[id]` statistics.
-- The second file in each table includes NULL ids. Exact comparisons omit the
-- original qual from `plan.qual`, so the Iceberg predicate itself must match
-- PostgreSQL three-valued WHERE semantics for these NULL-bearing files.
-- ============================================================================

CREATE TABLE customscan_exact_pushdown_int4 (
    id integer,
    payload text
) USING iceberg;

-- File 1: id in [1, 50]
INSERT INTO customscan_exact_pushdown_int4
SELECT g, 'i4_a_' || g
FROM generate_series(1, 50) AS g;

-- File 2: id in [100, 150] with three NULLs interleaved
INSERT INTO customscan_exact_pushdown_int4
SELECT
    CASE WHEN g % 17 = 0 THEN NULL ELSE g END,
    'i4_b_' || g
FROM generate_series(100, 150) AS g;

-- File 3: id in [1000, 1050]
INSERT INTO customscan_exact_pushdown_int4
SELECT g, 'i4_c_' || g
FROM generate_series(1000, 1050) AS g;

SELECT COUNT(*) AS int4_total_rows FROM customscan_exact_pushdown_int4;
SELECT COUNT(*) AS int4_null_rows
FROM customscan_exact_pushdown_int4 WHERE id IS NULL;

CREATE TABLE customscan_exact_pushdown_int8 (
    id bigint,
    payload text
) USING iceberg;

-- File 1: id in [1, 50]
INSERT INTO customscan_exact_pushdown_int8
SELECT g::bigint, 'i8_a_' || g
FROM generate_series(1, 50) AS g;

-- File 2: id in [100, 150] with three NULLs interleaved
INSERT INTO customscan_exact_pushdown_int8
SELECT
    CASE WHEN g % 17 = 0 THEN NULL ELSE g::bigint END,
    'i8_b_' || g
FROM generate_series(100, 150) AS g;

-- File 3: id in [10_000_000_000, 10_000_000_050]
INSERT INTO customscan_exact_pushdown_int8
SELECT (10000000000 + g)::bigint, 'i8_c_' || g
FROM generate_series(0, 50) AS g;

SELECT COUNT(*) AS int8_total_rows FROM customscan_exact_pushdown_int8;
SELECT COUNT(*) AS int8_null_rows
FROM customscan_exact_pushdown_int8 WHERE id IS NULL;

-- ============================================================================
-- Block A: int4 Exact-promoted operators (opnos 96, 518, 97, 523, 521, 525)
-- ============================================================================

-- A.1 int4eq (=), opno 96
SET pg_lakebase.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id = 25;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id = 25;

SET pg_lakebase.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id = 25;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id = 25;

-- A.2 int4ne (<>), opno 518. This also checks NULL semantics: the three
-- NULL rows must not satisfy `NULL <> 25`.
SET pg_lakebase.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id <> 25;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id <> 25;

SET pg_lakebase.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id <> 25;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id <> 25;

-- A.3 int4lt (<), opno 97
SET pg_lakebase.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id < 120;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id < 120;

SET pg_lakebase.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id < 120;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id < 120;

-- A.4 int4le (<=), opno 523
SET pg_lakebase.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id <= 120;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id <= 120;

SET pg_lakebase.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id <= 120;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id <= 120;

-- A.5 int4gt (>), opno 521
SET pg_lakebase.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id > 120;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id > 120;

SET pg_lakebase.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id > 120;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id > 120;

-- A.6 int4ge (>=), opno 525
SET pg_lakebase.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id >= 120;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id >= 120;

SET pg_lakebase.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id >= 120;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id >= 120;

-- ============================================================================
-- Block B: int8 Exact-promoted operators (opnos 410, 411, 412, 414, 413, 415)
-- ============================================================================

-- B.1 int8eq (=), opno 410. The literal lives in the >2^32 file to exercise
-- the 64-bit comparison path specifically.
SET pg_lakebase.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id = 10000000025;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id = 10000000025;

SET pg_lakebase.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id = 10000000025;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id = 10000000025;

-- B.2 int8ne (<>), opno 411
SET pg_lakebase.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id <> 25::bigint;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id <> 25::bigint;

SET pg_lakebase.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id <> 25::bigint;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id <> 25::bigint;

-- B.3 int8lt (<), opno 412
SET pg_lakebase.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id < 120::bigint;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id < 120::bigint;

SET pg_lakebase.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id < 120::bigint;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id < 120::bigint;

-- B.4 int8le (<=), opno 414
SET pg_lakebase.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id <= 120::bigint;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id <= 120::bigint;

SET pg_lakebase.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id <= 120::bigint;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id <= 120::bigint;

-- B.5 int8gt (>), opno 413
SET pg_lakebase.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id > 9999999999::bigint;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id > 9999999999::bigint;

SET pg_lakebase.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id > 9999999999::bigint;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id > 9999999999::bigint;

-- B.6 int8ge (>=), opno 415
SET pg_lakebase.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id >= 10000000000::bigint;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id >= 10000000000::bigint;

SET pg_lakebase.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id >= 10000000000::bigint;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id >= 10000000000::bigint;

-- ============================================================================
-- Block C: NULL semantics and AND composition
-- ============================================================================

-- C.1 equality on the NULL-bearing int4 file.
SET pg_lakebase.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id = 119;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id = 119;

SET pg_lakebase.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id = 119;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id = 119;

-- C.2 inequality on the NULL-bearing int4 file. NULL ids must not satisfy
-- `NULL <> 119` on either path.
SET pg_lakebase.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id <> 119;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id <> 119;

SET pg_lakebase.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id <> 119;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id <> 119;

-- C.3 AND of two Exact range clauses. Both clauses should be pushed and
-- recorded for recheck; neither should remain residual.
SET pg_lakebase.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id >= 100 AND id <= 150;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id >= 100 AND id <= 150;

SET pg_lakebase.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id >= 100 AND id <= 150;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id >= 100 AND id <= 150;

-- ============================================================================
-- Block D: Type resolution through PG's resolved operator identity
-- ============================================================================

-- D.1 explicit int8 literal against an int8 column. This is the same
-- allowlisted operator reached without relying on unknown-literal coercion.
SET pg_lakebase.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id = 25::bigint;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id = 25::bigint;

SET pg_lakebase.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id = 25::bigint;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id = 25::bigint;

-- ============================================================================
-- Block E: Collation / text comparisons are not Exact-promoted
-- The Exact allowlist is restricted to collation-agnostic integer triples.
-- These text predicates provide negative coverage for operator identity
--: the classifier must NOT promote text/collation
-- triples to `Exact` even though the structural shape (`column op literal`)
-- matches the Exact template.
-- We assert this under `pg_lakebase.customscan_mode = 'force'` deliberately:
-- `force` biases CustomPaths the framework deems legal and selects them
-- regardless of cost. So the EXPLAIN under `force` directly reveals the
-- classifier's decision for text:
--   - `Seq Scan` ⇒ classifier returned `Unsupported`, no CustomPath emitted
--     for text equality at all (the strongest negative case).
--   - `Custom Scan` whose text clause was classified
--     `ConservativePruning` ⇒ classifier emitted a CustomPath. This is
--     also acceptable: the ConservativePruning clause is pushed (it appears
--     on the `Pushed Filter:` line, and under VERBOSE on the
--     `Pushed Filter Conservative Pruning:` labeled line) but the original clause is
--     ALSO retained as a local residual `Filter:` line — importantly it is
--     NOT classified `Exact` (which would strip it from `plan.qual` and,
--     under VERBOSE, label it `Pushed Filter Exact:`), which is what
--     the Exact-pushdown soundness rule (recheckable clauses must never
--     be classified Exact) forbids.
-- Either outcome — but never an Exact classification of the text clause —
-- proves the negative assertion. Using `force` here is consistent with the
-- rest of this file and gives stronger diagnostic output than `auto`, where
-- a SeqScan plan
-- would conflate "no path emitted" with "path emitted but cost-lost".
-- ============================================================================

CREATE TABLE customscan_exact_pushdown_text (
    id integer,
    label text COLLATE "C"
) USING iceberg;

INSERT INTO customscan_exact_pushdown_text VALUES
    (1, 'apple'),
    (2, 'Banana'),
    (3, 'banana'),
    (4, 'Cherry'),
    (5, 'cherry');

-- E.1 text equality under the column collation. Under `force`, this must
-- not classify the text clause as Exact (it must NOT appear on a VERBOSE
-- `Pushed Filter Exact:` line) — text/collation triples are not allowlisted
--.
SET pg_lakebase.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || label, E'\n' ORDER BY id, label), '')) AS row_digest
FROM customscan_exact_pushdown_text
WHERE label = 'banana';

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || label, E'\n' ORDER BY id, label), '')) AS row_digest
FROM customscan_exact_pushdown_text
WHERE label = 'banana';

SET pg_lakebase.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || label, E'\n' ORDER BY id, label), '')) AS row_digest
FROM customscan_exact_pushdown_text
WHERE label = 'banana';

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || label, E'\n' ORDER BY id, label), '')) AS row_digest
FROM customscan_exact_pushdown_text
WHERE label = 'banana';

-- E.2 explicit COLLATE clause. This is also outside the Exact proof surface
--. Same negative assertion as E.1 under `force`.
SET pg_lakebase.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || label, E'\n' ORDER BY id, label), '')) AS row_digest
FROM customscan_exact_pushdown_text
WHERE label COLLATE "POSIX" = 'banana';

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || label, E'\n' ORDER BY id, label), '')) AS row_digest
FROM customscan_exact_pushdown_text
WHERE label COLLATE "POSIX" = 'banana';

SET pg_lakebase.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || label, E'\n' ORDER BY id, label), '')) AS row_digest
FROM customscan_exact_pushdown_text
WHERE label COLLATE "POSIX" = 'banana';

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || label, E'\n' ORDER BY id, label), '')) AS row_digest
FROM customscan_exact_pushdown_text
WHERE label COLLATE "POSIX" = 'banana';

-- ============================================================================
-- Cleanup
-- ============================================================================
RESET pg_lakebase.customscan_mode;
DROP TABLE customscan_exact_pushdown_int4;
DROP TABLE customscan_exact_pushdown_int8;
DROP TABLE customscan_exact_pushdown_text;
-- customscan_partial_pushdown.sql
-- Partial pushdown: pushed + residual filters and results.


-- ============================================================================
-- Setup: an Iceberg table whose schema (`a integer, b text`) lets us
-- exercise both an Exact pushable predicate (`a = 1`) and a
-- structurally-unsupported predicate (`length(b) > 0`) over the same
-- row population. Three rows give us non-trivial result-set parity
-- against the SeqScan baseline.
-- ============================================================================
CREATE TABLE customscan_partial_pushdown_t (
    a integer,
    b text
) USING iceberg;

INSERT INTO customscan_partial_pushdown_t VALUES (1, 'one');
INSERT INTO customscan_partial_pushdown_t VALUES (2, 'two');
INSERT INTO customscan_partial_pushdown_t VALUES (3, '');

SELECT COUNT(*) AS total_rows FROM customscan_partial_pushdown_t;

-- ============================================================================
-- Test 1: AND-partial pushdown
-- ============================================================================
-- `a = 1 AND length(b) > 0`:
--   - `a = 1` is classified Exact and pushed (and recorded in recheck).
--   - `length(b) > 0` stays in residual.
--   - Result set must equal the SeqScan baseline (single row: (1, 'one')).

-- CustomScan path (default TEXT): the CustomScan node carries a
-- single `Pushed Filter: (a = 1)` line for the pushed (remote)
-- predicate, while the local residual `length(b) > 0` is shown only
-- by PG's standard `Filter:` line above it:
--   Filter: (length(b) > 0)
--   Pushed Filter: (a = 1)
-- (no old-style diagnostic title, no default-mode provider-identity
-- line, no counts)
SET pg_lakebase.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT a, b FROM customscan_partial_pushdown_t
WHERE a = 1 AND length(b) > 0;

-- SeqScan baseline: filter is the original AND clause verbatim.
SET pg_lakebase.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT a, b FROM customscan_partial_pushdown_t
WHERE a = 1 AND length(b) > 0;

-- Result-set parity.
SET pg_lakebase.customscan_mode = 'force';
SELECT a, b FROM customscan_partial_pushdown_t
WHERE a = 1 AND length(b) > 0
ORDER BY a, b;

SET pg_lakebase.customscan_mode = 'off';
SELECT a, b FROM customscan_partial_pushdown_t
WHERE a = 1 AND length(b) > 0
ORDER BY a, b;

-- VERBOSE: under VERBOSE the deparsed predicate text appears on
-- labeled lines for each non-empty class.
-- `Pushed Filter Exact:` and `Recheck:` both print the deparsed
-- `(a = 1)` (v1 stores Exact pushed expressions in recheck verbatim
-- — see design Data Models). The `Pushed Filter Conservative Pruning:` class is
-- empty and its label line is omitted (
-- non-empty classes print labeled predicate lines); no numeric count
-- lines appear.
SET pg_lakebase.customscan_mode = 'force';
EXPLAIN (VERBOSE, COSTS OFF)
SELECT a, b FROM customscan_partial_pushdown_t
WHERE a = 1 AND length(b) > 0;

-- ============================================================================
-- Test 2: OR-no-pushdown
-- ============================================================================
-- `a = 1 OR length(b) > 0`:
--   - The Unsupported child kills both the OR-Exact 
--     4.5) and the OR-ConservativePruning-widening branches.
--   - The whole OR is Unsupported; `split.pushed` is empty;
--     Iceberg's `create_path` declines the variant; PG plans a
--     SeqScan with the OR clause as Filter.
--   - Result set must equal the SeqScan baseline run with
--     `customscan_mode = 'off'`.

-- "CustomScan" path under `force`. With no pushable clauses there
-- is no CustomPath to bias toward, so this plan must be SeqScan.
SET pg_lakebase.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT a, b FROM customscan_partial_pushdown_t
WHERE a = 1 OR length(b) > 0;

-- SeqScan baseline: same filter shape.
SET pg_lakebase.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT a, b FROM customscan_partial_pushdown_t
WHERE a = 1 OR length(b) > 0;

-- Result-set parity.
SET pg_lakebase.customscan_mode = 'force';
SELECT a, b FROM customscan_partial_pushdown_t
WHERE a = 1 OR length(b) > 0
ORDER BY a, b;

SET pg_lakebase.customscan_mode = 'off';
SELECT a, b FROM customscan_partial_pushdown_t
WHERE a = 1 OR length(b) > 0
ORDER BY a, b;

-- ============================================================================
-- Cleanup
-- ============================================================================
RESET pg_lakebase.customscan_mode;
DROP TABLE customscan_partial_pushdown_t;
-- customscan_auto_mode_cost.sql
-- Cost-based path selection under the default pg_lakebase.customscan_mode = 'auto'.

-- ============================================================================
-- Auto-mode cost-based selection (the PRODUCTION default).
-- Every other block in this suite pins 'force' or 'off' so the plan SHAPE is
-- deterministic regardless of cost. This block instead exercises the default
-- mode ('auto'), where the planner chooses purely on cost — the path the real
-- workload takes:
--   * A pushable predicate makes `create_path` emit a CustomPath. Its cost
--     model (pg-lakebase-core `compute_costs`) does NOT reduce output rows
--     (`path.rows` stays `parent->rows`); pruning savings land only in the
--     SCANNED-volume terms: `scanned_pages = baserel.pages * fraction` and
--     `scanned_tuples = baserel.tuples * fraction`, which drive the disk
--     (`seq_page_cost * scanned_pages`) and per-tuple-CPU costs. `fraction` is
--     `clauselist_selectivity` of ONLY the costed-pruning pushed clauses
--     (`split.costed_pruning_exprs()`), clamped from below by
--     `pg_iceberg_am.customscan_min_scan_fraction` (0.02). `id = 1500` is
--     int4eq → ExactRowFilter → CostedPruning, so it counts; an unANALYZEd
--     table gives it PG's DEFAULT_EQ_SEL (0.005), which the floor lifts to
--     0.02. Either way fraction << 1, so the scaled disk+CPU cost sits far
--     below the full SeqScan and 'auto' picks the Custom Scan WITHOUT any force
--     bias. (The floor is a guard against a bogus near-zero selectivity, not
--     the reason CustomScan wins.)
--   * With NO pushable predicate, `create_path` returns `None` — no CustomPath
--     is emitted at all — so 'auto' can only fall back to the Seq Scan
--     baseline; the cost model never even gets a CustomScan to weigh.
-- NB: only COSTED-pruning pushes scale the estimate. An UncostedBestEffort push
-- (e.g. a date/timestamp ConservativePruning clause) is still applied for
-- runtime pruning but leaves `fraction = 1.0`, so it would NOT flip 'auto' to a
-- Custom Scan. That is exactly why this block uses an int equality — the case
-- that wins on cost deterministically.
-- The relation is sized across two data files so the Iceberg snapshot summary
-- (`total-records` / `total-files-size`, surfaced via `relation_estimate_size`)
-- gives the planner a real, non-zero (pages, tuples) baseline. EXPLAIN
-- (COSTS OFF) asserts plan SHAPE only, so the exact cost numbers (which depend
-- on parquet file size) never reach the .out and the assertion stays stable.
-- ============================================================================
CREATE TABLE customscan_auto_cost_t (
    id integer,
    payload text
) USING iceberg;

-- File 1: id in [1, 2000]
INSERT INTO customscan_auto_cost_t
SELECT g, 'auto_' || g
FROM generate_series(1, 2000) AS g;

-- File 2: id in [10000, 11999]
INSERT INTO customscan_auto_cost_t
SELECT g, 'auto_' || g
FROM generate_series(10000, 11999) AS g;

SELECT COUNT(*) AS auto_cost_rows FROM customscan_auto_cost_t;

-- The default mode is already 'auto'; set it explicitly so the block is
-- self-documenting and independent of any prior section's GUC state.
SET pg_lakebase.customscan_mode = 'auto';

-- A.1 Pushable equality under 'auto': the CustomPath's scan cost (disk +
-- per-tuple CPU, scaled down by the costed-pruning selectivity) sits far below
-- the full SeqScan, so the planner picks the Custom Scan on cost alone (no
-- force bias). Default TEXT shows the `Pushed Filter: (id = 1500)` line; the
-- Exact clause is stripped from `plan.qual`, so there is no residual `Filter:`.
EXPLAIN (COSTS OFF)
SELECT id, payload FROM customscan_auto_cost_t WHERE id = 1500;

-- A.2 No predicate under 'auto': `create_path` sees an empty pushed set and
-- returns `None`, so no CustomPath is emitted and the only candidate is the
-- Seq Scan baseline.
EXPLAIN (COSTS OFF)
SELECT id, payload FROM customscan_auto_cost_t;

-- A.3 The same predicate-free query under 'force' is STILL a Seq Scan: this
-- proves A.2's Seq Scan is "no CustomPath was emitted" (empty pushed set), not
-- merely "a CustomPath lost on cost". `force` can only bias CustomPaths the
-- framework already emitted, and here there is none to bias.
SET pg_lakebase.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT id, payload FROM customscan_auto_cost_t;

-- A.4 Result-set parity: the cost-selected Custom Scan ('auto') returns the
-- same row as the SeqScan baseline ('off').
SET pg_lakebase.customscan_mode = 'auto';
SELECT id, payload FROM customscan_auto_cost_t WHERE id = 1500 ORDER BY id, payload;

SET pg_lakebase.customscan_mode = 'off';
SELECT id, payload FROM customscan_auto_cost_t WHERE id = 1500 ORDER BY id, payload;

-- ============================================================================
-- Cleanup
-- ============================================================================
RESET pg_lakebase.customscan_mode;
DROP TABLE customscan_auto_cost_t;
