-- customscan_projection_pushdown.sql
-- Needed-column projection from targetlist/qual/recheck.

DROP EXTENSION IF EXISTS pg_iceberg_am CASCADE;
CREATE EXTENSION IF NOT EXISTS pg_iceberg_am;

-- ============================================================================
-- Part A: dropped-column table (the key correctness fixture)
-- Create a 4-column table, then drop a MIDDLE column (`b`). The surviving
-- live columns are id (attno 1), label (attno 3), amount (attno 4); attno 2
-- becomes a dropped gap. The Iceberg metadata schema still carries the field
-- for the dropped `b`, so the live PG columns are narrower than the Iceberg
-- field list — the exact divergence the dropped-column alignment fix
-- addresses.
-- ============================================================================
CREATE TABLE customscan_proj_dropcol (
    id integer,
    b integer,
    label text,
    amount integer
) USING iceberg;

INSERT INTO customscan_proj_dropcol VALUES (1, 100, 'one', 11);
INSERT INTO customscan_proj_dropcol VALUES (2, 200, 'two', 22);
INSERT INTO customscan_proj_dropcol VALUES (3, 300, 'three', 33);

-- Drop the middle column. attno 2 is now a dropped gap; label stays attno 3
-- and amount stays attno 4 in the live TupleDesc.
ALTER TABLE customscan_proj_dropcol DROP COLUMN b;

-- Confirm the dropped-attno gap is present in the live descriptor.
SELECT attname, attnum, attisdropped
FROM pg_attribute
WHERE attrelid = 'customscan_proj_dropcol'::regclass AND attnum > 0
ORDER BY attnum;

-- --- A.1 plan guard: a pushable predicate selects the CustomScan path ------
SET pg_lakebase.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT id, label FROM customscan_proj_dropcol WHERE id >= 1;

SET pg_lakebase.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT id, label FROM customscan_proj_dropcol WHERE id >= 1;

-- --- A.2 SELECT <subset> across a dropped column ---------------------------
-- Projecting id (attno 1) + amount (attno 4) skips the dropped attno-2 gap
-- AND the live label column. Each value must land at its own attno-1 slot.
SET pg_lakebase.customscan_mode = 'force';
SELECT id, amount FROM customscan_proj_dropcol WHERE id >= 1 ORDER BY id;

SET pg_lakebase.customscan_mode = 'off';
SELECT id, amount FROM customscan_proj_dropcol WHERE id >= 1 ORDER BY id;

-- --- A.3 SELECT <subset>: a single column past the dropped gap -------------
SET pg_lakebase.customscan_mode = 'force';
SELECT label FROM customscan_proj_dropcol WHERE id >= 1 ORDER BY label;

SET pg_lakebase.customscan_mode = 'off';
SELECT label FROM customscan_proj_dropcol WHERE id >= 1 ORDER BY label;

-- --- A.4 SELECT * over a dropped column ------------------------------------
-- The dropped column must NOT appear; surviving columns keep their values
-- (no shift into the dropped position).
SET pg_lakebase.customscan_mode = 'force';
SELECT * FROM customscan_proj_dropcol WHERE id >= 1 ORDER BY id;

SET pg_lakebase.customscan_mode = 'off';
SELECT * FROM customscan_proj_dropcol WHERE id >= 1 ORDER BY id;

-- --- A.5 SELECT count(*) ----------------------------------------------------
-- count(*) references no user column (empty subset). The provider's
-- first-live-column policy still selects exactly one column to scan; the
-- count must match the baseline.
SET pg_lakebase.customscan_mode = 'force';
SELECT count(*) FROM customscan_proj_dropcol WHERE id >= 1;

SET pg_lakebase.customscan_mode = 'off';
SELECT count(*) FROM customscan_proj_dropcol WHERE id >= 1;

-- --- A.6 WHERE references a NON-projected column ----------------------------
-- The output list is just `id`, but the qual filters on `label` (not in the
-- select list). `label` is not pushable, so it stays a residual the executor
-- applies above the scan — which means the scan must still decode `label`
-- even though it is not projected into the output. The pushable `id >= 1`
-- selects the CustomScan path; the non-projected `label` filter must produce
-- identical rows on both paths.
SET pg_lakebase.customscan_mode = 'force';
SELECT id FROM customscan_proj_dropcol WHERE id >= 1 AND label = 'two' ORDER BY id;

SET pg_lakebase.customscan_mode = 'off';
SELECT id FROM customscan_proj_dropcol WHERE id >= 1 AND label = 'two' ORDER BY id;

-- --- A.7 WHERE on a non-projected column, exact-id pushdown -----------------
SET pg_lakebase.customscan_mode = 'force';
SELECT amount FROM customscan_proj_dropcol WHERE id = 3 ORDER BY amount;

SET pg_lakebase.customscan_mode = 'off';
SELECT amount FROM customscan_proj_dropcol WHERE id = 3 ORDER BY amount;

-- --- A.8 targetlist expression references a non-filter column --------------
-- `label` is hidden under FuncExpr.  The pushable `id = 2` predicate selects
-- the CustomScan path, so projection must still read `label` for the upper
-- projection node.
SET pg_lakebase.customscan_mode = 'force';
SELECT lower(label) AS lowered FROM customscan_proj_dropcol WHERE id = 2 ORDER BY lowered;

SET pg_lakebase.customscan_mode = 'off';
SELECT lower(label) AS lowered FROM customscan_proj_dropcol WHERE id = 2 ORDER BY lowered;

-- --- A.9 residual qual expression references a non-projected column --------
-- `lower(label)` is not pushable, but it remains a PG residual qual above the
-- scan.  The scan must decode `label` even though the output list is just `id`.
SET pg_lakebase.customscan_mode = 'force';
SELECT id FROM customscan_proj_dropcol WHERE id >= 1 AND lower(label) = 'three' ORDER BY id;

SET pg_lakebase.customscan_mode = 'off';
SELECT id FROM customscan_proj_dropcol WHERE id >= 1 AND lower(label) = 'three' ORDER BY id;

-- ============================================================================
-- Part B: clean table (no dropped columns) — full-table equivalence guard

-- relation with NO dropped columns behave exactly as the positional reader.
-- Same force/off parity discipline.
-- ============================================================================
CREATE TABLE customscan_proj_clean (
    id integer,
    label text,
    amount integer
) USING iceberg;

INSERT INTO customscan_proj_clean VALUES (1, 'alpha', 10);
INSERT INTO customscan_proj_clean VALUES (2, 'beta', 20);
INSERT INTO customscan_proj_clean VALUES (3, 'gamma', 30);
INSERT INTO customscan_proj_clean VALUES (4, 'delta', 40);

-- --- B.1 SELECT * (select-all equivalence) ---------------------------------
SET pg_lakebase.customscan_mode = 'force';
SELECT * FROM customscan_proj_clean WHERE id >= 1 ORDER BY id;

SET pg_lakebase.customscan_mode = 'off';
SELECT * FROM customscan_proj_clean WHERE id >= 1 ORDER BY id;

-- --- B.2 SELECT <subset> ---------------------------------------------------
SET pg_lakebase.customscan_mode = 'force';
SELECT amount, id FROM customscan_proj_clean WHERE id <= 3 ORDER BY id;

SET pg_lakebase.customscan_mode = 'off';
SELECT amount, id FROM customscan_proj_clean WHERE id <= 3 ORDER BY id;

-- --- B.3 SELECT count(*) ---------------------------------------------------
SET pg_lakebase.customscan_mode = 'force';
SELECT count(*) FROM customscan_proj_clean WHERE id >= 1;

SET pg_lakebase.customscan_mode = 'off';
SELECT count(*) FROM customscan_proj_clean WHERE id >= 1;

-- --- B.4 WHERE references a non-projected column ----------------------------
SET pg_lakebase.customscan_mode = 'force';
SELECT id FROM customscan_proj_clean WHERE id >= 1 AND label = 'gamma' ORDER BY id;

SET pg_lakebase.customscan_mode = 'off';
SELECT id FROM customscan_proj_clean WHERE id >= 1 AND label = 'gamma' ORDER BY id;

-- --- B.5 targetlist CoalesceExpr references a non-filter column ------------
SET pg_lakebase.customscan_mode = 'force';
SELECT coalesce(label, '') AS safe_label FROM customscan_proj_clean WHERE id = 4 ORDER BY safe_label;

SET pg_lakebase.customscan_mode = 'off';
SELECT coalesce(label, '') AS safe_label FROM customscan_proj_clean WHERE id = 4 ORDER BY safe_label;

-- --- B.6 residual CaseExpr references a non-projected column ---------------
SET pg_lakebase.customscan_mode = 'force';
SELECT amount FROM customscan_proj_clean
WHERE id >= 1
  AND CASE WHEN amount >= 0 THEN lower(label) ELSE '' END = 'alpha'
ORDER BY amount;

SET pg_lakebase.customscan_mode = 'off';
SELECT amount FROM customscan_proj_clean
WHERE id >= 1
  AND CASE WHEN amount >= 0 THEN lower(label) ELSE '' END = 'alpha'
ORDER BY amount;

-- ============================================================================
-- Cleanup
-- ============================================================================
RESET pg_lakebase.customscan_mode;
DROP TABLE customscan_proj_dropcol;
DROP TABLE customscan_proj_clean;
