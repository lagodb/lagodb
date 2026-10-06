-- Path selection, projection dependencies, and plan-reference remapping.

-- DML targets, rowmarks, and system columns.

DROP EXTENSION IF EXISTS lagodb_iceberg CASCADE;
CREATE EXTENSION IF NOT EXISTS lagodb_iceberg;

-- Disable upper query offload to isolate relation-level CustomScan planning.
SET lagodb.query_offload_mode = 'off';

-- Use a heap DML target to isolate rowmark handling on the Iceberg source.
CREATE TABLE customscan_dml_lake (
    k integer,
    v integer
) USING iceberg;

INSERT INTO customscan_dml_lake VALUES (1, 100);
INSERT INTO customscan_dml_lake VALUES (2, 200);
INSERT INTO customscan_dml_lake VALUES (3, 300);

CREATE TABLE customscan_dml_other (
    k integer,
    v integer
);

INSERT INTO customscan_dml_other VALUES (1, 0);
INSERT INTO customscan_dml_other VALUES (2, 0);

SELECT COUNT(*) AS lake_rows FROM customscan_dml_lake;

-- DML targets require ModifyTarget scans even when optional read CustomScans are off.

SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
UPDATE customscan_dml_lake SET v = v + 1 WHERE k = 1;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
UPDATE customscan_dml_lake SET v = v + 1 WHERE k = 1;

SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
DELETE FROM customscan_dml_lake WHERE k = 1;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
DELETE FROM customscan_dml_lake WHERE k = 1;

SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
MERGE INTO customscan_dml_lake AS t
USING (VALUES (1, 999)) AS s(k, v)
ON t.k = s.k
WHEN MATCHED THEN UPDATE SET v = s.v;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
MERGE INTO customscan_dml_lake AS t
USING (VALUES (1, 999)) AS s(k, v)
ON t.k = s.k
WHEN MATCHED THEN UPDATE SET v = s.v;

-- An Iceberg source in UPDATE ... FROM has a rowmark and uses the native scan path.

SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
UPDATE customscan_dml_other AS o
SET v = l.v
FROM customscan_dml_lake AS l
WHERE o.k = l.k;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
UPDATE customscan_dml_other AS o
SET v = l.v
FROM customscan_dml_lake AS l
WHERE o.k = l.k;

-- FOR UPDATE and FOR SHARE reject optional CustomScans because they require rowmarks.

SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT k, v FROM customscan_dml_lake WHERE k = 1 FOR UPDATE;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT k, v FROM customscan_dml_lake WHERE k = 1 FOR UPDATE;

SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT k, v FROM customscan_dml_lake WHERE k = 1 FOR SHARE;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT k, v FROM customscan_dml_lake WHERE k = 1 FOR SHARE;

-- Unsupported system columns use the native scan path; tableoid remains supported.

SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT ctid FROM customscan_dml_lake WHERE k = 1;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT ctid FROM customscan_dml_lake WHERE k = 1;

SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT xmin, xmax FROM customscan_dml_lake WHERE k = 1;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT xmin, xmax FROM customscan_dml_lake WHERE k = 1;

-- Compare tableoid values from CustomScan and the native scan path.

SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT tableoid::regclass AS tableoid, k, v
FROM customscan_dml_lake
WHERE k = 1
ORDER BY k;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT tableoid::regclass AS tableoid, k, v
FROM customscan_dml_lake
WHERE k = 1
ORDER BY k;

SET lagodb.customscan_mode = 'force';
SELECT tableoid::regclass AS tableoid, k, v
FROM customscan_dml_lake
WHERE k = 1
ORDER BY k;

SET lagodb.customscan_mode = 'off';
SELECT tableoid::regclass AS tableoid, k, v
FROM customscan_dml_lake
WHERE k = 1
ORDER BY k;

-- Whole-row projection must materialize every live user column.

SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT customscan_dml_lake FROM customscan_dml_lake WHERE k = 1 ORDER BY k;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT customscan_dml_lake FROM customscan_dml_lake WHERE k = 1 ORDER BY k;

SET lagodb.customscan_mode = 'force';
SELECT customscan_dml_lake FROM customscan_dml_lake WHERE k = 1 ORDER BY k;

SET lagodb.customscan_mode = 'off';
SELECT customscan_dml_lake FROM customscan_dml_lake WHERE k = 1 ORDER BY k;

RESET lagodb.customscan_mode;
DROP TABLE customscan_dml_lake;
DROP TABLE customscan_dml_other;
-- Security barriers and join-clause movability.


-- Use IMMUTABLE, non-leakproof PL/pgSQL to isolate the security gate. PL/pgSQL prevents
-- inlining from replacing the comparison with a leakproof equality.
CREATE FUNCTION customscan_sm_secret_eq(int, int) RETURNS bool
LANGUAGE plpgsql
IMMUTABLE
AS $$ BEGIN RETURN $1 = $2; END $$;

CREATE OPERATOR === (
    LEFTARG = int4,
    RIGHTARG = int4,
    FUNCTION = customscan_sm_secret_eq
);

CREATE TABLE customscan_sm_lake (k integer, payload text) USING iceberg;
INSERT INTO customscan_sm_lake VALUES
    (1, 'one'),
    (2, 'two'),
    (3, 'three'),
    (4, 'four'),
    (5, 'five');

-- Use an Iceberg join partner so parameterized CustomPaths are eligible.
CREATE TABLE customscan_sm_other (k integer, v integer) USING iceberg;
INSERT INTO customscan_sm_other VALUES
    (1, 100),
    (3, 300),
    (5, 500);

-- A non-leakproof outer predicate must stay above the security barrier. Only the
-- leakproof inner predicate may be pushed.

CREATE VIEW customscan_sm_secure_view
    WITH (security_barrier = true)
    AS SELECT k, payload FROM customscan_sm_lake WHERE k >= 0;

SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT k, payload FROM customscan_sm_secure_view
WHERE k === 1
ORDER BY k, payload;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT k, payload FROM customscan_sm_secure_view
WHERE k === 1
ORDER BY k, payload;

-- Compare results across the security barrier with CustomScan enabled and disabled.
SET lagodb.customscan_mode = 'force';
SELECT k, payload FROM customscan_sm_secure_view
WHERE k === 1
ORDER BY k, payload;

SET lagodb.customscan_mode = 'off';
SELECT k, payload FROM customscan_sm_secure_view
WHERE k === 1
ORDER BY k, payload;

-- The LEFT JOIN equality must stay at the join level, preserving unmatched rows.

SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT lake.k AS lk, lake.payload, other.k AS ok, other.v
FROM customscan_sm_lake lake
LEFT JOIN customscan_sm_other other ON other.k = lake.k
ORDER BY lake.k, other.k;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT lake.k AS lk, lake.payload, other.k AS ok, other.v
FROM customscan_sm_lake lake
LEFT JOIN customscan_sm_other other ON other.k = lake.k
ORDER BY lake.k, other.k;

-- Unmatched lake keys 2 and 4 must remain as NULL-extended rows.
SET lagodb.customscan_mode = 'force';
SELECT lake.k AS lk, lake.payload, other.k AS ok, other.v
FROM customscan_sm_lake lake
LEFT JOIN customscan_sm_other other ON other.k = lake.k
ORDER BY lake.k, other.k;

SET lagodb.customscan_mode = 'off';
SELECT lake.k AS lk, lake.payload, other.k AS ok, other.v
FROM customscan_sm_lake lake
LEFT JOIN customscan_sm_other other ON other.k = lake.k
ORDER BY lake.k, other.k;

-- OFFSET 0 preserves the LATERAL dependency. The back-reference into lake must remain
-- outside its pushed filter.
SET enable_hashjoin = off;
SET enable_mergejoin = off;
SET enable_material = off;
SET enable_nestloop = on;

SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT l.k, l.payload, sub.v
FROM customscan_sm_lake l,
LATERAL (
    SELECT v FROM customscan_sm_other o
    WHERE o.k = l.k
    OFFSET 0
) sub
WHERE l.k >= 0
  AND l.k = sub.v
ORDER BY l.k, sub.v;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT l.k, l.payload, sub.v
FROM customscan_sm_lake l,
LATERAL (
    SELECT v FROM customscan_sm_other o
    WHERE o.k = l.k
    OFFSET 0
) sub
WHERE l.k >= 0
  AND l.k = sub.v
ORDER BY l.k, sub.v;

-- No rows satisfy both equalities: lake keys are 1..5, while the lateral values are
-- 100, 300, and 500.
SET lagodb.customscan_mode = 'force';
SELECT l.k, l.payload, sub.v
FROM customscan_sm_lake l,
LATERAL (
    SELECT v FROM customscan_sm_other o
    WHERE o.k = l.k
    OFFSET 0
) sub
WHERE l.k >= 0
  AND l.k = sub.v
ORDER BY l.k, sub.v;

SET lagodb.customscan_mode = 'off';
SELECT l.k, l.payload, sub.v
FROM customscan_sm_lake l,
LATERAL (
    SELECT v FROM customscan_sm_other o
    WHERE o.k = l.k
    OFFSET 0
) sub
WHERE l.k >= 0
  AND l.k = sub.v
ORDER BY l.k, sub.v;

RESET enable_hashjoin;
RESET enable_mergejoin;
RESET enable_material;
RESET enable_nestloop;
RESET lagodb.customscan_mode;

DROP VIEW customscan_sm_secure_view;
DROP TABLE customscan_sm_lake;
DROP TABLE customscan_sm_other;
DROP OPERATOR === (int4, int4);
DROP FUNCTION customscan_sm_secret_eq(int, int);

-- Projection layout and column dependencies.

DROP EXTENSION IF EXISTS lagodb_iceberg CASCADE;
CREATE EXTENSION IF NOT EXISTS lagodb_iceberg;

-- Drop a middle column to leave an attno gap; later columns must keep their values and
-- positions.
CREATE TABLE customscan_proj_dropcol (
    id integer,
    b integer,
    label text,
    amount integer
) USING iceberg;

INSERT INTO customscan_proj_dropcol VALUES (1, 100, 'one', 11);
INSERT INTO customscan_proj_dropcol VALUES (2, 200, 'two', 22);
INSERT INTO customscan_proj_dropcol VALUES (3, 300, 'three', 33);

ALTER TABLE customscan_proj_dropcol DROP COLUMN b;

-- Confirm the dropped-column gap in pg_attribute.
SELECT attname, attnum, attisdropped
FROM pg_attribute
WHERE attrelid = 'customscan_proj_dropcol'::regclass AND attnum > 0
ORDER BY attnum;

SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT id, label FROM customscan_proj_dropcol WHERE id >= 1;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT id, label FROM customscan_proj_dropcol WHERE id >= 1;

-- Project columns on both sides of the dropped-column gap.
SET lagodb.customscan_mode = 'force';
SELECT id, amount FROM customscan_proj_dropcol WHERE id >= 1 ORDER BY id;

SET lagodb.customscan_mode = 'off';
SELECT id, amount FROM customscan_proj_dropcol WHERE id >= 1 ORDER BY id;

-- Project a single column after the dropped-column gap.
SET lagodb.customscan_mode = 'force';
SELECT label FROM customscan_proj_dropcol WHERE id >= 1 ORDER BY label;

SET lagodb.customscan_mode = 'off';
SELECT label FROM customscan_proj_dropcol WHERE id >= 1 ORDER BY label;

-- SELECT * excludes the dropped column without shifting live values.
SET lagodb.customscan_mode = 'force';
SELECT * FROM customscan_proj_dropcol WHERE id >= 1 ORDER BY id;

SET lagodb.customscan_mode = 'off';
SELECT * FROM customscan_proj_dropcol WHERE id >= 1 ORDER BY id;

-- count(*) must work when no user column appears in the query.
SET lagodb.customscan_mode = 'force';
SELECT count(*) FROM customscan_proj_dropcol WHERE id >= 1;

SET lagodb.customscan_mode = 'off';
SELECT count(*) FROM customscan_proj_dropcol WHERE id >= 1;

-- A residual on an unprojected column still requires that column to be read.
SET lagodb.customscan_mode = 'force';
SELECT id FROM customscan_proj_dropcol WHERE id >= 1 AND label = 'two' ORDER BY id;

SET lagodb.customscan_mode = 'off';
SELECT id FROM customscan_proj_dropcol WHERE id >= 1 AND label = 'two' ORDER BY id;

-- Read an unprojected residual column alongside exact id pushdown.
SET lagodb.customscan_mode = 'force';
SELECT amount FROM customscan_proj_dropcol WHERE id = 3 ORDER BY amount;

SET lagodb.customscan_mode = 'off';
SELECT amount FROM customscan_proj_dropcol WHERE id = 3 ORDER BY amount;

-- Read label for a targetlist expression even when only id is filtered.
SET lagodb.customscan_mode = 'force';
SELECT lower(label) AS lowered FROM customscan_proj_dropcol WHERE id = 2 ORDER BY lowered;

SET lagodb.customscan_mode = 'off';
SELECT lower(label) AS lowered FROM customscan_proj_dropcol WHERE id = 2 ORDER BY lowered;

-- Read label for a residual expression even when only id is projected.
SET lagodb.customscan_mode = 'force';
SELECT id FROM customscan_proj_dropcol WHERE id >= 1 AND lower(label) = 'three' ORDER BY id;

SET lagodb.customscan_mode = 'off';
SELECT id FROM customscan_proj_dropcol WHERE id >= 1 AND lower(label) = 'three' ORDER BY id;

-- Compare projection results on a table without dropped columns.

CREATE TABLE customscan_proj_clean (
    id integer,
    label text,
    amount integer
) USING iceberg;

INSERT INTO customscan_proj_clean VALUES (1, 'alpha', 10);
INSERT INTO customscan_proj_clean VALUES (2, 'beta', 20);
INSERT INTO customscan_proj_clean VALUES (3, 'gamma', 30);
INSERT INTO customscan_proj_clean VALUES (4, 'delta', 40);

SET lagodb.customscan_mode = 'force';
SELECT * FROM customscan_proj_clean WHERE id >= 1 ORDER BY id;

SET lagodb.customscan_mode = 'off';
SELECT * FROM customscan_proj_clean WHERE id >= 1 ORDER BY id;

SET lagodb.customscan_mode = 'force';
SELECT amount, id FROM customscan_proj_clean WHERE id <= 3 ORDER BY id;

SET lagodb.customscan_mode = 'off';
SELECT amount, id FROM customscan_proj_clean WHERE id <= 3 ORDER BY id;

SET lagodb.customscan_mode = 'force';
SELECT count(*) FROM customscan_proj_clean WHERE id >= 1;

SET lagodb.customscan_mode = 'off';
SELECT count(*) FROM customscan_proj_clean WHERE id >= 1;

-- Read columns referenced only by the residual filter.
SET lagodb.customscan_mode = 'force';
SELECT id FROM customscan_proj_clean WHERE id >= 1 AND label = 'gamma' ORDER BY id;

SET lagodb.customscan_mode = 'off';
SELECT id FROM customscan_proj_clean WHERE id >= 1 AND label = 'gamma' ORDER BY id;

-- Read columns used inside a targetlist CoalesceExpr.
SET lagodb.customscan_mode = 'force';
SELECT coalesce(label, '') AS safe_label FROM customscan_proj_clean WHERE id = 4 ORDER BY safe_label;

SET lagodb.customscan_mode = 'off';
SELECT coalesce(label, '') AS safe_label FROM customscan_proj_clean WHERE id = 4 ORDER BY safe_label;

-- Read columns used inside a residual CaseExpr.
SET lagodb.customscan_mode = 'force';
SELECT amount FROM customscan_proj_clean
WHERE id >= 1
  AND CASE WHEN amount >= 0 THEN lower(label) ELSE '' END = 'alpha'
ORDER BY amount;

SET lagodb.customscan_mode = 'off';
SELECT amount FROM customscan_proj_clean
WHERE id >= 1
  AND CASE WHEN amount >= 0 THEN lower(label) ELSE '' END = 'alpha'
ORDER BY amount;

-- tableoid requires a relation-shaped tuple, but storage should still read only
-- referenced user columns.
CREATE TABLE customscan_proj_tableoid (
    id integer,
    label text,
    amount integer,
    tag text,
    score integer
) USING iceberg;

INSERT INTO customscan_proj_tableoid VALUES (1, 'alpha', 10, 'a', 100);
INSERT INTO customscan_proj_tableoid VALUES (2, 'beta', 20, 'b', 200);
INSERT INTO customscan_proj_tableoid VALUES (3, 'gamma', 30, 'c', 300);

SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT tableoid::regclass AS tbl, id, amount
FROM customscan_proj_tableoid
WHERE id >= 1
ORDER BY id;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT tableoid::regclass AS tbl, id, amount
FROM customscan_proj_tableoid
WHERE id >= 1
ORDER BY id;

-- Project tableoid, id, and amount without reading unrelated columns.
SET lagodb.customscan_mode = 'force';
SELECT tableoid::regclass AS tbl, id, amount
FROM customscan_proj_tableoid
WHERE id >= 1
ORDER BY id;

SET lagodb.customscan_mode = 'off';
SELECT tableoid::regclass AS tbl, id, amount
FROM customscan_proj_tableoid
WHERE id >= 1
ORDER BY id;

-- Project tableoid and one user column.
SET lagodb.customscan_mode = 'force';
SELECT tableoid::regclass AS tbl, label
FROM customscan_proj_tableoid
WHERE id = 2
ORDER BY label;

SET lagodb.customscan_mode = 'off';
SELECT tableoid::regclass AS tbl, label
FROM customscan_proj_tableoid
WHERE id = 2
ORDER BY label;

-- Read amount for the qual even though only tableoid and id are projected.
SET lagodb.customscan_mode = 'force';
SELECT tableoid::regclass AS tbl, id
FROM customscan_proj_tableoid
WHERE id >= 1 AND amount > 15
ORDER BY id;

SET lagodb.customscan_mode = 'off';
SELECT tableoid::regclass AS tbl, id
FROM customscan_proj_tableoid
WHERE id >= 1 AND amount > 15
ORDER BY id;

-- NOT IN retains a SubPlan because of NULL semantics. Its local Vars must be remapped
-- for the narrow scan tuple; an independent pushable predicate selects CustomScan.
CREATE TABLE customscan_proj_subplan_helper (
    x integer
);

INSERT INTO customscan_proj_subplan_helper VALUES (1);
INSERT INTO customscan_proj_subplan_helper VALUES (3);

-- Read id and label for the NOT IN SubPlan.
SET lagodb.customscan_mode = 'force';
SELECT id, label
FROM customscan_proj_tableoid
WHERE id >= 1
  AND id NOT IN (SELECT x FROM customscan_proj_subplan_helper)
ORDER BY id;

SET lagodb.customscan_mode = 'off';
SELECT id, label
FROM customscan_proj_tableoid
WHERE id >= 1
  AND id NOT IN (SELECT x FROM customscan_proj_subplan_helper)
ORDER BY id;

-- Read amount for the SubPlan even when only id is projected.
SET lagodb.customscan_mode = 'force';
SELECT id
FROM customscan_proj_tableoid
WHERE id >= 1
  AND amount NOT IN (SELECT x * 10 FROM customscan_proj_subplan_helper)
ORDER BY id;

SET lagodb.customscan_mode = 'off';
SELECT id
FROM customscan_proj_tableoid
WHERE id >= 1
  AND amount NOT IN (SELECT x * 10 FROM customscan_proj_subplan_helper)
ORDER BY id;

-- Combine a SubPlan with tableoid while reading only the referenced user columns.
SET lagodb.customscan_mode = 'force';
SELECT tableoid::regclass AS tbl, id, label
FROM customscan_proj_tableoid
WHERE id >= 1
  AND id NOT IN (SELECT x FROM customscan_proj_subplan_helper)
ORDER BY id;

SET lagodb.customscan_mode = 'off';
SELECT tableoid::regclass AS tbl, id, label
FROM customscan_proj_tableoid
WHERE id >= 1
  AND id NOT IN (SELECT x FROM customscan_proj_subplan_helper)
ORDER BY id;

RESET lagodb.customscan_mode;
DROP TABLE customscan_proj_dropcol;
DROP TABLE customscan_proj_clean;
DROP TABLE customscan_proj_tableoid;
DROP TABLE customscan_proj_subplan_helper;

-- Plan-reference remapping through nested queries.

DROP EXTENSION IF EXISTS lagodb_iceberg CASCADE;
CREATE EXTENSION IF NOT EXISTS lagodb_iceberg;

-- Two data files exercise predicate binding and pruning after scan-relation RTIs are
-- remapped.
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

-- Use the bare-table query as the result baseline for the wrappers below.

SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT k, payload FROM customscan_rto_lake WHERE k = 3 ORDER BY k, payload;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT k, payload FROM customscan_rto_lake WHERE k = 3 ORDER BY k, payload;

SET lagodb.customscan_mode = 'force';
SELECT k, payload FROM customscan_rto_lake WHERE k = 3 ORDER BY k, payload;

SET lagodb.customscan_mode = 'off';
SELECT k, payload FROM customscan_rto_lake WHERE k = 3 ORDER BY k, payload;

-- OFFSET 0 prevents subquery pull-up, exercising scan-relation Var resolution after
-- setrefs applies rtoffset.

SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT sub.k, sub.payload
FROM (
    SELECT k, payload
    FROM customscan_rto_lake
    WHERE k = 3
    OFFSET 0
) sub
ORDER BY sub.k, sub.payload;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT sub.k, sub.payload
FROM (
    SELECT k, payload
    FROM customscan_rto_lake
    WHERE k = 3
    OFFSET 0
) sub
ORDER BY sub.k, sub.payload;

SET lagodb.customscan_mode = 'force';
SELECT sub.k, sub.payload
FROM (
    SELECT k, payload
    FROM customscan_rto_lake
    WHERE k = 3
    OFFSET 0
) sub
ORDER BY sub.k, sub.payload;

SET lagodb.customscan_mode = 'off';
SELECT sub.k, sub.payload
FROM (
    SELECT k, payload
    FROM customscan_rto_lake
    WHERE k = 3
    OFFSET 0
) sub
ORDER BY sub.k, sub.payload;

-- Repeat the wrapper with a key in the second data file.
SET lagodb.customscan_mode = 'force';
SELECT sub.k, sub.payload
FROM (
    SELECT k, payload
    FROM customscan_rto_lake
    WHERE k = 102
    OFFSET 0
) sub
ORDER BY sub.k, sub.payload;

SET lagodb.customscan_mode = 'off';
SELECT sub.k, sub.payload
FROM (
    SELECT k, payload
    FROM customscan_rto_lake
    WHERE k = 102
    OFFSET 0
) sub
ORDER BY sub.k, sub.payload;

-- MATERIALIZED keeps the CTE in a separate subplan, exercising scan-relation Var
-- resolution after RTI remapping.

SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
WITH lake_cte AS MATERIALIZED (
    SELECT k, payload FROM customscan_rto_lake WHERE k = 3
)
SELECT k, payload FROM lake_cte ORDER BY k, payload;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
WITH lake_cte AS MATERIALIZED (
    SELECT k, payload FROM customscan_rto_lake WHERE k = 3
)
SELECT k, payload FROM lake_cte ORDER BY k, payload;

SET lagodb.customscan_mode = 'force';
WITH lake_cte AS MATERIALIZED (
    SELECT k, payload FROM customscan_rto_lake WHERE k = 3
)
SELECT k, payload FROM lake_cte ORDER BY k, payload;

SET lagodb.customscan_mode = 'off';
WITH lake_cte AS MATERIALIZED (
    SELECT k, payload FROM customscan_rto_lake WHERE k = 3
)
SELECT k, payload FROM lake_cte ORDER BY k, payload;

-- Repeat the CTE query with a key in the second data file.
SET lagodb.customscan_mode = 'force';
WITH lake_cte AS MATERIALIZED (
    SELECT k, payload FROM customscan_rto_lake WHERE k = 102
)
SELECT k, payload FROM lake_cte ORDER BY k, payload;

SET lagodb.customscan_mode = 'off';
WITH lake_cte AS MATERIALIZED (
    SELECT k, payload FROM customscan_rto_lake WHERE k = 102
)
SELECT k, payload FROM lake_cte ORDER BY k, payload;

-- The LATERAL wrapper combines RTI remapping with PARAM_EXEC binding. OFFSET 0 and the
-- planner settings keep the lake on the rescanned inner side.

-- Outer keys cover matches in both files and a missing key.
CREATE TABLE customscan_rto_outer (id integer);
INSERT INTO customscan_rto_outer VALUES (3), (50), (102);

SET enable_hashjoin = off;
SET enable_mergejoin = off;
SET enable_material = off;
SET enable_nestloop = on;

SET lagodb.customscan_mode = 'force';
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

SET lagodb.customscan_mode = 'off';
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

-- Keys 3 and 102 match; key 50 produces no row.
SET lagodb.customscan_mode = 'force';
SELECT o.id, sub.k, sub.payload
FROM customscan_rto_outer o,
LATERAL (
    SELECT k, payload
    FROM customscan_rto_lake
    WHERE k = o.id
    OFFSET 0
) sub
ORDER BY o.id, sub.k, sub.payload;

SET lagodb.customscan_mode = 'off';
SELECT o.id, sub.k, sub.payload
FROM customscan_rto_outer o,
LATERAL (
    SELECT k, payload
    FROM customscan_rto_lake
    WHERE k = o.id
    OFFSET 0
) sub
ORDER BY o.id, sub.k, sub.payload;

-- A MATERIALIZED CTE inside an OFFSET 0 subquery exercises Var resolution through
-- nested RTI remapping.

SET lagodb.customscan_mode = 'force';
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

SET lagodb.customscan_mode = 'off';
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

SET lagodb.customscan_mode = 'force';
SELECT outer_sub.k, outer_sub.payload
FROM (
    WITH inner_cte AS MATERIALIZED (
        SELECT k, payload FROM customscan_rto_lake WHERE k = 3
    )
    SELECT k, payload FROM inner_cte
    OFFSET 0
) outer_sub
ORDER BY outer_sub.k, outer_sub.payload;

SET lagodb.customscan_mode = 'off';
SELECT outer_sub.k, outer_sub.payload
FROM (
    WITH inner_cte AS MATERIALIZED (
        SELECT k, payload FROM customscan_rto_lake WHERE k = 3
    )
    SELECT k, payload FROM inner_cte
    OFFSET 0
) outer_sub
ORDER BY outer_sub.k, outer_sub.payload;

RESET enable_hashjoin;
RESET enable_mergejoin;
RESET enable_material;
RESET enable_nestloop;
RESET lagodb.customscan_mode;
RESET lagodb.query_offload_mode;

DROP TABLE customscan_rto_outer;
DROP TABLE customscan_rto_lake;
