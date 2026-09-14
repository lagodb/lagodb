-- query_offload_joins.sql
-- Integrated serial join semantics and capability-boundary coverage.

DROP EXTENSION IF EXISTS lagodb_iceberg CASCADE;
CREATE EXTENSION IF NOT EXISTS lagodb_iceberg;

SET lagodb.query_batch_rows = 2;
SET timezone = 'UTC';

CREATE TABLE query_offload_join_left (
    id integer,
    key_i2 smallint,
    key_i4 integer,
    key_i8 bigint,
    key_bool boolean,
    key_date date,
    key_uuid uuid,
    key_numeric numeric(12, 2),
    key_text text,
    key_f4 real,
    key_f8 double precision,
    key_timestamp timestamp,
    measure integer,
    payload text
) USING iceberg;

CREATE TABLE query_offload_join_right (
    id integer,
    key_i2 smallint,
    key_i4 integer,
    key_i8 bigint,
    key_bool boolean,
    key_date date,
    key_uuid uuid,
    key_numeric numeric(12, 2),
    key_text text,
    key_f4 real,
    key_f8 double precision,
    key_timestamp timestamp,
    measure integer,
    payload text
) USING iceberg;

INSERT INTO query_offload_join_left VALUES
    (1, 1, 1, 10, true, DATE '2024-01-01',
     UUID '00000000-0000-0000-0000-000000000001', 1.00, 'alpha',
     1.5, 10.5, TIMESTAMP '2024-01-01 10:00:00', 10, 'left-1'),
    (2, 2, 2, 20, false, DATE '2024-01-02',
     UUID '00000000-0000-0000-0000-000000000002', 2.00, 'beta',
     2.5, 20.5, TIMESTAMP '2024-01-02 10:00:00', 20, 'left-2'),
    (3, 2, 2, 20, false, DATE '2024-01-02',
     UUID '00000000-0000-0000-0000-000000000002', 2.00, 'beta',
     2.5, 20.5, TIMESTAMP '2024-01-02 10:00:00', 30, 'left-3'),
    (4, 3, 3, 30, true, DATE '2024-01-03',
     UUID '00000000-0000-0000-0000-000000000003', 3.00, 'gamma',
     3.5, 30.5, TIMESTAMP '2024-01-03 10:00:00', 40, 'left-4'),
    (5, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
     'NaN'::real, 'NaN'::double precision, NULL, 50, 'left-null'),
    (6, 6, 6, 60, false, DATE '2024-01-06',
     UUID '00000000-0000-0000-0000-000000000006', 6.00, 'zeta',
     6.5, 60.5, TIMESTAMP '2024-01-06 10:00:00', 60, 'left-6');

INSERT INTO query_offload_join_right VALUES
    (101, 1, 1, 10, true, DATE '2024-01-01',
     UUID '00000000-0000-0000-0000-000000000001', 1.00, 'alpha',
     1.5, 10.5, TIMESTAMP '2024-01-01 10:00:00', 100, 'right-1'),
    (102, 2, 2, 20, false, DATE '2024-01-02',
     UUID '00000000-0000-0000-0000-000000000002', 2.00, 'beta',
     2.5, 20.5, TIMESTAMP '2024-01-02 10:00:00', 200, 'right-2'),
    (103, 2, 2, 20, false, DATE '2024-01-02',
     UUID '00000000-0000-0000-0000-000000000002', 2.00, 'beta',
     2.5, 20.5, TIMESTAMP '2024-01-02 10:00:00', 300, 'right-3'),
    (104, 4, 4, 40, true, DATE '2024-01-04',
     UUID '00000000-0000-0000-0000-000000000004', 4.00, 'delta',
     4.5, 40.5, TIMESTAMP '2024-01-04 10:00:00', 400, 'right-4'),
    (105, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
     'NaN'::real, 'NaN'::double precision, NULL, 500, 'right-null');

CREATE TABLE query_offload_join_aux (
    key_i4 integer,
    label text
) USING iceberg;

INSERT INTO query_offload_join_aux VALUES
    (1, 'aux-1'),
    (2, 'aux-2'),
    (4, 'aux-4');

-- ============================================================================
-- Exact inner joins across the implemented key representations. The first
-- case also protects multi-key identity, scan-local filters, ON residuals,
-- post-join filters, duplicates, and multi-batch execution.
-- ============================================================================

SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'force';
EXPLAIN (VERBOSE, COSTS OFF)
SELECT l.id AS left_id, r.id AS right_id
FROM query_offload_join_left AS l
JOIN query_offload_join_right AS r
  ON l.key_i4 = r.key_i4
 AND l.key_i8 = r.key_i8
 AND l.measure < r.measure
WHERE l.id >= 2
  AND l.measure + r.measure >= 220
ORDER BY left_id, right_id;

SELECT l.id AS left_id, r.id AS right_id
FROM query_offload_join_left AS l
JOIN query_offload_join_right AS r
  ON l.key_i4 = r.key_i4
 AND l.key_i8 = r.key_i8
 AND l.measure < r.measure
WHERE l.id >= 2
  AND l.measure + r.measure >= 220
ORDER BY left_id, right_id;

SET lagodb.query_offload_mode = 'off';
SELECT l.id AS left_id, r.id AS right_id
FROM query_offload_join_left AS l
JOIN query_offload_join_right AS r
  ON l.key_i4 = r.key_i4
 AND l.key_i8 = r.key_i8
 AND l.measure < r.measure
WHERE l.id >= 2
  AND l.measure + r.measure >= 220
ORDER BY left_id, right_id;

SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT count(*) AS matches
FROM query_offload_join_left AS l
JOIN query_offload_join_right AS r
  ON l.key_i2 = r.key_i2
 AND l.key_i4 = r.key_i4
 AND l.key_i8 = r.key_i8;

SELECT count(*) AS matches
FROM query_offload_join_left AS l
JOIN query_offload_join_right AS r
  ON l.key_i2 = r.key_i2
 AND l.key_i4 = r.key_i4
 AND l.key_i8 = r.key_i8;

SET lagodb.query_offload_mode = 'off';
SELECT count(*) AS matches
FROM query_offload_join_left AS l
JOIN query_offload_join_right AS r
  ON l.key_i2 = r.key_i2
 AND l.key_i4 = r.key_i4
 AND l.key_i8 = r.key_i8;

SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT count(*) AS matches
FROM query_offload_join_left AS l
JOIN query_offload_join_right AS r
  ON l.key_bool = r.key_bool
 AND l.key_date = r.key_date
 AND l.key_uuid = r.key_uuid;

SELECT count(*) AS matches
FROM query_offload_join_left AS l
JOIN query_offload_join_right AS r
  ON l.key_bool = r.key_bool
 AND l.key_date = r.key_date
 AND l.key_uuid = r.key_uuid;

SET lagodb.query_offload_mode = 'off';
SELECT count(*) AS matches
FROM query_offload_join_left AS l
JOIN query_offload_join_right AS r
  ON l.key_bool = r.key_bool
 AND l.key_date = r.key_date
 AND l.key_uuid = r.key_uuid;

SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT count(*) AS matches
FROM query_offload_join_left AS l
JOIN query_offload_join_right AS r
  ON l.key_numeric = r.key_numeric
 AND l.key_text = r.key_text;

SELECT count(*) AS matches
FROM query_offload_join_left AS l
JOIN query_offload_join_right AS r
  ON l.key_numeric = r.key_numeric
 AND l.key_text = r.key_text;

SET lagodb.query_offload_mode = 'off';
SELECT count(*) AS matches
FROM query_offload_join_left AS l
JOIN query_offload_join_right AS r
  ON l.key_numeric = r.key_numeric
 AND l.key_text = r.key_text;

-- PostgreSQL makes every NaN equal for float join operators, while the
-- DataFusion hash-key implementation does not provide that contract. Force
-- mode must therefore retain the PostgreSQL plan for both float widths.
SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT count(*) AS matches
FROM query_offload_join_left AS l
JOIN query_offload_join_right AS r
  ON l.key_f4 = r.key_f4
 AND l.key_f8 = r.key_f8;

SELECT count(*) AS matches
FROM query_offload_join_left AS l
JOIN query_offload_join_right AS r
  ON l.key_f4 = r.key_f4
 AND l.key_f8 = r.key_f8;

SET lagodb.query_offload_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT count(*) AS matches
FROM query_offload_join_left AS l
JOIN query_offload_join_right AS r
  ON l.key_f4 = r.key_f4
 AND l.key_f8 = r.key_f8;

SELECT count(*) AS matches
FROM query_offload_join_left AS l
JOIN query_offload_join_right AS r
  ON l.key_f4 = r.key_f4
 AND l.key_f8 = r.key_f8;

-- ============================================================================
-- Outer, semi, anti, and NULL-aware join semantics.
-- ============================================================================

SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT l.id AS left_id, r.id AS right_id
FROM query_offload_join_left AS l
LEFT JOIN query_offload_join_right AS r USING (key_i4)
ORDER BY left_id, right_id NULLS LAST;
SELECT l.id AS left_id, r.id AS right_id
FROM query_offload_join_left AS l
LEFT JOIN query_offload_join_right AS r USING (key_i4)
ORDER BY left_id, right_id NULLS LAST;
SET lagodb.query_offload_mode = 'off';
SELECT l.id AS left_id, r.id AS right_id
FROM query_offload_join_left AS l
LEFT JOIN query_offload_join_right AS r USING (key_i4)
ORDER BY left_id, right_id NULLS LAST;

SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT l.id AS left_id, r.id AS right_id
FROM query_offload_join_left AS l
RIGHT JOIN query_offload_join_right AS r USING (key_i4)
ORDER BY right_id, left_id NULLS LAST;
SELECT l.id AS left_id, r.id AS right_id
FROM query_offload_join_left AS l
RIGHT JOIN query_offload_join_right AS r USING (key_i4)
ORDER BY right_id, left_id NULLS LAST;
SET lagodb.query_offload_mode = 'off';
SELECT l.id AS left_id, r.id AS right_id
FROM query_offload_join_left AS l
RIGHT JOIN query_offload_join_right AS r USING (key_i4)
ORDER BY right_id, left_id NULLS LAST;

SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT l.id AS left_id, r.id AS right_id
FROM query_offload_join_left AS l
FULL JOIN query_offload_join_right AS r USING (key_i4)
ORDER BY left_id NULLS LAST, right_id NULLS LAST;
SELECT l.id AS left_id, r.id AS right_id
FROM query_offload_join_left AS l
FULL JOIN query_offload_join_right AS r USING (key_i4)
ORDER BY left_id NULLS LAST, right_id NULLS LAST;
SET lagodb.query_offload_mode = 'off';
SELECT l.id AS left_id, r.id AS right_id
FROM query_offload_join_left AS l
FULL JOIN query_offload_join_right AS r USING (key_i4)
ORDER BY left_id NULLS LAST, right_id NULLS LAST;

SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT l.id
FROM query_offload_join_left AS l
WHERE EXISTS (
    SELECT 1 FROM query_offload_join_right AS r
    WHERE r.key_i4 = l.key_i4
)
ORDER BY l.id;
SELECT l.id
FROM query_offload_join_left AS l
WHERE EXISTS (
    SELECT 1 FROM query_offload_join_right AS r
    WHERE r.key_i4 = l.key_i4
)
ORDER BY l.id;
SET lagodb.query_offload_mode = 'off';
SELECT l.id
FROM query_offload_join_left AS l
WHERE EXISTS (
    SELECT 1 FROM query_offload_join_right AS r
    WHERE r.key_i4 = l.key_i4
)
ORDER BY l.id;

SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT l.id
FROM query_offload_join_left AS l
WHERE NOT EXISTS (
    SELECT 1 FROM query_offload_join_right AS r
    WHERE r.key_i4 = l.key_i4
)
ORDER BY l.id;
SELECT l.id
FROM query_offload_join_left AS l
WHERE NOT EXISTS (
    SELECT 1 FROM query_offload_join_right AS r
    WHERE r.key_i4 = l.key_i4
)
ORDER BY l.id;
SET lagodb.query_offload_mode = 'off';
SELECT l.id
FROM query_offload_join_left AS l
WHERE NOT EXISTS (
    SELECT 1 FROM query_offload_join_right AS r
    WHERE r.key_i4 = l.key_i4
)
ORDER BY l.id;

-- A WHERE qual that still references the nullable side is pushed down at the
-- anti-join relation after PostgreSQL reduces LEFT JOIN ... IS NULL. It must
-- remain a post-join filter rather than participate in anti-match identity.
-- Because the anti output no longer contains right-side columns, query offload
-- must decline this complete shape and leave it with the PostgreSQL executor.
SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT l.id
FROM query_offload_join_left AS l
LEFT JOIN query_offload_join_right AS r ON l.key_i4 = r.key_i4
WHERE r.key_i4 IS NULL
  AND (l.measure = r.measure) IS NOT FALSE
ORDER BY l.id;
SELECT l.id
FROM query_offload_join_left AS l
LEFT JOIN query_offload_join_right AS r ON l.key_i4 = r.key_i4
WHERE r.key_i4 IS NULL
  AND (l.measure = r.measure) IS NOT FALSE
ORDER BY l.id;
SET lagodb.query_offload_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT l.id
FROM query_offload_join_left AS l
LEFT JOIN query_offload_join_right AS r ON l.key_i4 = r.key_i4
WHERE r.key_i4 IS NULL
  AND (l.measure = r.measure) IS NOT FALSE
ORDER BY l.id;
SELECT l.id
FROM query_offload_join_left AS l
LEFT JOIN query_offload_join_right AS r ON l.key_i4 = r.key_i4
WHERE r.key_i4 IS NULL
  AND (l.measure = r.measure) IS NOT FALSE
ORDER BY l.id;

SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT l.id
FROM query_offload_join_left AS l
WHERE l.key_i4 NOT IN (
    SELECT r.key_i4 FROM query_offload_join_right AS r
)
ORDER BY l.id;
SELECT l.id
FROM query_offload_join_left AS l
WHERE l.key_i4 NOT IN (
    SELECT r.key_i4 FROM query_offload_join_right AS r
)
ORDER BY l.id;
SET lagodb.query_offload_mode = 'off';
SELECT l.id
FROM query_offload_join_left AS l
WHERE l.key_i4 NOT IN (
    SELECT r.key_i4 FROM query_offload_join_right AS r
)
ORDER BY l.id;

-- For parameterized query offload, PostgreSQL drives the outer LATERAL
-- relation; the correlated inner
-- two-source join is the query-offload consumer rebuilt for each PARAM_EXEC.
SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT l.id, nested.right_id, nested.label
FROM query_offload_join_left AS l
CROSS JOIN LATERAL (
    SELECT r.id AS right_id, a.label
    FROM query_offload_join_right AS r
    JOIN query_offload_join_aux AS a USING (key_i4)
    WHERE r.key_i4 = l.key_i4
) AS nested
ORDER BY l.id, nested.right_id;
SELECT l.id, nested.right_id, nested.label
FROM query_offload_join_left AS l
CROSS JOIN LATERAL (
    SELECT r.id AS right_id, a.label
    FROM query_offload_join_right AS r
    JOIN query_offload_join_aux AS a USING (key_i4)
    WHERE r.key_i4 = l.key_i4
) AS nested
ORDER BY l.id, nested.right_id;
SET lagodb.query_offload_mode = 'off';
SELECT l.id, nested.right_id, nested.label
FROM query_offload_join_left AS l
CROSS JOIN LATERAL (
    SELECT r.id AS right_id, a.label
    FROM query_offload_join_right AS r
    JOIN query_offload_join_aux AS a USING (key_i4)
    WHERE r.key_i4 = l.key_i4
) AS nested
ORDER BY l.id, nested.right_id;

-- ============================================================================
-- Negative capability gates: forcing query offload cannot bypass an
-- unsupported timestamp equality key or invent an equi key for a non-equi
-- join. Both plans and results must remain PostgreSQL-native.
-- ============================================================================

SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT l.id AS left_id, r.id AS right_id
FROM query_offload_join_left AS l
JOIN query_offload_join_right AS r
  ON l.key_timestamp = r.key_timestamp
ORDER BY left_id, right_id;
SELECT l.id AS left_id, r.id AS right_id
FROM query_offload_join_left AS l
JOIN query_offload_join_right AS r
  ON l.key_timestamp = r.key_timestamp
ORDER BY left_id, right_id;
SET lagodb.query_offload_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT l.id AS left_id, r.id AS right_id
FROM query_offload_join_left AS l
JOIN query_offload_join_right AS r
  ON l.key_timestamp = r.key_timestamp
ORDER BY left_id, right_id;
SELECT l.id AS left_id, r.id AS right_id
FROM query_offload_join_left AS l
JOIN query_offload_join_right AS r
  ON l.key_timestamp = r.key_timestamp
ORDER BY left_id, right_id;

SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT count(*)
FROM query_offload_join_left AS l
JOIN query_offload_join_right AS r
  ON l.key_i4 < r.key_i4;
SELECT count(*)
FROM query_offload_join_left AS l
JOIN query_offload_join_right AS r
  ON l.key_i4 < r.key_i4;
SET lagodb.query_offload_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT count(*)
FROM query_offload_join_left AS l
JOIN query_offload_join_right AS r
  ON l.key_i4 < r.key_i4;
SELECT count(*)
FROM query_offload_join_left AS l
JOIN query_offload_join_right AS r
  ON l.key_i4 < r.key_i4;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP TABLE query_offload_join_aux;
DROP TABLE query_offload_join_right;
DROP TABLE query_offload_join_left;

RESET timezone;
RESET lagodb.query_batch_rows;
RESET lagodb.query_offload_mode;
RESET lagodb.customscan_mode;
