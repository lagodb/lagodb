-- query_offload_composition.sql
-- Cross-operator coverage. Individual aggregate and join capability matrices
-- remain in their focused suites; this file protects their composition.

DROP EXTENSION IF EXISTS lagodb_iceberg CASCADE;
CREATE EXTENSION IF NOT EXISTS lagodb_iceberg;

SET lagodb.query_batch_rows = 2;

CREATE TABLE query_offload_comp_left (
    id integer,
    key integer,
    measure integer
) USING iceberg;

CREATE TABLE query_offload_comp_right (
    id integer,
    key integer,
    measure integer,
    label text
) USING iceberg;

CREATE TABLE query_offload_comp_aux (
    key integer,
    label text,
    label_varchar varchar(8),
    label_bpchar character(8),
    label_name name
) USING iceberg;

CREATE TABLE query_offload_comp_empty (
    key integer,
    measure integer
) USING iceberg;

INSERT INTO query_offload_comp_left VALUES
    (1, 1, 10),
    (2, 2, 20),
    (3, 2, NULL),
    (4, 3, 40),
    (5, NULL, 50),
    (6, 6, 60);

INSERT INTO query_offload_comp_right VALUES
    (101, 1, 100, 'r1'),
    (102, 2, 200, 'r2a'),
    (103, 2, 300, 'r2b'),
    (104, 4, 400, 'r4'),
    (105, NULL, 500, 'rnull');

INSERT INTO query_offload_comp_aux VALUES
    (1, 'shared', 'shared', 'shared', 'shared'),
    (2, 'shared', 'shared', 'shared ', 'shared'),
    (4, 'four', 'four', 'four', 'four');

-- Duplicate fanout, NULL aggregate input, FILTER, and HAVING in one fragment.
SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT l.key,
       count(*) AS joined_rows,
       count(l.measure) AS nonnull_left,
       min(l.measure) AS min_left,
       max(r.measure) AS max_right,
       sum(l.measure) AS sum_left,
       avg(r.measure)::numeric(20, 2) AS avg_right,
       count(*) FILTER (WHERE r.measure >= 300) AS large_right
FROM query_offload_comp_left AS l
JOIN query_offload_comp_right AS r USING (key)
GROUP BY l.key
HAVING count(*) >= 2
ORDER BY l.key;

SELECT l.key,
       count(*) AS joined_rows,
       count(l.measure) AS nonnull_left,
       min(l.measure) AS min_left,
       max(r.measure) AS max_right,
       sum(l.measure) AS sum_left,
       avg(r.measure)::numeric(20, 2) AS avg_right,
       count(*) FILTER (WHERE r.measure >= 300) AS large_right
FROM query_offload_comp_left AS l
JOIN query_offload_comp_right AS r USING (key)
GROUP BY l.key
HAVING count(*) >= 2
ORDER BY l.key;

SET lagodb.query_offload_mode = 'off';
SELECT l.key,
       count(*) AS joined_rows,
       count(l.measure) AS nonnull_left,
       min(l.measure) AS min_left,
       max(r.measure) AS max_right,
       sum(l.measure) AS sum_left,
       avg(r.measure)::numeric(20, 2) AS avg_right,
       count(*) FILTER (WHERE r.measure >= 300) AS large_right
FROM query_offload_comp_left AS l
JOIN query_offload_comp_right AS r USING (key)
GROUP BY l.key
HAVING count(*) >= 2
ORDER BY l.key;

-- Scalar aggregate over an empty join input preserves COUNT versus nullable
-- aggregate semantics.
SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT count(*) AS rows,
       count(e.measure) AS values,
       min(e.measure) AS minimum,
       sum(e.measure) AS total
FROM query_offload_comp_left AS l
JOIN query_offload_comp_empty AS e USING (key);

SELECT count(*) AS rows,
       count(e.measure) AS values,
       min(e.measure) AS minimum,
       sum(e.measure) AS total
FROM query_offload_comp_left AS l
JOIN query_offload_comp_empty AS e USING (key);

SET lagodb.query_offload_mode = 'off';
SELECT count(*) AS rows,
       count(e.measure) AS values,
       min(e.measure) AS minimum,
       sum(e.measure) AS total
FROM query_offload_comp_left AS l
JOIN query_offload_comp_empty AS e USING (key);

-- Outer-join null extension must distinguish joined rows from right values.
SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT l.key, count(*) AS rows, count(r.id) AS matched_right
FROM query_offload_comp_left AS l
LEFT JOIN query_offload_comp_right AS r
  ON l.key = r.key AND r.measure >= 200
WHERE r.id IS NULL OR l.id <> 2
GROUP BY l.key
ORDER BY l.key NULLS LAST;

SELECT l.key, count(*) AS rows, count(r.id) AS matched_right
FROM query_offload_comp_left AS l
LEFT JOIN query_offload_comp_right AS r
  ON l.key = r.key AND r.measure >= 200
WHERE r.id IS NULL OR l.id <> 2
GROUP BY l.key
ORDER BY l.key NULLS LAST;

SET lagodb.query_offload_mode = 'off';
SELECT l.key, count(*) AS rows, count(r.id) AS matched_right
FROM query_offload_comp_left AS l
LEFT JOIN query_offload_comp_right AS r
  ON l.key = r.key AND r.measure >= 200
WHERE r.id IS NULL OR l.id <> 2
GROUP BY l.key
ORDER BY l.key NULLS LAST;

-- Semi and anti joins remain inside the fragment when consumed by an
-- aggregate. Duplicate and NULL keys on the inner side must not multiply or
-- suppress outer rows beyond PostgreSQL's EXISTS/NOT EXISTS semantics.
SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT count(*) AS matching_left, sum(l.measure) AS matching_total
FROM query_offload_comp_left AS l
WHERE EXISTS (
    SELECT 1
    FROM query_offload_comp_right AS r
    WHERE r.key = l.key
);

SELECT count(*) AS matching_left, sum(l.measure) AS matching_total
FROM query_offload_comp_left AS l
WHERE EXISTS (
    SELECT 1
    FROM query_offload_comp_right AS r
    WHERE r.key = l.key
);

SET lagodb.query_offload_mode = 'off';
SELECT count(*) AS matching_left, sum(l.measure) AS matching_total
FROM query_offload_comp_left AS l
WHERE EXISTS (
    SELECT 1
    FROM query_offload_comp_right AS r
    WHERE r.key = l.key
);

SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT count(*) AS missing_left, sum(l.measure) AS missing_total
FROM query_offload_comp_left AS l
WHERE NOT EXISTS (
    SELECT 1
    FROM query_offload_comp_right AS r
    WHERE r.key = l.key
);

SELECT count(*) AS missing_left, sum(l.measure) AS missing_total
FROM query_offload_comp_left AS l
WHERE NOT EXISTS (
    SELECT 1
    FROM query_offload_comp_right AS r
    WHERE r.key = l.key
);

SET lagodb.query_offload_mode = 'off';
SELECT count(*) AS missing_left, sum(l.measure) AS missing_total
FROM query_offload_comp_left AS l
WHERE NOT EXISTS (
    SELECT 1
    FROM query_offload_comp_right AS r
    WHERE r.key = l.key
);

-- NOT IN needs both controls missing from the join-only fixture: an inner
-- relation containing no NULLs and an empty inner relation.
SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT count(*) AS not_in_nonnull
FROM query_offload_comp_left AS l
WHERE l.key NOT IN (
    SELECT a.key FROM query_offload_comp_aux AS a
);

SELECT count(*) AS not_in_nonnull
FROM query_offload_comp_left AS l
WHERE l.key NOT IN (
    SELECT a.key FROM query_offload_comp_aux AS a
);

SET lagodb.query_offload_mode = 'off';
SELECT count(*) AS not_in_nonnull
FROM query_offload_comp_left AS l
WHERE l.key NOT IN (
    SELECT a.key FROM query_offload_comp_aux AS a
);

SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT count(*) AS not_in_empty
FROM query_offload_comp_left AS l
WHERE l.key NOT IN (
    SELECT e.key FROM query_offload_comp_empty AS e
);

SELECT count(*) AS not_in_empty
FROM query_offload_comp_left AS l
WHERE l.key NOT IN (
    SELECT e.key FROM query_offload_comp_empty AS e
);

SET lagodb.query_offload_mode = 'off';
SELECT count(*) AS not_in_empty
FROM query_offload_comp_left AS l
WHERE l.key NOT IN (
    SELECT e.key FROM query_offload_comp_empty AS e
);

-- A self join proves that source identity is query-local rather than keyed by
-- relation OID or display name.
SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT a.key, count(*) AS pairs, sum(b.measure) AS peer_total
FROM query_offload_comp_left AS a
JOIN query_offload_comp_left AS b
  ON a.key = b.key AND a.id < b.id
GROUP BY a.key
ORDER BY a.key;

SELECT a.key, count(*) AS pairs, sum(b.measure) AS peer_total
FROM query_offload_comp_left AS a
JOIN query_offload_comp_left AS b
  ON a.key = b.key AND a.id < b.id
GROUP BY a.key
ORDER BY a.key;

SET lagodb.query_offload_mode = 'off';
SELECT a.key, count(*) AS pairs, sum(b.measure) AS peer_total
FROM query_offload_comp_left AS a
JOIN query_offload_comp_left AS b
  ON a.key = b.key AND a.id < b.id
GROUP BY a.key
ORDER BY a.key;

-- A three-source tree protects recursive join-to-aggregate composition. TEXT
-- and VARCHAR group keys also protect deterministic-collation byte equality.
SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT a.label,
       a.label_varchar,
       count(*) AS rows,
       sum(r.measure) AS total
FROM query_offload_comp_left AS l
JOIN query_offload_comp_right AS r USING (key)
JOIN query_offload_comp_aux AS a USING (key)
GROUP BY a.label, a.label_varchar
ORDER BY a.label, a.label_varchar;

SELECT a.label,
       a.label_varchar,
       count(*) AS rows,
       sum(r.measure) AS total
FROM query_offload_comp_left AS l
JOIN query_offload_comp_right AS r USING (key)
JOIN query_offload_comp_aux AS a USING (key)
GROUP BY a.label, a.label_varchar
ORDER BY a.label, a.label_varchar;

SET lagodb.query_offload_mode = 'off';
SELECT a.label,
       a.label_varchar,
       count(*) AS rows,
       sum(r.measure) AS total
FROM query_offload_comp_left AS l
JOIN query_offload_comp_right AS r USING (key)
JOIN query_offload_comp_aux AS a USING (key)
GROUP BY a.label, a.label_varchar
ORDER BY a.label, a.label_varchar;

-- BPCHAR and NAME have PostgreSQL-specific equality semantics that are not part
-- of the provider-neutral hash-grouping contract. The joins may still offload,
-- but PostgreSQL must retain each Aggregate node.
SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT a.label_bpchar, count(*) AS rows
FROM query_offload_comp_left AS l
JOIN query_offload_comp_right AS r USING (key)
JOIN query_offload_comp_aux AS a USING (key)
GROUP BY a.label_bpchar;

EXPLAIN (COSTS OFF)
SELECT a.label_name, count(*) AS rows
FROM query_offload_comp_left AS l
JOIN query_offload_comp_right AS r USING (key)
JOIN query_offload_comp_aux AS a USING (key)
GROUP BY a.label_name;

-- DISTINCT aggregate followed by ORDER BY and LIMIT/OFFSET.
SET lagodb.query_offload_mode = 'force';
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT l.key,
       count(DISTINCT r.measure) AS distinct_values,
       max(r.measure) AS maximum,
       string_agg(r.label, ',' ORDER BY r.id) AS ordered_labels
FROM query_offload_comp_left AS l
JOIN query_offload_comp_right AS r USING (key)
WHERE l.id >= 1
GROUP BY l.key
ORDER BY maximum DESC, l.key
LIMIT 1 OFFSET 1;

SELECT l.key,
       count(DISTINCT r.measure) AS distinct_values,
       max(r.measure) AS maximum,
       string_agg(r.label, ',' ORDER BY r.id) AS ordered_labels
FROM query_offload_comp_left AS l
JOIN query_offload_comp_right AS r USING (key)
WHERE l.id >= 1
GROUP BY l.key
ORDER BY maximum DESC, l.key
LIMIT 1 OFFSET 1;

SET lagodb.query_offload_mode = 'off';
SELECT l.key,
       count(DISTINCT r.measure) AS distinct_values,
       max(r.measure) AS maximum,
       string_agg(r.label, ',' ORDER BY r.id) AS ordered_labels
FROM query_offload_comp_left AS l
JOIN query_offload_comp_right AS r USING (key)
WHERE l.id >= 1
GROUP BY l.key
ORDER BY maximum DESC, l.key
LIMIT 1 OFFSET 1;

-- ORDER BY needs measure as a resjunk input while the final output contains
-- only id. Since Sort cannot perform that target-list change, PG17 creates a
-- real (dummypp=false) ProjectionPath above it. Query offload may still own the
-- scan, but it must not absorb Sort or Limit through the PostgreSQL projection.
SET lagodb.query_offload_mode = 'force';
EXPLAIN (VERBOSE, COSTS OFF)
SELECT id
FROM query_offload_comp_left
ORDER BY measure NULLS LAST, id
LIMIT 3;

SELECT id
FROM query_offload_comp_left
ORDER BY measure NULLS LAST, id
LIMIT 3;

SET lagodb.query_offload_mode = 'off';
SELECT id
FROM query_offload_comp_left
ORDER BY measure NULLS LAST, id
LIMIT 3;

-- An unsupported non-equi join must reject the complete aggregate
-- composition instead of installing a partial query-offload fragment.
SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT count(*)
FROM query_offload_comp_left AS l
JOIN query_offload_comp_right AS r ON l.key < r.key;

SELECT count(*)
FROM query_offload_comp_left AS l
JOIN query_offload_comp_right AS r ON l.key < r.key;

SET lagodb.query_offload_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT count(*)
FROM query_offload_comp_left AS l
JOIN query_offload_comp_right AS r ON l.key < r.key;

SELECT count(*)
FROM query_offload_comp_left AS l
JOIN query_offload_comp_right AS r ON l.key < r.key;

DROP TABLE query_offload_comp_empty;
DROP TABLE query_offload_comp_aux;
DROP TABLE query_offload_comp_right;
DROP TABLE query_offload_comp_left;

RESET lagodb.query_batch_rows;
RESET lagodb.query_offload_mode;
RESET lagodb.customscan_mode;
