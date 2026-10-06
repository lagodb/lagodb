-- Predicate pushdown, residual filtering, pruning, and path costing.

-- Exact equality: plans and results.

DROP EXTENSION IF EXISTS lagodb_iceberg CASCADE;
CREATE EXTENSION IF NOT EXISTS lagodb_iceberg;

-- Disable upper query offload to isolate relation-level CustomScan behavior.
SET lagodb.query_offload_mode = 'off';

CREATE TABLE customscan_where_eq_t (
    a integer,
    b text
) USING iceberg;

INSERT INTO customscan_where_eq_t VALUES (1, 'one');
INSERT INTO customscan_where_eq_t VALUES (2, 'two');
INSERT INTO customscan_where_eq_t VALUES (3, 'three');

SELECT COUNT(*) AS total_rows FROM customscan_where_eq_t;


-- An exact equality is pushed without a local residual.
SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT a, b FROM customscan_where_eq_t WHERE a = 1;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT a, b FROM customscan_where_eq_t WHERE a = 1;


SET lagodb.customscan_mode = 'force';
SELECT a, b FROM customscan_where_eq_t WHERE a = 1 ORDER BY a, b;

SET lagodb.customscan_mode = 'off';
SELECT a, b FROM customscan_where_eq_t WHERE a = 1 ORDER BY a, b;

-- VERBOSE exposes the provider, exact filter, and recheck expression.

SET lagodb.customscan_mode = 'force';
EXPLAIN (VERBOSE, COSTS OFF)
SELECT a, b FROM customscan_where_eq_t WHERE a = 1;

RESET lagodb.customscan_mode;
DROP TABLE customscan_where_eq_t;
-- Exact integer predicates.


-- Use three files with disjoint integer ranges. The second includes NULL ids to check
-- strict comparison semantics.

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

-- Exact int4 comparisons.

SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id = 25;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id = 25;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id = 25;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id = 25;

-- NULL ids must not satisfy the inequality.
SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id <> 25;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id <> 25;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id <> 25;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id <> 25;

SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id < 120;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id < 120;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id < 120;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id < 120;

SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id <= 120;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id <= 120;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id <= 120;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id <= 120;

SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id > 120;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id > 120;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id > 120;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id > 120;

SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id >= 120;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id >= 120;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id >= 120;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id >= 120;

-- Exact int8 comparisons.

-- Use a literal above 2^32 to exercise the 64-bit comparison path.
SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id = 10000000025;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id = 10000000025;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id = 10000000025;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id = 10000000025;

SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id <> 25::bigint;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id <> 25::bigint;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id <> 25::bigint;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id <> 25::bigint;

SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id < 120::bigint;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id < 120::bigint;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id < 120::bigint;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id < 120::bigint;

SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id <= 120::bigint;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id <= 120::bigint;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id <= 120::bigint;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id <= 120::bigint;

SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id > 9999999999::bigint;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id > 9999999999::bigint;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id > 9999999999::bigint;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id > 9999999999::bigint;

SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id >= 10000000000::bigint;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id >= 10000000000::bigint;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id >= 10000000000::bigint;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id >= 10000000000::bigint;

-- NULL semantics and AND composition

-- Equality on the NULL-bearing int4 file.
SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id = 119;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id = 119;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id = 119;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id = 119;

-- NULL ids must not satisfy the inequality.
SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id <> 119;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id <> 119;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id <> 119;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id <> 119;

-- Both exact range clauses are pushed without a residual.
SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id >= 100 AND id <= 150;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id >= 100 AND id <= 150;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id >= 100 AND id <= 150;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int4
WHERE id >= 100 AND id <= 150;

-- Type resolution through PG's resolved operator identity

-- An explicitly typed int8 literal resolves to the same pushdown operator.
SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id = 25::bigint;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id = 25::bigint;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id = 25::bigint;

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || payload, E'\n' ORDER BY id, payload), '')) AS row_digest
FROM customscan_exact_pushdown_int8
WHERE id = 25::bigint;

-- Text equality is exact under deterministic collations. Keep the column on the
-- database default collation.

CREATE TABLE customscan_exact_pushdown_text (
    id integer,
    label text
) USING iceberg;

INSERT INTO customscan_exact_pushdown_text VALUES
    (1, 'apple'),
    (2, 'Banana'),
    (3, 'banana'),
    (4, 'Cherry'),
    (5, 'cherry');

-- Equality under the column default collation.
SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || label, E'\n' ORDER BY id, label), '')) AS row_digest
FROM customscan_exact_pushdown_text
WHERE label = 'banana';

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || label, E'\n' ORDER BY id, label), '')) AS row_digest
FROM customscan_exact_pushdown_text
WHERE label = 'banana';

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || label, E'\n' ORDER BY id, label), '')) AS row_digest
FROM customscan_exact_pushdown_text
WHERE label = 'banana';

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || label, E'\n' ORDER BY id, label), '')) AS row_digest
FROM customscan_exact_pushdown_text
WHERE label = 'banana';

-- An explicit deterministic predicate collation uses the same equality policy.
SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || label, E'\n' ORDER BY id, label), '')) AS row_digest
FROM customscan_exact_pushdown_text
WHERE label COLLATE "POSIX" = 'banana';

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || label, E'\n' ORDER BY id, label), '')) AS row_digest
FROM customscan_exact_pushdown_text
WHERE label COLLATE "POSIX" = 'banana';

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || label, E'\n' ORDER BY id, label), '')) AS row_digest
FROM customscan_exact_pushdown_text
WHERE label COLLATE "POSIX" = 'banana';

SELECT COUNT(*) AS row_count,
       md5(COALESCE(string_agg(id::text || '|' || label, E'\n' ORDER BY id, label), '')) AS row_digest
FROM customscan_exact_pushdown_text
WHERE label COLLATE "POSIX" = 'banana';

RESET lagodb.customscan_mode;
DROP TABLE customscan_exact_pushdown_int4;
DROP TABLE customscan_exact_pushdown_int8;
DROP TABLE customscan_exact_pushdown_text;
-- Partial pushdown and residual composition.


CREATE TABLE customscan_partial_pushdown_t (
    a integer,
    b text
) USING iceberg;

INSERT INTO customscan_partial_pushdown_t VALUES (1, 'one');
INSERT INTO customscan_partial_pushdown_t VALUES (2, 'two');
INSERT INTO customscan_partial_pushdown_t VALUES (3, '');

SELECT COUNT(*) AS total_rows FROM customscan_partial_pushdown_t;

-- Push a = 1 while PostgreSQL evaluates length(b) > 0 as a residual.

SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT a, b FROM customscan_partial_pushdown_t
WHERE a = 1 AND length(b) > 0;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT a, b FROM customscan_partial_pushdown_t
WHERE a = 1 AND length(b) > 0;

SET lagodb.customscan_mode = 'force';
SELECT a, b FROM customscan_partial_pushdown_t
WHERE a = 1 AND length(b) > 0
ORDER BY a, b;

SET lagodb.customscan_mode = 'off';
SELECT a, b FROM customscan_partial_pushdown_t
WHERE a = 1 AND length(b) > 0
ORDER BY a, b;

-- VERBOSE separates the exact filter and recheck from the local residual.
SET lagodb.customscan_mode = 'force';
EXPLAIN (VERBOSE, COSTS OFF)
SELECT a, b FROM customscan_partial_pushdown_t
WHERE a = 1 AND length(b) > 0;

-- An OR with an unsupported child remains entirely residual.

EXPLAIN (COSTS OFF)
SELECT a, b FROM customscan_partial_pushdown_t
WHERE a = 1 OR length(b) > 0;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT a, b FROM customscan_partial_pushdown_t
WHERE a = 1 OR length(b) > 0;

SET lagodb.customscan_mode = 'force';
SELECT a, b FROM customscan_partial_pushdown_t
WHERE a = 1 OR length(b) > 0
ORDER BY a, b;

SET lagodb.customscan_mode = 'off';
SELECT a, b FROM customscan_partial_pushdown_t
WHERE a = 1 OR length(b) > 0
ORDER BY a, b;

-- Row-valued IS NULL and IS NOT NULL are not complements. Keep NOT (t IS NULL) as the
-- original residual when widening the OR; row (1, NULL) distinguishes the results.
INSERT INTO customscan_partial_pushdown_t VALUES (1, NULL);

SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT a, b
FROM customscan_partial_pushdown_t AS t
WHERE (a = 1 AND NOT (t IS NULL)) OR a = 2;

SELECT count(*) AS matched_rows
FROM customscan_partial_pushdown_t AS t
WHERE (a = 1 AND NOT (t IS NULL)) OR a = 2;

SET lagodb.customscan_mode = 'off';
SELECT count(*) AS matched_rows
FROM customscan_partial_pushdown_t AS t
WHERE (a = 1 AND NOT (t IS NULL)) OR a = 2;

RESET lagodb.customscan_mode;
DROP TABLE customscan_partial_pushdown_t;
-- Cost-based path selection in auto mode.

-- Compare costed integer pushdown, uncosted date pruning, and an unfiltered scan. Two
-- data files provide nonzero size estimates; COSTS OFF keeps file-size-dependent costs
-- out of expected.
CREATE TABLE customscan_auto_cost_t (
    id integer,
    payload text,
    event_date date
) USING iceberg;

-- File 1: id in [1, 2000]
INSERT INTO customscan_auto_cost_t
SELECT g, 'auto_' || g, DATE '2024-01-01'
FROM generate_series(1, 2000) AS g;

-- File 2: id in [10000, 11999]
INSERT INTO customscan_auto_cost_t
SELECT g, 'auto_' || g, DATE '2025-01-01'
FROM generate_series(10000, 11999) AS g;

SELECT COUNT(*) AS auto_cost_rows FROM customscan_auto_cost_t;

SET lagodb.customscan_mode = 'auto';

-- Costed integer equality selects CustomScan in auto mode.
EXPLAIN (COSTS OFF)
SELECT id, payload FROM customscan_auto_cost_t WHERE id = 1500;

-- Uncosted date pruning leaves SeqScan selected in auto mode.
EXPLAIN (COSTS OFF)
SELECT id, payload FROM customscan_auto_cost_t
WHERE event_date = DATE '2024-01-01';

-- Force selects the date CustomPath, distinguishing uncosted pruning from an
-- unsupported predicate.
SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT id, payload FROM customscan_auto_cost_t
WHERE event_date = DATE '2024-01-01';

-- An unfiltered scan remains a SeqScan in auto mode.
SET lagodb.customscan_mode = 'auto';
EXPLAIN (COSTS OFF)
SELECT id, payload FROM customscan_auto_cost_t;

-- Force keeps an unfiltered relation CustomScan available.
SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT id, payload FROM customscan_auto_cost_t;

SET lagodb.customscan_mode = 'auto';
SELECT id, payload FROM customscan_auto_cost_t WHERE id = 1500 ORDER BY id, payload;

SET lagodb.customscan_mode = 'off';
SELECT id, payload FROM customscan_auto_cost_t WHERE id = 1500 ORDER BY id, payload;

RESET lagodb.customscan_mode;
DROP TABLE customscan_auto_cost_t;

-- Conservative pruning retains the PostgreSQL residual.

DROP EXTENSION IF EXISTS lagodb_iceberg CASCADE;
CREATE EXTENSION IF NOT EXISTS lagodb_iceberg;

-- Use four files with disjoint date ranges. Conservative filters prune files while
-- PostgreSQL rechecks returned rows.
CREATE TABLE customscan_conservative_pruning_t (
    val date,
    payload text
) USING iceberg;

-- File 1: val ∈ [2001-01-01 + 1, 2001-01-01 + 100]
INSERT INTO customscan_conservative_pruning_t
SELECT DATE '2001-01-01' + g, 'file1_' || g
FROM generate_series(1, 100) AS g;

-- File 2: val ∈ [2001-01-01 + 200, 2001-01-01 + 300]
INSERT INTO customscan_conservative_pruning_t
SELECT DATE '2001-01-01' + g, 'file2_' || g
FROM generate_series(200, 300) AS g;

-- File 3: val ∈ [2001-01-01 + 500, 2001-01-01 + 600]
INSERT INTO customscan_conservative_pruning_t
SELECT DATE '2001-01-01' + g, 'file3_' || g
FROM generate_series(500, 600) AS g;

-- File 4: val ∈ [2001-01-01 + 800, 2001-01-01 + 900]
INSERT INTO customscan_conservative_pruning_t
SELECT DATE '2001-01-01' + g, 'file4_' || g
FROM generate_series(800, 900) AS g;

SELECT COUNT(*) AS total_rows FROM customscan_conservative_pruning_t;

-- Equality selects a value in the second file.

SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT val, payload
FROM customscan_conservative_pruning_t
WHERE val = DATE '2001-01-01' + 250
ORDER BY val, payload;

SELECT val, payload
FROM customscan_conservative_pruning_t
WHERE val = DATE '2001-01-01' + 250
ORDER BY val, payload;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT val, payload
FROM customscan_conservative_pruning_t
WHERE val = DATE '2001-01-01' + 250
ORDER BY val, payload;

SELECT val, payload
FROM customscan_conservative_pruning_t
WHERE val = DATE '2001-01-01' + 250
ORDER BY val, payload;

-- Equality in a gap between files returns no rows.

SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT val, payload
FROM customscan_conservative_pruning_t
WHERE val = DATE '2001-01-01' + 150
ORDER BY val, payload;

SELECT val, payload
FROM customscan_conservative_pruning_t
WHERE val = DATE '2001-01-01' + 150
ORDER BY val, payload;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT val, payload
FROM customscan_conservative_pruning_t
WHERE val = DATE '2001-01-01' + 150
ORDER BY val, payload;

SELECT val, payload
FROM customscan_conservative_pruning_t
WHERE val = DATE '2001-01-01' + 150
ORDER BY val, payload;

-- The range covers file 1 and part of file 2: 150 rows.

SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS pushdown_count
FROM customscan_conservative_pruning_t
WHERE val < DATE '2001-01-01' + 250;

SELECT COUNT(*) AS pushdown_count
FROM customscan_conservative_pruning_t
WHERE val < DATE '2001-01-01' + 250;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS baseline_count
FROM customscan_conservative_pruning_t
WHERE val < DATE '2001-01-01' + 250;

SELECT COUNT(*) AS baseline_count
FROM customscan_conservative_pruning_t
WHERE val < DATE '2001-01-01' + 250;

-- The range crosses three files: 51 + 101 + 51 = 203 rows.

SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS pushdown_count
FROM customscan_conservative_pruning_t
WHERE val >= DATE '2001-01-01' + 50 AND val <= DATE '2001-01-01' + 550;

SELECT COUNT(*) AS pushdown_count
FROM customscan_conservative_pruning_t
WHERE val >= DATE '2001-01-01' + 50 AND val <= DATE '2001-01-01' + 550;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT COUNT(*) AS baseline_count
FROM customscan_conservative_pruning_t
WHERE val >= DATE '2001-01-01' + 50 AND val <= DATE '2001-01-01' + 550;

SELECT COUNT(*) AS baseline_count
FROM customscan_conservative_pruning_t
WHERE val >= DATE '2001-01-01' + 50 AND val <= DATE '2001-01-01' + 550;

RESET lagodb.customscan_mode;
DROP TABLE customscan_conservative_pruning_t;
-- EXPLAIN separates exact filters from conservative pruning.


-- Integer comparisons are exact, date comparisons conservative, and ordered default-
-- collation text comparisons residual.
CREATE TABLE customscan_explain_split_t (
    id integer,
    d date,
    descr text
) USING iceberg;

-- File 1: id in [1, 3], d in 2023 (before the 2024-01-01 boundary)
INSERT INTO customscan_explain_split_t VALUES
    (1, DATE '2023-01-15', 'apple'),
    (2, DATE '2023-06-20', 'mango'),
    (3, DATE '2023-12-31', 'cherry');
-- File 2: id in [4, 6], d in 2024+ (on/after the boundary)
INSERT INTO customscan_explain_split_t VALUES
    (4, DATE '2024-01-10', 'date'),
    (5, DATE '2024-06-15', 'fig'),
    (6, DATE '2024-12-31', 'grape');

SELECT COUNT(*) AS total_rows FROM customscan_explain_split_t;

-- Only the date clause remains in the residual; both date and integer clauses appear in
-- the pushed filter.

SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT id, d FROM customscan_explain_split_t
WHERE id = 2 AND d < DATE '2024-01-01';

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT id, d FROM customscan_explain_split_t
WHERE id = 2 AND d < DATE '2024-01-01';

SET lagodb.customscan_mode = 'force';
SELECT id, d FROM customscan_explain_split_t
WHERE id = 2 AND d < DATE '2024-01-01'
ORDER BY id;

SET lagodb.customscan_mode = 'off';
SELECT id, d FROM customscan_explain_split_t
WHERE id = 2 AND d < DATE '2024-01-01'
ORDER BY id;

-- Ordered text comparison under the default collation stays residual even on a forced
-- CustomScan.

SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT id, descr FROM customscan_explain_split_t
WHERE descr < 'mango';

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT id, descr FROM customscan_explain_split_t
WHERE descr < 'mango';

SET lagodb.customscan_mode = 'force';
SELECT id, descr FROM customscan_explain_split_t
WHERE descr < 'mango'
ORDER BY id;

SET lagodb.customscan_mode = 'off';
SELECT id, descr FROM customscan_explain_split_t
WHERE descr < 'mango'
ORDER BY id;

RESET lagodb.customscan_mode;
DROP TABLE customscan_explain_split_t;
-- Result parity across operand types.


-- Fix the timezone for timestamptz comparisons.
SET timezone = 'UTC';

-- Numeric comparisons stay residual, including NaN and out-of-range literals.
CREATE TABLE rq_numeric (
    id integer,
    val numeric(10, 2)
) USING iceberg;

-- File 1: val in [10.50, 99.99]
INSERT INTO rq_numeric VALUES (1, 10.50), (2, 50.00), (3, 99.99);
-- File 2: val in [100.50, 999.99]
INSERT INTO rq_numeric VALUES (4, 100.50), (5, 200.00), (6, 999.99);

SELECT COUNT(*) AS rq_numeric_rows FROM rq_numeric;

-- Numeric ordered comparison remains residual.
SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT id, val FROM rq_numeric WHERE val < 100.5 ORDER BY id;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT id, val FROM rq_numeric WHERE val < 100.5 ORDER BY id;

SET lagodb.customscan_mode = 'force';
SELECT id, val FROM rq_numeric WHERE val < 100.5 ORDER BY id;

SET lagodb.customscan_mode = 'off';
SELECT id, val FROM rq_numeric WHERE val < 100.5 ORDER BY id;

-- NaN sorts above finite numeric values, so every stored value matches without error.
SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT id, val FROM rq_numeric WHERE val < 'NaN'::numeric ORDER BY id;

SELECT id, val FROM rq_numeric WHERE val < 'NaN'::numeric ORDER BY id;

SET lagodb.customscan_mode = 'off';
SELECT id, val FROM rq_numeric WHERE val < 'NaN'::numeric ORDER BY id;

-- An out-of-range numeric literal must be evaluated by PostgreSQL without a storage
-- conversion error.
SET lagodb.customscan_mode = 'force';
SELECT id, val FROM rq_numeric WHERE val < 1e40::numeric ORDER BY id;

SET lagodb.customscan_mode = 'off';
SELECT id, val FROM rq_numeric WHERE val < 1e40::numeric ORDER BY id;

-- Temporal pruning must agree with PostgreSQL epoch conversion across the 2024
-- boundary.
CREATE TABLE rq_temporal (
    id integer,
    d date,
    ts timestamp,
    tstz timestamptz
) USING iceberg;

-- File 1: pre-2024
INSERT INTO rq_temporal VALUES
    (1, DATE '2020-03-15', TIMESTAMP '2020-03-15 08:00:00', TIMESTAMPTZ '2020-03-15 08:00:00+00'),
    (2, DATE '2021-07-20', TIMESTAMP '2021-07-20 16:30:00', TIMESTAMPTZ '2021-07-20 16:30:00+00');
-- File 2: 2024 and later
INSERT INTO rq_temporal VALUES
    (3, DATE '2024-06-15', TIMESTAMP '2024-06-15 12:34:56', TIMESTAMPTZ '2024-06-15 12:34:56+00'),
    (4, DATE '2025-01-10', TIMESTAMP '2025-01-10 23:59:59', TIMESTAMPTZ '2025-01-10 23:59:59+00');

SELECT COUNT(*) AS rq_temporal_rows FROM rq_temporal;

-- Date comparison.
SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT id, d FROM rq_temporal WHERE d >= DATE '2024-01-01' ORDER BY id;

SELECT id, d FROM rq_temporal WHERE d >= DATE '2024-01-01' ORDER BY id;

SET lagodb.customscan_mode = 'off';
SELECT id, d FROM rq_temporal WHERE d >= DATE '2024-01-01' ORDER BY id;

-- Timestamp comparison.
SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT id, ts FROM rq_temporal WHERE ts >= TIMESTAMP '2024-01-01 00:00:00' ORDER BY id;

SELECT id, ts FROM rq_temporal WHERE ts >= TIMESTAMP '2024-01-01 00:00:00' ORDER BY id;

SET lagodb.customscan_mode = 'off';
SELECT id, ts FROM rq_temporal WHERE ts >= TIMESTAMP '2024-01-01 00:00:00' ORDER BY id;

-- Timestamptz comparison.
SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT id, tstz FROM rq_temporal WHERE tstz >= TIMESTAMPTZ '2024-01-01 00:00:00+00' ORDER BY id;

SELECT id, tstz FROM rq_temporal WHERE tstz >= TIMESTAMPTZ '2024-01-01 00:00:00+00' ORDER BY id;

SET lagodb.customscan_mode = 'off';
SELECT id, tstz FROM rq_temporal WHERE tstz >= TIMESTAMPTZ '2024-01-01 00:00:00+00' ORDER BY id;

-- Default-collation text equality, ordering, and inequality.
CREATE TABLE rq_text (
    id integer,
    label text,
    note text
) USING iceberg;

-- File 1
INSERT INTO rq_text VALUES (1, 'alpha', 'apple'), (2, 'bravo', 'banana'), (3, 'charlie', 'cherry');
-- File 2
INSERT INTO rq_text VALUES (4, 'delta', 'date'), (5, 'echo', 'elderberry'), (6, 'foxtrot', 'fig');

SELECT COUNT(*) AS rq_text_rows FROM rq_text;

-- Text equality under the database default collation.
SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT id, label FROM rq_text WHERE label = 'bravo' ORDER BY id;

SELECT id, label FROM rq_text WHERE label = 'bravo' ORDER BY id;

SET lagodb.customscan_mode = 'off';
SELECT id, label FROM rq_text WHERE label = 'bravo' ORDER BY id;

-- Ordered text comparison under the database default collation remains residual.
SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT id, label FROM rq_text WHERE label < 'delta' ORDER BY id;

SELECT id, label FROM rq_text WHERE label < 'delta' ORDER BY id;

SET lagodb.customscan_mode = 'off';
SELECT id, label FROM rq_text WHERE label < 'delta' ORDER BY id;

-- Text inequality is exact under a deterministic default collation.
SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT id, label FROM rq_text WHERE label <> 'bravo' ORDER BY id;

SELECT id, label FROM rq_text WHERE label <> 'bravo' ORDER BY id;

SET lagodb.customscan_mode = 'off';
SELECT id, label FROM rq_text WHERE label <> 'bravo' ORDER BY id;

-- Repeat the ordered residual comparison on another column.
SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT id, note FROM rq_text WHERE note < 'cherry' ORDER BY id;

SELECT id, note FROM rq_text WHERE note < 'cherry' ORDER BY id;

SET lagodb.customscan_mode = 'off';
SELECT id, note FROM rq_text WHERE note < 'cherry' ORDER BY id;

RESET lagodb.customscan_mode;
RESET timezone;
DROP TABLE rq_numeric;
DROP TABLE rq_temporal;
DROP TABLE rq_text;
-- Exact text inequality under the default deterministic collation.


CREATE TABLE customscan_unsupported_only_t (
    id integer,
    label text
) USING iceberg;

INSERT INTO customscan_unsupported_only_t VALUES (1, 'alpha');
INSERT INTO customscan_unsupported_only_t VALUES (2, 'bravo');
INSERT INTO customscan_unsupported_only_t VALUES (3, 'charlie');

SELECT COUNT(*) AS total_rows FROM customscan_unsupported_only_t;

-- Text inequality is pushed without a local residual.

SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT id, label FROM customscan_unsupported_only_t
WHERE label <> 'bravo'
ORDER BY id, label;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT id, label FROM customscan_unsupported_only_t
WHERE label <> 'bravo'
ORDER BY id, label;

SET lagodb.customscan_mode = 'force';
SELECT id, label FROM customscan_unsupported_only_t
WHERE label <> 'bravo'
ORDER BY id, label;

SET lagodb.customscan_mode = 'off';
SELECT id, label FROM customscan_unsupported_only_t
WHERE label <> 'bravo'
ORDER BY id, label;

-- Use integer equality as the exact-pushdown control on the same table.

SET lagodb.customscan_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT id, label FROM customscan_unsupported_only_t
WHERE id = 2
ORDER BY id, label;

SET lagodb.customscan_mode = 'off';
EXPLAIN (COSTS OFF)
SELECT id, label FROM customscan_unsupported_only_t
WHERE id = 2
ORDER BY id, label;

SET lagodb.customscan_mode = 'force';
SELECT id, label FROM customscan_unsupported_only_t
WHERE id = 2
ORDER BY id, label;

SET lagodb.customscan_mode = 'off';
SELECT id, label FROM customscan_unsupported_only_t
WHERE id = 2
ORDER BY id, label;

RESET lagodb.customscan_mode;
RESET lagodb.query_offload_mode;
DROP TABLE customscan_unsupported_only_t;
