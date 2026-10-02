-- query_offload_foreign_tables.sql
-- Positive query-offload coverage for Iceberg foreign-table sources. The
-- operator capability matrices live in the managed-table suites; this file
-- verifies that each basic operator family accepts foreign scan providers and
-- preserves PostgreSQL results.

\set ECHO none
\setenv PGDATABASE :DBNAME
\set iceberg_fixture query-offload-writes
\i include/iceberg_fixture.sql
SELECT rest_uri AS regress_rest_uri
FROM lagodb_regress.object_storage_fixture
\gset

SET client_min_messages = warning;
DROP EXTENSION IF EXISTS lagodb_iceberg CASCADE;
CREATE EXTENSION lagodb_iceberg;
CREATE SCHEMA query_offload_foreign;
CREATE SERVER query_offload_foreign_rest
TYPE 'rest'
FOREIGN DATA WRAPPER lagodb_iceberg
OPTIONS (uri :'regress_rest_uri');
CREATE USER MAPPING FOR CURRENT_USER SERVER query_offload_foreign_rest;
RESET client_min_messages;

\set ECHO all
CREATE FOREIGN TABLE query_offload_foreign.left_source (
    id integer,
    group_key integer,
    measure integer,
    payload text
)
SERVER query_offload_foreign_rest
OPTIONS (
    catalog_name 'regress',
    catalog_namespace 'query_offload_reads',
    catalog_table_name 'left_source',
    mode 'read_only'
);

CREATE FOREIGN TABLE query_offload_foreign.right_source (
    id integer,
    group_key integer,
    measure integer,
    payload text
)
SERVER query_offload_foreign_rest
OPTIONS (
    catalog_name 'regress',
    catalog_namespace 'query_offload_reads',
    catalog_table_name 'right_source',
    mode 'read_only'
);

CREATE FOREIGN TABLE query_offload_foreign.writable_source (
    id integer,
    group_key integer,
    measure integer,
    payload text
)
SERVER query_offload_foreign_rest
OPTIONS (
    catalog_name 'regress',
    catalog_namespace 'query_offload_writes',
    catalog_table_name 'left_source',
    mode 'read_write'
);

CREATE FOREIGN TABLE query_offload_foreign.parallel_source (
    id integer,
    group_key integer,
    measure integer,
    file_group integer
)
SERVER query_offload_foreign_rest
OPTIONS (
    catalog_name 'regress',
    catalog_namespace 'query_offload_writes',
    catalog_table_name 'parallel_source',
    mode 'read_write'
);

SET lagodb.query_batch_rows = 2;
SET lagodb.customscan_mode = 'off';
SET lagodb.query_offload_mode = 'force';
SET max_parallel_workers_per_gather = 0;

-- A column-free aggregate exercises the foreign CountRows scan projection.
-- The corresponding native result protects the remote row count.
EXPLAIN (COSTS OFF)
SELECT count(*) AS rows
FROM query_offload_foreign.left_source;

SELECT count(*) AS rows
FROM query_offload_foreign.left_source;

SET lagodb.query_offload_mode = 'off';
SELECT count(*) AS rows
FROM query_offload_foreign.left_source;

-- A scalar aggregate makes the foreign scan a query-offload candidate while
-- exercising projection, an exact provider filter, and scalar expressions.
SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT count(*) AS rows,
       sum(abs(measure)) AS absolute_total,
       count(*) FILTER (WHERE payload IS NOT NULL) AS payload_rows
FROM query_offload_foreign.left_source
WHERE id >= 2;

SELECT count(*) AS rows,
       sum(abs(measure)) AS absolute_total,
       count(*) FILTER (WHERE payload IS NOT NULL) AS payload_rows
FROM query_offload_foreign.left_source
WHERE id >= 2;

SET lagodb.query_offload_mode = 'off';
SELECT count(*) AS rows,
       sum(abs(measure)) AS absolute_total,
       count(*) FILTER (WHERE payload IS NOT NULL) AS payload_rows
FROM query_offload_foreign.left_source
WHERE id >= 2;

-- A generic parameter is retained as a runtime binding and can execute with
-- different values against the same planned foreign source.
SET lagodb.query_offload_mode = 'force';
SET plan_cache_mode = force_generic_plan;
PREPARE query_offload_foreign_by_id(integer) AS
SELECT count(*) AS rows, sum(measure) AS total
FROM query_offload_foreign.left_source
WHERE id >= $1;

EXPLAIN (COSTS OFF) EXECUTE query_offload_foreign_by_id(3);
EXECUTE query_offload_foreign_by_id(3);
EXECUTE query_offload_foreign_by_id(5);
DEALLOCATE query_offload_foreign_by_id;

SET lagodb.query_offload_mode = 'off';
PREPARE query_offload_foreign_by_id(integer) AS
SELECT count(*) AS rows, sum(measure) AS total
FROM query_offload_foreign.left_source
WHERE id >= $1;
EXECUTE query_offload_foreign_by_id(3);
EXECUTE query_offload_foreign_by_id(5);
DEALLOCATE query_offload_foreign_by_id;
RESET plan_cache_mode;

-- Grouping, aggregate FILTER, NULL inputs, and HAVING consume the foreign
-- source through the same provider contract as a managed Iceberg table.
SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT group_key,
       count(*) AS rows,
       count(measure) AS values,
       sum(measure) FILTER (WHERE id >= 2) AS filtered_total
FROM query_offload_foreign.left_source
GROUP BY group_key
HAVING count(*) >= 1
ORDER BY group_key NULLS LAST
LIMIT 2 OFFSET 1;

SELECT group_key,
       count(*) AS rows,
       count(measure) AS values,
       sum(measure) FILTER (WHERE id >= 2) AS filtered_total
FROM query_offload_foreign.left_source
GROUP BY group_key
HAVING count(*) >= 1
ORDER BY group_key NULLS LAST
LIMIT 2 OFFSET 1;

SET lagodb.query_offload_mode = 'off';
SELECT group_key,
       count(*) AS rows,
       count(measure) AS values,
       sum(measure) FILTER (WHERE id >= 2) AS filtered_total
FROM query_offload_foreign.left_source
GROUP BY group_key
HAVING count(*) >= 1
ORDER BY group_key NULLS LAST
LIMIT 2 OFFSET 1;

-- DISTINCT owns the foreign scan; PostgreSQL may retain sorting and pagination
-- above that offloaded fragment according to its normal upper-path rules.
SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT DISTINCT payload
FROM query_offload_foreign.left_source
ORDER BY payload NULLS LAST
LIMIT 3 OFFSET 1;

SELECT DISTINCT payload
FROM query_offload_foreign.left_source
ORDER BY payload NULLS LAST
LIMIT 3 OFFSET 1;

SET lagodb.query_offload_mode = 'off';
SELECT DISTINCT payload
FROM query_offload_foreign.left_source
ORDER BY payload NULLS LAST
LIMIT 3 OFFSET 1;

-- Two foreign sources owned by the same provider participate in one offloaded
-- join while preserving duplicate fanout and scan-local filters.
SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT l.id AS left_id,
       r.id AS right_id,
       l.measure + r.measure AS combined_measure
FROM query_offload_foreign.left_source AS l
JOIN query_offload_foreign.right_source AS r USING (group_key)
WHERE l.id >= 2
ORDER BY left_id, right_id;

SELECT l.id AS left_id,
       r.id AS right_id,
       l.measure + r.measure AS combined_measure
FROM query_offload_foreign.left_source AS l
JOIN query_offload_foreign.right_source AS r USING (group_key)
WHERE l.id >= 2
ORDER BY left_id, right_id;

SET lagodb.query_offload_mode = 'off';
SELECT l.id AS left_id,
       r.id AS right_id,
       l.measure + r.measure AS combined_measure
FROM query_offload_foreign.left_source AS l
JOIN query_offload_foreign.right_source AS r USING (group_key)
WHERE l.id >= 2
ORDER BY left_id, right_id;

-- A foreign source and a managed source must resolve independently rather
-- than coupling either adapter to the other.
CREATE TABLE query_offload_foreign_managed (
    group_key integer,
    label text
) USING iceberg;
INSERT INTO query_offload_foreign_managed VALUES
    (1, 'managed-one'),
    (2, 'managed-two'),
    (4, 'managed-four');

SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT f.id, m.label
FROM query_offload_foreign.left_source AS f
JOIN query_offload_foreign_managed AS m USING (group_key)
ORDER BY f.id;

SELECT f.id, m.label
FROM query_offload_foreign.left_source AS f
JOIN query_offload_foreign_managed AS m USING (group_key)
ORDER BY f.id;

SET lagodb.query_offload_mode = 'off';
SELECT f.id, m.label
FROM query_offload_foreign.left_source AS f
JOIN query_offload_foreign_managed AS m USING (group_key)
ORDER BY f.id;

-- A writable source binds through the transaction-local Iceberg view. The
-- offloaded scan must observe the staged data file before PostgreSQL commits,
-- and the native FDW path must expose the same statement-visible rows.
BEGIN;
INSERT INTO query_offload_foreign.writable_source
VALUES (6, 4, 60, 'staged');

SET lagodb.query_offload_mode = 'force';
EXPLAIN (COSTS OFF)
SELECT count(*) AS rows, sum(measure) AS total
FROM query_offload_foreign.writable_source
WHERE id >= 5;

SELECT count(*) AS rows, sum(measure) AS total
FROM query_offload_foreign.writable_source
WHERE id >= 5;

SET lagodb.query_offload_mode = 'off';
SELECT count(*) AS rows, sum(measure) AS total
FROM query_offload_foreign.writable_source
WHERE id >= 5;
ROLLBACK;

-- The writable source has enough independently written files for at least two
-- Iceberg work groups. Worker reconstruction must preserve its read-write
-- identity while executing every work group and reporting complete metrics.
SET max_parallel_workers_per_gather = 2;
SET lagodb.query_offload_parallel_min_scan_rows = 0;
SET lagodb.query_offload_mode = 'force';

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT group_key, count(*) AS rows, sum(measure) AS total
FROM query_offload_foreign.parallel_source
WHERE id >= 1
GROUP BY group_key
ORDER BY group_key;

SELECT group_key, count(*) AS rows, sum(measure) AS total
FROM query_offload_foreign.parallel_source
WHERE id >= 1
GROUP BY group_key
ORDER BY group_key;

SET lagodb.query_offload_mode = 'off';
SELECT group_key, count(*) AS rows, sum(measure) AS total
FROM query_offload_foreign.parallel_source
WHERE id >= 1
GROUP BY group_key
ORDER BY group_key;

DROP TABLE query_offload_foreign_managed;
SET client_min_messages = warning;
DROP SCHEMA query_offload_foreign CASCADE;
DROP SERVER query_offload_foreign_rest CASCADE;
RESET client_min_messages;

RESET max_parallel_workers_per_gather;
RESET lagodb.query_offload_parallel_min_scan_rows;
RESET lagodb.query_offload_mode;
RESET lagodb.customscan_mode;
RESET lagodb.query_batch_rows;
