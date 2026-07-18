-- Cross-DSO routing/GUC authority and end-to-end row preservation.
DROP EXTENSION IF EXISTS pg_iceberg_am CASCADE;
CREATE EXTENSION pg_iceberg_am;
CREATE SCHEMA vacuum_correctness_test;

SELECT extension_name, worker_name
FROM lakebase.workers
WHERE extension_name = 'pg_iceberg_am'
  AND worker_name = 'iceberg_automatic_maintenance';
SELECT current_setting('pg_iceberg_am.auto_maintenance_enabled') AS auto_enabled,
       current_setting('pg_iceberg_am.auto_maintenance_interval_s') AS auto_interval_s,
       current_setting('pg_iceberg_am.auto_maintenance_max_tables') AS auto_max_tables,
       current_setting('pg_iceberg_am.auto_maintenance_jitter_percent') AS auto_jitter,
       current_setting('pg_iceberg_am.auto_maintenance_failure_backoff_max_s')
           AS auto_backoff_max_s;

CREATE TABLE vacuum_correctness_test.t (
    id integer,
    payload text
) USING iceberg;
CREATE TABLE vacuum_correctness_test.t_v1 (
    id integer,
    payload text
) USING iceberg WITH ("format-version" = 1);
CREATE TABLE vacuum_correctness_test.t_v3 (
    id integer,
    payload text
) USING iceberg WITH (
    "format-version" = 3,
    "history.expire.max-snapshot-age-ms" = '0',
    "history.expire.min-snapshots-to-keep" = '1'
);
\set VERBOSITY terse
ALTER TABLE vacuum_correctness_test.t SET ("format-version" = 3);
ALTER TABLE vacuum_correctness_test.t RESET ("format-version");
\set VERBOSITY default
ALTER TABLE vacuum_correctness_test.t SET (
    "history.expire.max-snapshot-age-ms" = '0',
    "history.expire.min-snapshots-to-keep" = '1'
);
CREATE TABLE vacuum_correctness_test.t_full (
    id integer,
    payload text
) USING iceberg WITH (
    "history.expire.max-snapshot-age-ms" = '0',
    "history.expire.min-snapshots-to-keep" = '1'
);
CREATE TABLE vacuum_correctness_test.t_v1_full (
    id integer,
    payload text
) USING iceberg WITH (
    "format-version" = 1,
    "history.expire.max-snapshot-age-ms" = '0',
    "history.expire.min-snapshots-to-keep" = '1'
);
CREATE TABLE vacuum_correctness_test.t_v3_full (
    id integer,
    payload text
) USING iceberg WITH (
    "format-version" = 3,
    "history.expire.max-snapshot-age-ms" = '0',
    "history.expire.min-snapshots-to-keep" = '1'
);

INSERT INTO vacuum_correctness_test.t VALUES (1, 'one');
INSERT INTO vacuum_correctness_test.t VALUES (2, 'two');
INSERT INTO vacuum_correctness_test.t VALUES (3, 'three');
INSERT INTO vacuum_correctness_test.t VALUES (4, 'four');
INSERT INTO vacuum_correctness_test.t VALUES (5, 'five');
INSERT INTO vacuum_correctness_test.t VALUES (6, 'six');
INSERT INTO vacuum_correctness_test.t_v1 VALUES (1, 'one');
INSERT INTO vacuum_correctness_test.t_v1 VALUES (2, 'two');
INSERT INTO vacuum_correctness_test.t_v1 VALUES (3, 'three');
INSERT INTO vacuum_correctness_test.t_v1 VALUES (4, 'four');
INSERT INTO vacuum_correctness_test.t_v1 VALUES (5, 'five');
INSERT INTO vacuum_correctness_test.t_v1 VALUES (6, 'six');
INSERT INTO vacuum_correctness_test.t_v3 VALUES (1, 'one');
INSERT INTO vacuum_correctness_test.t_v3 VALUES (2, 'two');
INSERT INTO vacuum_correctness_test.t_v3 VALUES (3, 'three');
INSERT INTO vacuum_correctness_test.t_v3 VALUES (4, 'four');
INSERT INTO vacuum_correctness_test.t_v3 VALUES (5, 'five');
INSERT INTO vacuum_correctness_test.t_v3 VALUES (6, 'six');
INSERT INTO vacuum_correctness_test.t_full VALUES (1, 'one');
INSERT INTO vacuum_correctness_test.t_full VALUES (2, 'two');
INSERT INTO vacuum_correctness_test.t_full VALUES (3, 'three');
INSERT INTO vacuum_correctness_test.t_full VALUES (4, 'four');
INSERT INTO vacuum_correctness_test.t_full VALUES (5, 'five');
INSERT INTO vacuum_correctness_test.t_full VALUES (6, 'six');
INSERT INTO vacuum_correctness_test.t_v1_full VALUES (1, 'one');
INSERT INTO vacuum_correctness_test.t_v1_full VALUES (2, 'two');
INSERT INTO vacuum_correctness_test.t_v1_full VALUES (3, 'three');
INSERT INTO vacuum_correctness_test.t_v1_full VALUES (4, 'four');
INSERT INTO vacuum_correctness_test.t_v1_full VALUES (5, 'five');
INSERT INTO vacuum_correctness_test.t_v1_full VALUES (6, 'six');
INSERT INTO vacuum_correctness_test.t_v3_full VALUES (1, 'one');
INSERT INTO vacuum_correctness_test.t_v3_full VALUES (2, 'two');
INSERT INTO vacuum_correctness_test.t_v3_full VALUES (3, 'three');
INSERT INTO vacuum_correctness_test.t_v3_full VALUES (4, 'four');
INSERT INTO vacuum_correctness_test.t_v3_full VALUES (5, 'five');
INSERT INTO vacuum_correctness_test.t_v3_full VALUES (6, 'six');

-- Exercise v2 position-delete and v3 deletion-vector/lineage rewrite inputs.
UPDATE vacuum_correctness_test.t SET payload = 'two-updated' WHERE id = 2;
DELETE FROM vacuum_correctness_test.t WHERE id = 3;
UPDATE vacuum_correctness_test.t_v3 SET payload = 'two-updated' WHERE id = 2;
DELETE FROM vacuum_correctness_test.t_v3 WHERE id = 3;
UPDATE vacuum_correctness_test.t_full SET payload = 'two-updated' WHERE id = 2;
DELETE FROM vacuum_correctness_test.t_full WHERE id = 3;
UPDATE vacuum_correctness_test.t_v3_full
SET payload = 'two-updated' WHERE id = 2;
DELETE FROM vacuum_correctness_test.t_v3_full WHERE id = 3;

SELECT pg_relation_filepath('vacuum_correctness_test.t') || '_iceberg'
    AS v2_root \gset
SELECT pg_relation_filepath('vacuum_correctness_test.t_full') || '_iceberg'
    AS full_root \gset
SELECT count(*) AS v2_parquet_before
FROM pg_ls_dir(:'v2_root' || '/data', true, false)
WHERE pg_ls_dir LIKE '%.parquet'
\gset
SELECT count(*) AS full_parquet_before
FROM pg_ls_dir(:'full_root' || '/data', true, false)
WHERE pg_ls_dir LIKE '%.parquet'
\gset

CREATE TEMP TABLE vacuum_before AS
SELECT format, count(*) AS row_count,
       md5(string_agg(id::text || ':' || payload, ',' ORDER BY id)) AS digest
FROM (
    SELECT 'v1'::text AS format, * FROM vacuum_correctness_test.t_v1
    UNION ALL
    SELECT 'v2'::text AS format, * FROM vacuum_correctness_test.t
    UNION ALL
    SELECT 'v3'::text AS format, * FROM vacuum_correctness_test.t_v3
    UNION ALL
    SELECT 'v1-full'::text AS format, *
    FROM vacuum_correctness_test.t_v1_full
    UNION ALL
    SELECT 'v2-full'::text AS format, * FROM vacuum_correctness_test.t_full
    UNION ALL
    SELECT 'v3-full'::text AS format, *
    FROM vacuum_correctness_test.t_v3_full
) AS rows
GROUP BY format;

-- This shared setting is registered/backed by runtime but consumed in the AM.
-- A value of one prevents the minimum-five-file rewrite group from forming.
SET pg_lakebase.vacuum_max_group_objects = 1;
VACUUM vacuum_correctness_test.t;
SELECT provider, format, current_data_objects
FROM lakebase.table_maintenance_stats('vacuum_correctness_test.t');

RESET pg_lakebase.vacuum_max_group_objects;
VACUUM vacuum_correctness_test.t;
VACUUM vacuum_correctness_test.t_v1;
VACUUM vacuum_correctness_test.t_v3;

CREATE TABLE vacuum_correctness_test.heap_t (id integer);
INSERT INTO vacuum_correctness_test.heap_t VALUES (10), (20);
VACUUM (FULL)
    vacuum_correctness_test.heap_t,
    vacuum_correctness_test.t_v1,
    vacuum_correctness_test.t,
    vacuum_correctness_test.t_v3,
    vacuum_correctness_test.t_v1_full,
    vacuum_correctness_test.t_full,
    vacuum_correctness_test.t_v3_full;
SELECT (SELECT array_agg(id ORDER BY id) FROM vacuum_correctness_test.heap_t)
           = ARRAY[10, 20] AS native_rows_preserved;

WITH after AS (
    SELECT format, count(*) AS row_count,
           md5(string_agg(id::text || ':' || payload, ',' ORDER BY id)) AS digest
    FROM (
        SELECT 'v1'::text AS format, * FROM vacuum_correctness_test.t_v1
        UNION ALL
        SELECT 'v2'::text AS format, * FROM vacuum_correctness_test.t
        UNION ALL
        SELECT 'v3'::text AS format, * FROM vacuum_correctness_test.t_v3
        UNION ALL
        SELECT 'v1-full'::text AS format, *
        FROM vacuum_correctness_test.t_v1_full
        UNION ALL
        SELECT 'v2-full'::text AS format, *
        FROM vacuum_correctness_test.t_full
        UNION ALL
        SELECT 'v3-full'::text AS format, *
        FROM vacuum_correctness_test.t_v3_full
    ) AS rows
    GROUP BY format
)
SELECT bool_and(before.row_count = after.row_count) AS row_count_preserved,
       bool_and(before.digest = after.digest) AS content_preserved
FROM vacuum_before AS before
JOIN after USING (format);

SELECT count(*) = 1 AS v2_has_one_parquet,
       count(*) < :v2_parquet_before::bigint AS v2_reclaimed_parquet
FROM pg_ls_dir(:'v2_root' || '/data', true, false)
WHERE pg_ls_dir LIKE '%.parquet';
SELECT count(*) = 1 AS full_has_one_parquet,
       count(*) < :full_parquet_before::bigint AS full_reclaimed_parquet
FROM pg_ls_dir(:'full_root' || '/data', true, false)
WHERE pg_ls_dir LIKE '%.parquet';

SELECT stats.provider, stats.format, stats.current_data_objects
FROM (VALUES
    ('vacuum_correctness_test.t_v1'::regclass),
    ('vacuum_correctness_test.t'::regclass),
    ('vacuum_correctness_test.t_v3'::regclass),
    ('vacuum_correctness_test.t_v1_full'::regclass),
    ('vacuum_correctness_test.t_full'::regclass),
    ('vacuum_correctness_test.t_v3_full'::regclass)
) AS relations(relid)
CROSS JOIN LATERAL lakebase.table_maintenance_stats(relations.relid) AS stats
ORDER BY stats.format;

DROP SCHEMA vacuum_correctness_test CASCADE;
DROP EXTENSION pg_iceberg_am CASCADE;
