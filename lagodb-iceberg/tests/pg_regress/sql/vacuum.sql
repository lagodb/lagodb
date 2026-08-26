-- Local VACUUM correctness, failure recovery, and FULL routing coverage.

-- Cross-DSO routing/GUC authority and end-to-end row preservation.
DROP EXTENSION IF EXISTS lagodb_iceberg CASCADE;
CREATE EXTENSION lagodb_iceberg;
CREATE SCHEMA vacuum_correctness_test;

SELECT extension_name, worker_name
FROM lakebase.workers
WHERE extension_name = 'lagodb_iceberg'
  AND worker_name = 'iceberg_maintenance';
SELECT current_setting('lagodb_iceberg.auto_maintenance_enabled') AS auto_enabled,
       current_setting('lagodb_iceberg.auto_maintenance_naptime_s')
           AS auto_naptime_s;

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
DROP EXTENSION lagodb_iceberg CASCADE;

-- A failed rewrite must leave the old snapshot queryable, clean its attempt
-- artifacts, and permit a later VACUUM to succeed.
DROP EXTENSION IF EXISTS lagodb_iceberg CASCADE;
CREATE EXTENSION lagodb_iceberg;
CREATE EXTENSION injection_points;
\setenv PGDATABASE :DBNAME

CREATE TABLE vacuum_failure_recovery_t (id integer, payload text)
USING iceberg;
INSERT INTO vacuum_failure_recovery_t VALUES (1, 'one');
INSERT INTO vacuum_failure_recovery_t VALUES (2, 'two');
INSERT INTO vacuum_failure_recovery_t VALUES (3, 'three');
INSERT INTO vacuum_failure_recovery_t VALUES (4, 'four');
INSERT INTO vacuum_failure_recovery_t VALUES (5, 'five');
INSERT INTO vacuum_failure_recovery_t VALUES (6, 'six');

SELECT count(*) AS rows_before,
       md5(string_agg(id::text || ':' || payload, ',' ORDER BY id))
           AS digest_before
FROM vacuum_failure_recovery_t
\gset
SELECT pg_relation_filepath('vacuum_failure_recovery_t') || '_iceberg'
       AS failure_root
\gset
SELECT count(*) AS objects_before_failure
FROM pg_ls_dir(:'failure_root', true, false)
\gset

\! output="$(psql -XAtq -v ON_ERROR_STOP=1 -c "SELECT injection_points_set_local()" -c "SELECT injection_points_attach('lakebase-iceberg-vacuum-after-rewrite', 'error')" -c "VACUUM vacuum_failure_recovery_t" 2>&1)"; status=$?; if test "$status" -ne 0 && printf '%s\n' "$output" | grep -Fq 'error triggered for injection point lakebase-iceberg-vacuum-after-rewrite'; then echo "injected_vacuum_failed: true"; else echo "injected_vacuum_failed: false"; fi

SELECT count(*) = :rows_before::bigint AS rows_preserved_after_failure,
       md5(string_agg(id::text || ':' || payload, ',' ORDER BY id))
           = :'digest_before' AS content_preserved_after_failure
FROM vacuum_failure_recovery_t;
SELECT current_data_objects = 6 AS failed_attempt_not_published
FROM lakebase.table_maintenance_stats('vacuum_failure_recovery_t');
SELECT count(*) = :objects_before_failure::bigint
       AS failed_attempt_artifacts_cleaned
FROM pg_ls_dir(:'failure_root', true, false);

VACUUM vacuum_failure_recovery_t;
SELECT count(*) = :rows_before::bigint AS rows_preserved_after_retry,
       md5(string_agg(id::text || ':' || payload, ',' ORDER BY id))
           = :'digest_before' AS content_preserved_after_retry
FROM vacuum_failure_recovery_t;
SELECT current_data_objects = 1 AS retry_compacted
FROM lakebase.table_maintenance_stats('vacuum_failure_recovery_t');

DROP TABLE vacuum_failure_recovery_t;
DROP EXTENSION lagodb_iceberg CASCADE;
DROP EXTENSION injection_points;

-- FULL routing: partition expansion, mixed native/provider, ANALYZE, and
-- database-wide expansion must preserve data and process each provider leaf.
DROP EXTENSION IF EXISTS lagodb_iceberg CASCADE;
CREATE EXTENSION lagodb_iceberg;
CREATE SCHEMA vacuum_full_routing_test;

CREATE TABLE vacuum_full_routing_test.partitioned_t (id integer)
PARTITION BY RANGE (id) USING iceberg;
CREATE TABLE vacuum_full_routing_test.partitioned_t_a
PARTITION OF vacuum_full_routing_test.partitioned_t
FOR VALUES FROM (0) TO (100) USING iceberg;
CREATE TABLE vacuum_full_routing_test.partitioned_t_b
PARTITION OF vacuum_full_routing_test.partitioned_t
FOR VALUES FROM (100) TO (200) USING iceberg;

INSERT INTO vacuum_full_routing_test.partitioned_t VALUES (1);
INSERT INTO vacuum_full_routing_test.partitioned_t VALUES (2);
INSERT INTO vacuum_full_routing_test.partitioned_t VALUES (3);
INSERT INTO vacuum_full_routing_test.partitioned_t VALUES (101);
INSERT INTO vacuum_full_routing_test.partitioned_t VALUES (102);
INSERT INTO vacuum_full_routing_test.partitioned_t VALUES (103);
INSERT INTO vacuum_full_routing_test.partitioned_t VALUES (4);
INSERT INTO vacuum_full_routing_test.partitioned_t VALUES (5);
INSERT INTO vacuum_full_routing_test.partitioned_t VALUES (104);
INSERT INTO vacuum_full_routing_test.partitioned_t VALUES (105);

CREATE TABLE vacuum_full_routing_test.heap_t (id integer);
INSERT INTO vacuum_full_routing_test.heap_t VALUES (10), (20);

VACUUM (FULL, ANALYZE)
    vacuum_full_routing_test.heap_t,
    vacuum_full_routing_test.partitioned_t;
SELECT bool_and(current_data_objects = 1) AS full_analyze_compacted_each_leaf
FROM (VALUES
    ('vacuum_full_routing_test.partitioned_t_a'::regclass),
    ('vacuum_full_routing_test.partitioned_t_b'::regclass)
) AS leaves(relid)
CROSS JOIN LATERAL lakebase.table_maintenance_stats(leaves.relid);
SELECT bool_and(reltuples::bigint = 5) AS full_analyze_updated_each_leaf
FROM pg_class
WHERE oid IN (
    'vacuum_full_routing_test.partitioned_t_a'::regclass,
    'vacuum_full_routing_test.partitioned_t_b'::regclass
);

VACUUM (FULL)
    vacuum_full_routing_test.heap_t,
    vacuum_full_routing_test.partitioned_t;

SELECT array_agg(id ORDER BY id)
       = ARRAY[1, 2, 3, 4, 5, 101, 102, 103, 104, 105]
       AS partition_rows_preserved
FROM vacuum_full_routing_test.partitioned_t;
SELECT bool_and(current_data_objects = 1) AS each_leaf_compacted
FROM (VALUES
    ('vacuum_full_routing_test.partitioned_t_a'::regclass),
    ('vacuum_full_routing_test.partitioned_t_b'::regclass)
) AS leaves(relid)
CROSS JOIN LATERAL lakebase.table_maintenance_stats(leaves.relid);
SELECT array_agg(id ORDER BY id) = ARRAY[10, 20] AS heap_rows_preserved
FROM vacuum_full_routing_test.heap_t;

CREATE TABLE vacuum_full_routing_test.database_wide_t (id integer)
USING iceberg;
INSERT INTO vacuum_full_routing_test.database_wide_t VALUES (1);
INSERT INTO vacuum_full_routing_test.database_wide_t VALUES (2);
INSERT INTO vacuum_full_routing_test.database_wide_t VALUES (3);
INSERT INTO vacuum_full_routing_test.database_wide_t VALUES (4);
INSERT INTO vacuum_full_routing_test.database_wide_t VALUES (5);
INSERT INTO vacuum_full_routing_test.database_wide_t VALUES (6);

VACUUM (FULL, SKIP_LOCKED);
SELECT current_data_objects = 1 AS database_wide_provider_routed
FROM lakebase.table_maintenance_stats(
    'vacuum_full_routing_test.database_wide_t'
);
SELECT array_agg(id ORDER BY id) = ARRAY[1, 2, 3, 4, 5, 6]
       AS database_wide_rows_preserved
FROM vacuum_full_routing_test.database_wide_t;

CREATE TABLE vacuum_full_routing_test.security_t (id integer) USING iceberg;
INSERT INTO vacuum_full_routing_test.security_t VALUES (1);
INSERT INTO vacuum_full_routing_test.security_t VALUES (2);
INSERT INTO vacuum_full_routing_test.security_t VALUES (3);
INSERT INTO vacuum_full_routing_test.security_t VALUES (4);
INSERT INTO vacuum_full_routing_test.security_t VALUES (5);
INSERT INTO vacuum_full_routing_test.security_t VALUES (6);
CREATE ROLE vacuum_full_nonowner;
GRANT USAGE ON SCHEMA vacuum_full_routing_test TO vacuum_full_nonowner;
GRANT SELECT ON vacuum_full_routing_test.security_t TO vacuum_full_nonowner;
SET ROLE vacuum_full_nonowner;
SET client_min_messages = error;
VACUUM (FULL) vacuum_full_routing_test.security_t;
RESET client_min_messages;
RESET ROLE;
SELECT current_data_objects = 6 AS nonowner_did_not_rewrite
FROM lakebase.table_maintenance_stats(
    'vacuum_full_routing_test.security_t'
);
VACUUM (FULL) vacuum_full_routing_test.security_t;
SELECT current_data_objects = 1 AS owner_rewrite_succeeded
FROM lakebase.table_maintenance_stats(
    'vacuum_full_routing_test.security_t'
);
REVOKE SELECT ON vacuum_full_routing_test.security_t FROM vacuum_full_nonowner;
REVOKE USAGE ON SCHEMA vacuum_full_routing_test FROM vacuum_full_nonowner;
DROP ROLE vacuum_full_nonowner;

DROP SCHEMA vacuum_full_routing_test CASCADE;
DROP EXTENSION lagodb_iceberg CASCADE;

\set ECHO none

-- Object storage VACUUM correctness and asynchronous cleanup.
\setenv PGDATABASE :DBNAME

SELECT endpoint AS lakebase_regress_endpoint,
       bucket AS lakebase_regress_bucket,
       region AS lakebase_regress_region,
       access_key_id AS lakebase_regress_access_key_id,
       secret_access_key AS lakebase_regress_secret_access_key
FROM lakebase_regress.object_storage_fixture
\gset

SET client_min_messages = warning;
DROP EXTENSION IF EXISTS lagodb_iceberg CASCADE;
CREATE EXTENSION lagodb_iceberg;
DROP TABLESPACE IF EXISTS regress_vacuum_object_matrix;
RESET client_min_messages;

\! mkdir -p /tmp/iceberg_regress_vacuum_object_matrix
\! rm -rf /tmp/iceberg_regress_vacuum_object_matrix/*

SELECT 'regress-vacuum-matrix-' || gen_random_uuid() AS volume_name \gset
SELECT lakebase.create_storage_volume(
    :'volume_name',
    format('s3://%s', :'lakebase_regress_bucket'),
    jsonb_build_object(
        'type', 's3_access_key',
        'access_key_id', :'lakebase_regress_access_key_id',
        'secret_access_key', :'lakebase_regress_secret_access_key'
    ),
    jsonb_build_object(
        'region', :'lakebase_regress_region',
        'endpoint', :'lakebase_regress_endpoint',
        'allow_http', true
    )
) AS created_volume \gset

CREATE TABLESPACE regress_vacuum_object_matrix
LOCATION '/tmp/iceberg_regress_vacuum_object_matrix'
WITH (storage_volume = :'volume_name');

SELECT internal_volume_id AS volume_id,
       regexp_replace(effective_location, '^[^:]+://[^/]+/', '') AS effective_root
FROM lakebase.storage_volumes
WHERE storage_volume_name = :'volume_name'
\gset

\setenv LAKEBASE_REGRESS_VOLUME_ID :volume_id
\setenv LAKEBASE_REGRESS_OBJECT_NAMESPACE :lakebase_regress_bucket
\! bin/wait_for_object_store 30

CREATE TABLE object_v1_ordinary (id integer, payload text)
USING iceberg
WITH (
    "format-version" = 1,
    "history.expire.max-snapshot-age-ms" = '0',
    "history.expire.min-snapshots-to-keep" = '1'
)
TABLESPACE regress_vacuum_object_matrix;
CREATE TABLE object_v2_ordinary (id integer, payload text)
USING iceberg
WITH (
    "format-version" = 2,
    "history.expire.max-snapshot-age-ms" = '0',
    "history.expire.min-snapshots-to-keep" = '1'
)
TABLESPACE regress_vacuum_object_matrix;
CREATE TABLE object_v3_ordinary (id integer, payload text)
USING iceberg
WITH (
    "format-version" = 3,
    "history.expire.max-snapshot-age-ms" = '0',
    "history.expire.min-snapshots-to-keep" = '1'
)
TABLESPACE regress_vacuum_object_matrix;
CREATE TABLE object_v1_full (id integer, payload text)
USING iceberg
WITH (
    "format-version" = 1,
    "history.expire.max-snapshot-age-ms" = '0',
    "history.expire.min-snapshots-to-keep" = '1'
)
TABLESPACE regress_vacuum_object_matrix;
CREATE TABLE object_v2_full (id integer, payload text)
USING iceberg
WITH (
    "format-version" = 2,
    "history.expire.max-snapshot-age-ms" = '0',
    "history.expire.min-snapshots-to-keep" = '1'
)
TABLESPACE regress_vacuum_object_matrix;
CREATE TABLE object_v3_full (id integer, payload text)
USING iceberg
WITH (
    "format-version" = 3,
    "history.expire.max-snapshot-age-ms" = '0',
    "history.expire.min-snapshots-to-keep" = '1'
)
TABLESPACE regress_vacuum_object_matrix;

SELECT format(
    'INSERT INTO %I VALUES (%s, %L)',
    relation_name,
    id,
    'value-' || id
)
FROM unnest(ARRAY[
    'object_v1_ordinary', 'object_v2_ordinary', 'object_v3_ordinary',
    'object_v1_full', 'object_v2_full', 'object_v3_full'
]) AS relations(relation_name)
CROSS JOIN generate_series(1, 6) AS rows(id)
\gexec

SELECT format(
    'UPDATE %I SET payload = %L WHERE id = 2; DELETE FROM %I WHERE id = 3',
    relation_name,
    'updated-2',
    relation_name
)
FROM unnest(ARRAY[
    'object_v2_ordinary', 'object_v3_ordinary',
    'object_v2_full', 'object_v3_full'
]) AS relations(relation_name)
\gexec

CREATE TEMP TABLE object_matrix_before AS
SELECT format, count(*) AS row_count,
       md5(string_agg(id::text || ':' || payload, ',' ORDER BY id)) AS digest
FROM (
    SELECT 'v1-ordinary' AS format, * FROM object_v1_ordinary
    UNION ALL SELECT 'v2-ordinary', * FROM object_v2_ordinary
    UNION ALL SELECT 'v3-ordinary', * FROM object_v3_ordinary
    UNION ALL SELECT 'v1-full', * FROM object_v1_full
    UNION ALL SELECT 'v2-full', * FROM object_v2_full
    UNION ALL SELECT 'v3-full', * FROM object_v3_full
) AS visible_rows
GROUP BY format;

CREATE TEMP TABLE object_matrix_roots AS
WITH relations(format, relid) AS (
    VALUES
        ('v1-ordinary', 'object_v1_ordinary'::regclass),
        ('v2-ordinary', 'object_v2_ordinary'::regclass),
        ('v3-ordinary', 'object_v3_ordinary'::regclass),
        ('v1-full', 'object_v1_full'::regclass),
        ('v2-full', 'object_v2_full'::regclass),
        ('v3-full', 'object_v3_full'::regclass)
), roots AS (
    SELECT format, relid,
           :'effective_root' || '/'
           || (SELECT oid::text FROM pg_tablespace
            WHERE spcname = 'regress_vacuum_object_matrix')
           || '/' || (SELECT oid::text FROM pg_database
                      WHERE datname = current_database())
           || '/' || pg_relation_filenode(relid)::text || '_iceberg/' AS prefix
    FROM relations
)
SELECT roots.*, observed.objects AS objects_before
FROM roots
CROSS JOIN LATERAL lakebase.observe_object_tree(
    :'volume_id',
    :'lakebase_regress_bucket',
    roots.prefix
) AS observed;

VACUUM object_v1_ordinary;
VACUUM object_v2_ordinary;
VACUUM object_v3_ordinary;
VACUUM (FULL) object_v1_full;
VACUUM (FULL) object_v2_full;
VACUUM (FULL) object_v3_full;
SELECT prefix AS object_path FROM object_matrix_roots
WHERE format = 'v1-ordinary' \gset
\setenv LAKEBASE_REGRESS_OBJECT_PATH :object_path
\! bin/wait_for_maintenance_item 30
SELECT prefix AS object_path FROM object_matrix_roots
WHERE format = 'v2-ordinary' \gset
\setenv LAKEBASE_REGRESS_OBJECT_PATH :object_path
\! bin/wait_for_maintenance_item 30
SELECT prefix AS object_path FROM object_matrix_roots
WHERE format = 'v3-ordinary' \gset
\setenv LAKEBASE_REGRESS_OBJECT_PATH :object_path
\! bin/wait_for_maintenance_item 30
SELECT prefix AS object_path FROM object_matrix_roots
WHERE format = 'v1-full' \gset
\setenv LAKEBASE_REGRESS_OBJECT_PATH :object_path
\! bin/wait_for_maintenance_item 30
SELECT prefix AS object_path FROM object_matrix_roots
WHERE format = 'v2-full' \gset
\setenv LAKEBASE_REGRESS_OBJECT_PATH :object_path
\! bin/wait_for_maintenance_item 30
SELECT prefix AS object_path FROM object_matrix_roots
WHERE format = 'v3-full' \gset
\setenv LAKEBASE_REGRESS_OBJECT_PATH :object_path
\! bin/wait_for_maintenance_item 30

\set ECHO all
WITH after AS (
    SELECT format, count(*) AS row_count,
           md5(string_agg(id::text || ':' || payload, ',' ORDER BY id)) AS digest
    FROM (
        SELECT 'v1-ordinary' AS format, * FROM object_v1_ordinary
        UNION ALL SELECT 'v2-ordinary', * FROM object_v2_ordinary
        UNION ALL SELECT 'v3-ordinary', * FROM object_v3_ordinary
        UNION ALL SELECT 'v1-full', * FROM object_v1_full
        UNION ALL SELECT 'v2-full', * FROM object_v2_full
        UNION ALL SELECT 'v3-full', * FROM object_v3_full
    ) AS visible_rows
    GROUP BY format
)
SELECT count(*) = 6 AS all_cases_present,
       bool_and(before.row_count = after.row_count) AS row_count_preserved,
       bool_and(before.digest = after.digest) AS content_preserved
FROM object_matrix_before AS before
JOIN after USING (format);

SELECT roots.format,
       CASE WHEN stats.current_data_objects = 1
            THEN NULL
            ELSE stats.current_data_objects
       END AS current_data_on_failure,
       stats.current_data_objects = 1 AS one_current_data_file
FROM object_matrix_roots AS roots
CROSS JOIN LATERAL lakebase.table_maintenance_stats(roots.relid) AS stats
ORDER BY roots.format;

WITH observations AS (
    SELECT roots.format,
           roots.objects_before,
           observed.objects AS objects_after,
           stats.history_points,
           stats.current_content_objects,
           stats.retained_content_objects,
           stats.current_data_objects,
           stats.retained_data_objects
    FROM object_matrix_roots AS roots
    CROSS JOIN LATERAL lakebase.observe_object_tree(
        :'volume_id',
        :'lakebase_regress_bucket',
        roots.prefix
    ) AS observed
    CROSS JOIN LATERAL lakebase.table_maintenance_stats(roots.relid) AS stats
)
SELECT format,
       CASE WHEN objects_after < objects_before
            THEN NULL
            ELSE objects_before
       END AS failed_before,
       CASE WHEN objects_after < objects_before
            THEN NULL
            ELSE objects_after
       END AS failed_after,
       CASE WHEN objects_after < objects_before
            THEN NULL
            ELSE pg_catalog.format(
                'history=%s current_content=%s retained_content=%s '
                'current_data=%s retained_data=%s',
                history_points,
                current_content_objects,
                retained_content_objects,
                current_data_objects,
                retained_data_objects
            )
       END AS failed_state,
       objects_after < objects_before AS reclaimed
FROM observations
ORDER BY format;

\set ECHO none
DROP TABLE object_v1_ordinary, object_v2_ordinary, object_v3_ordinary,
           object_v1_full, object_v2_full, object_v3_full;
SELECT prefix AS object_path FROM object_matrix_roots
WHERE format = 'v1-ordinary' \gset
\setenv LAKEBASE_REGRESS_OBJECT_PATH :object_path
\! bin/wait_for_maintenance_item 30
SELECT prefix AS object_path FROM object_matrix_roots
WHERE format = 'v2-ordinary' \gset
\setenv LAKEBASE_REGRESS_OBJECT_PATH :object_path
\! bin/wait_for_maintenance_item 30
SELECT prefix AS object_path FROM object_matrix_roots
WHERE format = 'v3-ordinary' \gset
\setenv LAKEBASE_REGRESS_OBJECT_PATH :object_path
\! bin/wait_for_maintenance_item 30
SELECT prefix AS object_path FROM object_matrix_roots
WHERE format = 'v1-full' \gset
\setenv LAKEBASE_REGRESS_OBJECT_PATH :object_path
\! bin/wait_for_maintenance_item 30
SELECT prefix AS object_path FROM object_matrix_roots
WHERE format = 'v2-full' \gset
\setenv LAKEBASE_REGRESS_OBJECT_PATH :object_path
\! bin/wait_for_maintenance_item 30
SELECT prefix AS object_path FROM object_matrix_roots
WHERE format = 'v3-full' \gset
\setenv LAKEBASE_REGRESS_OBJECT_PATH :object_path
\! bin/wait_for_maintenance_item 30
DROP TABLESPACE regress_vacuum_object_matrix;
DROP EXTENSION lagodb_iceberg CASCADE;
\! rm -rf /tmp/iceberg_regress_vacuum_object_matrix/*
