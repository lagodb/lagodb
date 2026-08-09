\set ECHO none
\setenv PGDATABASE :DBNAME

SELECT endpoint AS lakebase_regress_endpoint,
       bucket AS lakebase_regress_bucket,
       region AS lakebase_regress_region,
       access_key_id AS lakebase_regress_access_key_id,
       secret_access_key AS lakebase_regress_secret_access_key
FROM lakebase_regress.object_storage_fixture
\gset

SET client_min_messages = warning;
DROP EXTENSION IF EXISTS pg_iceberg_am CASCADE;
CREATE EXTENSION pg_iceberg_am;
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
WITH (lakebase_storage_volume = :'volume_name');

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
DROP EXTENSION pg_iceberg_am CASCADE;
\! rm -rf /tmp/iceberg_regress_vacuum_object_matrix/*
