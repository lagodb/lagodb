\set ECHO none
-- Object-backed VACUUM FULL must preserve visible data while asynchronously
-- reclaiming superseded data/delete/metadata objects.
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
DROP TABLESPACE IF EXISTS regress_vacuum_object;
RESET client_min_messages;

\! mkdir -p /tmp/iceberg_regress_vacuum_object
\! rm -rf /tmp/iceberg_regress_vacuum_object/*

SELECT 'regress-vacuum-' || gen_random_uuid() AS volume_name \gset
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

CREATE TABLESPACE regress_vacuum_object
LOCATION '/tmp/iceberg_regress_vacuum_object'
WITH (lakebase_storage_volume = :'volume_name');

SELECT internal_store_id AS store_id,
       regexp_replace(effective_location, '^[^:]+://[^/]+/', '') AS effective_root
FROM lakebase.storage_volumes
WHERE storage_volume_name = :'volume_name'
\gset

\setenv LAKEBASE_REGRESS_STORE_ID :store_id
\setenv LAKEBASE_REGRESS_OBJECT_NAMESPACE :lakebase_regress_bucket
\! bin/wait_for_object_store 30

\set ECHO all
CREATE TABLE vacuum_object_t (id integer, payload text)
USING iceberg
WITH (
    "format-version" = 3,
    "history.expire.max-snapshot-age-ms" = '0',
    "history.expire.min-snapshots-to-keep" = '1'
)
TABLESPACE regress_vacuum_object;
INSERT INTO vacuum_object_t VALUES (1, 'one');
INSERT INTO vacuum_object_t VALUES (2, 'two');
INSERT INTO vacuum_object_t VALUES (3, 'three');
INSERT INTO vacuum_object_t VALUES (4, 'four');
INSERT INTO vacuum_object_t VALUES (5, 'five');
INSERT INTO vacuum_object_t VALUES (6, 'six');
UPDATE vacuum_object_t SET payload = 'two-updated' WHERE id = 2;
DELETE FROM vacuum_object_t WHERE id = 3;

SELECT count(*) AS rows_before,
       md5(string_agg(id::text || ':' || payload, ',' ORDER BY id)) AS digest_before
FROM vacuum_object_t
\gset

SELECT :'store_id' AS store_id,
       :'lakebase_regress_bucket' AS object_namespace,
       :'effective_root' || '/'
       || (SELECT oid::text FROM pg_tablespace
        WHERE spcname = 'regress_vacuum_object')
       || '/' || (SELECT oid::text FROM pg_database
                  WHERE datname = current_database())
       || '/' || pg_relation_filenode('vacuum_object_t')::text
       || '_iceberg/' AS object_path
\gset
\setenv LAKEBASE_REGRESS_STORE_ID :store_id
\setenv LAKEBASE_REGRESS_OBJECT_NAMESPACE :object_namespace
\setenv LAKEBASE_REGRESS_OBJECT_PATH :object_path

SELECT objects AS objects_before
FROM lakebase.observe_object_tree(
    :'store_id', :'object_namespace', :'object_path'
)
\gset

VACUUM (FULL) vacuum_object_t;
\! bin/wait_for_maintenance_item 30

SELECT count(*) = :rows_before::bigint AS row_count_preserved,
       md5(string_agg(id::text || ':' || payload, ',' ORDER BY id))
           = :'digest_before' AS content_preserved
FROM vacuum_object_t;
SELECT current_data_objects = 1 AS one_current_data_file
FROM lakebase.table_maintenance_stats('vacuum_object_t');
SELECT objects < :objects_before::bigint AS physical_objects_reclaimed
FROM lakebase.observe_object_tree(
    :'store_id', :'object_namespace', :'object_path'
);

DROP TABLE vacuum_object_t;
\! bin/wait_for_maintenance_item 30
DROP TABLESPACE regress_vacuum_object;
DROP EXTENSION pg_iceberg_am CASCADE;
\! rm -rf /tmp/iceberg_regress_vacuum_object/*
