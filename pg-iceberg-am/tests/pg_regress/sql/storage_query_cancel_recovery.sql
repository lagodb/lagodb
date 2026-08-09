\set ECHO none
-- Verify query cancellation during storage I/O and post-cancel recovery:
-- cleanup defers PostgreSQL ERROR until Drop finishes, while a foreground
-- response wait processes cancel immediately and poisons its connection.
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
CREATE EXTENSION injection_points;
DROP TABLESPACE IF EXISTS regress_storage_socket_cancel_contexts;
RESET client_min_messages;

\! mkdir -p /tmp/iceberg_regress_storage_socket_cancel_contexts
\! rm -rf /tmp/iceberg_regress_storage_socket_cancel_contexts/*

SELECT 'regress-cleanup-cancel-' || gen_random_uuid() AS volume_name
\gset
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
) AS created_volume
\gset

CREATE TABLESPACE regress_storage_socket_cancel_contexts
LOCATION '/tmp/iceberg_regress_storage_socket_cancel_contexts'
WITH (lakebase_storage_volume = :'volume_name');

SELECT internal_volume_id AS volume_id
FROM lakebase.storage_volumes
WHERE storage_volume_name = :'volume_name'
\gset
\setenv LAKEBASE_REGRESS_VOLUME_ID :volume_id
\setenv LAKEBASE_REGRESS_OBJECT_NAMESPACE :lakebase_regress_bucket
\! bin/wait_for_object_store 30

CREATE TABLE storage_socket_cancel_contexts_t (id integer)
USING iceberg
TABLESPACE regress_storage_socket_cancel_contexts;
INSERT INTO storage_socket_cancel_contexts_t VALUES (1);

\! sh bin/storage_socket_cancel_contexts

SELECT CASE WHEN count(*) = 1 THEN 'true' ELSE 'false' END
       AS storage_usable_after_cancel
FROM storage_socket_cancel_contexts_t
\gset
\echo storage_usable_after_cancel: :storage_usable_after_cancel

DROP TABLE storage_socket_cancel_contexts_t;
DROP TABLESPACE regress_storage_socket_cancel_contexts;
DROP EXTENSION pg_iceberg_am CASCADE;
DROP EXTENSION injection_points;
\! rm -rf /tmp/iceberg_regress_storage_socket_cancel_contexts/*
