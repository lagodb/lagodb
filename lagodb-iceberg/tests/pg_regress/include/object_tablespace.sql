-- Caller supplies object_tablespace. This include reads the shared fixture and
-- exports LAGODB_REGRESS_TABLESPACE_PATH for teardown by the caller.
SELECT endpoint AS object_endpoint, bucket AS object_bucket,
       region AS object_region,
       access_key_id AS object_access_key,
       secret_access_key AS object_secret_key
FROM lagodb_regress.object_storage_fixture
\gset

SET client_min_messages = warning;
DROP TABLESPACE IF EXISTS :"object_tablespace";
RESET client_min_messages;
\set object_tablespace_path /tmp/iceberg_ :object_tablespace
\setenv LAGODB_REGRESS_TABLESPACE_PATH :object_tablespace_path
\! mkdir -p "$LAGODB_REGRESS_TABLESPACE_PATH"
\! rm -rf "$LAGODB_REGRESS_TABLESPACE_PATH"/*
CREATE TABLESPACE :"object_tablespace" LOCATION :'object_tablespace_path';
SELECT replace(:'object_tablespace', '_', '-') || '-' || gen_random_uuid()
       AS object_volume
\gset
SELECT lagodb.create_storage_volume(
    :'object_volume', :'object_tablespace', format('s3://%s', :'object_bucket'),
    jsonb_build_object('type', 's3_access_key',
                       'access_key_id', :'object_access_key',
                       'secret_access_key', :'object_secret_key'),
    jsonb_build_object('region', :'object_region', 'endpoint', :'object_endpoint',
                       'allow_http', true)
) AS object_created_volume
\gset
SELECT internal_volume_id AS object_volume_id
FROM lagodb.storage_volumes WHERE storage_volume_name = :'object_volume'
\gset
\setenv LAGODB_REGRESS_VOLUME_ID :object_volume_id
\setenv LAGODB_REGRESS_OBJECT_NAMESPACE :object_bucket
\! bin/wait_for_object_store 30
