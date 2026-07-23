\set ECHO none
-- End-to-end asynchronous remote DROP. 000_object_storage_setup provisions
-- the MinIO fixture and records its connection details in the test database.

-- pg_regress unsets PGDATABASE and passes the database only via psql -d. Export
-- it so the `\!` helper scripts connect to this regress database. `\!` does not
-- interpolate psql variables (OT_WHOLE_LINE); `\setenv` (OT_NORMAL) does.
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
DROP TABLE IF EXISTS maintenance_remote_drop;
DROP TABLE IF EXISTS maintenance_remote_rollback;
DROP TABLESPACE IF EXISTS regress_object;
RESET client_min_messages;

\! mkdir -p /tmp/iceberg_regress_object
\! rm -rf /tmp/iceberg_regress_object/*

SELECT 'regress-object-' || gen_random_uuid() AS volume_name \gset
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

CREATE TABLESPACE regress_object
LOCATION '/tmp/iceberg_regress_object'
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
SELECT list_succeeded
       AND write_succeeded
       AND read_succeeded
       AND delete_succeeded
       AND succeeded
       AND error IS NULL AS storage_volume_probe_ok
FROM lakebase.probe_storage_volume(:'volume_name');

CREATE TABLE maintenance_remote_drop (id integer)
USING iceberg TABLESPACE regress_object;
INSERT INTO maintenance_remote_drop
SELECT generate_series(1, 1000);

SELECT :'store_id' AS store_id,
       :'lakebase_regress_bucket' AS object_namespace,
       :'effective_root' || '/'
       || (SELECT oid::text FROM pg_tablespace WHERE spcname = 'regress_object')
       || '/' || (SELECT oid::text FROM pg_database WHERE datname = current_database())
       || '/' || pg_relation_filenode('maintenance_remote_drop')::text
       || '_iceberg/' AS object_path
\gset

\setenv LAKEBASE_REGRESS_STORE_ID :store_id
\setenv LAKEBASE_REGRESS_OBJECT_NAMESPACE :object_namespace
\setenv LAKEBASE_REGRESS_OBJECT_PATH :object_path
SELECT objects > 0 AS tree_exists_before_drop
FROM lakebase.observe_object_tree(
    :'store_id', :'object_namespace', :'object_path'
);

DROP TABLE maintenance_remote_drop;

\! bin/wait_for_maintenance_item 30

SELECT objects = 0 AS tree_empty_after_drop
FROM lakebase.observe_object_tree(
    :'store_id', :'object_namespace', :'object_path'
);
SELECT count(*) AS relation_gone
FROM pg_class WHERE relname = 'maintenance_remote_drop';
SELECT process_state || '/' || dispatch_state AS maintenance_worker_state
FROM lakebase.worker_runtime_status
WHERE database_oid = (SELECT oid FROM pg_catalog.pg_database
                      WHERE datname = pg_catalog.current_database())
  AND extension_name = 'pg_lakebase_runtime' AND worker_name = 'maintenance'
\gset
\echo maintenance_worker_state: :maintenance_worker_state

CREATE TABLE maintenance_remote_rollback (id integer)
USING iceberg TABLESPACE regress_object;
INSERT INTO maintenance_remote_rollback VALUES (1), (2);
BEGIN;
DROP TABLE maintenance_remote_rollback;
ROLLBACK;
SELECT array_agg(id ORDER BY id) AS rows_after_drop_rollback
FROM maintenance_remote_rollback;

SELECT :'store_id' AS store_id,
       :'lakebase_regress_bucket' AS object_namespace,
       :'effective_root' || '/'
       || (SELECT oid::text FROM pg_tablespace WHERE spcname = 'regress_object')
       || '/' || (SELECT oid::text FROM pg_database WHERE datname = current_database())
       || '/' || pg_relation_filenode('maintenance_remote_rollback')::text
       || '_iceberg/' AS object_path
\gset
\setenv LAKEBASE_REGRESS_STORE_ID :store_id
\setenv LAKEBASE_REGRESS_OBJECT_NAMESPACE :object_namespace
\setenv LAKEBASE_REGRESS_OBJECT_PATH :object_path
DROP TABLE maintenance_remote_rollback;

\! bin/wait_for_maintenance_item 30

SELECT objects = 0 AS rollback_tree_empty_after_cleanup
FROM lakebase.observe_object_tree(
    :'store_id', :'object_namespace', :'object_path'
);

DROP TABLESPACE regress_object;
\! rm -rf /tmp/iceberg_regress_object/*
