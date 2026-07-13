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

\! mkdir -p /tmp/iceberg_regress_object
\! rm -rf /tmp/iceberg_regress_object/*

SET client_min_messages = warning;
DROP EXTENSION IF EXISTS pg_iceberg_am CASCADE;
CREATE EXTENSION pg_iceberg_am;
DROP TABLE IF EXISTS maintenance_remote_drop;
DROP TABLE IF EXISTS maintenance_remote_rollback;
DROP TABLESPACE IF EXISTS regress_object;
RESET client_min_messages;

CREATE TABLESPACE regress_object
LOCATION '/tmp/iceberg_regress_object'
WITH (
    protocol = 's3',
    bucket = :'lakebase_regress_bucket',
    region = :'lakebase_regress_region',
    endpoint = :'lakebase_regress_endpoint',
    allow_http = true,
    access_key_id = :'lakebase_regress_access_key_id',
    secret_access_key = :'lakebase_regress_secret_access_key'
);

\setenv LAKEBASE_REGRESS_STORE_ID regress_object
\setenv LAKEBASE_REGRESS_OBJECT_NAMESPACE :lakebase_regress_bucket
\! bin/wait_for_object_store 30

CREATE TABLE maintenance_remote_drop (id integer)
USING iceberg TABLESPACE regress_object;
INSERT INTO maintenance_remote_drop
SELECT generate_series(1, 1000);

SELECT 'regress_object' AS store_id,
       :'lakebase_regress_bucket' AS object_namespace,
       (SELECT oid::text FROM pg_tablespace WHERE spcname = 'regress_object')
       || '/' || (SELECT oid::text FROM pg_database WHERE datname = current_database())
       || '/' || pg_relation_filenode('maintenance_remote_drop')::text
       || '_iceberg/' AS object_path
\gset

\setenv LAKEBASE_REGRESS_STORE_ID :store_id
\setenv LAKEBASE_REGRESS_OBJECT_NAMESPACE :object_namespace
\setenv LAKEBASE_REGRESS_OBJECT_PATH :object_path
SELECT NOT iceberg.object_tree_is_empty(
    :'store_id', :'object_namespace', :'object_path'
) AS tree_exists_before_drop;

DROP TABLE maintenance_remote_drop;

\! bin/wait_for_maintenance_item 30

SELECT iceberg.object_tree_is_empty(
    :'store_id', :'object_namespace', :'object_path'
) AS tree_empty_after_drop;
SELECT count(*) AS relation_gone
FROM pg_class WHERE relname = 'maintenance_remote_drop';
SELECT state AS maintenance_worker_state
FROM lakebase.worker_runtime_status
WHERE extension_name = 'pg_lakebase_runtime' AND worker_name = 'maintenance'
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

SELECT 'regress_object' AS store_id,
       :'lakebase_regress_bucket' AS object_namespace,
       (SELECT oid::text FROM pg_tablespace WHERE spcname = 'regress_object')
       || '/' || (SELECT oid::text FROM pg_database WHERE datname = current_database())
       || '/' || pg_relation_filenode('maintenance_remote_rollback')::text
       || '_iceberg/' AS object_path
\gset
\setenv LAKEBASE_REGRESS_STORE_ID :store_id
\setenv LAKEBASE_REGRESS_OBJECT_NAMESPACE :object_namespace
\setenv LAKEBASE_REGRESS_OBJECT_PATH :object_path
DROP TABLE maintenance_remote_rollback;

\! bin/wait_for_maintenance_item 30

SELECT iceberg.object_tree_is_empty(
    :'store_id', :'object_namespace', :'object_path'
) AS rollback_tree_empty_after_cleanup;

DROP TABLESPACE regress_object;
\! rm -rf /tmp/iceberg_regress_object/*
