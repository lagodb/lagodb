
-- Storage volume registry reload and replacement.
-- The storage process starts before database-local extension workers and loads
-- its desired registry exclusively from the machine-managed volume snapshot.
SELECT count(*) = 1 AS worker_running
FROM pg_stat_activity
WHERE backend_type = 'lagodb-storage';

SELECT current_setting('data_directory') || '/lagodb/storage.sock'
       AS lagodb_regress_storage_socket
\gset
\setenv LAGODB_REGRESS_STORAGE_SOCKET :lagodb_regress_storage_socket
\! test -S "$LAGODB_REGRESS_STORAGE_SOCKET" && echo "socket_exists: true" || echo "socket_exists: false"
\setenv LAGODB_REGRESS_STORAGE_SOCKET

SELECT current_setting(
           'lagodb.storage_volume_retirement_grace_period_seconds'
) = '604800' AS volume_retirement_grace_period_default;

SELECT loaded_volume_count AS loaded_before
FROM lagodb.storage_service_status
\gset

CREATE TEMP TABLE storage_volume_reload_baseline AS
SELECT reload_generation,
       loaded_volume_count::bigint AS initial_loaded_volume_count
FROM lagodb.storage_service_status;

CREATE FUNCTION pg_temp.storage_volume_wait_for_reload(
    require_loaded boolean,
    require_replacement boolean
) RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    deadline timestamptz := clock_timestamp() + interval '30 seconds';
    current_status record;
BEGIN
    LOOP
        SELECT *
        INTO current_status
        FROM lagodb.storage_service_status;

        EXIT WHEN current_status.reload_generation > (
                      SELECT reload_generation
                      FROM storage_volume_reload_baseline
                  )
                  AND (
                      NOT require_loaded
                      OR current_status.loaded_volume_count >= (
                          SELECT initial_loaded_volume_count + 1
                          FROM storage_volume_reload_baseline
                      )
                  )
                  AND (
                      NOT require_replacement
                      OR current_status.last_reload_replaced >= 1
                  )
                  AND current_status.last_error IS NULL;

        IF clock_timestamp() >= deadline THEN
            RAISE EXCEPTION 'storage volume registry reload timed out'
                USING DETAIL = format(
                    'storage_service_status=%s',
                    row_to_json(current_status)
                );
        END IF;

        PERFORM pg_sleep(0.1);
    END LOOP;
END
$$;

SELECT 'regress-bgw-' || gen_random_uuid() AS volume_name
\gset
SELECT lagodb.create_storage_volume(
    :'volume_name',
    's3://storage-bgworker-regress/root',
    '{"type":"anonymous"}'::jsonb,
    '{"region":"us-east-1"}'::jsonb
) AS created_name
\gset

SELECT count(*) = 1 AS config_visible
FROM lagodb.storage_volumes
WHERE storage_volume_name = :'volume_name'
  AND provider = 's3'
  AND credential_type = 'anonymous'
  AND bound_tablespace_oid IS NULL;

SELECT pg_temp.storage_volume_wait_for_reload(true, false);

SELECT loaded_volume_count >= :loaded_before::bigint + 1
           AND last_error IS NULL AS registry_loaded
FROM lagodb.storage_service_status;

SELECT :'volume_name' || '-renamed' AS renamed_volume
\gset
SELECT lagodb.rename_storage_volume(:'volume_name', :'renamed_volume');
SELECT count(*) = 1 AS rename_visible
FROM lagodb.storage_volumes
WHERE storage_volume_name = :'renamed_volume'
  AND internal_volume_id IS NOT NULL;

UPDATE storage_volume_reload_baseline AS baseline
SET reload_generation = status.reload_generation
FROM lagodb.storage_service_status AS status;

SELECT lagodb.update_storage_volume_credentials(
    :'renamed_volume',
    '{"type":"s3_access_key","access_key_id":"regress-key",'
    '"secret_access_key":"regress-secret"}'::jsonb
);
SELECT count(*) = 1 AS credential_update_visible
FROM lagodb.storage_volumes
WHERE storage_volume_name = :'renamed_volume'
  AND credential_type = 's3_access_key';

SELECT pg_temp.storage_volume_wait_for_reload(true, true);

SELECT loaded_volume_count >= :loaded_before::bigint + 1
           AND last_reload_replaced >= 1
           AND last_error IS NULL AS replacement_loaded
FROM lagodb.storage_service_status;

UPDATE storage_volume_reload_baseline AS baseline
SET reload_generation = status.reload_generation
FROM lagodb.storage_service_status AS status;

SELECT lagodb.reload_storage_volumes();

SELECT pg_temp.storage_volume_wait_for_reload(false, false);

SELECT count(*) = 1 AS worker_still_running
FROM pg_stat_activity
WHERE backend_type = 'lagodb-storage';

DROP FUNCTION pg_temp.storage_volume_wait_for_reload(boolean, boolean);
DROP TABLE storage_volume_reload_baseline;

-- Tablespace binding and DDL rules.

-- Storage-volume-backed tablespace DDL rules.
\! mkdir -p /tmp/lagodb_iceberg_regress_guard_dist
\! rm -rf /tmp/lagodb_iceberg_regress_guard_dist/*
\! mkdir -p /tmp/lagodb_iceberg_regress_guard_native
\! rm -rf /tmp/lagodb_iceberg_regress_guard_native/*

SET client_min_messages = warning;
DROP TABLESPACE IF EXISTS iceberg_guard_dist;
DROP TABLESPACE IF EXISTS iceberg_guard_dist_renamed;
DROP TABLESPACE IF EXISTS iceberg_guard_native;
RESET client_min_messages;

SELECT 'regress-guard-' || gen_random_uuid() AS volume_name
\gset
SELECT lagodb.create_storage_volume(
    :'volume_name',
    's3://tablespace-guard-regress/root',
    '{"type":"anonymous"}'::jsonb,
    '{"region":"us-east-1"}'::jsonb
) AS ignored
\gset

CREATE TABLESPACE iceberg_guard_dist
LOCATION '/tmp/lagodb_iceberg_regress_guard_dist'
WITH (storage_volume = :'volume_name');
CREATE TABLESPACE iceberg_guard_native
LOCATION '/tmp/lagodb_iceberg_regress_guard_native';

-- Rename is allowed; every SET/RESET is rejected for a LagoDB tablespace.
ALTER TABLESPACE iceberg_guard_dist RENAME TO iceberg_guard_dist_renamed;
SELECT count(*) = 1 AS rename_allowed
FROM lagodb.storage_volumes AS volume
JOIN pg_tablespace AS tablespace
  ON tablespace.oid = volume.bound_tablespace_oid
WHERE volume.storage_volume_name = :'volume_name'
  AND tablespace.spcname = 'iceberg_guard_dist_renamed'
  AND EXISTS (
      SELECT 1 FROM unnest(tablespace.spcoptions) AS option
      WHERE option LIKE 'lagodb_volume_id=%'
  );

-- Binding options are immutable, and native relations cannot enter a volume.
CREATE TABLE storage_volume_move_candidate (id integer);
-- Keep each rejected command in its own rolled-back child transaction.
\set VERBOSITY terse
BEGIN;
SAVEPOINT volume_guard;
ALTER TABLESPACE iceberg_guard_dist_renamed SET (storage_volume = 'another-volume');
\echo public_binding_alter_sqlstate: :SQLSTATE
ROLLBACK TO SAVEPOINT volume_guard;
RELEASE SAVEPOINT volume_guard;
SAVEPOINT volume_guard;
ALTER TABLESPACE iceberg_guard_dist_renamed SET (lagodb_volume_id = 999);
\echo internal_binding_alter_sqlstate: :SQLSTATE
ROLLBACK TO SAVEPOINT volume_guard;
RELEASE SAVEPOINT volume_guard;
SAVEPOINT volume_guard;
ALTER TABLESPACE iceberg_guard_dist_renamed SET (seq_page_cost = 1.25);
\echo native_alter_sqlstate: :SQLSTATE
ROLLBACK TO SAVEPOINT volume_guard;
RELEASE SAVEPOINT volume_guard;
SAVEPOINT volume_guard;
ALTER TABLESPACE iceberg_guard_dist_renamed RESET (seq_page_cost);
\echo native_reset_sqlstate: :SQLSTATE
ROLLBACK TO SAVEPOINT volume_guard;
RELEASE SAVEPOINT volume_guard;
SAVEPOINT volume_guard;
CREATE TABLE storage_volume_local_table (id integer) TABLESPACE iceberg_guard_dist_renamed;
\echo volume_local_table_sqlstate: :SQLSTATE
ROLLBACK TO SAVEPOINT volume_guard;
RELEASE SAVEPOINT volume_guard;
SAVEPOINT volume_guard;
CREATE TABLE storage_volume_local_ctas TABLESPACE iceberg_guard_dist_renamed AS SELECT 1 AS id;
\echo volume_local_ctas_sqlstate: :SQLSTATE
ROLLBACK TO SAVEPOINT volume_guard;
RELEASE SAVEPOINT volume_guard;
SAVEPOINT volume_guard;
ALTER TABLE storage_volume_move_candidate SET TABLESPACE iceberg_guard_dist_renamed;
\echo volume_existing_table_move_sqlstate: :SQLSTATE
ROLLBACK TO SAVEPOINT volume_guard;
RELEASE SAVEPOINT volume_guard;
COMMIT;
\set VERBOSITY default
SET client_min_messages = warning;
DROP TABLE IF EXISTS storage_volume_local_table;
DROP TABLE IF EXISTS storage_volume_local_ctas;
RESET client_min_messages;
DROP TABLE storage_volume_move_candidate;

-- Native tablespaces continue to use PostgreSQL's SET/RESET path.
ALTER TABLESPACE iceberg_guard_native SET (seq_page_cost = 1.25);
ALTER TABLESPACE iceberg_guard_native RESET (seq_page_cost);
SELECT count(*) = 1 AS internal_id_unchanged
FROM pg_tablespace
WHERE spcname = 'iceberg_guard_dist_renamed'
  AND array_length(spcoptions, 1) = 1
  AND EXISTS (
      SELECT 1 FROM unnest(spcoptions) AS option
      WHERE option LIKE 'lagodb_volume_id=%'
  );

DROP TABLESPACE iceberg_guard_dist_renamed;
DROP TABLESPACE iceberg_guard_native;

-- Storage-volume binding metadata and drop lifecycle.
\! mkdir -p /tmp/lagodb_iceberg_regress_spc
\! rm -rf /tmp/lagodb_iceberg_regress_spc/*
SET client_min_messages = warning;
DROP TABLESPACE IF EXISTS iceberg_volume_test;
RESET client_min_messages;

SELECT 'regress-tablespace-' || gen_random_uuid() AS volume_name
\gset
SELECT lagodb.create_storage_volume(
    :'volume_name',
    's3://tablespace-option-regress/root',
    '{"type":"anonymous"}'::jsonb,
    '{"region":"us-east-1"}'::jsonb
) AS ignored
\gset

CREATE TABLESPACE iceberg_volume_test
LOCATION '/tmp/lagodb_iceberg_regress_spc'
WITH (storage_volume = :'volume_name');

SELECT array_length(spcoptions, 1) = 1
       AND (SELECT count(*) FROM unnest(spcoptions) AS option
            WHERE option LIKE 'lagodb_volume_id=%') = 1
       AND NOT EXISTS (
           SELECT 1 FROM unnest(spcoptions) AS option
           WHERE option LIKE 'storage_volume=%'
       ) AS internal_id_only
FROM pg_tablespace
WHERE spcname = 'iceberg_volume_test';

SELECT count(*) = 1 AS binding_visible
FROM lagodb.storage_volumes AS volume
JOIN pg_tablespace AS tablespace
  ON tablespace.oid = volume.bound_tablespace_oid
WHERE volume.storage_volume_name = :'volume_name'
  AND tablespace.spcname = 'iceberg_volume_test';

DROP TABLESPACE iceberg_volume_test;
SELECT count(*) = 1 AS retirement_visible_after_drop
FROM lagodb.storage_volumes
WHERE storage_volume_name = :'volume_name'
  AND lifecycle = 'retiring'
  AND bound_tablespace_oid IS NULL
  AND retired_tablespace_oid IS NOT NULL
  AND binding_present = false;

-- Storage I/O cancellation and recovery.

-- Verify query cancellation during storage I/O and post-cancel recovery:
-- cleanup defers PostgreSQL ERROR until Drop finishes, while a foreground
-- response wait processes cancel immediately and poisons its connection.
\setenv PGDATABASE :DBNAME

SELECT endpoint AS lagodb_regress_endpoint,
       bucket AS lagodb_regress_bucket,
       region AS lagodb_regress_region,
       access_key_id AS lagodb_regress_access_key_id,
       secret_access_key AS lagodb_regress_secret_access_key
FROM lagodb_regress.object_storage_fixture
\gset

SET client_min_messages = warning;
DROP EXTENSION IF EXISTS lagodb_iceberg CASCADE;
RESET client_min_messages;
CREATE EXTENSION lagodb_iceberg;
CREATE EXTENSION injection_points;
SET client_min_messages = warning;
DROP TABLESPACE IF EXISTS regress_storage_socket_cancel_contexts;
RESET client_min_messages;

\! mkdir -p /tmp/iceberg_regress_storage_socket_cancel_contexts
\! rm -rf /tmp/iceberg_regress_storage_socket_cancel_contexts/*

SELECT 'regress-cleanup-cancel-' || gen_random_uuid() AS volume_name
\gset
SELECT lagodb.create_storage_volume(
    :'volume_name',
    format('s3://%s', :'lagodb_regress_bucket'),
    jsonb_build_object(
        'type', 's3_access_key',
        'access_key_id', :'lagodb_regress_access_key_id',
        'secret_access_key', :'lagodb_regress_secret_access_key'
    ),
    jsonb_build_object(
        'region', :'lagodb_regress_region',
        'endpoint', :'lagodb_regress_endpoint',
        'allow_http', true
    )
) AS created_volume
\gset

CREATE TABLESPACE regress_storage_socket_cancel_contexts
LOCATION '/tmp/iceberg_regress_storage_socket_cancel_contexts'
WITH (storage_volume = :'volume_name');

SELECT internal_volume_id AS volume_id
FROM lagodb.storage_volumes
WHERE storage_volume_name = :'volume_name'
\gset
\setenv LAGODB_REGRESS_VOLUME_ID :volume_id
\setenv LAGODB_REGRESS_OBJECT_NAMESPACE :lagodb_regress_bucket
\! bin/wait_for_object_store 30

CREATE TABLE storage_socket_cancel_contexts_t (id integer)
USING iceberg
TABLESPACE regress_storage_socket_cancel_contexts;
SELECT count(*) = 1 AS object_tablespace_dependency_visible
FROM pg_catalog.pg_shdepend AS dependency
WHERE dependency.dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
  AND dependency.classid = 'pg_catalog.pg_class'::regclass
  AND dependency.objid = 'storage_socket_cancel_contexts_t'::regclass
  AND dependency.refclassid = 'pg_catalog.pg_tablespace'::regclass
  AND dependency.refobjid = (
      SELECT oid FROM pg_tablespace
      WHERE spcname = 'regress_storage_socket_cancel_contexts'
  )
  AND dependency.deptype = 't';
INSERT INTO storage_socket_cancel_contexts_t VALUES (1);

\! sh bin/storage_socket_cancel_contexts

SELECT CASE WHEN count(*) = 1 THEN 'true' ELSE 'false' END
       AS storage_usable_after_cancel
FROM storage_socket_cancel_contexts_t;

DROP TABLE storage_socket_cancel_contexts_t;
DROP TABLESPACE regress_storage_socket_cancel_contexts;
DROP EXTENSION lagodb_iceberg CASCADE;
DROP EXTENSION injection_points;
\! rm -rf /tmp/iceberg_regress_storage_socket_cancel_contexts/*
