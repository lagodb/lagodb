\set ECHO none

-- Storage volume registry reload and replacement.
-- The storage process starts before database-local extension workers and loads
-- its desired registry exclusively from the machine-managed volume snapshot.
SELECT count(*) = 1 AS worker_running
FROM pg_stat_activity
WHERE backend_type = 'lagodb-storage'
\gset
\echo worker_running: :worker_running

COPY (SELECT current_setting('data_directory') || '/pg_lakebase/storage.sock')
TO '/tmp/_regress_socket_path.txt';
\! test -S "$(cat /tmp/_regress_socket_path.txt)" && echo "socket_exists: true" || echo "socket_exists: false"
\! rm -f /tmp/_regress_socket_path.txt

SELECT current_setting(
           'lagodb.storage_volume_retirement_grace_period_seconds'
) = '604800' AS volume_retirement_grace_period_default
\gset
\echo volume_retirement_grace_period_default: :volume_retirement_grace_period_default

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
  AND bound_tablespace_oid IS NULL
\gset
\echo config_visible: :config_visible

SELECT pg_temp.storage_volume_wait_for_reload(true, false) AS ignored
\gset

SELECT loaded_volume_count >= :loaded_before::bigint + 1
           AND last_error IS NULL AS registry_loaded
FROM lagodb.storage_service_status
\gset
\echo registry_loaded: :registry_loaded

SELECT :'volume_name' || '-renamed' AS renamed_volume
\gset
SELECT lagodb.rename_storage_volume(:'volume_name', :'renamed_volume')
       AS ignored
\gset
SELECT count(*) = 1 AS rename_visible
FROM lagodb.storage_volumes
WHERE storage_volume_name = :'renamed_volume'
  AND internal_volume_id IS NOT NULL
\gset
\echo rename_visible: :rename_visible

UPDATE storage_volume_reload_baseline AS baseline
SET reload_generation = status.reload_generation
FROM lagodb.storage_service_status AS status;

SELECT lagodb.update_storage_volume_credentials(
    :'renamed_volume',
    '{"type":"s3_access_key","access_key_id":"regress-key",'
    '"secret_access_key":"regress-secret"}'::jsonb
) AS ignored
\gset
SELECT count(*) = 1 AS credential_update_visible
FROM lagodb.storage_volumes
WHERE storage_volume_name = :'renamed_volume'
  AND credential_type = 's3_access_key'
\gset
\echo credential_update_visible: :credential_update_visible

SELECT pg_temp.storage_volume_wait_for_reload(true, true) AS ignored
\gset

SELECT loaded_volume_count >= :loaded_before::bigint + 1
           AND last_reload_replaced >= 1
           AND last_error IS NULL AS replacement_loaded
FROM lagodb.storage_service_status
\gset
\echo replacement_loaded: :replacement_loaded

UPDATE storage_volume_reload_baseline AS baseline
SET reload_generation = status.reload_generation
FROM lagodb.storage_service_status AS status;

SELECT lagodb.reload_storage_volumes() AS ignored
\gset

SELECT pg_temp.storage_volume_wait_for_reload(false, false) AS ignored
\gset

SELECT count(*) = 1 AS worker_still_running
FROM pg_stat_activity
WHERE backend_type = 'lagodb-storage'
\gset
\echo worker_still_running: :worker_still_running

DROP FUNCTION pg_temp.storage_volume_wait_for_reload(boolean, boolean);
DROP TABLE storage_volume_reload_baseline;

-- Tablespace binding and DDL rules.
\set ECHO none

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

-- Rename is allowed; every SET/RESET is rejected for a Lakebase tablespace.
ALTER TABLESPACE iceberg_guard_dist RENAME TO iceberg_guard_dist_renamed;
SELECT count(*) = 1 AS rename_allowed
FROM lagodb.storage_volumes AS volume
JOIN pg_tablespace AS tablespace
  ON tablespace.oid = volume.bound_tablespace_oid
WHERE volume.storage_volume_name = :'volume_name'
  AND tablespace.spcname = 'iceberg_guard_dist_renamed'
  AND EXISTS (
      SELECT 1 FROM unnest(tablespace.spcoptions) AS option
      WHERE option LIKE 'lakebase_volume_id=%'
  )
\gset
\echo rename_allowed: :rename_allowed

-- Public, internal and native options are all immutable after binding.
CREATE TEMP TABLE guard_results (
    public_alter_rejected boolean,
    internal_alter_rejected boolean,
    native_alter_rejected boolean,
    native_reset_rejected boolean
);
DO $guard$
DECLARE
    public_rejected boolean := false;
    internal_rejected boolean := false;
    native_rejected boolean := false;
    reset_rejected boolean := false;
BEGIN
    BEGIN
        EXECUTE 'ALTER TABLESPACE iceberg_guard_dist_renamed SET '
                '(storage_volume = ''another-volume'')';
    EXCEPTION WHEN feature_not_supported THEN
        public_rejected := true;
    END;
    BEGIN
        EXECUTE 'ALTER TABLESPACE iceberg_guard_dist_renamed SET '
                '(lakebase_volume_id = 999)';
    EXCEPTION WHEN feature_not_supported THEN
        internal_rejected := true;
    END;
    BEGIN
        EXECUTE 'ALTER TABLESPACE iceberg_guard_dist_renamed SET '
                '(seq_page_cost = 1.25)';
    EXCEPTION WHEN feature_not_supported THEN
        native_rejected := true;
    END;
    BEGIN
        EXECUTE 'ALTER TABLESPACE iceberg_guard_dist_renamed RESET '
                '(seq_page_cost)';
    EXCEPTION WHEN feature_not_supported THEN
        reset_rejected := true;
    END;
    INSERT INTO guard_results VALUES (
        public_rejected,
        internal_rejected,
        native_rejected,
        reset_rejected
    );
END
$guard$;
SELECT public_alter_rejected AS public_binding_alter_rejected,
       internal_alter_rejected AS internal_binding_alter_rejected,
       native_alter_rejected,
       native_reset_rejected
FROM guard_results
\gset
\echo public_binding_alter_rejected: :public_binding_alter_rejected
\echo internal_binding_alter_rejected: :internal_binding_alter_rejected
\echo native_alter_rejected: :native_alter_rejected
\echo native_reset_rejected: :native_reset_rejected

-- Native tablespaces continue to use PostgreSQL's SET/RESET path.
ALTER TABLESPACE iceberg_guard_native SET (seq_page_cost = 1.25);
ALTER TABLESPACE iceberg_guard_native RESET (seq_page_cost);
SELECT count(*) = 1 AS internal_id_unchanged
FROM pg_tablespace
WHERE spcname = 'iceberg_guard_dist_renamed'
  AND array_length(spcoptions, 1) = 1
  AND EXISTS (
      SELECT 1 FROM unnest(spcoptions) AS option
      WHERE option LIKE 'lakebase_volume_id=%'
  )
\gset
\echo internal_id_unchanged: :internal_id_unchanged

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
            WHERE option LIKE 'lakebase_volume_id=%') = 1
       AND NOT EXISTS (
           SELECT 1 FROM unnest(spcoptions) AS option
           WHERE option LIKE 'storage_volume=%'
       ) AS internal_id_only
FROM pg_tablespace
WHERE spcname = 'iceberg_volume_test'
\gset
\echo internal_id_only: :internal_id_only

SELECT count(*) = 1 AS binding_visible
FROM lagodb.storage_volumes AS volume
JOIN pg_tablespace AS tablespace
  ON tablespace.oid = volume.bound_tablespace_oid
WHERE volume.storage_volume_name = :'volume_name'
  AND tablespace.spcname = 'iceberg_volume_test'
\gset
\echo binding_visible: :binding_visible

DROP TABLESPACE iceberg_volume_test;
SELECT count(*) = 1 AS retirement_visible_after_drop
FROM lagodb.storage_volumes
WHERE storage_volume_name = :'volume_name'
  AND lifecycle = 'retiring'
  AND bound_tablespace_oid IS NULL
  AND retired_tablespace_oid IS NOT NULL
  AND binding_present = false
\gset
\echo retirement_visible_after_drop: :retirement_visible_after_drop

-- Storage I/O cancellation and recovery.
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
DROP EXTENSION IF EXISTS lagodb_iceberg CASCADE;
CREATE EXTENSION lagodb_iceberg;
CREATE EXTENSION injection_points;
DROP TABLESPACE IF EXISTS regress_storage_socket_cancel_contexts;
RESET client_min_messages;

\! mkdir -p /tmp/iceberg_regress_storage_socket_cancel_contexts
\! rm -rf /tmp/iceberg_regress_storage_socket_cancel_contexts/*

SELECT 'regress-cleanup-cancel-' || gen_random_uuid() AS volume_name
\gset
SELECT lagodb.create_storage_volume(
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
WITH (storage_volume = :'volume_name');

SELECT internal_volume_id AS volume_id
FROM lagodb.storage_volumes
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
DROP EXTENSION lagodb_iceberg CASCADE;
DROP EXTENSION injection_points;
\! rm -rf /tmp/iceberg_regress_storage_socket_cancel_contexts/*
