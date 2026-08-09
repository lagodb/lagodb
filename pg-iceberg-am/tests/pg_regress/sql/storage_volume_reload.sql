\set ECHO none

-- The storage process starts before database-local extension workers and loads
-- its desired registry exclusively from the machine-managed volume snapshot.
SELECT count(*) = 1 AS worker_running
FROM pg_stat_activity
WHERE backend_type = 'pg-lakebase-storage'
\gset
\echo worker_running: :worker_running

COPY (SELECT current_setting('data_directory') || '/pg_lakebase/storage.sock')
TO '/tmp/_regress_socket_path.txt';
\! test -S "$(cat /tmp/_regress_socket_path.txt)" && echo "socket_exists: true" || echo "socket_exists: false"
\! rm -f /tmp/_regress_socket_path.txt

SELECT current_setting(
           'pg_lakebase.storage_volume_retirement_grace_period_seconds'
) = '604800' AS volume_retirement_grace_period_default
\gset
\echo volume_retirement_grace_period_default: :volume_retirement_grace_period_default

SELECT loaded_volume_count AS loaded_before
FROM lakebase.storage_service_status
\gset

CREATE TEMP TABLE storage_volume_reload_baseline AS
SELECT reload_generation,
       loaded_volume_count::bigint AS initial_loaded_volume_count
FROM lakebase.storage_service_status;

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
        FROM lakebase.storage_service_status;

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
SELECT lakebase.create_storage_volume(
    :'volume_name',
    's3://storage-bgworker-regress/root',
    '{"type":"anonymous"}'::jsonb,
    '{"region":"us-east-1"}'::jsonb
) AS created_name
\gset

SELECT count(*) = 1 AS config_visible
FROM lakebase.storage_volumes
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
FROM lakebase.storage_service_status
\gset
\echo registry_loaded: :registry_loaded

SELECT :'volume_name' || '-renamed' AS renamed_volume
\gset
SELECT lakebase.rename_storage_volume(:'volume_name', :'renamed_volume')
       AS ignored
\gset
SELECT count(*) = 1 AS rename_visible
FROM lakebase.storage_volumes
WHERE storage_volume_name = :'renamed_volume'
  AND internal_volume_id IS NOT NULL
\gset
\echo rename_visible: :rename_visible

UPDATE storage_volume_reload_baseline AS baseline
SET reload_generation = status.reload_generation
FROM lakebase.storage_service_status AS status;

SELECT lakebase.update_storage_volume_credentials(
    :'renamed_volume',
    '{"type":"s3_access_key","access_key_id":"regress-key",'
    '"secret_access_key":"regress-secret"}'::jsonb
) AS ignored
\gset
SELECT count(*) = 1 AS credential_update_visible
FROM lakebase.storage_volumes
WHERE storage_volume_name = :'renamed_volume'
  AND credential_type = 's3_access_key'
\gset
\echo credential_update_visible: :credential_update_visible

SELECT pg_temp.storage_volume_wait_for_reload(true, true) AS ignored
\gset

SELECT loaded_volume_count >= :loaded_before::bigint + 1
           AND last_reload_replaced >= 1
           AND last_error IS NULL AS replacement_loaded
FROM lakebase.storage_service_status
\gset
\echo replacement_loaded: :replacement_loaded

UPDATE storage_volume_reload_baseline AS baseline
SET reload_generation = status.reload_generation
FROM lakebase.storage_service_status AS status;

SELECT lakebase.reload_storage_volumes() AS ignored
\gset

SELECT pg_temp.storage_volume_wait_for_reload(false, false) AS ignored
\gset

SELECT count(*) = 1 AS worker_still_running
FROM pg_stat_activity
WHERE backend_type = 'pg-lakebase-storage'
\gset
\echo worker_still_running: :worker_still_running

DROP FUNCTION pg_temp.storage_volume_wait_for_reload(boolean, boolean);
DROP TABLE storage_volume_reload_baseline;
