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
           'pg_lakebase.storage_server_volume_reconcile_interval_ms'
       ) = '30000' AS volume_reload_interval_default
\gset
\echo volume_reload_interval_default: :volume_reload_interval_default

SELECT loaded_volume_count AS loaded_before
FROM lakebase.storage_runtime_status
\gset

SELECT 'regress-bgw-' || gen_random_uuid() AS volume_name
\gset
SELECT lakebase.create_storage_volume(
    :'volume_name',
    's3://storage-bgworker-regress/root',
    '{"type":"anonymous"}'::jsonb,
    '{"region":"us-east-1"}'::jsonb
) AS created_name
\gset

SELECT pg_sleep(0.5) AS ignored
\gset
SELECT count(*) = 1 AS config_visible
FROM lakebase.storage_volumes
WHERE storage_volume_name = :'volume_name'
  AND provider = 's3'
  AND credential_type = 'anonymous'
  AND bound_tablespace_oid IS NULL
\gset
\echo config_visible: :config_visible

SELECT loaded_volume_count >= :loaded_before::bigint + 1
           AND last_error IS NULL AS registry_loaded
FROM lakebase.storage_runtime_status
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

SELECT lakebase.update_storage_volume_credentials(
    :'renamed_volume',
    '{"type":"s3_access_key","access_key_id":"regress-key",'
    '"secret_access_key":"regress-secret"}'::jsonb
) AS ignored
\gset
SELECT pg_sleep(0.5) AS ignored
\gset
SELECT count(*) = 1 AS credential_update_visible
FROM lakebase.storage_volumes
WHERE storage_volume_name = :'renamed_volume'
  AND credential_type = 's3_access_key'
\gset
\echo credential_update_visible: :credential_update_visible

SELECT loaded_volume_count >= :loaded_before::bigint + 1
           AND last_reload_replaced >= 1
           AND last_error IS NULL AS replacement_loaded
FROM lakebase.storage_runtime_status
\gset
\echo replacement_loaded: :replacement_loaded

SELECT lakebase.reload_storage_volumes() AS ignored
\gset
SELECT pg_sleep(0.5) AS ignored
\gset
SELECT count(*) = 1 AS worker_still_running
FROM pg_stat_activity
WHERE backend_type = 'pg-lakebase-storage'
\gset
\echo worker_still_running: :worker_still_running
