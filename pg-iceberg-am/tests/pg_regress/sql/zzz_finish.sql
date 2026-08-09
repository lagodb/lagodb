-- Release global fixtures after the complete regression run.
\setenv PGDATABASE :DBNAME
\set QUIET 1
SET client_min_messages = warning;
SELECT pid::text AS lakebase_regress_storage_pid
FROM pg_stat_activity
WHERE backend_type = 'pg-lakebase-storage'
\gset
SELECT current_setting('port') || '-' || current_database()
       AS lakebase_regress_slot
\gset
\setenv LAKEBASE_REGRESS_STORAGE_PID :lakebase_regress_storage_pid
\setenv LAKEBASE_REGRESS_SLOT :lakebase_regress_slot
\! bin/storage_worker_pause_guard recover
CREATE EXTENSION IF NOT EXISTS injection_points;
DO $$
BEGIN
    PERFORM injection_points_detach(
        'lakebase-worker-after-database-connection'
    );
EXCEPTION WHEN internal_error THEN
    IF SQLERRM <>
       'could not detach injection point "lakebase-worker-after-database-connection"'
    THEN
        RAISE;
    END IF;
END
$$;
DROP EXTENSION injection_points;
DROP DATABASE IF EXISTS lakebase_runtime_source WITH (FORCE);
DROP ROLE IF EXISTS lakebase_runtime_non_superuser;
SELECT 'regress-worker-statement-cancel-' || current_database()
       AS cancel_volume_name
\gset
DELETE FROM lakebase.maintenance_queue
WHERE item_id = '00000000-0000-0000-0000-000000000004';
SELECT lakebase.drop_storage_volume(:'cancel_volume_name')
FROM lakebase.storage_volumes
WHERE storage_volume_name = :'cancel_volume_name';
RESET client_min_messages;
\set QUIET 0
SET client_min_messages = warning;
DROP TABLE IF EXISTS maintenance_remote_drop;
DROP TABLE IF EXISTS maintenance_remote_rollback;
DROP TABLESPACE IF EXISTS regress_object;
DROP TABLE IF EXISTS lakebase_regress.object_storage_fixture;
DROP SCHEMA IF EXISTS lakebase_regress;
RESET client_min_messages;
\! bin/object_storage_fixture teardown
