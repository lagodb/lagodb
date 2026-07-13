-- Test: storage background worker (startup, catalog scan, reconcile).
--
-- Sections:
--   A. Startup verification
--   B. SIGHUP GUC reload
--   C. Catalog scan (distributed registered, native ignored)
--   D. Full reconcile lifecycle (CREATE → DROP)

DROP EXTENSION IF EXISTS pg_iceberg_am CASCADE;
CREATE EXTENSION IF NOT EXISTS pg_iceberg_am;

-- ============================================================
-- A. Startup verification
-- ============================================================

-- A1. Bgworker appears in pg_stat_activity
SELECT count(*) AS bgworker_running
FROM pg_stat_activity
WHERE backend_type = 'pg-lakebase-storage';

SELECT count(*) AS maintenance_worker_running
FROM pg_stat_activity
WHERE backend_type = 'pg-lakebase-runtime extension worker';

SELECT state AS maintenance_worker_state
FROM lakebase.worker_runtime_status
WHERE extension_name = 'pg_lakebase_runtime' AND worker_name = 'maintenance'
\gset
\echo maintenance_worker_state: :maintenance_worker_state

SELECT count(*) AS launcher_running
FROM lakebase.process_runtime_status
WHERE process_kind = 'launcher' AND state = 'running'
\gset
\echo launcher_running: :launcher_running

-- A2. Socket file exists at default path ($PGDATA/pg_lakebase/storage.sock)
COPY (SELECT current_setting('data_directory') || '/pg_lakebase/storage.sock') TO '/tmp/_regress_socket_path.txt';
\! test -S "$(cat /tmp/_regress_socket_path.txt)" && echo "socket_exists: true" || echo "socket_exists: false"
\! rm -f /tmp/_regress_socket_path.txt

-- A3. GUCs are readable with expected defaults
SELECT current_setting('pg_lakebase.storage_server_enabled') AS enabled;
SELECT current_setting('pg_lakebase.storage_server_shutdown_timeout_ms') AS shutdown_timeout_ms;
SELECT current_setting('pg_lakebase.storage_server_tablespace_reconcile_interval_ms') AS reconcile_interval_ms;
SELECT current_setting('pg_lakebase.storage_server_worker_threads') AS worker_threads;
SELECT current_setting('pg_lakebase.maintenance_worker_enabled') AS maintenance_enabled;
SELECT current_setting('pg_lakebase.maintenance_actor_threads') AS maintenance_actor_threads;
SELECT current_setting('pg_lakebase.maintenance_batch_items') AS maintenance_batch_items;

-- ============================================================
-- B. SIGHUP GUC reload
-- ============================================================

-- B1. Change a Sighup-reloadable GUC via ALTER SYSTEM
ALTER SYSTEM SET pg_lakebase.storage_server_shutdown_timeout_ms = 9999;
SELECT pg_reload_conf();
SELECT pg_sleep(0.5);
SELECT current_setting('pg_lakebase.storage_server_shutdown_timeout_ms') AS shutdown_timeout_after_reload;

-- B2. Restore original default
ALTER SYSTEM RESET pg_lakebase.storage_server_shutdown_timeout_ms;
SELECT pg_reload_conf();
SELECT pg_sleep(0.5);
SELECT current_setting('pg_lakebase.storage_server_shutdown_timeout_ms') AS shutdown_timeout_after_reset;

-- ============================================================
-- C. Catalog scan (distributed registered, native ignored)
-- ============================================================

\! mkdir -p /tmp/pg_regress_bgw_dist1
\! rm -rf /tmp/pg_regress_bgw_dist1/*
\! mkdir -p /tmp/pg_regress_bgw_dist2
\! rm -rf /tmp/pg_regress_bgw_dist2/*
\! mkdir -p /tmp/pg_regress_bgw_native1
\! rm -rf /tmp/pg_regress_bgw_native1/*
\! mkdir -p /tmp/pg_regress_bgw_native2
\! rm -rf /tmp/pg_regress_bgw_native2/*

SET client_min_messages = warning;
DROP TABLESPACE IF EXISTS regress_bgw_dist1;
DROP TABLESPACE IF EXISTS regress_bgw_dist2;
DROP TABLESPACE IF EXISTS regress_bgw_native1;
DROP TABLESPACE IF EXISTS regress_bgw_native2;
RESET client_min_messages;

-- C1. Distributed tablespace → registered (worker stays healthy)
CREATE TABLESPACE regress_bgw_dist1 LOCATION '/tmp/pg_regress_bgw_dist1' WITH (
    protocol = 's3',
    bucket = 'scan-bucket-1',
    region = 'us-east-1'
);
SELECT pg_sleep(0.5);

SELECT count(*) AS bgworker_after_dist1
FROM pg_stat_activity
WHERE backend_type = 'pg-lakebase-storage';

-- C2. Native tablespace → ignored (worker stays healthy)
CREATE TABLESPACE regress_bgw_native1 LOCATION '/tmp/pg_regress_bgw_native1';
SELECT pg_sleep(0.5);

SELECT count(*) AS bgworker_after_native1
FROM pg_stat_activity
WHERE backend_type = 'pg-lakebase-storage';

-- C3. Second distributed tablespace → also registered
CREATE TABLESPACE regress_bgw_dist2 LOCATION '/tmp/pg_regress_bgw_dist2' WITH (
    protocol = 's3',
    bucket = 'scan-bucket-2',
    region = 'eu-west-1'
);
SELECT pg_sleep(0.5);

SELECT count(*) AS bgworker_after_dist2
FROM pg_stat_activity
WHERE backend_type = 'pg-lakebase-storage';

-- C4. Native tablespace with only PG options → still ignored
CREATE TABLESPACE regress_bgw_native2 LOCATION '/tmp/pg_regress_bgw_native2'
    WITH (seq_page_cost = 1.5);
SELECT pg_sleep(0.5);

SELECT count(*) AS bgworker_after_native2
FROM pg_stat_activity
WHERE backend_type = 'pg-lakebase-storage';

-- C5. Catalog state: 2 distributed with options, 2 native
SELECT spcname, spcoptions
FROM pg_tablespace
WHERE spcname LIKE 'regress_bgw_%'
ORDER BY spcname;

-- ============================================================
-- D. Full reconcile lifecycle (CREATE → DROP)
-- ============================================================

-- D1. DROP removes the stores (worker stays healthy)
DROP TABLESPACE regress_bgw_dist1;
DROP TABLESPACE regress_bgw_dist2;
SELECT pg_sleep(0.5);

SELECT count(*) AS bgworker_after_drops
FROM pg_stat_activity
WHERE backend_type = 'pg-lakebase-storage';

SELECT EXISTS (
    SELECT 1 FROM pg_tablespace WHERE spcname = 'regress_bgw_dist1'
) AS dist1_gone;

-- D2. Re-create with different config → reconcile replaces
\! rm -rf /tmp/pg_regress_bgw_dist1/*
CREATE TABLESPACE regress_bgw_dist1 LOCATION '/tmp/pg_regress_bgw_dist1' WITH (
    protocol = 's3',
    bucket = 'scan-bucket-1-v2',
    region = 'ap-southeast-1'
);
SELECT pg_sleep(0.5);

SELECT count(*) AS bgworker_after_recreate
FROM pg_stat_activity
WHERE backend_type = 'pg-lakebase-storage';

SELECT spcname, spcoptions
FROM pg_tablespace
WHERE spcname = 'regress_bgw_dist1';

-- D3. Rapid CREATE + DROP (within same reconcile window)
\! mkdir -p /tmp/pg_regress_bgw_rapid
\! rm -rf /tmp/pg_regress_bgw_rapid/*
SET client_min_messages = warning;
DROP TABLESPACE IF EXISTS regress_bgw_rapid;
RESET client_min_messages;

CREATE TABLESPACE regress_bgw_rapid LOCATION '/tmp/pg_regress_bgw_rapid' WITH (
    protocol = 's3',
    bucket = 'rapid-bucket',
    region = 'us-east-1'
);
DROP TABLESPACE regress_bgw_rapid;
SELECT pg_sleep(0.5);

SELECT count(*) AS bgworker_after_rapid
FROM pg_stat_activity
WHERE backend_type = 'pg-lakebase-storage';

SELECT EXISTS (
    SELECT 1 FROM pg_tablespace WHERE spcname = 'regress_bgw_rapid'
) AS rapid_gone;

-- ============================================================
-- Cleanup
-- ============================================================
DROP TABLESPACE regress_bgw_dist1;
DROP TABLESPACE regress_bgw_native1;
DROP TABLESPACE regress_bgw_native2;

-- Final health check
SELECT count(*) AS bgworker_final
FROM pg_stat_activity
WHERE backend_type = 'pg-lakebase-storage';
-- End storage background worker test.
