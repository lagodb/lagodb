-- Exercise the worker framework's transaction, scheduling, process, and
-- cancellation lifecycles. SQL owns every database assertion and fixture;
-- the pause guard below performs only SIGSTOP/SIGCONT process control.
\set ECHO none
\set QUIET 1
\set regress_database :DBNAME
\set runtime_database lakebase_runtime_source
\set runtime_template_database lakebase_runtime_template
\set runtime_template_copy lakebase_runtime_template_copy
\set runtime_role lakebase_runtime_non_superuser
\setenv PGDATABASE :regress_database
SET client_min_messages = warning;

-- 1. Initial recovery and state normalization.
DO $$
BEGIN
    IF EXISTS (
        SELECT FROM pg_database
        WHERE datname = 'lakebase_runtime_template_copy'
          AND datistemplate
    ) THEN
        EXECUTE 'ALTER DATABASE lakebase_runtime_template_copy IS_TEMPLATE false';
    END IF;
    IF EXISTS (
        SELECT FROM pg_database
        WHERE datname = 'lakebase_runtime_template'
          AND datistemplate
    ) THEN
        EXECUTE 'ALTER DATABASE lakebase_runtime_template IS_TEMPLATE false';
    END IF;
END
$$;
DROP DATABASE IF EXISTS :runtime_database WITH (FORCE);
DROP DATABASE IF EXISTS :runtime_template_copy WITH (FORCE);
DROP DATABASE IF EXISTS :runtime_template_database WITH (FORCE);
DROP ROLE IF EXISTS :runtime_role;
CREATE ROLE :runtime_role;
CREATE DATABASE :runtime_database;
CREATE DATABASE :runtime_template_database IS_TEMPLATE true;
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
RESET client_min_messages;

-- Template databases must never enter the coordinator scheduler through
-- registration, explicit wake, ALTER DATABASE SET, or deregistration.
\connect :runtime_template_database
CREATE EXTENSION pg_lakebase_runtime;
CREATE PROCEDURE public.assert_template_worker_excluded(test_case text)
LANGUAGE plpgsql
AS $$
DECLARE
    database_id oid := (
        SELECT oid FROM pg_database WHERE datname = current_database()
    );
BEGIN
    IF EXISTS (
           SELECT FROM lakebase.process_status
           WHERE process_kind = 'coordinator'
             AND database_oid = database_id
       ) OR EXISTS (
           SELECT FROM lakebase.worker_status
           WHERE database_oid = database_id
       ) OR EXISTS (
           SELECT FROM pg_stat_activity
           WHERE datid = database_id
             AND backend_type IN (
                 'pg-lakebase coordinator',
                 'pg-lakebase worker'
             )
       )
    THEN
        RAISE EXCEPTION '% started a Lakebase worker', test_case;
    END IF;
END
$$;
SELECT pg_sleep(1);
CALL public.assert_template_worker_excluded('template registration');
SELECT lakebase.request_worker_wakeup('pg_lakebase_runtime', 'maintenance');
SELECT pg_sleep(1);
CALL public.assert_template_worker_excluded('template SQL wake');
ALTER DATABASE :runtime_template_database
    SET pg_lakebase.customscan_mode = auto;
SELECT pg_sleep(1);
CALL public.assert_template_worker_excluded('template ALTER DATABASE SET');
SELECT lakebase.deregister_worker('maintenance');
SELECT pg_sleep(1);
CALL public.assert_template_worker_excluded('template deregistration');
DROP PROCEDURE public.assert_template_worker_excluded(text);
\connect :regress_database
CREATE DATABASE :runtime_template_copy
    WITH TEMPLATE :runtime_template_database IS_TEMPLATE true;
ALTER DATABASE :runtime_template_copy IS_TEMPLATE false;
DROP DATABASE :runtime_template_copy;
ALTER DATABASE :runtime_template_database IS_TEMPLATE false;
DROP DATABASE :runtime_template_database;

-- 2. Runtime CREATE/DROP transaction lifecycle in an isolated database.
\connect :runtime_database
DO $$
DECLARE
    denied boolean := false;
BEGIN
    SET LOCAL ROLE lakebase_runtime_non_superuser;
    BEGIN
        EXECUTE 'CREATE EXTENSION pg_lakebase_runtime';
    EXCEPTION WHEN insufficient_privilege THEN
        denied := true;
    END;
    IF NOT denied OR EXISTS (
        SELECT FROM pg_extension WHERE extname = 'pg_lakebase_runtime'
    ) THEN
        RAISE EXCEPTION 'non-superuser Lakebase installation was allowed';
    END IF;
END
$$;

BEGIN;
CREATE EXTENSION pg_lakebase_runtime;
ROLLBACK;
DO $$
BEGIN
    IF EXISTS (
        SELECT FROM pg_extension WHERE extname = 'pg_lakebase_runtime'
    ) THEN
        RAISE EXCEPTION 'CREATE EXTENSION rollback leaked the extension';
    END IF;
END
$$;

BEGIN;
SAVEPOINT before_runtime;
CREATE EXTENSION pg_lakebase_runtime;
ROLLBACK TO SAVEPOINT before_runtime;
COMMIT;
DO $$
BEGIN
    IF EXISTS (
        SELECT FROM pg_extension WHERE extname = 'pg_lakebase_runtime'
    ) THEN
        RAISE EXCEPTION 'savepoint rollback leaked the extension';
    END IF;
END
$$;

BEGIN;
SAVEPOINT commit_runtime;
CREATE EXTENSION pg_lakebase_runtime;
RELEASE SAVEPOINT commit_runtime;
COMMIT;

CREATE PROCEDURE public.worker_regress_assert_runtime_idle(test_case text)
LANGUAGE plpgsql
AS $$
DECLARE
    observed boolean := false;
    status_details text;
    deadline timestamptz := clock_timestamp() + interval '15 seconds';
BEGIN
    LOOP
        SELECT EXISTS (
            SELECT FROM lakebase.worker_status
            WHERE database_oid = (
                    SELECT oid FROM pg_database
                    WHERE datname = current_database()
                  )
              AND extension_name = 'pg_lakebase_runtime'
              AND worker_name = 'maintenance'
              AND registration_state = 'registered'
              AND NOT needs_restart
              AND process_state = 'stopped'
              AND pid IS NULL
        ) INTO observed;
        EXIT WHEN observed;
        IF clock_timestamp() >= deadline THEN
            EXIT;
        END IF;
        PERFORM pg_sleep(0.05);
    END LOOP;
    IF NOT observed THEN
        SELECT coalesce(
            jsonb_agg(to_jsonb(status) ORDER BY database_oid,
                                                  extension_name,
                                                  worker_name)::text,
            '[]'
        )
        INTO status_details
        FROM lakebase.worker_status AS status
        WHERE database_oid = (
            SELECT oid FROM pg_database WHERE datname = current_database()
        );
        RAISE EXCEPTION '% did not restore an idle worker', test_case
            USING DETAIL = status_details;
    END IF;
END
$$;
CALL public.worker_regress_assert_runtime_idle('CREATE EXTENSION commit');

BEGIN;
SAVEPOINT before_runtime_drop;
DROP EXTENSION pg_lakebase_runtime;
ROLLBACK TO SAVEPOINT before_runtime_drop;
COMMIT;
CALL public.worker_regress_assert_runtime_idle(
    'DROP EXTENSION savepoint rollback'
);

BEGIN;
DROP EXTENSION pg_lakebase_runtime;
ROLLBACK;
CALL public.worker_regress_assert_runtime_idle('DROP EXTENSION rollback');

CREATE EXTENSION lagodb_iceberg;
DO $$
DECLARE
    rejected boolean := false;
BEGIN
    BEGIN
        EXECUTE 'DROP EXTENSION pg_lakebase_runtime';
    EXCEPTION WHEN dependent_objects_still_exist THEN
        rejected := true;
    END;
    IF NOT rejected
       OR NOT EXISTS (
           SELECT FROM pg_extension WHERE extname = 'pg_lakebase_runtime'
       )
       OR NOT EXISTS (
           SELECT FROM pg_extension WHERE extname = 'lagodb_iceberg'
       )
    THEN
        RAISE EXCEPTION 'PostgreSQL RESTRICT did not preserve dependent extensions';
    END IF;
END
$$;

INSERT INTO lakebase.maintenance_queue (
    item_id, operation, volume_id, object_namespace, object_path, producer,
    attempt_count, not_before, failed, created_at
) VALUES (
    '00000000-0000-0000-0000-000000000001', 1, 1, 'test', 'test',
    'worker-lifecycle-test', 0, clock_timestamp(), true, clock_timestamp()
);

BEGIN;
SET LOCAL client_min_messages = warning;
DROP EXTENSION pg_lakebase_runtime CASCADE;
DO $$
BEGIN
    IF EXISTS (
           SELECT FROM pg_extension
           WHERE extname IN ('pg_lakebase_runtime', 'lagodb_iceberg')
       ) OR to_regclass('lakebase.maintenance_queue') IS NOT NULL
    THEN
        RAISE EXCEPTION 'CASCADE did not remove runtime-owned SQL state';
    END IF;
END
$$;
ROLLBACK;
CALL public.worker_regress_assert_runtime_idle(
    'DROP EXTENSION CASCADE rollback'
);
SELECT EXISTS (
           SELECT FROM pg_extension
           WHERE extname = 'lagodb_iceberg'
       ) AND EXISTS (
           SELECT FROM lakebase.maintenance_queue
           WHERE item_id = '00000000-0000-0000-0000-000000000001'
       ) AS cascade_rollback_restored_sql_state;
DO $$
DECLARE
    observed boolean := false;
    deadline timestamptz := clock_timestamp() + interval '15 seconds';
BEGIN
    LOOP
        SELECT EXISTS (
            SELECT FROM lakebase.worker_status
            WHERE extension_name = 'lagodb_iceberg'
              AND worker_name = 'iceberg_maintenance'
              AND registration_state = 'registered'
              AND process_state = 'stopped'
              AND NOT needs_restart
              AND pid IS NULL
        ) INTO observed;
        EXIT WHEN observed;
        IF clock_timestamp() >= deadline THEN
            EXIT;
        END IF;
        PERFORM pg_sleep(0.05);
    END LOOP;
    IF NOT observed THEN
        RAISE EXCEPTION 'CASCADE rollback did not restore worker state';
    END IF;
END
$$;

DO $$
DECLARE
    denied boolean := false;
BEGIN
    SET LOCAL ROLE lakebase_runtime_non_superuser;
    BEGIN
        PERFORM lakebase.deregister_worker('maintenance');
    EXCEPTION WHEN insufficient_privilege THEN
        denied := true;
    END;
    IF NOT denied THEN
        RAISE EXCEPTION 'non-superuser worker deregistration was allowed';
    END IF;
END
$$;

-- 3. Worker deregistration transaction lifecycle in the regression database.
\connect :regress_database
CREATE TEMP TABLE worker_deregister_results (
    test_case text PRIMARY KEY
);

CREATE PROCEDURE pg_temp.assert_iceberg_worker_registered(test_case text)
LANGUAGE plpgsql
AS $$
DECLARE
    observed boolean := false;
    details text;
    deadline timestamptz := clock_timestamp() + interval '3 seconds';
BEGIN
    IF NOT EXISTS (
        SELECT FROM lakebase.workers
        WHERE worker_name = 'iceberg_maintenance'
    ) THEN
        RAISE EXCEPTION '% did not restore the worker catalog registration',
            test_case;
    END IF;
    LOOP
        SELECT EXISTS (
            SELECT FROM lakebase.worker_status
            WHERE extension_name = 'lagodb_iceberg'
              AND worker_name = 'iceberg_maintenance'
              AND registration_state = 'registered'
              AND process_state = 'stopped'
              AND NOT needs_restart
              AND pid IS NULL
        ) INTO observed;
        EXIT WHEN observed;
        IF clock_timestamp() >= deadline THEN
            EXIT;
        END IF;
        PERFORM pg_sleep(0.01);
    END LOOP;
    IF NOT observed THEN
        SELECT jsonb_build_object(
            'catalog', coalesce((
                SELECT jsonb_agg(to_jsonb(worker))
                FROM lakebase.workers AS worker
            ), '[]'::jsonb),
            'worker', coalesce((
                SELECT jsonb_agg(to_jsonb(status))
                FROM lakebase.worker_status AS status
            ), '[]'::jsonb)
        )::text INTO details;
        RAISE EXCEPTION '% did not restore the worker registration', test_case
            USING DETAIL = details;
    END IF;
    INSERT INTO worker_deregister_results VALUES (test_case);
END
$$;

CREATE PROCEDURE pg_temp.assert_iceberg_worker_absent(test_case text)
LANGUAGE plpgsql
AS $$
DECLARE
    observed boolean := false;
    details text;
    deadline timestamptz := clock_timestamp() + interval '3 seconds';
BEGIN
    IF EXISTS (
        SELECT FROM lakebase.workers
        WHERE worker_name = 'iceberg_maintenance'
    ) THEN
        RAISE EXCEPTION '% retained the worker catalog registration', test_case;
    END IF;
    LOOP
        SELECT NOT EXISTS (
            SELECT FROM lakebase.worker_status
            WHERE extension_name = 'lagodb_iceberg'
              AND worker_name = 'iceberg_maintenance'
        ) INTO observed;
        EXIT WHEN observed;
        IF clock_timestamp() >= deadline THEN
            EXIT;
        END IF;
        PERFORM pg_sleep(0.01);
    END LOOP;
    IF NOT observed THEN
        SELECT jsonb_build_object(
            'catalog', coalesce((
                SELECT jsonb_agg(to_jsonb(worker))
                FROM lakebase.workers AS worker
            ), '[]'::jsonb),
            'worker', coalesce((
                SELECT jsonb_agg(to_jsonb(status))
                FROM lakebase.worker_status AS status
            ), '[]'::jsonb)
        )::text INTO details;
        RAISE EXCEPTION '% retained the worker registration', test_case
            USING DETAIL = details;
    END IF;
    INSERT INTO worker_deregister_results VALUES (test_case);
END
$$;

SET client_min_messages = warning;
DROP EXTENSION IF EXISTS lagodb_iceberg;
RESET client_min_messages;
CREATE EXTENSION lagodb_iceberg;
CALL pg_temp.assert_iceberg_worker_registered('initial registration');
DELETE FROM worker_deregister_results;

SELECT lakebase.deregister_worker(
    'lakebase-worker-does-not-exist', true
);
SELECT lakebase.deregister_worker(-2147483648, true);

BEGIN;
SELECT lakebase.deregister_worker('iceberg_maintenance');
ROLLBACK;
CALL pg_temp.assert_iceberg_worker_registered('worker_deregister_name_abort');

BEGIN;
SAVEPOINT before_name_deregister;
SELECT lakebase.deregister_worker('iceberg_maintenance');
ROLLBACK TO SAVEPOINT before_name_deregister;
COMMIT;
CALL pg_temp.assert_iceberg_worker_registered(
    'worker_deregister_name_savepoint_rollback'
);

SELECT lakebase.deregister_worker('iceberg_maintenance');
CALL pg_temp.assert_iceberg_worker_absent('worker_deregister_name_commit');

DROP EXTENSION lagodb_iceberg;
CREATE EXTENSION lagodb_iceberg;
CALL pg_temp.assert_iceberg_worker_registered('ID transaction setup');
DELETE FROM worker_deregister_results
WHERE test_case = 'ID transaction setup';

BEGIN;
SELECT lakebase.deregister_worker((
    SELECT worker_id FROM lakebase.workers
    WHERE worker_name = 'iceberg_maintenance'
));
ROLLBACK;
CALL pg_temp.assert_iceberg_worker_registered('worker_deregister_id_abort');

BEGIN;
SAVEPOINT before_id_deregister;
SELECT lakebase.deregister_worker((
    SELECT worker_id FROM lakebase.workers
    WHERE worker_name = 'iceberg_maintenance'
));
ROLLBACK TO SAVEPOINT before_id_deregister;
COMMIT;
CALL pg_temp.assert_iceberg_worker_registered(
    'worker_deregister_id_savepoint_rollback'
);

SELECT lakebase.deregister_worker((
    SELECT worker_id FROM lakebase.workers
    WHERE worker_name = 'iceberg_maintenance'
));
CALL pg_temp.assert_iceberg_worker_absent('worker_deregister_id_commit');

DROP EXTENSION lagodb_iceberg;
CREATE EXTENSION lagodb_iceberg;
CALL pg_temp.assert_iceberg_worker_registered('final registration');
DELETE FROM worker_deregister_results WHERE test_case = 'final registration';

\pset format unaligned
\pset tuples_only on
\set QUIET 0
SELECT test_case || ': true'
FROM worker_deregister_results
ORDER BY test_case;
\set QUIET 1

-- 4. Wake, RunAfter, and crash-backoff scheduling.
\connect :runtime_database
INSERT INTO lakebase.maintenance_queue (
    item_id, operation, volume_id, object_namespace, object_path, producer,
    attempt_count, not_before, failed, created_at
) VALUES (
    '00000000-0000-0000-0000-000000000002', 1, 1, 'test', 'future',
    'runtime-lifecycle-test', 0, clock_timestamp() + interval '1 hour', false,
    clock_timestamp()
);
SELECT lakebase.request_worker_wakeup('pg_lakebase_runtime', 'maintenance');
DO $$
DECLARE
    observed boolean := false;
    status_details text;
    deadline timestamptz := clock_timestamp() + interval '15 seconds';
BEGIN
    LOOP
        SELECT EXISTS (
            SELECT FROM lakebase.worker_status
            WHERE database_oid = (
                    SELECT oid FROM pg_database
                    WHERE datname = current_database()
                  )
              AND extension_name = 'pg_lakebase_runtime'
              AND worker_name = 'maintenance'
              AND needs_restart
              AND restart_after_ms IS NOT NULL
              AND process_state = 'restarting'
        ) INTO observed;
        EXIT WHEN observed;
        IF clock_timestamp() >= deadline THEN
            EXIT;
        END IF;
        PERFORM pg_sleep(0.05);
    END LOOP;
    IF NOT observed THEN
        SELECT coalesce(jsonb_agg(to_jsonb(status))::text, '[]')
        INTO status_details
        FROM lakebase.worker_status AS status
        WHERE database_oid = (
            SELECT oid FROM pg_database WHERE datname = current_database()
        );
        RAISE EXCEPTION 'maintenance worker did not publish its future schedule'
            USING DETAIL = status_details;
    END IF;
END
$$;

INSERT INTO lakebase.maintenance_queue (
    item_id, operation, volume_id, object_namespace, object_path, producer,
    attempt_count, not_before, failed, created_at
) VALUES (
    '00000000-0000-0000-0000-000000000003', 999, 1, 'test', 'ready',
    'runtime-lifecycle-test', 0, clock_timestamp(), false, clock_timestamp()
);
SET ROLE lakebase_runtime_non_superuser;
SELECT lakebase.request_worker_wakeup('pg_lakebase_runtime', 'maintenance');
RESET ROLE;
DO $$
DECLARE
    status_details text;
    deadline timestamptz := clock_timestamp() + interval '15 seconds';
BEGIN
    LOOP
        EXIT WHEN EXISTS (
            SELECT FROM lakebase.maintenance_queue
            WHERE item_id = '00000000-0000-0000-0000-000000000003'
              AND failed
        );
        IF clock_timestamp() >= deadline THEN
            EXIT;
        END IF;
        PERFORM pg_sleep(0.05);
    END LOOP;
    IF NOT EXISTS (
        SELECT FROM lakebase.maintenance_queue
        WHERE item_id = '00000000-0000-0000-0000-000000000003'
          AND failed
    ) THEN
        SELECT jsonb_build_object(
            'workers', coalesce((
                SELECT jsonb_agg(to_jsonb(status))
                FROM lakebase.worker_status AS status
            ), '[]'::jsonb),
            'queue', coalesce((
                SELECT jsonb_agg(to_jsonb(item))
                FROM lakebase.maintenance_status AS item
            ), '[]'::jsonb)
        )::text INTO status_details;
        RAISE EXCEPTION 'committed wakeup did not advance scheduled worker'
            USING DETAIL = status_details;
    END IF;
END
$$;
DELETE FROM lakebase.maintenance_queue
WHERE item_id IN (
    '00000000-0000-0000-0000-000000000002',
    '00000000-0000-0000-0000-000000000003'
);

\connect :regress_database
SELECT injection_points_attach(
    'lakebase-worker-after-database-connection',
    'error'
);
\connect :runtime_database
SELECT lakebase.request_worker_wakeup('pg_lakebase_runtime', 'maintenance');
DO $$
DECLARE
    observed boolean := false;
    status_details text;
    deadline timestamptz := clock_timestamp() + interval '15 seconds';
BEGIN
    LOOP
        SELECT EXISTS (
            SELECT FROM lakebase.worker_status
            WHERE database_oid = (
                    SELECT oid FROM pg_database
                    WHERE datname = current_database()
                  )
              AND extension_name = 'pg_lakebase_runtime'
              AND worker_name = 'maintenance'
              AND needs_restart
              AND process_state = 'restarting'
              AND restart_after_ms IS NOT NULL
        ) INTO observed;
        EXIT WHEN observed;
        IF clock_timestamp() >= deadline THEN
            EXIT;
        END IF;
        PERFORM pg_sleep(0.05);
    END LOOP;
    IF NOT observed THEN
        SELECT coalesce(jsonb_agg(to_jsonb(status))::text, '[]')
        INTO status_details
        FROM lakebase.worker_status AS status;
        RAISE EXCEPTION 'worker injection did not enter crash backoff'
            USING DETAIL = status_details;
    END IF;
END
$$;
\connect :regress_database
SELECT injection_points_detach(
    'lakebase-worker-after-database-connection'
);
\connect :runtime_database
SELECT lakebase.request_worker_wakeup('pg_lakebase_runtime', 'maintenance');
CALL public.worker_regress_assert_runtime_idle('injection recovery');

SELECT oid::text AS runtime_database_oid
FROM pg_database
WHERE datname = current_database()
\gset
DROP PROCEDURE public.worker_regress_assert_runtime_idle(text);
SET client_min_messages = warning;
DROP EXTENSION pg_lakebase_runtime CASCADE;
RESET client_min_messages;
DO $$
BEGIN
    IF EXISTS (
           SELECT FROM pg_extension
           WHERE extname IN ('pg_lakebase_runtime', 'lagodb_iceberg')
       ) OR to_regclass('lakebase.maintenance_queue') IS NOT NULL
    THEN
        RAISE EXCEPTION 'committed CASCADE retained runtime-owned SQL state';
    END IF;
END
$$;
\connect :regress_database
SELECT set_config(
    'pg_lakebase.worker_regress_runtime_database_oid',
    :'runtime_database_oid',
    false
) AS configured_runtime_database_oid
\gset
DO $$
DECLARE
    observed boolean := false;
    deadline timestamptz := clock_timestamp() + interval '3 seconds';
BEGIN
    LOOP
        SELECT NOT EXISTS (
            SELECT FROM lakebase.worker_status
            WHERE database_oid = current_setting(
                'pg_lakebase.worker_regress_runtime_database_oid'
            )::oid
        ) INTO observed;
        EXIT WHEN observed;
        IF clock_timestamp() >= deadline THEN
            EXIT;
        END IF;
        PERFORM pg_sleep(0.01);
    END LOOP;
    IF NOT observed THEN
        RAISE EXCEPTION 'committed runtime drop retained shared worker state';
    END IF;
END
$$;
DROP DATABASE :runtime_database;
DROP ROLE :runtime_role;
DROP EXTENSION injection_points;

-- 5. Extension-worker statement cancellation on a real storage request.
SELECT endpoint AS lakebase_regress_endpoint,
       bucket AS lakebase_regress_bucket,
       region AS lakebase_regress_region,
       access_key_id AS lakebase_regress_access_key_id,
       secret_access_key AS lakebase_regress_secret_access_key
FROM lakebase_regress.object_storage_fixture
\gset
SELECT 'regress-worker-statement-cancel-' || current_database()
       AS cancel_volume_name
\gset
SELECT lakebase.create_storage_volume(
    :'cancel_volume_name',
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
) AS created_cancel_volume
\gset
SELECT internal_volume_id::text AS cancel_volume_id
FROM lakebase.storage_volumes
WHERE storage_volume_name = :'cancel_volume_name'
\gset
CREATE TEMP TABLE worker_cancel_fixture AS
SELECT :'cancel_volume_id'::bigint AS volume_id,
       :'lakebase_regress_bucket'::text AS object_namespace;
DO $$
DECLARE
    ready boolean := false;
    fixture worker_cancel_fixture%ROWTYPE;
    last_error text;
    deadline timestamptz := clock_timestamp() + interval '30 seconds';
BEGIN
    SELECT * INTO STRICT fixture FROM worker_cancel_fixture;
    LOOP
        BEGIN
            SELECT objects = 0
            INTO ready
            FROM lakebase.observe_object_tree(
                fixture.volume_id,
                fixture.object_namespace,
                '__lakebase_regress_probe__'
            );
        EXCEPTION WHEN OTHERS THEN
            ready := false;
            GET STACKED DIAGNOSTICS last_error = MESSAGE_TEXT;
        END;
        EXIT WHEN ready;
        IF clock_timestamp() >= deadline THEN
            EXIT;
        END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ready THEN
        RAISE EXCEPTION 'cancellation storage volume did not become ready'
            USING DETAIL = coalesce(last_error, 'readiness probe returned false');
    END IF;
END
$$;

DO $$
DECLARE
    ready boolean := false;
    deadline timestamptz := clock_timestamp() + interval '15 seconds';
BEGIN
    LOOP
        SELECT EXISTS (
            SELECT FROM lakebase.worker_status
            WHERE database_oid = (
                    SELECT oid FROM pg_database
                    WHERE datname = current_database()
                  )
              AND extension_name = 'pg_lakebase_runtime'
              AND worker_name = 'maintenance'
              AND process_state = 'stopped'
              AND pid IS NULL
              AND NOT needs_restart
        ) INTO ready;
        EXIT WHEN ready;
        IF clock_timestamp() >= deadline THEN
            EXIT;
        END IF;
        PERFORM pg_sleep(0.05);
    END LOOP;
    IF NOT ready THEN
        RAISE EXCEPTION 'maintenance worker was not idle before cancellation';
    END IF;
END
$$;

INSERT INTO lakebase.maintenance_queue (
    item_id, operation, volume_id, object_namespace, object_path, producer,
    attempt_count, not_before, failed, created_at
) VALUES (
    '00000000-0000-0000-0000-000000000004', 1, :'cancel_volume_id',
    :'lakebase_regress_bucket', 'worker-cancel-test/blocked-object',
    'worker-cancel-test', 0, clock_timestamp(), false, clock_timestamp()
);

SELECT pid::text AS lakebase_regress_storage_pid
FROM pg_stat_activity
WHERE backend_type = 'pg-lakebase-storage'
\gset
SELECT pg_backend_pid()::text AS lakebase_regress_backend_pid,
       current_setting('port') || '-' || current_database()
           AS lakebase_regress_slot
\gset
\setenv LAKEBASE_REGRESS_STORAGE_PID :lakebase_regress_storage_pid
\setenv LAKEBASE_REGRESS_BACKEND_PID :lakebase_regress_backend_pid
\setenv LAKEBASE_REGRESS_SLOT :lakebase_regress_slot
\! bin/storage_worker_pause_guard start
\if :SHELL_ERROR
    \quit 1
\endif

SELECT lakebase.request_worker_wakeup('pg_lakebase_runtime', 'maintenance');
DO $$
DECLARE
    reached boolean := false;
    deadline timestamptz := clock_timestamp() + interval '15 seconds';
BEGIN
    LOOP
        SELECT EXISTS (
            SELECT FROM lakebase.worker_status AS worker
            JOIN pg_stat_activity AS activity ON activity.pid = worker.pid
            WHERE worker.database_oid = (
                    SELECT oid FROM pg_database
                    WHERE datname = current_database()
                  )
              AND worker.extension_name = 'pg_lakebase_runtime'
              AND worker.worker_name = 'maintenance'
              AND worker.process_state = 'running'
              AND activity.wait_event_type = 'Extension'
        ) INTO reached;
        EXIT WHEN reached;
        IF clock_timestamp() >= deadline THEN
            EXIT;
        END IF;
        PERFORM pg_sleep(0.05);
    END LOOP;
    IF NOT reached THEN
        RAISE EXCEPTION 'maintenance worker did not reach the storage wait';
    END IF;
END
$$;
SELECT worker.pid::text AS cancel_worker_pid
FROM lakebase.worker_status AS worker
JOIN pg_stat_activity AS activity ON activity.pid = worker.pid
WHERE worker.database_oid = (
        SELECT oid FROM pg_database WHERE datname = current_database()
      )
  AND worker.extension_name = 'pg_lakebase_runtime'
  AND worker.worker_name = 'maintenance'
  AND worker.process_state = 'running'
  AND activity.wait_event_type = 'Extension'
\gset
SELECT pg_cancel_backend(:cancel_worker_pid) AS cancel_sent
\gset
\if :cancel_sent
\else
    \! bin/storage_worker_pause_guard release
    \quit 1
\endif

DO $$
DECLARE
    entered_backoff boolean := false;
    deadline timestamptz := clock_timestamp() + interval '15 seconds';
BEGIN
    LOOP
        SELECT EXISTS (
            SELECT FROM lakebase.worker_status
            WHERE database_oid = (
                    SELECT oid FROM pg_database
                    WHERE datname = current_database()
                  )
              AND extension_name = 'pg_lakebase_runtime'
              AND worker_name = 'maintenance'
              AND failure_count = 1
              AND needs_restart
              AND process_state = 'restarting'
              AND pid IS NULL
        ) INTO entered_backoff;
        EXIT WHEN entered_backoff;
        IF clock_timestamp() >= deadline THEN
            EXIT;
        END IF;
        PERFORM pg_sleep(0.05);
    END LOOP;
    IF NOT entered_backoff THEN
        RAISE EXCEPTION 'statement cancellation did not enter crash backoff';
    END IF;
END
$$;

-- 6. Restore process state and remove every worker-owned fixture.
\! bin/storage_worker_pause_guard release
\if :SHELL_ERROR
    \quit 1
\endif
DO $$
DECLARE
    recovered boolean := false;
    deadline timestamptz := clock_timestamp() + interval '20 seconds';
BEGIN
    LOOP
        SELECT NOT EXISTS (
            SELECT FROM lakebase.maintenance_queue
            WHERE item_id = '00000000-0000-0000-0000-000000000004'
        ) AND EXISTS (
            SELECT FROM lakebase.worker_status
            WHERE database_oid = (
                    SELECT oid FROM pg_database
                    WHERE datname = current_database()
                  )
              AND extension_name = 'pg_lakebase_runtime'
              AND worker_name = 'maintenance'
              AND failure_count = 0
              AND process_state = 'stopped'
              AND pid IS NULL
              AND NOT needs_restart
        ) INTO recovered;
        EXIT WHEN recovered;
        IF clock_timestamp() >= deadline THEN
            EXIT;
        END IF;
        PERFORM pg_sleep(0.05);
    END LOOP;
    IF NOT recovered THEN
        RAISE EXCEPTION 'workers did not recover after statement cancellation';
    END IF;
END
$$;
DELETE FROM lakebase.maintenance_queue
WHERE item_id = '00000000-0000-0000-0000-000000000004';
SELECT lakebase.drop_storage_volume(:'cancel_volume_name');

SELECT 'worker_test_restored_iceberg_registration: ' || EXISTS (
           SELECT FROM lakebase.worker_status
           WHERE extension_name = 'lagodb_iceberg'
             AND worker_name = 'iceberg_maintenance'
             AND registration_state = 'registered'
             AND process_state = 'stopped'
             AND NOT needs_restart
       );

\set QUIET 0
\echo worker_create_rollback: true
\echo worker_savepoint_rollback: true
\echo worker_savepoint_commit: true
\echo worker_drop_rollback: true
\echo worker_drop_savepoint_rollback: true
\echo worker_drop_commit: true
\echo worker_drop_native_restrict: true
\echo worker_drop_cascade_rollback: true
\echo worker_drop_queue_discard: true
\echo worker_non_superuser_guard: true
\echo worker_non_superuser_wakeup: true
\echo worker_scheduled_wakeup: true
\echo worker_template_database_excluded: true
\echo worker_drop_database: true
\echo worker_cancel_entered_backoff: true
\echo worker_cancel_recovered: true
