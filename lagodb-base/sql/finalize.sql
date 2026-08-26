CREATE FUNCTION lagodb.register_worker(worker_name text, entrypoint regprocedure)
RETURNS integer
LANGUAGE SQL
AS $$
    SELECT lagodb.register_worker_impl($1, $2::oid)
$$;

REVOKE ALL ON FUNCTION lagodb.register_worker(text, regprocedure) FROM PUBLIC;
REVOKE ALL ON FUNCTION lagodb.register_worker_impl(text, oid) FROM PUBLIC;
ALTER FUNCTION lagodb.register_worker(text, regprocedure) SECURITY DEFINER;
ALTER FUNCTION lagodb.register_worker(text, regprocedure)
    SET search_path = pg_catalog;
CREATE FUNCTION lagodb.deregister_worker(worker_id integer, missing_ok bool DEFAULT false)
RETURNS void
LANGUAGE SQL
AS $$
    SELECT lagodb.deregister_worker_by_id($1, $2)
$$;
REVOKE ALL ON FUNCTION lagodb.deregister_worker(text, boolean) FROM PUBLIC;
REVOKE ALL ON FUNCTION lagodb.deregister_worker(integer, boolean) FROM PUBLIC;
REVOKE ALL ON FUNCTION lagodb.deregister_worker_by_id(integer, boolean) FROM PUBLIC;
ALTER FUNCTION lagodb.deregister_worker(text, boolean) SECURITY DEFINER;
ALTER FUNCTION lagodb.deregister_worker(text, boolean)
    SET search_path = pg_catalog;
ALTER FUNCTION lagodb.deregister_worker(integer, boolean) SECURITY DEFINER;
ALTER FUNCTION lagodb.deregister_worker(integer, boolean)
    SET search_path = pg_catalog;
REVOKE ALL ON FUNCTION lagodb.worker_status() FROM PUBLIC;
REVOKE ALL ON FUNCTION lagodb.process_status() FROM PUBLIC;
REVOKE ALL ON FUNCTION lagodb.storage_service_status() FROM PUBLIC;
REVOKE ALL ON FUNCTION lagodb.create_storage_volume(text, text, jsonb, jsonb, bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION lagodb.rename_storage_volume(text, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION lagodb.drop_storage_volume(text) FROM PUBLIC;
REVOKE ALL ON FUNCTION lagodb.repair_storage_volume_retirement(text) FROM PUBLIC;
REVOKE ALL ON FUNCTION lagodb.update_storage_volume_credentials(text, jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION lagodb.probe_storage_volume(text) FROM PUBLIC;
COMMENT ON FUNCTION lagodb.create_storage_volume(text, text, jsonb, jsonb, bigint) IS 'Nontransactional administration operation. Invoke as the only expression in a standalone top-level SELECT; do not call from a function, procedure, trigger, DO block, CTE, subquery, or pipelined batch. A durable config-file replacement is not rolled back if the surrounding statement later fails.';
COMMENT ON FUNCTION lagodb.rename_storage_volume(text, text) IS 'Nontransactional administration operation. Invoke as the only expression in a standalone top-level SELECT; do not call from a function, procedure, trigger, DO block, CTE, subquery, or pipelined batch. A durable config-file replacement is not rolled back if the surrounding statement later fails.';
COMMENT ON FUNCTION lagodb.drop_storage_volume(text) IS 'Nontransactional administration operation. Removes only an unbound storage volume configuration.';
COMMENT ON FUNCTION lagodb.repair_storage_volume_retirement(text) IS 'Nontransactional administration operation. Converts a bound orphan into a retiring volume after confirming its catalog binding is gone.';
COMMENT ON FUNCTION lagodb.update_storage_volume_credentials(text, jsonb) IS 'Nontransactional administration operation. Invoke as the only expression in a standalone top-level SELECT; do not call from a function, procedure, trigger, DO block, CTE, subquery, or pipelined batch. A durable config-file replacement is not rolled back if the surrounding statement later fails.';
COMMENT ON FUNCTION lagodb.probe_storage_volume(text) IS 'Explicit nontransactional diagnostic. Uses the worker registered backend to list and create, read back, then delete a unique create-only object under the Volume root. Returns one structured result row; it does not modify the durable Volume configuration.';
REVOKE ALL ON FUNCTION lagodb.reload_storage_volumes() FROM PUBLIC;
REVOKE ALL ON FUNCTION lagodb.storage_volumes_internal() FROM PUBLIC;
REVOKE ALL ON FUNCTION lagodb.retry_maintenance_item(uuid) FROM PUBLIC;

-- Table owners must be able to publish committed maintenance wakeups without
-- receiving access to the private worker registry. The implementation uses
-- fully qualified catalog names; pin search_path for the definer boundary.
GRANT USAGE ON SCHEMA lagodb TO PUBLIC;
GRANT EXECUTE ON FUNCTION lagodb.request_worker_wakeup(text, text) TO PUBLIC;
ALTER FUNCTION lagodb.request_worker_wakeup(text, text) SECURITY DEFINER;
ALTER FUNCTION lagodb.request_worker_wakeup(text, text)
    SET search_path = pg_catalog;

-- Keep these views security-invoker so a future SELECT grant cannot bypass the
-- underlying status functions' EXECUTE revokes.
CREATE VIEW lagodb.worker_status
WITH (security_invoker = true) AS
SELECT e.extname AS extension_name,
       s.worker_name,
       s.database_oid,
       s.worker_id,
       s.extension_oid,
       s.registration_state,
       s.process_state,
       s.pid,
       s.needs_restart,
       s.restart_after_ms,
       s.failure_count,
       s.stop_requested
FROM lagodb.worker_status() AS s
LEFT JOIN pg_catalog.pg_extension AS e
  ON e.oid = s.extension_oid
 AND s.database_oid = (SELECT oid FROM pg_catalog.pg_database
                       WHERE datname = pg_catalog.current_database());
REVOKE ALL ON TABLE lagodb.worker_status FROM PUBLIC;

CREATE VIEW lagodb.process_status
WITH (security_invoker = true) AS
SELECT * FROM lagodb.process_status();
REVOKE ALL ON TABLE lagodb.process_status FROM PUBLIC;

CREATE VIEW lagodb.storage_service_status
WITH (security_invoker = true) AS
SELECT * FROM lagodb.storage_service_status();
REVOKE ALL ON TABLE lagodb.storage_service_status FROM PUBLIC;

CREATE VIEW lagodb.storage_volumes
WITH (security_invoker = true) AS
SELECT * FROM lagodb.storage_volumes_internal();
REVOKE ALL ON TABLE lagodb.storage_volumes FROM PUBLIC;

SELECT lagodb.register_worker(
    'maintenance',
    'lagodb.maintenance_worker(internal)'::regprocedure
);
