CREATE FUNCTION lakebase.register_worker(worker_name text, entrypoint regprocedure)
RETURNS integer
LANGUAGE SQL
AS $$
    SELECT lakebase.register_worker_impl($1, $2::oid)
$$;

REVOKE ALL ON FUNCTION lakebase.register_worker(text, regprocedure) FROM PUBLIC;
REVOKE ALL ON FUNCTION lakebase.register_worker_impl(text, oid) FROM PUBLIC;
ALTER FUNCTION lakebase.register_worker(text, regprocedure) SECURITY DEFINER;
ALTER FUNCTION lakebase.register_worker(text, regprocedure)
    SET search_path = pg_catalog;
CREATE FUNCTION lakebase.deregister_worker(worker_id integer, missing_ok bool DEFAULT false)
RETURNS void
LANGUAGE SQL
AS $$
    SELECT lakebase.deregister_worker_by_id($1, $2)
$$;
REVOKE ALL ON FUNCTION lakebase.deregister_worker(text, boolean) FROM PUBLIC;
REVOKE ALL ON FUNCTION lakebase.deregister_worker(integer, boolean) FROM PUBLIC;
REVOKE ALL ON FUNCTION lakebase.deregister_worker_by_id(integer, boolean) FROM PUBLIC;
ALTER FUNCTION lakebase.deregister_worker(text, boolean) SECURITY DEFINER;
ALTER FUNCTION lakebase.deregister_worker(text, boolean)
    SET search_path = pg_catalog;
ALTER FUNCTION lakebase.deregister_worker(integer, boolean) SECURITY DEFINER;
ALTER FUNCTION lakebase.deregister_worker(integer, boolean)
    SET search_path = pg_catalog;
REVOKE ALL ON FUNCTION lakebase.worker_status() FROM PUBLIC;
REVOKE ALL ON FUNCTION lakebase.process_status() FROM PUBLIC;
REVOKE ALL ON FUNCTION lakebase.storage_service_status() FROM PUBLIC;
REVOKE ALL ON FUNCTION lakebase.create_storage_volume(text, text, jsonb, jsonb, bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION lakebase.rename_storage_volume(text, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION lakebase.drop_storage_volume(text) FROM PUBLIC;
REVOKE ALL ON FUNCTION lakebase.repair_storage_volume_retirement(text) FROM PUBLIC;
REVOKE ALL ON FUNCTION lakebase.update_storage_volume_credentials(text, jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION lakebase.probe_storage_volume(text) FROM PUBLIC;
COMMENT ON FUNCTION lakebase.create_storage_volume(text, text, jsonb, jsonb, bigint) IS 'Nontransactional administration operation. Invoke as the only expression in a standalone top-level SELECT; do not call from a function, procedure, trigger, DO block, CTE, subquery, or pipelined batch. A durable config-file replacement is not rolled back if the surrounding statement later fails.';
COMMENT ON FUNCTION lakebase.rename_storage_volume(text, text) IS 'Nontransactional administration operation. Invoke as the only expression in a standalone top-level SELECT; do not call from a function, procedure, trigger, DO block, CTE, subquery, or pipelined batch. A durable config-file replacement is not rolled back if the surrounding statement later fails.';
COMMENT ON FUNCTION lakebase.drop_storage_volume(text) IS 'Nontransactional administration operation. Removes only an unbound storage volume configuration.';
COMMENT ON FUNCTION lakebase.repair_storage_volume_retirement(text) IS 'Nontransactional administration operation. Converts a bound orphan into a retiring volume after confirming its catalog binding is gone.';
COMMENT ON FUNCTION lakebase.update_storage_volume_credentials(text, jsonb) IS 'Nontransactional administration operation. Invoke as the only expression in a standalone top-level SELECT; do not call from a function, procedure, trigger, DO block, CTE, subquery, or pipelined batch. A durable config-file replacement is not rolled back if the surrounding statement later fails.';
COMMENT ON FUNCTION lakebase.probe_storage_volume(text) IS 'Explicit nontransactional diagnostic. Uses the worker registered backend to list and create, read back, then delete a unique create-only object under the Volume root. Returns one structured result row; it does not modify the durable Volume configuration.';
REVOKE ALL ON FUNCTION lakebase.reload_storage_volumes() FROM PUBLIC;
REVOKE ALL ON FUNCTION lakebase.storage_volumes_internal() FROM PUBLIC;
REVOKE ALL ON FUNCTION lakebase.retry_maintenance_item(uuid) FROM PUBLIC;

-- Table owners must be able to publish committed maintenance wakeups without
-- receiving access to the private worker registry. The implementation uses
-- fully qualified catalog names; pin search_path for the definer boundary.
GRANT USAGE ON SCHEMA lakebase TO PUBLIC;
GRANT EXECUTE ON FUNCTION lakebase.request_worker_wakeup(text, text) TO PUBLIC;
ALTER FUNCTION lakebase.request_worker_wakeup(text, text) SECURITY DEFINER;
ALTER FUNCTION lakebase.request_worker_wakeup(text, text)
    SET search_path = pg_catalog;

-- Keep these views security-invoker so a future SELECT grant cannot bypass the
-- underlying status functions' EXECUTE revokes.
CREATE VIEW lakebase.worker_status
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
FROM lakebase.worker_status() AS s
LEFT JOIN pg_catalog.pg_extension AS e
  ON e.oid = s.extension_oid
 AND s.database_oid = (SELECT oid FROM pg_catalog.pg_database
                       WHERE datname = pg_catalog.current_database());
REVOKE ALL ON TABLE lakebase.worker_status FROM PUBLIC;

CREATE VIEW lakebase.process_status
WITH (security_invoker = true) AS
SELECT * FROM lakebase.process_status();
REVOKE ALL ON TABLE lakebase.process_status FROM PUBLIC;

CREATE VIEW lakebase.storage_service_status
WITH (security_invoker = true) AS
SELECT * FROM lakebase.storage_service_status();
REVOKE ALL ON TABLE lakebase.storage_service_status FROM PUBLIC;

CREATE VIEW lakebase.storage_volumes
WITH (security_invoker = true) AS
SELECT * FROM lakebase.storage_volumes_internal();
REVOKE ALL ON TABLE lakebase.storage_volumes FROM PUBLIC;

SELECT lakebase.register_worker(
    'maintenance',
    'lakebase.maintenance_worker(internal)'::regprocedure
);
