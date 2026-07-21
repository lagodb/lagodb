CREATE FUNCTION lakebase.register_worker(worker_name text, entrypoint regprocedure)
RETURNS void
LANGUAGE SQL
AS $$
    SELECT lakebase.register_worker_impl($1, $2::oid)
$$;

REVOKE ALL ON FUNCTION lakebase.register_worker(text, regprocedure) FROM PUBLIC;
REVOKE ALL ON FUNCTION lakebase.register_worker_impl(text, oid) FROM PUBLIC;
REVOKE ALL ON FUNCTION lakebase.deregister_worker(text) FROM PUBLIC;
REVOKE ALL ON FUNCTION lakebase.worker_runtime_status() FROM PUBLIC;
REVOKE ALL ON FUNCTION lakebase.process_runtime_status() FROM PUBLIC;
REVOKE ALL ON FUNCTION lakebase.storage_runtime_status() FROM PUBLIC;
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
CREATE VIEW lakebase.worker_runtime_status
WITH (security_invoker = true) AS
SELECT e.extname AS extension_name,
       s.worker_name,
       s.database_oid,
       s.extension_oid,
       s.registration_state,
       s.dispatch_state,
       s.process_state,
       s.pid,
       s.generation,
       s.not_before_ms,
       s.stop_requested,
       s.launcher_epoch,
       s.recovery_state
FROM lakebase.worker_runtime_status() AS s
LEFT JOIN pg_catalog.pg_extension AS e
  ON e.oid = s.extension_oid
 AND s.database_oid = (SELECT oid FROM pg_catalog.pg_database
                       WHERE datname = pg_catalog.current_database());
REVOKE ALL ON TABLE lakebase.worker_runtime_status FROM PUBLIC;

CREATE VIEW lakebase.process_runtime_status
WITH (security_invoker = true) AS
SELECT * FROM lakebase.process_runtime_status();
REVOKE ALL ON TABLE lakebase.process_runtime_status FROM PUBLIC;

CREATE VIEW lakebase.storage_runtime_status
WITH (security_invoker = true) AS
SELECT * FROM lakebase.storage_runtime_status();
REVOKE ALL ON TABLE lakebase.storage_runtime_status FROM PUBLIC;

SELECT lakebase.register_worker(
    'maintenance',
    'lakebase.maintenance_worker(internal)'::regprocedure
);
