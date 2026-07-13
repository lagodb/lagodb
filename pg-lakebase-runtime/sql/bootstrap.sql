CREATE SCHEMA lakebase;

-- Database-local extension worker registrations. Runtime-owned singleton
-- services, such as the storage server, are static bgworkers and are not stored
-- in this table.
CREATE TABLE lakebase.workers (
    extension_name name NOT NULL,
    worker_name text NOT NULL,
    entrypoint_schema name NOT NULL,
    entrypoint_function name NOT NULL,
    PRIMARY KEY (extension_name, worker_name),
    CHECK (octet_length(worker_name) BETWEEN 1 AND 255)
) WITH (user_catalog_table = true);

CREATE TABLE lakebase.table_options (
    relid regclass NOT NULL,
    options text[],
    PRIMARY KEY (relid)
) WITH (user_catalog_table = true);

SELECT pg_catalog.pg_extension_config_dump('lakebase.table_options', '');

CREATE TABLE lakebase.maintenance_queue (
    item_id uuid PRIMARY KEY,
    operation smallint NOT NULL,
    store_id text NOT NULL,
    object_namespace text NOT NULL,
    object_path text NOT NULL,
    producer text NOT NULL,
    source_relid oid,
    source_name text,
    attempt_count integer NOT NULL,
    not_before timestamptz NOT NULL,
    failed boolean NOT NULL,
    last_error text,
    created_at timestamptz NOT NULL
) WITH (user_catalog_table = true);

CREATE INDEX maintenance_queue_ready_idx
    ON lakebase.maintenance_queue (failed, not_before, item_id);

CREATE INDEX maintenance_queue_target_idx
    ON lakebase.maintenance_queue
       (operation, store_id, object_namespace, object_path);

CREATE VIEW lakebase.maintenance_status AS
SELECT item_id,
       CASE operation WHEN 1 THEN 'delete_object' WHEN 2 THEN 'delete_tree' END AS operation,
       store_id,
       object_namespace,
       object_path,
       producer,
       source_relid,
       source_name,
       CASE
           WHEN failed THEN 'failed'
           WHEN not_before > clock_timestamp() THEN 'retry_wait'
           ELSE 'ready'
       END AS state,
       attempt_count,
       not_before,
       last_error,
       created_at
FROM lakebase.maintenance_queue;
