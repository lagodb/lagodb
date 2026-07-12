CREATE SCHEMA IF NOT EXISTS lakebase;

-- We use a custom table to store table options instead of reliance solely on pg_class.reloptions.
-- This bypasses the strict validation whitelist of the pg_class machinery.
CREATE TABLE IF NOT EXISTS lakebase.table_options (
    relid regclass NOT NULL,
    options text[],
    PRIMARY KEY (relid)
) WITH (user_catalog_table = true);

SELECT pg_catalog.pg_extension_config_dump('lakebase.table_options', '');

CREATE TABLE IF NOT EXISTS lakebase.iceberg_metadata (
    relid regclass NOT NULL,
    metadata_location text,
    previous_metadata_location text,
    default_spec_id integer,
    PRIMARY KEY (relid)
) WITH (user_catalog_table = true);

SELECT pg_catalog.pg_extension_config_dump('lakebase.iceberg_metadata', '');

-- Format-neutral durable physical maintenance outbox. This is operational
-- state and must not be copied by logical pg_dump/restore.
CREATE TABLE IF NOT EXISTS lakebase.maintenance_queue (
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

CREATE INDEX IF NOT EXISTS maintenance_queue_ready_idx
    ON lakebase.maintenance_queue (failed, not_before, item_id);

CREATE INDEX IF NOT EXISTS maintenance_queue_target_idx
    ON lakebase.maintenance_queue
       (operation, store_id, object_namespace, object_path);

CREATE OR REPLACE VIEW lakebase.maintenance_status AS
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
