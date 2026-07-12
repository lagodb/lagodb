-- maintenance_queue.sql
-- Transactional and format-neutral queue catalog semantics. All rows remain
-- uncommitted so the concurrently running worker cannot observe them.

DROP EXTENSION IF EXISTS pg_iceberg_am CASCADE;
CREATE EXTENSION pg_iceberg_am;

SELECT operation, state, attempt_count
FROM lakebase.maintenance_status
ORDER BY item_id;

BEGIN;
INSERT INTO lakebase.maintenance_queue
    (item_id, operation, store_id, object_namespace, object_path, producer,
     attempt_count, not_before, failed, created_at)
VALUES
    (gen_random_uuid(), 1, 'regress-store', 'bucket', 'data/a.parquet',
     'synthetic-iceberg', 0, clock_timestamp(), false, clock_timestamp()),
    (gen_random_uuid(), 1, 'regress-store', 'bucket', 'data/a.parquet',
     'synthetic-delta', 0, clock_timestamp(), false, clock_timestamp());
SELECT count(*) AS duplicate_objects_are_distinct
FROM lakebase.maintenance_queue;
ROLLBACK;

SELECT count(*) AS abort_removed_items
FROM lakebase.maintenance_queue;

BEGIN;
SAVEPOINT maintenance_sp;
INSERT INTO lakebase.maintenance_queue
    (item_id, operation, store_id, object_namespace, object_path, producer,
     attempt_count, not_before, failed, created_at)
VALUES (gen_random_uuid(), 2, 'regress-store', 'bucket', 'table-root/',
        'synthetic-drop', 0, clock_timestamp(), false, clock_timestamp());
INSERT INTO lakebase.maintenance_queue
    (item_id, operation, store_id, object_namespace, object_path, producer,
     attempt_count, not_before, failed, created_at)
VALUES (gen_random_uuid(), 2, 'regress-store', 'bucket', 'table-root/',
        'synthetic-drop', 0, clock_timestamp(), false, clock_timestamp());
SELECT count(*) AS duplicate_trees_are_distinct
FROM lakebase.maintenance_queue;
ROLLBACK TO SAVEPOINT maintenance_sp;
SELECT count(*) AS savepoint_removed_items
FROM lakebase.maintenance_queue;
COMMIT;

SELECT count(*) AS queue_empty_at_end
FROM lakebase.maintenance_queue;

-- The operator retry path is a Rust catalog update, not an SQL UPDATE helper.
SELECT gen_random_uuid() AS retry_id \gset
BEGIN;
INSERT INTO lakebase.maintenance_queue
    (item_id, operation, store_id, object_namespace, object_path, producer,
     attempt_count, not_before, failed, last_error, created_at)
VALUES (:'retry_id', 1, 'regress-store', 'bucket', 'retry/object',
        'synthetic-retry', 7, clock_timestamp(), true, 'forced failure',
        clock_timestamp());
SELECT lakebase.retry_maintenance_item(:'retry_id') AS failed_item_retried;
SELECT state, attempt_count, last_error IS NULL AS error_cleared
FROM lakebase.maintenance_status
WHERE item_id = :'retry_id';
SELECT lakebase.retry_maintenance_item(:'retry_id') AS ready_item_not_retried;
ROLLBACK;
