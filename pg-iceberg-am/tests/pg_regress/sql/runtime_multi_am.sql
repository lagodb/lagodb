-- A second provider cdylib must join runtime's one backend-local directory.
DROP EXTENSION IF EXISTS pg_iceberg_am CASCADE;
CREATE EXTENSION pg_iceberg_am;
LOAD '$libdir/pg_delta_am';
CREATE EXTENSION pg_delta_am;

CREATE TABLE runtime_multi_am_delta (id integer) USING delta;
INSERT INTO runtime_multi_am_delta VALUES (1), (2);

SELECT pre_count AS utility_pre_before, post_count AS utility_post_before
FROM delta.utility_hook_counts() \gset
COMMENT ON TABLE runtime_multi_am_delta IS 'runtime-owned utility router';
SELECT pre_count = :utility_pre_before + 1
       AND post_count = :utility_post_before + 1
       AS delta_utility_hook_advanced
FROM delta.utility_hook_counts();

-- Loading Delta must not replace the Iceberg callbacks already registered in
-- the same runtime-owned directories.
CREATE TABLE runtime_multi_am_iceberg (id integer) USING iceberg;
INSERT INTO runtime_multi_am_iceberg VALUES (1);
SELECT pg_relation_filepath('runtime_multi_am_iceberg') || '_iceberg'
       AS iceberg_path \gset
DROP TABLE runtime_multi_am_iceberg;
SELECT pg_stat_file(:'iceberg_path', true) IS NULL
       AS iceberg_drop_hook_survived_delta_registration;

SELECT provider, format, current_data_objects
FROM lakebase.table_maintenance_stats('runtime_multi_am_delta');

SELECT delta.duplicate_iceberg_registration_rejected()
       AS duplicate_am_owner_rejected;

SELECT provider, format
FROM lakebase.table_maintenance_stats(
    (SELECT oid FROM pg_class WHERE relname = 'runtime_multi_am_delta')
);

SELECT delta.object_access_drop_count() AS object_drop_before \gset
DROP TABLE runtime_multi_am_delta;
SELECT delta.object_access_drop_count() > :object_drop_before
       AS delta_saw_target_relation_drop;
DROP EXTENSION pg_delta_am;
DROP EXTENSION pg_iceberg_am CASCADE;
