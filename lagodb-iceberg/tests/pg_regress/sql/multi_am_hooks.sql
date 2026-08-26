-- A second provider loaded by the runtime must join the unified directory.
DROP EXTENSION IF EXISTS lagodb_iceberg CASCADE;
CREATE EXTENSION lagodb_iceberg;
CREATE EXTENSION pg_delta_am;

CREATE TABLE multi_am_delta (id integer) USING delta;
INSERT INTO multi_am_delta VALUES (1), (2);

SELECT pre_count AS utility_pre_before, post_count AS utility_post_before
FROM delta.utility_hook_counts() \gset
COMMENT ON TABLE multi_am_delta IS 'Lakebase utility router';
SELECT pre_count = :utility_pre_before + 1
       AND post_count = :utility_post_before + 1
       AS delta_utility_hook_advanced
FROM delta.utility_hook_counts();

-- Bootstrapping Delta must not replace the Iceberg callbacks registered in
-- the same Lakebase hook directories.
CREATE TABLE multi_am_iceberg (id integer) USING iceberg;
INSERT INTO multi_am_iceberg VALUES (1);
SELECT pg_relation_filepath('multi_am_iceberg') || '_iceberg'
       AS iceberg_path \gset
DROP TABLE multi_am_iceberg;
SELECT pg_stat_file(:'iceberg_path', true) IS NULL
       AS iceberg_drop_hook_survived_delta_registration;

SELECT provider, format, current_data_objects
FROM lagodb.table_maintenance_stats('multi_am_delta');

SELECT delta.duplicate_iceberg_registration_rejected()
       AS duplicate_am_owner_rejected;

SELECT provider, format
FROM lagodb.table_maintenance_stats(
    (SELECT oid FROM pg_class WHERE relname = 'multi_am_delta')
);

SELECT delta.object_access_drop_count() AS object_drop_before \gset
DROP TABLE multi_am_delta;
SELECT delta.object_access_drop_count() > :object_drop_before
       AS delta_saw_target_relation_drop;
DROP EXTENSION pg_delta_am;
DROP EXTENSION lagodb_iceberg CASCADE;
