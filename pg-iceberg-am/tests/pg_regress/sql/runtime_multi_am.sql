-- A second provider cdylib must join runtime's one backend-local directory.
DROP EXTENSION IF EXISTS pg_iceberg_am CASCADE;
CREATE EXTENSION pg_iceberg_am;
LOAD '$libdir/pg_delta_am';
CREATE EXTENSION pg_delta_am;

CREATE TABLE runtime_multi_am_delta (id integer) USING delta;
INSERT INTO runtime_multi_am_delta VALUES (1), (2);

SELECT provider, format, current_data_objects
FROM lakebase.table_maintenance_stats('runtime_multi_am_delta');

SELECT delta.duplicate_iceberg_registration_rejected()
       AS duplicate_am_owner_rejected;

SELECT provider, format
FROM lakebase.table_maintenance_stats(
    (SELECT oid FROM pg_class WHERE relname = 'runtime_multi_am_delta')
);

DROP TABLE runtime_multi_am_delta;
DROP EXTENSION pg_delta_am;
DROP EXTENSION pg_iceberg_am CASCADE;
