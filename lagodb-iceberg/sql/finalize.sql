-- SQL statements are intended to go after all other generated SQL.

CREATE ACCESS METHOD iceberg TYPE TABLE HANDLER iceberg_table_am_handler;

CREATE FOREIGN DATA WRAPPER lagodb_iceberg
  HANDLER lagodb_iceberg_fdw_handler
  VALIDATOR lagodb_iceberg_fdw_validator;

REVOKE ALL ON FUNCTION iceberg.maintenance_worker(internal) FROM PUBLIC;

SELECT lagodb.register_worker(
    'iceberg_maintenance',
    'iceberg.maintenance_worker(internal)'::regprocedure
);
