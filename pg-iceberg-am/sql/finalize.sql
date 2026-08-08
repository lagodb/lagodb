-- SQL statements are intended to go after all other generated SQL.

CREATE ACCESS METHOD iceberg TYPE TABLE HANDLER iceberg_table_am_handler;

REVOKE ALL ON FUNCTION iceberg.maintenance_worker(internal) FROM PUBLIC;

SELECT lakebase.register_worker(
    'iceberg_maintenance',
    'iceberg.maintenance_worker(internal)'::regprocedure
);
