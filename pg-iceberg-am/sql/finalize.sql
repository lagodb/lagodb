-- SQL statements are intended to go after all other generated SQL.

CREATE ACCESS METHOD iceberg TYPE TABLE HANDLER iceberg_table_am_handler;

REVOKE ALL ON FUNCTION iceberg.automatic_maintenance_worker(internal) FROM PUBLIC;

SELECT lakebase.register_worker(
    'iceberg_automatic_maintenance',
    'iceberg.automatic_maintenance_worker(internal)'::regprocedure
);
