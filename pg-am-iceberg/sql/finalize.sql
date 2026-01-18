-- SQL statements are intended to go after all other generated SQL.

CREATE ACCESS METHOD iceberg TYPE TABLE HANDLER iceberg_table_am_handler;
