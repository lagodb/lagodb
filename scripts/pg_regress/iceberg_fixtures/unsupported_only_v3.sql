-- IMPORT skips a whole table when no fields have supported source types.
DROP TABLE IF EXISTS $table;
CREATE TABLE $table (unsupported void)
USING iceberg TBLPROPERTIES ('format-version'='3');
