-- Change both UUID and definition: execution must reject the stale plan before
-- attempting to bind its old payload column against the replacement table.
DROP TABLE IF EXISTS $table;
CREATE TABLE $table (id integer) USING iceberg
TBLPROPERTIES ('format-version'='2');
INSERT INTO $table VALUES (2);
