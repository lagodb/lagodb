-- Schema 0 supplies the imported source types. Current schema 2 promotes a
-- decimal's precision and an integer's width; files span both definitions.
DROP TABLE IF EXISTS $table;
CREATE TABLE $table (id integer, amount decimal(5,2), promoted integer)
USING iceberg TBLPROPERTIES ('format-version'='2');
INSERT INTO $table VALUES (1, 12.34, 10);
ALTER TABLE $table ALTER COLUMN amount TYPE decimal(8,2);
ALTER TABLE $table ALTER COLUMN promoted TYPE bigint;
INSERT INTO $table VALUES (2, 123.45, 20), (3, 12345.67, 30);
