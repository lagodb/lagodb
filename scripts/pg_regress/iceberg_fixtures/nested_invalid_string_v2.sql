-- Arrow strings may contain NUL; PostgreSQL JSONB cannot represent it.
DROP TABLE IF EXISTS $table;
CREATE TABLE $table (
    id integer,
    value struct<text:string>
) USING iceberg
TBLPROPERTIES ('format-version'='2');
INSERT INTO $table
SELECT 1, named_struct('text', concat('before', chr(0), 'after'));
