-- Thirty-three partitions guarantee multiple query-offload work groups
-- under the default 4 MiB open-file cost and 128 MiB group target.
DROP TABLE IF EXISTS $table;
CREATE TABLE $table (
    id integer,
    value struct<id:integer,values:array<integer>>
) USING iceberg
PARTITIONED BY (truncate(128, id))
TBLPROPERTIES ('format-version'='2');
INSERT INTO $table
SELECT CAST(id AS integer),
       named_struct('id', CAST(id AS integer),
                    'values', array(CAST(id AS integer), CAST(NULL AS integer)))
FROM range(4097);
