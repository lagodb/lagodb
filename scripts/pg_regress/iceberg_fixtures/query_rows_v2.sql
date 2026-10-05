-- Common relation shape for query-offload join and aggregation sources.
DROP TABLE IF EXISTS $table;
CREATE TABLE $table (
    id integer,
    group_key integer,
    measure integer,
    payload string
) USING iceberg
TBLPROPERTIES ('format-version'='2');
INSERT INTO $table VALUES $values;
