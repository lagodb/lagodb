-- Iceberg charges its open-file cost to task grouping. Thirty-three nonempty
-- identity partitions create at least thirty-three data files in one
-- set-oriented write. Their default 4 MiB open costs exceed the 128 MiB target
-- and therefore guarantee at least two independently claimable work groups.
DROP TABLE IF EXISTS $table;
CREATE TABLE $table (
    id integer,
    group_key integer,
    measure integer,
    file_group integer
) USING iceberg
PARTITIONED BY (file_group)
TBLPROPERTIES ('format-version'='2');
INSERT INTO $table
SELECT CAST(id AS integer),
       CAST(id % 4 AS integer),
       CAST(id * 10 AS integer),
       CAST(id AS integer)
FROM range(1, 34);
