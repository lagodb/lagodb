DROP TABLE IF EXISTS $table;
CREATE TABLE $table (
    id integer,
    payload string
) USING iceberg
PARTITIONED BY (bucket(2, id))
TBLPROPERTIES (
    'format-version'='3',
    'write.delete.mode'='merge-on-read',
    'write.update.mode'='merge-on-read'
);
INSERT INTO $table VALUES
    (1, 'one'),
    (2, 'two'),
    (3, 'three'),
    (4, 'four');
DELETE FROM $table WHERE id = 2;
UPDATE $table
SET payload = 'three-spark' WHERE id = 3;

