-- The live schema no longer contains the source column used by the historical
-- spec. This read fixture retains the old files and their partition metadata.
DROP TABLE IF EXISTS $table;
CREATE TABLE $table (
    id integer,
    category string,
    payload string
) USING iceberg
PARTITIONED BY (truncate(2, category))
TBLPROPERTIES (
    'format-version'='2',
    'write.delete.mode'='merge-on-read',
    'write.update.mode'='merge-on-read'
);
INSERT INTO $table VALUES
    (1, 'alpha', 'one'),
    (2, 'beta', 'two');
ALTER TABLE $table
DROP PARTITION FIELD truncate(2, category);
ALTER TABLE $table
DROP COLUMN category;
