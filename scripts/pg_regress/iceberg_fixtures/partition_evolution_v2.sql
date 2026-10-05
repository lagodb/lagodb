-- Historical day/truncate files and current day/truncate/bucket files.
-- Read consumers use metrics=none to prove partition projection independently
-- of column metrics; write consumers retain Iceberg's default metrics mode.
DROP TABLE IF EXISTS $table;
CREATE TABLE $table (
    id integer,
    category string,
    event_date date,
    payload string
) USING iceberg
PARTITIONED BY (day(event_date), truncate(2, category))
TBLPROPERTIES (
    'format-version'='2',
    'write.metadata.metrics.default'='$metrics_mode',
    'write.delete.mode'='merge-on-read',
    'write.update.mode'='merge-on-read'
);
INSERT INTO $table VALUES
    (1, 'alpha', DATE '2024-01-01', 'old-one'),
    (2, 'alpine', DATE '2024-01-01', 'old-two'),
    (3, 'beta', DATE '2024-01-02', 'old-three');
ALTER TABLE $table
ADD PARTITION FIELD bucket(4, id);
INSERT INTO $table VALUES
    (4, 'alpha', DATE '2024-01-03', 'new-four'),
    (5, 'beta', DATE '2024-01-04', 'new-five'),
    (6, NULL, NULL, 'new-six');
