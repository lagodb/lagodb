CREATE NAMESPACE IF NOT EXISTS rest.fdw_regress;

DROP TABLE IF EXISTS rest.fdw_regress.writable;
CREATE TABLE rest.fdw_regress.writable (
    id integer,
    payload string
) USING iceberg
TBLPROPERTIES (
    'format-version'='2',
    'write.delete.mode'='merge-on-read',
    'write.update.mode'='merge-on-read'
);
INSERT INTO rest.fdw_regress.writable VALUES
    (1, 'one'),
    (2, 'two'),
    (3, 'three');

DROP TABLE IF EXISTS rest.fdw_regress.second;
CREATE TABLE rest.fdw_regress.second (
    id integer,
    payload string
) USING iceberg
TBLPROPERTIES ('format-version'='2');
INSERT INTO rest.fdw_regress.second VALUES (10, 'ten');

DROP TABLE IF EXISTS rest.fdw_regress.read_filters;
CREATE TABLE rest.fdw_regress.read_filters (
    id integer,
    payload string,
    event_date date
) USING iceberg
TBLPROPERTIES ('format-version'='2');
INSERT INTO rest.fdw_regress.read_filters VALUES
    (1, 'one', DATE '2024-01-01'),
    (2, 'two', DATE '2024-01-02'),
    (3, NULL, DATE '2024-01-03'),
    (4, 'four', NULL);

DROP TABLE IF EXISTS rest.fdw_regress.v3_mutations;
CREATE TABLE rest.fdw_regress.v3_mutations (
    id integer,
    payload string
) USING iceberg
TBLPROPERTIES (
    'format-version'='3',
    'write.delete.mode'='merge-on-read',
    'write.update.mode'='merge-on-read'
);
INSERT INTO rest.fdw_regress.v3_mutations VALUES
    (1, 'one'),
    (2, 'two'),
    (3, 'three'),
    (4, 'four');
DELETE FROM rest.fdw_regress.v3_mutations WHERE id = 2;
UPDATE rest.fdw_regress.v3_mutations
SET payload = 'three-spark' WHERE id = 3;

CREATE NAMESPACE IF NOT EXISTS fallback.fdw_regress;

DROP TABLE IF EXISTS fallback.fdw_regress.writable;
CREATE TABLE fallback.fdw_regress.writable (
    id integer,
    payload string
) USING iceberg
TBLPROPERTIES ('format-version'='2');
INSERT INTO fallback.fdw_regress.writable VALUES (100, 'fallback');

DROP TABLE IF EXISTS fallback.fdw_regress.second_bucket;
CREATE TABLE fallback.fdw_regress.second_bucket (
    id integer,
    payload string
) USING iceberg
LOCATION 's3://${hiveconf:fallback_second_bucket}/iceberg-fallback/fdw_regress/second_bucket'
TBLPROPERTIES ('format-version'='2');
INSERT INTO fallback.fdw_regress.second_bucket VALUES (200, 'second-bucket');
