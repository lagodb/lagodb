CREATE NAMESPACE IF NOT EXISTS rest.fdw_regress;
CREATE NAMESPACE IF NOT EXISTS rest.query_offload_regress;

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

-- Dedicated immutable sources for query-offload coverage. Keeping these
-- separate from the FDW read/write fixtures makes the expected rows stable
-- regardless of which earlier regression scripts mutate or reprovision them.
DROP TABLE IF EXISTS rest.query_offload_regress.left_source;
CREATE TABLE rest.query_offload_regress.left_source (
    id integer,
    group_key integer,
    measure integer,
    payload string
) USING iceberg
TBLPROPERTIES ('format-version'='2');
INSERT INTO rest.query_offload_regress.left_source VALUES
    (1, 1, 10, 'alpha'),
    (2, 2, 20, 'beta'),
    (3, 2, NULL, 'beta'),
    (4, 3, 40, NULL),
    (5, NULL, 50, 'orphan');

DROP TABLE IF EXISTS rest.query_offload_regress.right_source;
CREATE TABLE rest.query_offload_regress.right_source (
    id integer,
    group_key integer,
    measure integer,
    payload string
) USING iceberg
TBLPROPERTIES ('format-version'='2');
INSERT INTO rest.query_offload_regress.right_source VALUES
    (101, 1, 100, 'one'),
    (102, 2, 200, 'two-a'),
    (103, 2, 300, 'two-b'),
    (104, 4, 400, 'four');

-- Iceberg charges its open-file cost to task grouping. Thirty-three nonempty
-- identity partitions create at least thirty-three data files in one
-- set-oriented write. Their default 4 MiB open costs exceed the 128 MiB target
-- and therefore guarantee at least two independently claimable work groups.
DROP TABLE IF EXISTS rest.query_offload_regress.parallel_source;
CREATE TABLE rest.query_offload_regress.parallel_source (
    id integer,
    group_key integer,
    measure integer,
    file_group integer
) USING iceberg
PARTITIONED BY (file_group)
TBLPROPERTIES ('format-version'='2');
INSERT INTO rest.query_offload_regress.parallel_source
SELECT CAST(id AS integer),
       CAST(id % 4 AS integer),
       CAST(id * 10 AS integer),
       CAST(id AS integer)
FROM range(1, 34);

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
