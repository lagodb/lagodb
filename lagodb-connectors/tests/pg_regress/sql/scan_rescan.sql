\i include/column_definitions.sql

-- FDW ReScan coverage for each independent reader-state implementation.
-- Text and CSV share DelimitedScanState, so Text represents that class.

SET client_min_messages = warning;

SELECT bucket AS lakebase_regress_bucket
FROM lakebase_regress.object_storage_fixture
\gset

SELECT format('s3://%s/lagodb-connectors/seed/common-prefix/',
              :'lakebase_regress_bucket') AS text_path,
       format('s3://%s/lagodb-connectors/seed/json-prefix/',
              :'lakebase_regress_bucket') AS json_path,
       format('s3://%s/lagodb-connectors/seed/common-prefix-avro/',
              :'lakebase_regress_bucket') AS avro_path,
       format('s3://%s/lagodb-connectors/seed/parquet-prefix/',
              :'lakebase_regress_bucket') AS parquet_path
\gset rescan_

CREATE FOREIGN TABLE lagodb_connectors_regress.rescan_text
    (:common_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'rescan_text_path', format 'text');
CREATE FOREIGN TABLE lagodb_connectors_regress.rescan_json
    (:json_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'rescan_json_path', format 'json');
CREATE FOREIGN TABLE lagodb_connectors_regress.rescan_avro
    (:common_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'rescan_avro_path', format 'avro');
CREATE FOREIGN TABLE lagodb_connectors_regress.rescan_parquet
    (:parquet_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'rescan_parquet_path', format 'parquet');

SET enable_hashjoin = off;
SET enable_mergejoin = off;
SET enable_material = off;
SET enable_nestloop = on;

-- OFFSET 0 prevents pull-up. The correlated inner foreign scan is rescanned
-- for each outer id, including one id with no match after prior successful
-- rescans.
SELECT outer_rel.id AS outer_id,
       inner_rel.id AS inner_id,
       inner_rel.text_col
FROM (VALUES (1), (2), (999)) AS outer_rel(id)
LEFT JOIN LATERAL (
    SELECT id, text_col
    FROM lagodb_connectors_regress.rescan_text AS inner_rel
    WHERE inner_rel.id = outer_rel.id
    OFFSET 0
) AS inner_rel ON true
ORDER BY outer_rel.id;

SELECT outer_rel.id AS outer_id,
       inner_rel.id AS inner_id,
       inner_rel.text_col
FROM (VALUES (1), (2), (999)) AS outer_rel(id)
LEFT JOIN LATERAL (
    SELECT id, text_col
    FROM lagodb_connectors_regress.rescan_json AS inner_rel
    WHERE inner_rel.id = outer_rel.id
    OFFSET 0
) AS inner_rel ON true
ORDER BY outer_rel.id;

SELECT outer_rel.id AS outer_id,
       inner_rel.id AS inner_id,
       inner_rel.text_col
FROM (VALUES (1), (2), (999)) AS outer_rel(id)
LEFT JOIN LATERAL (
    SELECT id, text_col
    FROM lagodb_connectors_regress.rescan_avro AS inner_rel
    WHERE inner_rel.id = outer_rel.id
    OFFSET 0
) AS inner_rel ON true
ORDER BY outer_rel.id;

SELECT outer_rel.id AS outer_id,
       inner_rel.id AS inner_id,
       inner_rel.text_col
FROM (VALUES (1), (2), (999)) AS outer_rel(id)
LEFT JOIN LATERAL (
    SELECT id, text_col
    FROM lagodb_connectors_regress.rescan_parquet AS inner_rel
    WHERE inner_rel.id = outer_rel.id
    OFFSET 0
) AS inner_rel ON true
ORDER BY outer_rel.id;

RESET enable_hashjoin;
RESET enable_mergejoin;
RESET enable_material;
RESET enable_nestloop;
RESET client_min_messages;
