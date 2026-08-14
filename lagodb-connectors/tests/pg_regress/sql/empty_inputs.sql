-- Empty direct-COPY objects and empty foreign-table prefixes.

SET TIME ZONE 'UTC';
SET client_min_messages = warning;

SELECT bucket
FROM lakebase_regress.object_storage_fixture
\gset storage_

SELECT format('s3://%s/lagodb-connectors/empty/text.txt',
              :'storage_bucket') AS empty_text_exact,
       format('s3://%s/lagodb-connectors/empty/text-prefix/',
              :'storage_bucket') AS empty_text_prefix,
       format('s3://%s/lagodb-connectors/empty/csv.csv',
              :'storage_bucket') AS empty_csv_exact,
       format('s3://%s/lagodb-connectors/empty/csv-prefix/',
              :'storage_bucket') AS empty_csv_prefix,
       format('s3://%s/lagodb-connectors/empty/json.json',
              :'storage_bucket') AS empty_json_exact,
       format('s3://%s/lagodb-connectors/empty/json-prefix/',
              :'storage_bucket') AS empty_json_prefix,
       format('s3://%s/lagodb-connectors/empty/avro.avro',
              :'storage_bucket') AS empty_avro_exact,
       format('s3://%s/lagodb-connectors/empty/avro-prefix/',
              :'storage_bucket') AS empty_avro_prefix,
       format('s3://%s/lagodb-connectors/empty/parquet.parquet',
              :'storage_bucket') AS empty_parquet_exact,
       format('s3://%s/lagodb-connectors/empty/parquet-prefix/',
              :'storage_bucket') AS empty_parquet_prefix,
       format('s3://%s/lagodb-connectors/empty/no-objects/text/',
              :'storage_bucket') AS empty_text_missing_key,
       format('s3://%s/lagodb-connectors/empty/no-objects/csv/',
              :'storage_bucket') AS empty_csv_missing_key,
       format('s3://%s/lagodb-connectors/empty/no-objects/json/',
              :'storage_bucket') AS empty_json_missing_key,
       format('s3://%s/lagodb-connectors/empty/no-objects/avro/',
              :'storage_bucket') AS empty_avro_missing_key,
       format('s3://%s/lagodb-connectors/empty/no-objects/parquet/',
              :'storage_bucket') AS empty_parquet_missing_key
\gset edge_

-- COPY TO emits a physical empty object for every supported format, both for
-- an exact target and for a generated object under a prefix.
DROP TABLE IF EXISTS lagodb_connectors_regress.edge_empty_source CASCADE;
CREATE TABLE lagodb_connectors_regress.edge_empty_source (id integer);

COPY lagodb_connectors_regress.edge_empty_source
TO :'edge_empty_text_exact'
WITH (storage_server 'lagodb_connectors_regress_s3');
COPY lagodb_connectors_regress.edge_empty_source
TO :'edge_empty_text_prefix'
WITH (storage_server 'lagodb_connectors_regress_s3', format 'text');
COPY lagodb_connectors_regress.edge_empty_source
TO :'edge_empty_csv_exact'
WITH (storage_server 'lagodb_connectors_regress_s3');
COPY lagodb_connectors_regress.edge_empty_source
TO :'edge_empty_csv_prefix'
WITH (storage_server 'lagodb_connectors_regress_s3', format 'csv');
COPY lagodb_connectors_regress.edge_empty_source
TO :'edge_empty_json_exact'
WITH (storage_server 'lagodb_connectors_regress_s3');
COPY lagodb_connectors_regress.edge_empty_source
TO :'edge_empty_json_prefix'
WITH (storage_server 'lagodb_connectors_regress_s3', format 'json');
COPY lagodb_connectors_regress.edge_empty_source
TO :'edge_empty_avro_exact'
WITH (storage_server 'lagodb_connectors_regress_s3');
COPY lagodb_connectors_regress.edge_empty_source
TO :'edge_empty_avro_prefix'
WITH (storage_server 'lagodb_connectors_regress_s3', format 'avro');
COPY lagodb_connectors_regress.edge_empty_source
TO :'edge_empty_parquet_exact'
WITH (storage_server 'lagodb_connectors_regress_s3');
COPY lagodb_connectors_regress.edge_empty_source
TO :'edge_empty_parquet_prefix'
WITH (storage_server 'lagodb_connectors_regress_s3', format 'parquet');

DROP TABLE IF EXISTS lagodb_connectors_regress.edge_empty_text_from;
CREATE TABLE lagodb_connectors_regress.edge_empty_text_from (id integer);
COPY lagodb_connectors_regress.edge_empty_text_from
FROM :'edge_empty_text_exact'
WITH (storage_server 'lagodb_connectors_regress_s3');

DROP TABLE IF EXISTS lagodb_connectors_regress.edge_empty_csv_from;
CREATE TABLE lagodb_connectors_regress.edge_empty_csv_from (id integer);
COPY lagodb_connectors_regress.edge_empty_csv_from
FROM :'edge_empty_csv_exact'
WITH (storage_server 'lagodb_connectors_regress_s3');

DROP TABLE IF EXISTS lagodb_connectors_regress.edge_empty_json_from;
CREATE TABLE lagodb_connectors_regress.edge_empty_json_from (id integer);
COPY lagodb_connectors_regress.edge_empty_json_from
FROM :'edge_empty_json_exact'
WITH (storage_server 'lagodb_connectors_regress_s3');

DROP TABLE IF EXISTS lagodb_connectors_regress.edge_empty_avro_from;
CREATE TABLE lagodb_connectors_regress.edge_empty_avro_from (id integer);
COPY lagodb_connectors_regress.edge_empty_avro_from
FROM :'edge_empty_avro_exact'
WITH (storage_server 'lagodb_connectors_regress_s3');

DROP TABLE IF EXISTS lagodb_connectors_regress.edge_empty_parquet_from;
CREATE TABLE lagodb_connectors_regress.edge_empty_parquet_from (id integer);
COPY lagodb_connectors_regress.edge_empty_parquet_from
FROM :'edge_empty_parquet_exact'
WITH (storage_server 'lagodb_connectors_regress_s3');

SELECT relation, rows
FROM (
    SELECT 'copy-text' AS relation, count(*) AS rows
    FROM lagodb_connectors_regress.edge_empty_text_from
    UNION ALL
    SELECT 'copy-csv', count(*)
    FROM lagodb_connectors_regress.edge_empty_csv_from
    UNION ALL
    SELECT 'copy-json', count(*)
    FROM lagodb_connectors_regress.edge_empty_json_from
    UNION ALL
    SELECT 'copy-avro', count(*)
    FROM lagodb_connectors_regress.edge_empty_avro_from
    UNION ALL
    SELECT 'copy-parquet', count(*)
    FROM lagodb_connectors_regress.edge_empty_parquet_from
) AS empty_copy_results
ORDER BY relation;

-- A generated empty object is readable through the FDW. A prefix with no
-- matching object is also a valid empty input for every reader.
CREATE FOREIGN TABLE lagodb_connectors_regress.edge_empty_text_prefix_scan
    (id integer)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'edge_empty_text_prefix', format 'text');
CREATE FOREIGN TABLE lagodb_connectors_regress.edge_empty_csv_prefix_scan
    (id integer)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'edge_empty_csv_prefix', format 'csv');
CREATE FOREIGN TABLE lagodb_connectors_regress.edge_empty_json_prefix_scan
    (id integer)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'edge_empty_json_prefix', format 'json');
CREATE FOREIGN TABLE lagodb_connectors_regress.edge_empty_avro_prefix_scan
    (id integer)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'edge_empty_avro_prefix', format 'avro');
CREATE FOREIGN TABLE lagodb_connectors_regress.edge_empty_parquet_prefix_scan
    (id integer)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'edge_empty_parquet_prefix', format 'parquet');

CREATE FOREIGN TABLE lagodb_connectors_regress.edge_empty_text_missing_scan
    (id integer)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'edge_empty_text_missing_key', format 'text');
CREATE FOREIGN TABLE lagodb_connectors_regress.edge_empty_csv_missing_scan
    (id integer)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'edge_empty_csv_missing_key', format 'csv');
CREATE FOREIGN TABLE lagodb_connectors_regress.edge_empty_json_missing_scan
    (id integer)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'edge_empty_json_missing_key', format 'json');
CREATE FOREIGN TABLE lagodb_connectors_regress.edge_empty_avro_missing_scan
    (id integer)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'edge_empty_avro_missing_key', format 'avro');
CREATE FOREIGN TABLE lagodb_connectors_regress.edge_empty_parquet_missing_scan
    (id integer)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'edge_empty_parquet_missing_key', format 'parquet');

SELECT relation, rows
FROM (
    SELECT 'text-prefix-object' AS relation, count(*) AS rows
    FROM lagodb_connectors_regress.edge_empty_text_prefix_scan
    UNION ALL
    SELECT 'csv-prefix-object', count(*)
    FROM lagodb_connectors_regress.edge_empty_csv_prefix_scan
    UNION ALL
    SELECT 'json-prefix-object', count(*)
    FROM lagodb_connectors_regress.edge_empty_json_prefix_scan
    UNION ALL
    SELECT 'avro-prefix-object', count(*)
    FROM lagodb_connectors_regress.edge_empty_avro_prefix_scan
    UNION ALL
    SELECT 'parquet-prefix-object', count(*)
    FROM lagodb_connectors_regress.edge_empty_parquet_prefix_scan
    UNION ALL
    SELECT 'text-no-object', count(*)
    FROM lagodb_connectors_regress.edge_empty_text_missing_scan
    UNION ALL
    SELECT 'csv-no-object', count(*)
    FROM lagodb_connectors_regress.edge_empty_csv_missing_scan
    UNION ALL
    SELECT 'json-no-object', count(*)
    FROM lagodb_connectors_regress.edge_empty_json_missing_scan
    UNION ALL
    SELECT 'avro-no-object', count(*)
    FROM lagodb_connectors_regress.edge_empty_avro_missing_scan
    UNION ALL
    SELECT 'parquet-no-object', count(*)
    FROM lagodb_connectors_regress.edge_empty_parquet_missing_scan
) AS empty_scan_results
ORDER BY relation;
RESET client_min_messages;
