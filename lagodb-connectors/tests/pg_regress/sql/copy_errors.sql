\i include/column_definitions.sql

-- Direct COPY capability errors and their SQLSTATE contracts.

SET client_min_messages = warning;

SELECT bucket AS lakebase_regress_bucket
FROM lakebase_regress.object_storage_fixture
\gset

SELECT format('s3://%s/lagodb-connectors/seed/common-prefix/',
              :'lakebase_regress_bucket') AS text_prefix,
       format('s3://%s/lagodb-connectors/seed/common-prefix-csv/',
              :'lakebase_regress_bucket') AS csv_prefix,
       format('s3://%s/lagodb-connectors/seed/json-prefix/',
              :'lakebase_regress_bucket') AS json_prefix,
       format('s3://%s/lagodb-connectors/seed/common-prefix-avro/',
              :'lakebase_regress_bucket') AS avro_prefix,
       format('s3://%s/lagodb-connectors/copy-errors/missing.txt',
              :'lakebase_regress_bucket') AS missing_text,
       format('s3://%s/lagodb-connectors/copy-errors/negative.json',
              :'lakebase_regress_bucket') AS negative_json,
       format('s3://%s/lagodb-connectors/copy-errors/negative.parquet',
              :'lakebase_regress_bucket') AS negative_parquet,
       format('s3://%s/lagodb-connectors/copy-errors/unsupported.avro',
              :'lakebase_regress_bucket') AS unsupported_avro,
       format('s3://%s/lagodb-connectors/copy-errors/unsupported.parquet',
              :'lakebase_regress_bucket') AS unsupported_parquet
\gset copy_error_

DROP TABLE IF EXISTS lagodb_connectors_regress.copy_error_common;
CREATE TABLE lagodb_connectors_regress.copy_error_common
    (:common_columns);
DROP TABLE IF EXISTS lagodb_connectors_regress.copy_error_json;
CREATE TABLE lagodb_connectors_regress.copy_error_json
    (:json_columns);

\set VERBOSITY sqlstate

-- Implicit selection requires an accessible matching scope.
COPY lagodb_connectors_regress.common_source
TO 's3://invalid-bucket/lagodb-connectors/copy-errors/no-default.txt';

-- Stream, JSON, and Avro COPY FROM require an exact object. Parquet prefix
-- input is covered as a successful path by copy_native.sql.
COPY lagodb_connectors_regress.copy_error_common
FROM :'copy_error_text_prefix'
WITH (server 'lagodb_connectors_regress_s3', format 'text');
COPY lagodb_connectors_regress.copy_error_common
FROM :'copy_error_csv_prefix'
WITH (server 'lagodb_connectors_regress_s3', format 'csv');
COPY lagodb_connectors_regress.copy_error_json
FROM :'copy_error_json_prefix'
WITH (server 'lagodb_connectors_regress_s3', format 'json');
COPY lagodb_connectors_regress.copy_error_common
FROM :'copy_error_avro_prefix'
WITH (server 'lagodb_connectors_regress_s3', format 'avro');

-- A missing exact object locks direct StorageError mapping at the COPY FFI
-- boundary.
COPY lagodb_connectors_regress.copy_error_common
FROM :'copy_error_missing_text'
WITH (server 'lagodb_connectors_regress_s3', format 'text');

-- Native formats reject PostgreSQL text/CSV overrides before storage access.
COPY lagodb_connectors_regress.json_source
TO :'copy_error_negative_json'
WITH (
    server 'lagodb_connectors_regress_s3',
    format 'json',
    delimiter ';'
);
COPY lagodb_connectors_regress.parquet_source
TO :'copy_error_negative_parquet'
WITH (
    server 'lagodb_connectors_regress_s3',
    format 'parquet',
    header true
);

-- Avro has no JSON datum type, and Parquet has JSON but not JSONB.
COPY lagodb_connectors_regress.json_source
TO :'copy_error_unsupported_avro'
WITH (server 'lagodb_connectors_regress_s3', format 'avro');
COPY lagodb_connectors_regress.json_source
TO :'copy_error_unsupported_parquet'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');

\set VERBOSITY default
RESET client_min_messages;
