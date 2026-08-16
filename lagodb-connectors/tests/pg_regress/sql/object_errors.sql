\i include/column_definitions.sql

-- Malformed native objects, prefix schema drift, and catalog boundaries.

SET client_min_messages = warning;

SELECT endpoint,
       bucket,
       region,
       access_key_id,
       secret_access_key
FROM lakebase_regress.object_storage_fixture
\gset storage_

\setenv OBJECT_STORAGE_ENDPOINT :storage_endpoint
\setenv OBJECT_STORAGE_BUCKET :storage_bucket
\setenv OBJECT_STORAGE_REGION :storage_region
\setenv OBJECT_STORAGE_ACCESS_KEY_ID :storage_access_key_id
\setenv OBJECT_STORAGE_SECRET_ACCESS_KEY :storage_secret_access_key

SELECT format('s3://%s/lagodb-connectors/corrupt/truncated.json',
              :'storage_bucket') AS corrupt_json_path,
       format('s3://%s/lagodb-connectors/corrupt/truncated.parquet',
              :'storage_bucket') AS corrupt_parquet_path,
       format('s3://%s/lagodb-connectors/drift/parquet/',
              :'storage_bucket') AS drift_parquet_prefix,
       format('s3://%s/lagodb-connectors/drift/parquet/part-a.parquet',
              :'storage_bucket') AS drift_parquet_part_a,
       format('s3://%s/lagodb-connectors/drift/parquet/part-b.parquet',
              :'storage_bucket') AS drift_parquet_part_b,
       format('s3://%s/lagodb-connectors/drift/avro/',
              :'storage_bucket') AS drift_avro_prefix,
       format('s3://%s/lagodb-connectors/drift/avro/part-a.avro',
              :'storage_bucket') AS drift_avro_part_a,
       format('s3://%s/lagodb-connectors/drift/avro/part-b.avro',
              :'storage_bucket') AS drift_avro_part_b,
       format('s3://%s/lagodb-connectors/catalog/source.txt',
              :'storage_bucket') AS catalog_source_path,
       format('s3://%s/lagodb-connectors/allowed/',
              :'storage_bucket') AS denied_scope,
       'lagodb-connectors/corrupt/truncated.json' AS corrupt_json_key,
       'lagodb-connectors/corrupt/truncated.parquet' AS corrupt_parquet_key
\gset object_error_

COPY lagodb_connectors_regress.common_source
TO :'object_error_corrupt_json_path'
WITH (storage_server 'lagodb_connectors_regress_s3', format 'json');
\setenv OBJECT_STORAGE_KEY :object_error_corrupt_json_key
\setenv OBJECT_STORAGE_TRUNCATE_BYTES 2
\! sh bin/object_storage_tool truncate
SELECT lagodb.invalidate_object_cache(
           :'object_error_corrupt_json_path', 'lagodb_connectors_regress_s3'
       ) AS cache_invalidated
\gset

COPY lagodb_connectors_regress.parquet_source
TO :'object_error_corrupt_parquet_path'
WITH (storage_server 'lagodb_connectors_regress_s3', format 'parquet');
\setenv OBJECT_STORAGE_KEY :object_error_corrupt_parquet_key
\setenv OBJECT_STORAGE_TRUNCATE_BYTES 8
\! sh bin/object_storage_tool truncate
SELECT lagodb.invalidate_object_cache(
           :'object_error_corrupt_parquet_path', 'lagodb_connectors_regress_s3'
       ) AS cache_invalidated
\gset

DROP TABLE IF EXISTS lagodb_connectors_regress.object_error_json_sink;
CREATE TABLE lagodb_connectors_regress.object_error_json_sink
    (:json_columns);
DROP TABLE IF EXISTS lagodb_connectors_regress.object_error_parquet_sink;
CREATE TABLE lagodb_connectors_regress.object_error_parquet_sink
    (:parquet_columns);

\set VERBOSITY sqlstate
COPY lagodb_connectors_regress.object_error_json_sink
FROM :'object_error_corrupt_json_path'
WITH (storage_server 'lagodb_connectors_regress_s3');
COPY lagodb_connectors_regress.object_error_parquet_sink
FROM :'object_error_corrupt_parquet_path'
WITH (storage_server 'lagodb_connectors_regress_s3');
\set VERBOSITY default

-- Avro and Parquet prefix readers reject a later object with a different
-- writer schema. Parquet direct COPY FROM uses the same collection contract.
COPY (
    SELECT id, text_col
    FROM lagodb_connectors_regress.common_source
    WHERE id = 1
) TO :'object_error_drift_parquet_part_a'
WITH (storage_server 'lagodb_connectors_regress_s3', format 'parquet');
COPY (
    SELECT id, integer_col
    FROM lagodb_connectors_regress.common_source
    WHERE id = 2
) TO :'object_error_drift_parquet_part_b'
WITH (storage_server 'lagodb_connectors_regress_s3', format 'parquet');
COPY (
    SELECT id, text_col
    FROM lagodb_connectors_regress.common_source
    WHERE id = 1
) TO :'object_error_drift_avro_part_a'
WITH (storage_server 'lagodb_connectors_regress_s3', format 'avro');
COPY (
    SELECT id, integer_col
    FROM lagodb_connectors_regress.common_source
    WHERE id = 2
) TO :'object_error_drift_avro_part_b'
WITH (storage_server 'lagodb_connectors_regress_s3', format 'avro');

CREATE FOREIGN TABLE lagodb_connectors_regress.object_error_drift_parquet (
    id integer,
    text_col text
)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'object_error_drift_parquet_prefix', format 'parquet');
CREATE FOREIGN TABLE lagodb_connectors_regress.object_error_drift_avro (
    id integer,
    text_col text
)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'object_error_drift_avro_prefix', format 'avro');
DROP TABLE IF EXISTS lagodb_connectors_regress.object_error_drift_parquet_sink;
CREATE TABLE lagodb_connectors_regress.object_error_drift_parquet_sink (
    id integer,
    text_col text
);

\set VERBOSITY sqlstate
SELECT count(*) FROM lagodb_connectors_regress.object_error_drift_parquet;
SELECT count(*) FROM lagodb_connectors_regress.object_error_drift_avro;
COPY lagodb_connectors_regress.object_error_drift_parquet_sink
FROM :'object_error_drift_parquet_prefix'
WITH (storage_server 'lagodb_connectors_regress_s3', format 'parquet');
\set VERBOSITY default

-- Provider mismatch and scope denial fail at DDL validation. Missing user
-- mapping fails when execution first acquires storage access.
CREATE SERVER lagodb_connectors_regress_provider_gcs
    FOREIGN DATA WRAPPER lakebase_fdw
    OPTIONS (provider 'gcs');
CREATE SERVER lagodb_connectors_regress_scope
    FOREIGN DATA WRAPPER lakebase_fdw
    OPTIONS (
        provider 's3_compatible',
        endpoint :'storage_endpoint',
        region :'storage_region',
        scope :'object_error_denied_scope',
        allow_http 'true',
        virtual_hosted_style_request 'false'
    );
CREATE USER MAPPING FOR PUBLIC
    SERVER lagodb_connectors_regress_scope
    OPTIONS (
        access_key_id :'storage_access_key_id',
        secret_access_key :'storage_secret_access_key'
    );
CREATE SERVER lagodb_connectors_regress_missing_mapping
    FOREIGN DATA WRAPPER lakebase_fdw
    OPTIONS (
        provider 's3_compatible',
        endpoint :'storage_endpoint',
        region :'storage_region',
        allow_http 'true',
        virtual_hosted_style_request 'false'
    );

\set VERBOSITY sqlstate
CREATE FOREIGN TABLE lagodb_connectors_regress.object_error_provider_mismatch (
    id integer
)
SERVER lagodb_connectors_regress_provider_gcs
OPTIONS (path :'object_error_catalog_source_path', format 'text');
CREATE FOREIGN TABLE lagodb_connectors_regress.object_error_scope_denied (
    id integer
)
SERVER lagodb_connectors_regress_scope
OPTIONS (path :'object_error_catalog_source_path', format 'text');
CREATE FOREIGN TABLE lagodb_connectors_regress.object_error_missing_mapping (
    id integer
)
SERVER lagodb_connectors_regress_missing_mapping
OPTIONS (path :'object_error_catalog_source_path', format 'text');
SELECT count(*)
FROM lagodb_connectors_regress.object_error_missing_mapping;
\set VERBOSITY default

RESET client_min_messages;
