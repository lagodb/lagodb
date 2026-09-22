\i include/column_definitions.sql

-- Common foreign-table capability and storage-boundary coverage.

SELECT endpoint, bucket, region, access_key_id, secret_access_key
FROM lagodb_regress.object_storage_fixture
\gset storage_

SELECT format('s3://%s/lagodb-connectors/foreign/common.txt',
              :'storage_bucket') AS exact_path,
       format('s3://%s/lagodb-connectors/foreign/inference/',
              :'storage_bucket') AS inference_prefix,
       format('s3://%s/lagodb-connectors/foreign/inference/part-a.txt',
              :'storage_bucket') AS inference_part_a,
       format('s3://%s/lagodb-connectors/foreign/inference/part-b.txt',
              :'storage_bucket') AS inference_part_b,
       format('s3://%s/lagodb-connectors/foreign/empty-prefix/',
              :'storage_bucket') AS empty_prefix,
       format('s3://%s/lagodb-connectors/foreign/dml/',
              :'storage_bucket') AS dml_prefix,
       format('s3://%s/lagodb-connectors/foreign/allowed/',
              :'storage_bucket') AS denied_scope
\gset foreign_

COPY lagodb_connectors_regress.common_source
TO :'foreign_exact_path'
WITH (server 'lagodb_connectors_regress_s3', format 'text');

-- Prefix schema inference is a common DDL/object-discovery capability. The
-- format suites cover each concrete schema reader with an exact object; this
-- representative prefix protects LIST, stable first-object selection, schema
-- installation, and the subsequent scan without repeating it for all formats.
COPY (SELECT 1::integer AS id, 'alpha'::text AS payload)
TO :'foreign_inference_part_a'
WITH (server 'lagodb_connectors_regress_s3', format 'text');
COPY (SELECT 2::integer AS id, 'beta'::text AS payload)
TO :'foreign_inference_part_b'
WITH (server 'lagodb_connectors_regress_s3', format 'text');
CREATE FOREIGN TABLE lagodb_connectors_regress.foreign_inferred_prefix ()
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'foreign_inference_prefix', format 'text');
SELECT count(*) AS inferred_columns,
       string_agg(format_type(atttypid, atttypmod), ', ' ORDER BY attnum)
           AS inferred_types
FROM pg_attribute
WHERE attrelid =
          'lagodb_connectors_regress.foreign_inferred_prefix'::regclass
  AND attnum > 0
  AND NOT attisdropped;
SELECT count(*) AS inferred_rows,
       array_agg(to_jsonb(value) ORDER BY to_jsonb(value)) AS inferred_values
FROM lagodb_connectors_regress.foreign_inferred_prefix AS value;

-- A prefix with no matching objects is a valid empty foreign-table input. This
-- is object-discovery behavior and is therefore tested once, not per format.
CREATE FOREIGN TABLE lagodb_connectors_regress.foreign_empty_prefix
    (:common_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'foreign_empty_prefix', format 'text');
SELECT count(*) AS empty_prefix_rows
FROM lagodb_connectors_regress.foreign_empty_prefix;

-- Prefix targets support INSERT. Exact objects, UPDATE, and DELETE are outside
-- the connector FDW write contract.
CREATE FOREIGN TABLE lagodb_connectors_regress.foreign_exact_write_boundary
    (:common_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'foreign_exact_path', format 'text');
CREATE FOREIGN TABLE lagodb_connectors_regress.foreign_dml_boundary (
    id integer,
    payload text
)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'foreign_dml_prefix', format 'text');

\set VERBOSITY sqlstate
INSERT INTO lagodb_connectors_regress.foreign_exact_write_boundary
SELECT * FROM lagodb_connectors_regress.common_source;
UPDATE lagodb_connectors_regress.foreign_dml_boundary
SET payload = 'updated';
DELETE FROM lagodb_connectors_regress.foreign_dml_boundary;
\set VERBOSITY default

SELECT count(*) AS rows_after_rejected_dml
FROM lagodb_connectors_regress.foreign_dml_boundary;

-- Missing credentials and a path outside the configured scope are the two
-- representative catalog/storage configuration errors.
CREATE SERVER lagodb_connectors_regress_scope
    FOREIGN DATA WRAPPER lagodb_connectors
    OPTIONS (
        provider 's3_compatible',
        endpoint :'storage_endpoint',
        region :'storage_region',
        scope :'foreign_denied_scope',
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
    FOREIGN DATA WRAPPER lagodb_connectors
    OPTIONS (
        provider 's3_compatible',
        endpoint :'storage_endpoint',
        region :'storage_region',
        allow_http 'true',
        virtual_hosted_style_request 'false'
    );

\set VERBOSITY sqlstate
CREATE FOREIGN TABLE lagodb_connectors_regress.foreign_scope_denied (
    id integer
)
SERVER lagodb_connectors_regress_scope
OPTIONS (path :'foreign_exact_path', format 'text');
CREATE FOREIGN TABLE lagodb_connectors_regress.foreign_missing_mapping (
    id integer
)
SERVER lagodb_connectors_regress_missing_mapping
OPTIONS (path :'foreign_exact_path', format 'text');
\set VERBOSITY default
