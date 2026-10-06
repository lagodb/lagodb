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

-- Infer a prefix schema from its first object, then scan every matching object.
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

-- A prefix with no matching objects is valid empty input.
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

-- Local foreign tables require no storage profile or user mapping.
BEGIN;
CREATE SERVER lagodb_connectors_regress_local FOREIGN DATA WRAPPER lagodb_connectors;
SELECT current_setting('data_directory') || '/lagodb-foreign-' || pg_backend_pid()
       AS local_foreign_root
\gset

CREATE TABLE lagodb_connectors_regress.local_foreign_source (id integer, payload text);
INSERT INTO lagodb_connectors_regress.local_foreign_source
VALUES (1, E'comma,value "quoted"\nline'), (2, NULL), (3, ''), (4, '中文');

-- json: infer the schema from a native local file and compare its values.
\set local_foreign_file :local_foreign_root '.json'
COPY lagodb_connectors_regress.local_foreign_source TO :'local_foreign_file';
CREATE FOREIGN TABLE lagodb_connectors_regress.local_json_exact ()
SERVER lagodb_connectors_regress_local OPTIONS (path :'local_foreign_file');

(TABLE lagodb_connectors_regress.local_json_exact
 EXCEPT ALL TABLE lagodb_connectors_regress.local_foreign_source)
UNION ALL
(TABLE lagodb_connectors_regress.local_foreign_source
 EXCEPT ALL TABLE lagodb_connectors_regress.local_json_exact);

-- Directory INSERT uses the same format writer with local publication.
\set local_foreign_directory :local_foreign_root '-json/'
CREATE FOREIGN TABLE lagodb_connectors_regress.local_json_directory (id integer, payload text)
SERVER lagodb_connectors_regress_local
OPTIONS (path :'local_foreign_directory', format 'json');
INSERT INTO lagodb_connectors_regress.local_json_directory
SELECT * FROM lagodb_connectors_regress.local_foreign_source;

(TABLE lagodb_connectors_regress.local_json_directory
 EXCEPT ALL TABLE lagodb_connectors_regress.local_foreign_source)
UNION ALL
(TABLE lagodb_connectors_regress.local_foreign_source
 EXCEPT ALL TABLE lagodb_connectors_regress.local_json_directory);

-- A successful subtransaction write must disappear on savepoint rollback.
SAVEPOINT local_insert;
INSERT INTO lagodb_connectors_regress.local_json_directory VALUES (5, 'rollback');
SELECT count(*) AS rows_before_rollback
FROM lagodb_connectors_regress.local_json_directory;

ROLLBACK TO SAVEPOINT local_insert;
RELEASE SAVEPOINT local_insert;

(TABLE lagodb_connectors_regress.local_json_directory
 EXCEPT ALL TABLE lagodb_connectors_regress.local_foreign_source)
UNION ALL
(TABLE lagodb_connectors_regress.local_foreign_source
 EXCEPT ALL TABLE lagodb_connectors_regress.local_json_directory);

-- avro: infer the schema from a native local file and compare its values.
\set local_foreign_file :local_foreign_root '.avro'
COPY lagodb_connectors_regress.local_foreign_source TO :'local_foreign_file';
CREATE FOREIGN TABLE lagodb_connectors_regress.local_avro_exact ()
SERVER lagodb_connectors_regress_local OPTIONS (path :'local_foreign_file');

(TABLE lagodb_connectors_regress.local_avro_exact
 EXCEPT ALL TABLE lagodb_connectors_regress.local_foreign_source)
UNION ALL
(TABLE lagodb_connectors_regress.local_foreign_source
 EXCEPT ALL TABLE lagodb_connectors_regress.local_avro_exact);

-- Directory INSERT uses the same format writer with local publication.
\set local_foreign_directory :local_foreign_root '-avro/'
CREATE FOREIGN TABLE lagodb_connectors_regress.local_avro_directory (id integer, payload text)
SERVER lagodb_connectors_regress_local
OPTIONS (path :'local_foreign_directory', format 'avro');
INSERT INTO lagodb_connectors_regress.local_avro_directory
SELECT * FROM lagodb_connectors_regress.local_foreign_source;

(TABLE lagodb_connectors_regress.local_avro_directory
 EXCEPT ALL TABLE lagodb_connectors_regress.local_foreign_source)
UNION ALL
(TABLE lagodb_connectors_regress.local_foreign_source
 EXCEPT ALL TABLE lagodb_connectors_regress.local_avro_directory);

-- A successful subtransaction write must disappear on savepoint rollback.
SAVEPOINT local_insert;
INSERT INTO lagodb_connectors_regress.local_avro_directory VALUES (5, 'rollback');
SELECT count(*) AS rows_before_rollback
FROM lagodb_connectors_regress.local_avro_directory;

ROLLBACK TO SAVEPOINT local_insert;
RELEASE SAVEPOINT local_insert;

(TABLE lagodb_connectors_regress.local_avro_directory
 EXCEPT ALL TABLE lagodb_connectors_regress.local_foreign_source)
UNION ALL
(TABLE lagodb_connectors_regress.local_foreign_source
 EXCEPT ALL TABLE lagodb_connectors_regress.local_avro_directory);

-- parquet: infer the schema from a native local file and compare its values.
\set local_foreign_file :local_foreign_root '.parquet'
COPY lagodb_connectors_regress.local_foreign_source TO :'local_foreign_file';
CREATE FOREIGN TABLE lagodb_connectors_regress.local_parquet_exact ()
SERVER lagodb_connectors_regress_local OPTIONS (path :'local_foreign_file');

(TABLE lagodb_connectors_regress.local_parquet_exact
 EXCEPT ALL TABLE lagodb_connectors_regress.local_foreign_source)
UNION ALL
(TABLE lagodb_connectors_regress.local_foreign_source
 EXCEPT ALL TABLE lagodb_connectors_regress.local_parquet_exact);

-- Directory INSERT uses the same format writer with local publication.
\set local_foreign_directory :local_foreign_root '-parquet/'
CREATE FOREIGN TABLE lagodb_connectors_regress.local_parquet_directory (id integer, payload text)
SERVER lagodb_connectors_regress_local
OPTIONS (path :'local_foreign_directory', format 'parquet');
INSERT INTO lagodb_connectors_regress.local_parquet_directory
SELECT * FROM lagodb_connectors_regress.local_foreign_source;

(TABLE lagodb_connectors_regress.local_parquet_directory
 EXCEPT ALL TABLE lagodb_connectors_regress.local_foreign_source)
UNION ALL
(TABLE lagodb_connectors_regress.local_foreign_source
 EXCEPT ALL TABLE lagodb_connectors_regress.local_parquet_directory);

-- A successful subtransaction write must disappear on savepoint rollback.
SAVEPOINT local_insert;
INSERT INTO lagodb_connectors_regress.local_parquet_directory VALUES (5, 'rollback');
SELECT count(*) AS rows_before_rollback
FROM lagodb_connectors_regress.local_parquet_directory;

ROLLBACK TO SAVEPOINT local_insert;
RELEASE SAVEPOINT local_insert;

(TABLE lagodb_connectors_regress.local_parquet_directory
 EXCEPT ALL TABLE lagodb_connectors_regress.local_foreign_source)
UNION ALL
(TABLE lagodb_connectors_regress.local_foreign_source
 EXCEPT ALL TABLE lagodb_connectors_regress.local_parquet_directory);

-- COPY directory publication is also readable through a foreign table.
\set local_foreign_directory :local_foreign_root '-copy-json/'
COPY lagodb_connectors_regress.local_foreign_source TO :'local_foreign_directory'
WITH (format 'json');
CREATE FOREIGN TABLE lagodb_connectors_regress.local_copy_directory (id integer, payload text)
SERVER lagodb_connectors_regress_local
OPTIONS (path :'local_foreign_directory', format 'json');

(TABLE lagodb_connectors_regress.local_copy_directory
 EXCEPT ALL TABLE lagodb_connectors_regress.local_foreign_source)
UNION ALL
(TABLE lagodb_connectors_regress.local_foreign_source
 EXCEPT ALL TABLE lagodb_connectors_regress.local_copy_directory);

-- Exact local files share the FDW read-only contract across formats.
SAVEPOINT exact_write;
\set VERBOSITY sqlstate
INSERT INTO lagodb_connectors_regress.local_parquet_exact VALUES (5, 'read-only');

ROLLBACK TO SAVEPOINT exact_write;
RELEASE SAVEPOINT exact_write;
\set VERBOSITY default
CREATE ROLE lagodb_connectors_regress_local_reader;
GRANT USAGE, CREATE ON SCHEMA lagodb_connectors_regress TO lagodb_connectors_regress_local_reader;
GRANT USAGE ON FOREIGN SERVER lagodb_connectors_regress_local TO lagodb_connectors_regress_local_reader;
GRANT SELECT ON lagodb_connectors_regress.local_foreign_source,
    lagodb_connectors_regress.local_json_exact TO lagodb_connectors_regress_local_reader;
GRANT SELECT, INSERT ON lagodb_connectors_regress.local_parquet_directory
    TO lagodb_connectors_regress_local_reader;
CREATE TABLE lagodb_connectors_regress.local_privilege_sink (id integer, payload text);
GRANT INSERT ON lagodb_connectors_regress.local_privilege_sink TO lagodb_connectors_regress_local_reader;
\set local_foreign_file :local_foreign_root '.parquet'
SET LOCAL ROLE lagodb_connectors_regress_local_reader;
SELECT count(*) AS table_granted_rows FROM lagodb_connectors_regress.local_json_exact;

\set VERBOSITY sqlstate
SAVEPOINT copy_read;
COPY lagodb_connectors_regress.local_privilege_sink FROM :'local_foreign_file';

ROLLBACK TO SAVEPOINT copy_read;
RELEASE SAVEPOINT copy_read;

SAVEPOINT copy_write;
COPY lagodb_connectors_regress.local_foreign_source TO :'local_foreign_file';

ROLLBACK TO SAVEPOINT copy_write;
RELEASE SAVEPOINT copy_write;

SAVEPOINT path_ddl;
CREATE FOREIGN TABLE lagodb_connectors_regress.local_denied ()
SERVER lagodb_connectors_regress_local OPTIONS (path :'local_foreign_file');

ROLLBACK TO SAVEPOINT path_ddl;
RELEASE SAVEPOINT path_ddl;

SAVEPOINT foreign_write;
INSERT INTO lagodb_connectors_regress.local_parquet_directory VALUES (5, 'denied');

ROLLBACK TO SAVEPOINT foreign_write;
RELEASE SAVEPOINT foreign_write;
\set VERBOSITY default
RESET ROLE;
GRANT pg_write_server_files TO lagodb_connectors_regress_local_reader;
SET LOCAL ROLE lagodb_connectors_regress_local_reader;
INSERT INTO lagodb_connectors_regress.local_parquet_directory VALUES (5, 'authorized');
SELECT count(*) AS authorized_rows FROM lagodb_connectors_regress.local_parquet_directory;

RESET ROLE;
ROLLBACK;
