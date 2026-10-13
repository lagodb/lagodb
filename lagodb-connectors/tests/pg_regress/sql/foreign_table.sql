\i include/column_definitions.sql

-- Common foreign-table capability and storage-boundary coverage.

SELECT endpoint, bucket, region, access_key_id, secret_access_key
FROM lagodb_regress.object_storage_fixture
\gset storage_

\set foreign_exact_path 's3://' :storage_bucket '/lagodb-connectors/foreign/common.txt'
\set foreign_inference_prefix 's3://' :storage_bucket '/lagodb-connectors/foreign/inference/'
\set foreign_inference_part_a 's3://' :storage_bucket '/lagodb-connectors/foreign/inference/part-a.txt'
\set foreign_inference_part_b 's3://' :storage_bucket '/lagodb-connectors/foreign/inference/part-b.txt'
\set foreign_empty_prefix 's3://' :storage_bucket '/lagodb-connectors/foreign/empty-prefix/'
\set foreign_dml_prefix 's3://' :storage_bucket '/lagodb-connectors/foreign/dml/'
\set foreign_denied_scope 's3://' :storage_bucket '/lagodb-connectors/foreign/allowed/'

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

-- Explicit server scope restrictions still apply.
CREATE SERVER lagodb_connectors_regress_scope
    FOREIGN DATA WRAPPER lagodb_connectors
    OPTIONS (
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
\set VERBOSITY sqlstate
CREATE FOREIGN TABLE lagodb_connectors_regress.foreign_scope_denied (
    id integer
)
SERVER lagodb_connectors_regress_scope
OPTIONS (path :'foreign_exact_path', format 'text');
\set VERBOSITY default

-- Configuration-only coverage: no process credentials or AWS profile are needed.
CREATE SERVER credential_storage
FOREIGN DATA WRAPPER lagodb_connectors
OPTIONS (
    provider 's3_compatible', endpoint :'storage_endpoint',
    region :'storage_region', allow_http 'true',
    virtual_hosted_style_request 'false'
);
-- Explicit columns allow declaration without resolving default-chain credentials.
CREATE FOREIGN TABLE lagodb_connectors_regress.foreign_default_chain (id integer)
SERVER credential_storage OPTIONS (path :'foreign_exact_path', format 'text');
CREATE USER MAPPING FOR CURRENT_USER SERVER credential_storage
OPTIONS (aws_profile 'configuration-only-profile');
-- Explicit identities must be complete and must not be combined with a profile.
\set VERBOSITY terse
ALTER USER MAPPING FOR CURRENT_USER SERVER credential_storage
OPTIONS (ADD access_key_id 'incomplete');
ALTER USER MAPPING FOR CURRENT_USER SERVER credential_storage
OPTIONS (ADD access_key_id 'explicit', ADD secret_access_key 'explicit-secret');
\set VERBOSITY default
DROP FOREIGN TABLE lagodb_connectors_regress.foreign_default_chain;
DROP USER MAPPING FOR CURRENT_USER SERVER credential_storage;
DROP SERVER credential_storage;

-- Local foreign tables require no storage profile or user mapping.
-- Keep fixtures outside write transactions so permission errors run directly.
-- Directory writes still roll back their published objects after each case.
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
BEGIN;
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

ROLLBACK;

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
BEGIN;
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

ROLLBACK;

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
BEGIN;
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

ROLLBACK;

-- COPY directory publication is also readable through a foreign table.
\set local_foreign_directory :local_foreign_root '-copy-json/'
BEGIN;
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

ROLLBACK;

-- Exact local files share the FDW read-only contract across formats.
\set VERBOSITY sqlstate
INSERT INTO lagodb_connectors_regress.local_parquet_exact VALUES (5, 'read-only');

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
SET ROLE lagodb_connectors_regress_local_reader;
SELECT count(*) AS table_granted_rows FROM lagodb_connectors_regress.local_json_exact;

\set VERBOSITY sqlstate
COPY lagodb_connectors_regress.local_privilege_sink FROM :'local_foreign_file';

COPY lagodb_connectors_regress.local_foreign_source TO :'local_foreign_file';

CREATE FOREIGN TABLE lagodb_connectors_regress.local_denied ()
SERVER lagodb_connectors_regress_local OPTIONS (path :'local_foreign_file');

INSERT INTO lagodb_connectors_regress.local_parquet_directory VALUES (5, 'denied');

\set VERBOSITY default
RESET ROLE;
GRANT pg_write_server_files TO lagodb_connectors_regress_local_reader;
BEGIN;
INSERT INTO lagodb_connectors_regress.local_parquet_directory
SELECT * FROM lagodb_connectors_regress.local_foreign_source;
SET LOCAL ROLE lagodb_connectors_regress_local_reader;
INSERT INTO lagodb_connectors_regress.local_parquet_directory VALUES (5, 'authorized');
SELECT count(*) AS authorized_rows FROM lagodb_connectors_regress.local_parquet_directory;

RESET ROLE;
ROLLBACK;
DROP OWNED BY lagodb_connectors_regress_local_reader;
DROP ROLE lagodb_connectors_regress_local_reader;
DROP FOREIGN TABLE lagodb_connectors_regress.local_json_exact,
    lagodb_connectors_regress.local_json_directory,
    lagodb_connectors_regress.local_avro_exact,
    lagodb_connectors_regress.local_avro_directory,
    lagodb_connectors_regress.local_parquet_exact,
    lagodb_connectors_regress.local_parquet_directory;
DROP TABLE lagodb_connectors_regress.local_privilege_sink,
    lagodb_connectors_regress.local_foreign_source;
DROP SERVER lagodb_connectors_regress_local;
