\i include/column_definitions.sql

-- JSON format COPY, foreign-table, inference, and scan-state coverage.

SELECT endpoint, bucket, region, access_key_id, secret_access_key
FROM lagodb_regress.object_storage_fixture
\gset storage_

\setenv OBJECT_STORAGE_ENDPOINT :storage_endpoint
\setenv OBJECT_STORAGE_BUCKET :storage_bucket
\setenv OBJECT_STORAGE_REGION :storage_region
\setenv OBJECT_STORAGE_ACCESS_KEY_ID :storage_access_key_id
\setenv OBJECT_STORAGE_SECRET_ACCESS_KEY :storage_secret_access_key

\set json_base 's3://' :storage_bucket '/lagodb-connectors/json/'
\pset null _null_

-- JSON and JSONB values round-trip through an exact object.
\set json_exact_path :json_base 'exact.json'
COPY lagodb_connectors_regress.json_source TO :'json_exact_path'
WITH (server 'lagodb_connectors_regress_s3');
CREATE TABLE lagodb_connectors_regress.json_copy_sink (:json_columns);
COPY lagodb_connectors_regress.json_copy_sink FROM :'json_exact_path'
WITH (server 'lagodb_connectors_regress_s3');
SELECT * FROM lagodb_connectors_regress.json_copy_sink ORDER BY id;

-- The .ndjson suffix is an alias for the JSON format.
\set json_alias_path :json_base 'alias.ndjson'
COPY lagodb_connectors_regress.json_source TO :'json_alias_path'
WITH (server 'lagodb_connectors_regress_s3');
TRUNCATE lagodb_connectors_regress.json_copy_sink;
COPY lagodb_connectors_regress.json_copy_sink FROM :'json_alias_path'
WITH (server 'lagodb_connectors_regress_s3');
SELECT count(*) AS alias_rows FROM lagodb_connectors_regress.json_copy_sink;

-- An empty exact object remains valid JSON input.
\set json_empty_path :json_base 'empty.json'
COPY (SELECT * FROM lagodb_connectors_regress.json_source WHERE false)
TO :'json_empty_path'
WITH (server 'lagodb_connectors_regress_s3', format 'json');
TRUNCATE lagodb_connectors_regress.json_copy_sink;
COPY lagodb_connectors_regress.json_copy_sink FROM :'json_empty_path'
WITH (server 'lagodb_connectors_regress_s3', format 'json');
SELECT count(*) AS empty_rows FROM lagodb_connectors_regress.json_copy_sink;

-- Exact foreign scan and JSON-owned schema inference.
CREATE FOREIGN TABLE lagodb_connectors_regress.json_exact (:json_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'json_exact_path');
SELECT * FROM lagodb_connectors_regress.json_exact ORDER BY id;
CREATE FOREIGN TABLE lagodb_connectors_regress.json_inferred ()
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'json_exact_path');
SELECT attname, format_type(atttypid, atttypmod) AS type
FROM pg_attribute
WHERE attrelid = 'lagodb_connectors_regress.json_inferred'::regclass
  AND attnum > 0 AND NOT attisdropped
ORDER BY attnum;
SELECT count(*) AS inferred_rows FROM lagodb_connectors_regress.json_inferred;

-- A prefix scan reads every object under the prefix.
\set json_prefix_path :json_base 'prefix/'
\set json_part_path :json_prefix_path 'part-a.json'
COPY (SELECT * FROM lagodb_connectors_regress.json_source WHERE id = 1)
TO :'json_part_path'
WITH (server 'lagodb_connectors_regress_s3', format 'json');
\set json_part_path :json_prefix_path 'part-b.json'
COPY (SELECT * FROM lagodb_connectors_regress.json_source WHERE id = 2)
TO :'json_part_path'
WITH (server 'lagodb_connectors_regress_s3', format 'json');
CREATE FOREIGN TABLE lagodb_connectors_regress.json_prefix (:json_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'json_prefix_path', format 'json');
SELECT * FROM lagodb_connectors_regress.json_prefix ORDER BY id;

-- JsonScanState must restart for parameterized nested-loop rescans.
SELECT outer_rel.id AS outer_id, inner_rel.id AS inner_id
FROM (VALUES (1), (2), (999)) AS outer_rel(id)
LEFT JOIN LATERAL (
    SELECT id
    FROM lagodb_connectors_regress.json_prefix AS inner_rel
    WHERE inner_rel.id = outer_rel.id
    OFFSET 0
) AS inner_rel ON true
ORDER BY outer_rel.id;

-- Prefix foreign INSERT uses the JSON writer.
\set json_write_path :json_base 'write/'
CREATE FOREIGN TABLE lagodb_connectors_regress.json_write (:json_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'json_write_path', format 'json');
INSERT INTO lagodb_connectors_regress.json_write
SELECT * FROM lagodb_connectors_regress.json_source;
SELECT * FROM lagodb_connectors_regress.json_write ORDER BY id;

-- One truncated object protects the format's malformed-input error boundary.
\set json_corrupt_path :json_base 'corrupt.json'
COPY lagodb_connectors_regress.json_source TO :'json_corrupt_path'
WITH (server 'lagodb_connectors_regress_s3', format 'json');
\setenv OBJECT_STORAGE_KEY 'lagodb-connectors/json/corrupt.json'
\setenv OBJECT_STORAGE_TRUNCATE_BYTES 2
\! sh bin/object_storage_tool truncate
SELECT lagodb.invalidate_object_cache(
           :'json_corrupt_path', 'lagodb_connectors_regress_s3'
       )
\g /dev/null
\set VERBOSITY sqlstate
COPY lagodb_connectors_regress.json_copy_sink FROM :'json_corrupt_path'
WITH (server 'lagodb_connectors_regress_s3', format 'json');
\set VERBOSITY default

-- Each prefix member resolves its own codec: the INSERTs write gzip, zstd,
-- and plain objects.
\set json_mixed_path :json_base 'mixed/'
CREATE FOREIGN TABLE lagodb_connectors_regress.json_mixed (id integer, payload text)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'json_mixed_path', format 'json', compression 'gzip');
INSERT INTO lagodb_connectors_regress.json_mixed VALUES (1, 'gzip');
ALTER FOREIGN TABLE lagodb_connectors_regress.json_mixed OPTIONS (SET compression 'zstd');
INSERT INTO lagodb_connectors_regress.json_mixed VALUES (2, 'zstd');
ALTER FOREIGN TABLE lagodb_connectors_regress.json_mixed OPTIONS (DROP compression);
INSERT INTO lagodb_connectors_regress.json_mixed VALUES (3, 'plain');
SELECT * FROM lagodb_connectors_regress.json_mixed ORDER BY id;
