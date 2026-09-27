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

SELECT format('s3://%s/lagodb-connectors/json/exact.json',
              :'storage_bucket') AS exact_path,
       format('s3://%s/lagodb-connectors/json/prefix/',
              :'storage_bucket') AS prefix_path,
       format('s3://%s/lagodb-connectors/json/prefix/part-a.json',
              :'storage_bucket') AS part_a_path,
       format('s3://%s/lagodb-connectors/json/prefix/part-b.json',
              :'storage_bucket') AS part_b_path,
       format('s3://%s/lagodb-connectors/json/compressed.json.gz',
              :'storage_bucket') AS compressed_path,
       format('s3://%s/lagodb-connectors/json/alias.ndjson',
              :'storage_bucket') AS alias_path,
       format('s3://%s/lagodb-connectors/json/empty.json',
              :'storage_bucket') AS empty_path,
       format('s3://%s/lagodb-connectors/json/write/',
              :'storage_bucket') AS write_path,
       format('s3://%s/lagodb-connectors/json/corrupt.json',
              :'storage_bucket') AS corrupt_path,
       'lagodb-connectors/json/corrupt.json' AS corrupt_key
\gset json_

-- JSON and JSONB values round-trip through exact and compressed COPY paths.
COPY lagodb_connectors_regress.json_source
TO :'json_exact_path'
WITH (server 'lagodb_connectors_regress_s3');
COPY (
    SELECT * FROM lagodb_connectors_regress.json_source WHERE id = 1
) TO :'json_part_a_path'
WITH (server 'lagodb_connectors_regress_s3', format 'json');
COPY (
    SELECT * FROM lagodb_connectors_regress.json_source WHERE id = 2
) TO :'json_part_b_path'
WITH (server 'lagodb_connectors_regress_s3', format 'json');
COPY lagodb_connectors_regress.json_source
TO :'json_compressed_path'
WITH (
    server 'lagodb_connectors_regress_s3',
    format 'json',
    compression 'gzip'
);

-- Normalize json values for equality; compare both directions with ALL
-- so missing, extra, and duplicate rows are visible without aggregation.
CREATE TABLE lagodb_connectors_regress.json_copy_sink (:json_columns);
COPY lagodb_connectors_regress.json_copy_sink
FROM :'json_exact_path'
WITH (server 'lagodb_connectors_regress_s3');
(SELECT to_jsonb(value) AS row_data FROM lagodb_connectors_regress.json_copy_sink AS value
 EXCEPT ALL SELECT to_jsonb(value) FROM lagodb_connectors_regress.json_source AS value)
UNION ALL
(SELECT to_jsonb(value) FROM lagodb_connectors_regress.json_source AS value
 EXCEPT ALL SELECT to_jsonb(value) FROM lagodb_connectors_regress.json_copy_sink AS value);

TRUNCATE lagodb_connectors_regress.json_copy_sink;
COPY lagodb_connectors_regress.json_copy_sink
FROM :'json_compressed_path'
WITH (server 'lagodb_connectors_regress_s3', format 'json');
(SELECT to_jsonb(value) AS row_data FROM lagodb_connectors_regress.json_copy_sink AS value
 EXCEPT ALL SELECT to_jsonb(value) FROM lagodb_connectors_regress.json_source AS value)
UNION ALL
(SELECT to_jsonb(value) FROM lagodb_connectors_regress.json_source AS value
 EXCEPT ALL SELECT to_jsonb(value) FROM lagodb_connectors_regress.json_copy_sink AS value);

COPY lagodb_connectors_regress.json_source
TO :'json_alias_path'
WITH (server 'lagodb_connectors_regress_s3');
TRUNCATE lagodb_connectors_regress.json_copy_sink;
COPY lagodb_connectors_regress.json_copy_sink
FROM :'json_alias_path'
WITH (server 'lagodb_connectors_regress_s3');
SELECT count(*) AS alias_rows
FROM lagodb_connectors_regress.json_copy_sink;

-- An empty exact object remains valid JSON input.
COPY (
    SELECT * FROM lagodb_connectors_regress.json_source WHERE false
) TO :'json_empty_path'
WITH (server 'lagodb_connectors_regress_s3', format 'json');
TRUNCATE lagodb_connectors_regress.json_copy_sink;
COPY lagodb_connectors_regress.json_copy_sink
FROM :'json_empty_path'
WITH (server 'lagodb_connectors_regress_s3', format 'json');
SELECT count(*) AS empty_rows
FROM lagodb_connectors_regress.json_copy_sink;

-- Exact/prefix foreign scans and JSON-owned schema inference.
CREATE FOREIGN TABLE lagodb_connectors_regress.json_exact
    (:json_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'json_exact_path');
CREATE FOREIGN TABLE lagodb_connectors_regress.json_prefix
    (:json_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'json_prefix_path', format 'json');
CREATE FOREIGN TABLE lagodb_connectors_regress.json_inferred ()
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'json_compressed_path');

(SELECT to_jsonb(value) AS row_data FROM lagodb_connectors_regress.json_exact AS value
 EXCEPT ALL SELECT to_jsonb(value) FROM lagodb_connectors_regress.json_source AS value)
UNION ALL
(SELECT to_jsonb(value) FROM lagodb_connectors_regress.json_source AS value
 EXCEPT ALL SELECT to_jsonb(value) FROM lagodb_connectors_regress.json_exact AS value);

(SELECT to_jsonb(value) AS row_data FROM lagodb_connectors_regress.json_prefix AS value
 EXCEPT ALL SELECT to_jsonb(value) FROM lagodb_connectors_regress.json_source AS value)
UNION ALL
(SELECT to_jsonb(value) FROM lagodb_connectors_regress.json_source AS value
 EXCEPT ALL SELECT to_jsonb(value) FROM lagodb_connectors_regress.json_prefix AS value);

SELECT attname, format_type(atttypid, atttypmod) AS type
FROM pg_attribute
WHERE attrelid = 'lagodb_connectors_regress.json_inferred'::regclass
  AND attnum > 0 AND NOT attisdropped
ORDER BY attnum;
SELECT count(*) AS inferred_rows
FROM lagodb_connectors_regress.json_inferred;

CREATE FOREIGN TABLE lagodb_connectors_regress.json_write
    (:json_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'json_write_path', format 'json');
INSERT INTO lagodb_connectors_regress.json_write
SELECT * FROM lagodb_connectors_regress.json_source;
(SELECT to_jsonb(value) AS row_data FROM lagodb_connectors_regress.json_write AS value
 EXCEPT ALL SELECT to_jsonb(value) FROM lagodb_connectors_regress.json_source AS value)
UNION ALL
(SELECT to_jsonb(value) FROM lagodb_connectors_regress.json_source AS value
 EXCEPT ALL SELECT to_jsonb(value) FROM lagodb_connectors_regress.json_write AS value);

-- JsonScanState must restart for parameterized nested-loop rescans.
SET enable_hashjoin = off;
SET enable_mergejoin = off;
SET enable_material = off;
SET enable_nestloop = on;
SELECT outer_rel.id AS outer_id, inner_rel.id AS inner_id
FROM (VALUES (1), (2), (999)) AS outer_rel(id)
LEFT JOIN LATERAL (
    SELECT id
    FROM lagodb_connectors_regress.json_prefix AS inner_rel
    WHERE inner_rel.id = outer_rel.id
    OFFSET 0
) AS inner_rel ON true
ORDER BY outer_rel.id;
RESET enable_hashjoin;
RESET enable_mergejoin;
RESET enable_material;
RESET enable_nestloop;

-- One truncated object protects the format's malformed-input error boundary.
COPY lagodb_connectors_regress.json_source
TO :'json_corrupt_path'
WITH (server 'lagodb_connectors_regress_s3', format 'json');
\setenv OBJECT_STORAGE_KEY :json_corrupt_key
\setenv OBJECT_STORAGE_TRUNCATE_BYTES 2
\! sh bin/object_storage_tool truncate
SELECT lagodb.invalidate_object_cache(
           :'json_corrupt_path', 'lagodb_connectors_regress_s3'
       ) AS cache_invalidated
\gset

\set VERBOSITY sqlstate
COPY lagodb_connectors_regress.json_copy_sink
FROM :'json_corrupt_path'
WITH (server 'lagodb_connectors_regress_s3', format 'json');
\set VERBOSITY default
