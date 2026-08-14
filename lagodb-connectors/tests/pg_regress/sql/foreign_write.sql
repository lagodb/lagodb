-- Foreign-table INSERT paths and write-capability boundaries.

SET TIME ZONE 'UTC';
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

SELECT format('s3://%s/lagodb-connectors/foreign-write/text/',
              :'storage_bucket') AS text_path,
       format('s3://%s/lagodb-connectors/foreign-write/csv/',
              :'storage_bucket') AS csv_path,
       format('s3://%s/lagodb-connectors/foreign-write/json/',
              :'storage_bucket') AS json_path,
       format('s3://%s/lagodb-connectors/foreign-write/avro/',
              :'storage_bucket') AS avro_path,
       format('s3://%s/lagodb-connectors/foreign-write/parquet/',
              :'storage_bucket') AS parquet_path,
       format('s3://%s/lagodb-connectors/foreign-write/exact/text.txt',
              :'storage_bucket') AS exact_text_path,
       format('s3://%s/lagodb-connectors/foreign-write/exact/csv.csv',
              :'storage_bucket') AS exact_csv_path,
       format('s3://%s/lagodb-connectors/foreign-write/exact/json.json',
              :'storage_bucket') AS exact_json_path,
       format('s3://%s/lagodb-connectors/foreign-write/exact/avro.avro',
              :'storage_bucket') AS exact_avro_path,
       format('s3://%s/lagodb-connectors/foreign-write/exact/parquet.parquet',
              :'storage_bucket') AS exact_parquet_path,
       format('s3://%s/lagodb-connectors/foreign-write/dml/',
              :'storage_bucket') AS dml_path,
       'lagodb-connectors/foreign-write/exact/' AS exact_prefix_key,
       'lagodb-connectors/foreign-write/dml/' AS dml_prefix_key
\gset write_

CREATE FOREIGN TABLE lagodb_connectors_regress.write_text
    (LIKE lagodb_connectors_regress.common_source)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'write_text_path', format 'text');

CREATE FOREIGN TABLE lagodb_connectors_regress.write_csv
    (LIKE lagodb_connectors_regress.common_source)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'write_csv_path', format 'csv');

CREATE FOREIGN TABLE lagodb_connectors_regress.write_json
    (LIKE lagodb_connectors_regress.json_source)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'write_json_path', format 'json');

CREATE FOREIGN TABLE lagodb_connectors_regress.write_avro
    (LIKE lagodb_connectors_regress.common_source)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'write_avro_path', format 'avro');

CREATE FOREIGN TABLE lagodb_connectors_regress.write_parquet
    (LIKE lagodb_connectors_regress.parquet_source)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'write_parquet_path', format 'parquet');

INSERT INTO lagodb_connectors_regress.write_text
SELECT * FROM lagodb_connectors_regress.common_source;
INSERT INTO lagodb_connectors_regress.write_csv
SELECT * FROM lagodb_connectors_regress.common_source;
INSERT INTO lagodb_connectors_regress.write_json
SELECT * FROM lagodb_connectors_regress.json_source;
INSERT INTO lagodb_connectors_regress.write_avro
SELECT * FROM lagodb_connectors_regress.common_source;
INSERT INTO lagodb_connectors_regress.write_parquet
SELECT * FROM lagodb_connectors_regress.parquet_source;

SELECT relation, rows, digest
FROM (
    SELECT 'text' AS relation, count(*) AS rows,
           md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id)) AS digest
    FROM lagodb_connectors_regress.write_text AS value
    UNION ALL
    SELECT 'csv', count(*),
           md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id))
    FROM lagodb_connectors_regress.write_csv AS value
    UNION ALL
    SELECT 'json', count(*),
           md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id))
    FROM lagodb_connectors_regress.write_json AS value
    UNION ALL
    SELECT 'avro', count(*),
           md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id))
    FROM lagodb_connectors_regress.write_avro AS value
    UNION ALL
    SELECT 'parquet', count(*),
           md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id))
    FROM lagodb_connectors_regress.write_parquet AS value
) AS write_results
ORDER BY relation;

-- Exact locations are read-only foreign-table targets for every format.
CREATE FOREIGN TABLE lagodb_connectors_regress.write_exact_text
    (LIKE lagodb_connectors_regress.common_source)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'write_exact_text_path', format 'text');
CREATE FOREIGN TABLE lagodb_connectors_regress.write_exact_csv
    (LIKE lagodb_connectors_regress.common_source)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'write_exact_csv_path', format 'csv');
CREATE FOREIGN TABLE lagodb_connectors_regress.write_exact_json
    (LIKE lagodb_connectors_regress.json_source)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'write_exact_json_path', format 'json');
CREATE FOREIGN TABLE lagodb_connectors_regress.write_exact_avro
    (LIKE lagodb_connectors_regress.common_source)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'write_exact_avro_path', format 'avro');
CREATE FOREIGN TABLE lagodb_connectors_regress.write_exact_parquet
    (LIKE lagodb_connectors_regress.parquet_source)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'write_exact_parquet_path', format 'parquet');

\set VERBOSITY sqlstate
INSERT INTO lagodb_connectors_regress.write_exact_text
SELECT * FROM lagodb_connectors_regress.common_source;
INSERT INTO lagodb_connectors_regress.write_exact_csv
SELECT * FROM lagodb_connectors_regress.common_source;
INSERT INTO lagodb_connectors_regress.write_exact_json
SELECT * FROM lagodb_connectors_regress.json_source;
INSERT INTO lagodb_connectors_regress.write_exact_avro
SELECT * FROM lagodb_connectors_regress.common_source;
INSERT INTO lagodb_connectors_regress.write_exact_parquet
SELECT * FROM lagodb_connectors_regress.parquet_source;
\set VERBOSITY default

\setenv OBJECT_STORAGE_PREFIX :write_exact_prefix_key
\! sh bin/object_storage_tool assert-prefix-empty

-- UPDATE and DELETE are not supported for prefix foreign tables. Planning
-- fails before a remote writer is opened, and the prefix remains empty.
CREATE FOREIGN TABLE lagodb_connectors_regress.write_dml_boundary (
    id integer,
    payload text
)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'write_dml_path', format 'text');

\set VERBOSITY sqlstate
UPDATE lagodb_connectors_regress.write_dml_boundary
SET payload = 'updated';
DELETE FROM lagodb_connectors_regress.write_dml_boundary;
\set VERBOSITY default

\setenv OBJECT_STORAGE_PREFIX :write_dml_prefix_key
\! sh bin/object_storage_tool assert-prefix-empty

RESET client_min_messages;
