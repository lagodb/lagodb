-- Prefix rollover for every writer implementation.

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

SELECT format('s3://%s/lagodb-connectors/rollover/text/',
              :'storage_bucket') AS text_path,
       format('s3://%s/lagodb-connectors/rollover/csv/',
              :'storage_bucket') AS csv_path,
       format('s3://%s/lagodb-connectors/rollover/json/',
              :'storage_bucket') AS json_path,
       format('s3://%s/lagodb-connectors/rollover/avro/',
              :'storage_bucket') AS avro_path,
       format('s3://%s/lagodb-connectors/rollover/parquet/',
              :'storage_bucket') AS parquet_path,
       'lagodb-connectors/rollover/text/' AS text_key,
       'lagodb-connectors/rollover/csv/' AS csv_key,
       'lagodb-connectors/rollover/json/' AS json_key,
       'lagodb-connectors/rollover/avro/' AS avro_key,
       'lagodb-connectors/rollover/parquet/' AS parquet_key
\gset rollover_

SET lagodb_connectors.target_file_size_mb = 1;
DROP TABLE IF EXISTS lagodb_connectors_regress.rollover_source CASCADE;
CREATE TABLE lagodb_connectors_regress.rollover_source (
    id integer,
    payload text
);
INSERT INTO lagodb_connectors_regress.rollover_source
SELECT row_id,
       string_agg(md5(row_id::text || ':' || chunk_id::text), '' ORDER BY chunk_id)
FROM generate_series(1, 800) AS row_values(row_id)
CROSS JOIN generate_series(1, 800) AS chunk_values(chunk_id)
GROUP BY row_id
ORDER BY row_id;

COPY lagodb_connectors_regress.rollover_source
TO :'rollover_text_path'
WITH (storage_server 'lagodb_connectors_regress_s3', format 'text');
COPY lagodb_connectors_regress.rollover_source
TO :'rollover_csv_path'
WITH (storage_server 'lagodb_connectors_regress_s3', format 'csv');
COPY lagodb_connectors_regress.rollover_source
TO :'rollover_json_path'
WITH (storage_server 'lagodb_connectors_regress_s3', format 'json');
COPY lagodb_connectors_regress.rollover_source
TO :'rollover_avro_path'
WITH (storage_server 'lagodb_connectors_regress_s3', format 'avro');
COPY lagodb_connectors_regress.rollover_source
TO :'rollover_parquet_path'
WITH (
    storage_server 'lagodb_connectors_regress_s3',
    format 'parquet',
    compression 'none'
);

\setenv OBJECT_STORAGE_PREFIX :rollover_text_key
\! sh bin/object_storage_tool assert-prefix-rollover
\setenv OBJECT_STORAGE_PREFIX :rollover_csv_key
\! sh bin/object_storage_tool assert-prefix-rollover
\setenv OBJECT_STORAGE_PREFIX :rollover_json_key
\! sh bin/object_storage_tool assert-prefix-rollover
\setenv OBJECT_STORAGE_PREFIX :rollover_avro_key
\! sh bin/object_storage_tool assert-prefix-rollover
\setenv OBJECT_STORAGE_PREFIX :rollover_parquet_key
\! sh bin/object_storage_tool assert-prefix-rollover

CREATE FOREIGN TABLE lagodb_connectors_regress.rollover_text
    (LIKE lagodb_connectors_regress.rollover_source)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'rollover_text_path', format 'text');
CREATE FOREIGN TABLE lagodb_connectors_regress.rollover_csv
    (LIKE lagodb_connectors_regress.rollover_source)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'rollover_csv_path', format 'csv');
CREATE FOREIGN TABLE lagodb_connectors_regress.rollover_json
    (LIKE lagodb_connectors_regress.rollover_source)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'rollover_json_path', format 'json');
CREATE FOREIGN TABLE lagodb_connectors_regress.rollover_avro
    (LIKE lagodb_connectors_regress.rollover_source)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'rollover_avro_path', format 'avro');
CREATE FOREIGN TABLE lagodb_connectors_regress.rollover_parquet
    (LIKE lagodb_connectors_regress.rollover_source)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'rollover_parquet_path', format 'parquet');

DROP TABLE IF EXISTS lagodb_connectors_regress.rollover_parquet_copy;
CREATE TABLE lagodb_connectors_regress.rollover_parquet_copy
    (LIKE lagodb_connectors_regress.rollover_source);
COPY lagodb_connectors_regress.rollover_parquet_copy
FROM :'rollover_parquet_path'
WITH (storage_server 'lagodb_connectors_regress_s3', format 'parquet');

SELECT relation, rows, digest
FROM (
    SELECT 'text' AS relation, count(*) AS rows,
           md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id)) AS digest
    FROM lagodb_connectors_regress.rollover_text AS value
    UNION ALL
    SELECT 'csv', count(*),
           md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id))
    FROM lagodb_connectors_regress.rollover_csv AS value
    UNION ALL
    SELECT 'json', count(*),
           md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id))
    FROM lagodb_connectors_regress.rollover_json AS value
    UNION ALL
    SELECT 'avro', count(*),
           md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id))
    FROM lagodb_connectors_regress.rollover_avro AS value
    UNION ALL
    SELECT 'parquet', count(*),
           md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id))
    FROM lagodb_connectors_regress.rollover_parquet AS value
    UNION ALL
    SELECT 'parquet-copy', count(*),
           md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id))
    FROM lagodb_connectors_regress.rollover_parquet_copy AS value
    UNION ALL
    SELECT 'source', count(*),
           md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id))
    FROM lagodb_connectors_regress.rollover_source AS value
) AS rollover_results
ORDER BY relation;

RESET lagodb_connectors.target_file_size_mb;
RESET client_min_messages;
