-- Parquet array element and shape boundaries.

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

SELECT format('s3://%s/lagodb-connectors/parquet-arrays/null-elements.parquet',
              :'storage_bucket') AS null_elements_path,
       format('s3://%s/lagodb-connectors/parquet-arrays/non-one-lower-bound.parquet',
              :'storage_bucket') AS lower_bound_path,
       format('s3://%s/lagodb-connectors/parquet-arrays/multidimensional/',
              :'storage_bucket') AS multidimensional_path,
       'lagodb-connectors/parquet-arrays/non-one-lower-bound.parquet' AS lower_bound_key,
       'lagodb-connectors/parquet-arrays/multidimensional/' AS multidimensional_key
\gset array_

DROP TABLE IF EXISTS lagodb_connectors_regress.parquet_null_array_source;
CREATE TABLE lagodb_connectors_regress.parquet_null_array_source
    (LIKE lagodb_connectors_regress.parquet_source);
INSERT INTO lagodb_connectors_regress.parquet_null_array_source
SELECT id,
       bool_col,
       smallint_col,
       integer_col,
       bigint_col,
       real_col,
       double_col,
       numeric_col,
       text_col,
       varchar_col,
       char_col,
       name_col,
       bytea_col,
       uuid_col,
       date_col,
       time_col,
       timestamp_col,
       timestamptz_col,
       json_col,
       ARRAY[true, NULL, false]::boolean[],
       ARRAY[1, NULL, 3]::smallint[],
       ARRAY[1, NULL, 3]::integer[],
       ARRAY[1, NULL, 3]::bigint[],
       ARRAY[1.0, NULL, 3.0]::real[],
       ARRAY[1.0, NULL, 3.0]::double precision[],
       ARRAY['left', NULL, 'right']::text[],
       ARRAY['left', NULL, 'right']::varchar(20)[],
       ARRAY['left', NULL, 'right']::character(5)[],
       ARRAY['left', NULL, 'right']::name[],
       ARRAY['{"side":"left"}'::json, NULL, '{"side":"right"}'::json]
FROM lagodb_connectors_regress.parquet_source
WHERE id = 1;

COPY lagodb_connectors_regress.parquet_null_array_source
TO :'array_null_elements_path'
WITH (storage_server 'lagodb_connectors_regress_s3', format 'parquet');

DROP TABLE IF EXISTS lagodb_connectors_regress.parquet_null_array_sink;
CREATE TABLE lagodb_connectors_regress.parquet_null_array_sink
    (LIKE lagodb_connectors_regress.parquet_source);
COPY lagodb_connectors_regress.parquet_null_array_sink
FROM :'array_null_elements_path'
WITH (storage_server 'lagodb_connectors_regress_s3', format 'parquet');

SELECT bool_array[2] IS NULL AS bool_null,
       smallint_array[2] IS NULL AS smallint_null,
       integer_array[2] IS NULL AS integer_null,
       bigint_array[2] IS NULL AS bigint_null,
       real_array[2] IS NULL AS real_null,
       double_array[2] IS NULL AS double_null,
       text_array[2] IS NULL AS text_null,
       varchar_array[2] IS NULL AS varchar_null,
       bpchar_array[2] IS NULL AS bpchar_null,
       name_array[2] IS NULL AS name_null,
       json_array[2] IS NULL AS json_null
FROM lagodb_connectors_regress.parquet_null_array_sink;

-- Arrow List has one dimension and an implicit lower bound of one. Reject
-- PostgreSQL shapes that cannot round-trip without changing subscripts.
\set VERBOSITY sqlstate
COPY (
    SELECT 1 AS id,
           '[0:1]={10,20}'::integer[] AS integer_array
) TO :'array_lower_bound_path'
WITH (storage_server 'lagodb_connectors_regress_s3', format 'parquet');
\set VERBOSITY default
\setenv OBJECT_STORAGE_PREFIX :array_lower_bound_key
\! sh bin/object_storage_tool assert-prefix-empty

CREATE FOREIGN TABLE lagodb_connectors_regress.parquet_multidimensional (
    id integer,
    integer_array integer[]
)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'array_multidimensional_path', format 'parquet');

\set VERBOSITY sqlstate
INSERT INTO lagodb_connectors_regress.parquet_multidimensional
VALUES (1, ARRAY[[1, 2], [3, 4]]);
\set VERBOSITY default
\setenv OBJECT_STORAGE_PREFIX :array_multidimensional_key
\! sh bin/object_storage_tool assert-prefix-empty

RESET client_min_messages;
