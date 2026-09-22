\i include/column_definitions.sql

-- CSV format COPY, options, foreign-table, and inference coverage.

SELECT endpoint, bucket, region, access_key_id, secret_access_key
FROM lagodb_regress.object_storage_fixture
\gset storage_

\setenv OBJECT_STORAGE_ENDPOINT :storage_endpoint
\setenv OBJECT_STORAGE_BUCKET :storage_bucket
\setenv OBJECT_STORAGE_REGION :storage_region
\setenv OBJECT_STORAGE_ACCESS_KEY_ID :storage_access_key_id
\setenv OBJECT_STORAGE_SECRET_ACCESS_KEY :storage_secret_access_key

SELECT format('s3://%s/lagodb-connectors/csv/exact.csv',
              :'storage_bucket') AS exact_path,
       format('s3://%s/lagodb-connectors/csv/prefix/',
              :'storage_bucket') AS prefix_path,
       format('s3://%s/lagodb-connectors/csv/prefix/part-a.csv',
              :'storage_bucket') AS part_a_path,
       format('s3://%s/lagodb-connectors/csv/prefix/part-b.csv',
              :'storage_bucket') AS part_b_path,
       format('s3://%s/lagodb-connectors/csv/header.csv',
              :'storage_bucket') AS header_path,
       format('s3://%s/lagodb-connectors/csv/custom.csv',
              :'storage_bucket') AS custom_path,
       format('s3://%s/lagodb-connectors/csv/compressed.csv.gz',
              :'storage_bucket') AS compressed_path,
       format('s3://%s/lagodb-connectors/csv/options.csv',
              :'storage_bucket') AS options_path,
       format('s3://%s/lagodb-connectors/csv/extra.csv',
              :'storage_bucket') AS extra_path,
       format('s3://%s/lagodb-connectors/csv/empty.csv',
              :'storage_bucket') AS empty_path,
       format('s3://%s/lagodb-connectors/csv/write/',
              :'storage_bucket') AS write_path,
       format('s3://%s/lagodb-connectors/csv/malformed-width.csv',
              :'storage_bucket') AS malformed_path,
       'lagodb-connectors/csv/malformed-width.csv' AS malformed_key
\gset csv_

-- Exact/prefix I/O, headers, and PostgreSQL CSV options.
COPY lagodb_connectors_regress.common_source
TO :'csv_exact_path'
WITH (server 'lagodb_connectors_regress_s3');
COPY (
    SELECT * FROM lagodb_connectors_regress.common_source WHERE id = 1
) TO :'csv_part_a_path'
WITH (server 'lagodb_connectors_regress_s3', format 'csv');
COPY (
    SELECT * FROM lagodb_connectors_regress.common_source WHERE id = 2
) TO :'csv_part_b_path'
WITH (server 'lagodb_connectors_regress_s3', format 'csv');
COPY lagodb_connectors_regress.common_source
TO :'csv_header_path'
WITH (server 'lagodb_connectors_regress_s3', format 'csv', header true);
COPY lagodb_connectors_regress.common_source
TO :'csv_custom_path'
WITH (
    server 'lagodb_connectors_regress_s3',
    format 'csv',
    delimiter ';',
    null '<NULL>',
    quote '"',
    escape '"'
);
COPY lagodb_connectors_regress.common_source
TO :'csv_compressed_path'
WITH (
    server 'lagodb_connectors_regress_s3',
    format 'csv',
    compression 'gzip'
);

CREATE TABLE lagodb_connectors_regress.csv_exact_sink
    (:common_columns);
COPY lagodb_connectors_regress.csv_exact_sink
FROM :'csv_exact_path'
WITH (server 'lagodb_connectors_regress_s3');
CREATE TABLE lagodb_connectors_regress.csv_header_sink
    (:common_columns);
COPY lagodb_connectors_regress.csv_header_sink
FROM :'csv_header_path'
WITH (server 'lagodb_connectors_regress_s3', format 'csv', header true);
CREATE TABLE lagodb_connectors_regress.csv_custom_sink
    (:common_columns);
COPY lagodb_connectors_regress.csv_custom_sink
FROM :'csv_custom_path'
WITH (
    server 'lagodb_connectors_regress_s3',
    format 'csv',
    delimiter ';',
    null '<NULL>',
    quote '"',
    escape '"'
);
CREATE TABLE lagodb_connectors_regress.csv_compressed_sink
    (:common_columns);
COPY lagodb_connectors_regress.csv_compressed_sink
FROM :'csv_compressed_path'
WITH (server 'lagodb_connectors_regress_s3');

SELECT relation, rows, round_trip
FROM (
    SELECT 'exact' AS relation, count(*) AS rows,
           array_agg(to_jsonb(value) ORDER BY to_jsonb(value)) =
               (SELECT array_agg(to_jsonb(source) ORDER BY to_jsonb(source))
                FROM lagodb_connectors_regress.common_source AS source)
               AS round_trip
    FROM lagodb_connectors_regress.csv_exact_sink AS value
    UNION ALL
    SELECT 'header', count(*),
           array_agg(to_jsonb(value) ORDER BY to_jsonb(value)) =
               (SELECT array_agg(to_jsonb(source) ORDER BY to_jsonb(source))
                FROM lagodb_connectors_regress.common_source AS source)
    FROM lagodb_connectors_regress.csv_header_sink AS value
    UNION ALL
    SELECT 'custom', count(*),
           array_agg(to_jsonb(value) ORDER BY to_jsonb(value)) =
               (SELECT array_agg(to_jsonb(source) ORDER BY to_jsonb(source))
                FROM lagodb_connectors_regress.common_source AS source)
    FROM lagodb_connectors_regress.csv_custom_sink AS value
    UNION ALL
    SELECT 'gzip', count(*),
           array_agg(to_jsonb(value) ORDER BY to_jsonb(value)) =
               (SELECT array_agg(to_jsonb(source) ORDER BY to_jsonb(source))
                FROM lagodb_connectors_regress.common_source AS source)
    FROM lagodb_connectors_regress.csv_compressed_sink AS value
) AS results
ORDER BY relation;

-- CSV quoting must preserve PostgreSQL JSON and array datum representations.
COPY lagodb_connectors_regress.stream_extra_source
TO :'csv_extra_path'
WITH (server 'lagodb_connectors_regress_s3', format 'csv');
CREATE TABLE lagodb_connectors_regress.csv_extra_sink
    (:stream_extra_columns);
COPY lagodb_connectors_regress.csv_extra_sink
FROM :'csv_extra_path'
WITH (server 'lagodb_connectors_regress_s3', format 'csv');
SELECT count(*) AS extra_rows,
       array_agg(to_jsonb(value) ORDER BY to_jsonb(value)) =
           (SELECT array_agg(to_jsonb(source) ORDER BY to_jsonb(source))
            FROM lagodb_connectors_regress.stream_extra_source AS source)
           AS matches_source
FROM lagodb_connectors_regress.csv_extra_sink AS value;

-- An empty exact object is valid CSV input.
COPY (
    SELECT * FROM lagodb_connectors_regress.common_source WHERE false
) TO :'csv_empty_path'
WITH (server 'lagodb_connectors_regress_s3', format 'csv');
CREATE TABLE lagodb_connectors_regress.csv_empty_sink
    (:common_columns);
COPY lagodb_connectors_regress.csv_empty_sink
FROM :'csv_empty_path'
WITH (server 'lagodb_connectors_regress_s3', format 'csv');
SELECT count(*) AS empty_rows
FROM lagodb_connectors_regress.csv_empty_sink;

-- Exact/prefix foreign scans and header-aware schema inference.
CREATE FOREIGN TABLE lagodb_connectors_regress.csv_exact
    (:common_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'csv_exact_path');
CREATE FOREIGN TABLE lagodb_connectors_regress.csv_prefix
    (:common_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'csv_prefix_path', format 'csv');
CREATE FOREIGN TABLE lagodb_connectors_regress.csv_inferred ()
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'csv_header_path', format 'csv', header 'match');

SELECT relation, rows, matches_source
FROM (
    SELECT 'exact' AS relation, count(*) AS rows,
           array_agg(to_jsonb(value) ORDER BY to_jsonb(value)) =
               (SELECT array_agg(to_jsonb(source) ORDER BY to_jsonb(source))
                FROM lagodb_connectors_regress.common_source AS source)
               AS matches_source
    FROM lagodb_connectors_regress.csv_exact AS value
    UNION ALL
    SELECT 'prefix', count(*),
           array_agg(to_jsonb(value) ORDER BY to_jsonb(value)) =
               (SELECT array_agg(to_jsonb(source) ORDER BY to_jsonb(source))
                FROM lagodb_connectors_regress.common_source AS source)
    FROM lagodb_connectors_regress.csv_prefix AS value
) AS results
ORDER BY relation;
SELECT count(*) AS inferred_columns,
       string_agg(format_type(atttypid, atttypmod), ', ' ORDER BY attnum)
           AS inferred_types
FROM pg_attribute
WHERE attrelid = 'lagodb_connectors_regress.csv_inferred'::regclass
  AND attnum > 0 AND NOT attisdropped;
SELECT count(*) AS inferred_rows
FROM lagodb_connectors_regress.csv_inferred;

-- CSV relation and column options must affect PostgreSQL CSV semantics, not
-- merely pass DDL validation. The writer quotes the literal NULL marker and
-- leaves SQL NULL unquoted so force_null and force_not_null are observable.
CREATE TABLE lagodb_connectors_regress.csv_option_source (
    id integer,
    force_null_col text,
    force_not_null_col text,
    payload text
);
INSERT INTO lagodb_connectors_regress.csv_option_source
VALUES (1, '<NULL>', NULL, 'delimiter;quote"value'),
       (2, 'ordinary', 'ordinary', E'escape\\value');
COPY lagodb_connectors_regress.csv_option_source
TO :'csv_options_path'
WITH (
    server 'lagodb_connectors_regress_s3',
    format 'csv',
    delimiter ';',
    null '<NULL>',
    quote '"',
    escape E'\\'
);
CREATE FOREIGN TABLE lagodb_connectors_regress.csv_options (
    id integer,
    force_null_col text OPTIONS (force_null 'true'),
    force_not_null_col text OPTIONS (force_not_null 'true'),
    payload text
)
SERVER lagodb_connectors_regress_s3
OPTIONS (
    path :'csv_options_path',
    format 'csv',
    delimiter ';',
    null '<NULL>',
    quote '"',
    escape E'\\',
    header 'false'
);
SELECT id,
       force_null_col IS NULL AS forced_null,
       force_not_null_col = '<NULL>' AS forced_not_null,
       payload
FROM lagodb_connectors_regress.csv_options
ORDER BY id;

CREATE FOREIGN TABLE lagodb_connectors_regress.csv_write
    (:common_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'csv_write_path', format 'csv');
INSERT INTO lagodb_connectors_regress.csv_write
SELECT * FROM lagodb_connectors_regress.common_source;
SELECT count(*) AS written_rows,
       array_agg(to_jsonb(value) ORDER BY to_jsonb(value)) =
           (SELECT array_agg(to_jsonb(source) ORDER BY to_jsonb(source))
            FROM lagodb_connectors_regress.common_source AS source)
           AS matches_source
FROM lagodb_connectors_regress.csv_write AS value;

-- One common malformed-row error protects CSV field-count validation.
\setenv OBJECT_STORAGE_FILE data/malformed_csv_width.csv
\setenv OBJECT_STORAGE_KEY :csv_malformed_key
\! sh bin/object_storage_tool put
CREATE FOREIGN TABLE lagodb_connectors_regress.csv_malformed (
    id integer,
    payload text
)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'csv_malformed_path', format 'csv');
\set VERBOSITY sqlstate
SELECT count(*) FROM lagodb_connectors_regress.csv_malformed;
\set VERBOSITY default
