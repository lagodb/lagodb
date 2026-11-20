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

\set csv_base 's3://' :storage_bucket '/lagodb-connectors/csv/'
\pset null _null_

-- COPY round trip through an exact object; the suffix selects the format.
\set csv_exact_path :csv_base 'exact.csv'
COPY lagodb_connectors_regress.common_source TO :'csv_exact_path'
WITH (server 'lagodb_connectors_regress_s3');
CREATE TABLE lagodb_connectors_regress.csv_copy_sink (:common_columns);
COPY lagodb_connectors_regress.csv_copy_sink FROM :'csv_exact_path'
WITH (server 'lagodb_connectors_regress_s3');
SELECT * FROM lagodb_connectors_regress.csv_copy_sink ORDER BY id;

-- A header line.
\set csv_header_path :csv_base 'header.csv'
COPY lagodb_connectors_regress.common_source TO :'csv_header_path'
WITH (server 'lagodb_connectors_regress_s3', format 'csv', header);
TRUNCATE lagodb_connectors_regress.csv_copy_sink;
COPY lagodb_connectors_regress.csv_copy_sink FROM :'csv_header_path'
WITH (server 'lagodb_connectors_regress_s3', format 'csv', header true);
SELECT * FROM lagodb_connectors_regress.csv_copy_sink ORDER BY id;

-- CSV quoting must preserve PostgreSQL JSON and array datum representations.
\set csv_extra_path :csv_base 'extra.csv'
COPY lagodb_connectors_regress.stream_extra_source TO :'csv_extra_path'
WITH (server 'lagodb_connectors_regress_s3', format 'csv');
CREATE TABLE lagodb_connectors_regress.csv_extra_sink (:stream_extra_columns);
COPY lagodb_connectors_regress.csv_extra_sink FROM :'csv_extra_path'
WITH (server 'lagodb_connectors_regress_s3', format 'csv');
SELECT * FROM lagodb_connectors_regress.csv_extra_sink ORDER BY id;

-- An empty exact object is valid CSV input.
\set csv_empty_path :csv_base 'empty.csv'
COPY (SELECT * FROM lagodb_connectors_regress.common_source WHERE false)
TO :'csv_empty_path'
WITH (server 'lagodb_connectors_regress_s3', format 'csv');
TRUNCATE lagodb_connectors_regress.csv_copy_sink;
COPY lagodb_connectors_regress.csv_copy_sink FROM :'csv_empty_path'
WITH (server 'lagodb_connectors_regress_s3', format 'csv');
SELECT count(*) AS empty_rows FROM lagodb_connectors_regress.csv_copy_sink;

-- Exact foreign scan and header-aware schema inference.
CREATE FOREIGN TABLE lagodb_connectors_regress.csv_exact (:common_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'csv_exact_path');
SELECT * FROM lagodb_connectors_regress.csv_exact ORDER BY id;
CREATE FOREIGN TABLE lagodb_connectors_regress.csv_inferred ()
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'csv_header_path', format 'csv', header 'match');
SELECT attname, format_type(atttypid, atttypmod) AS type
FROM pg_attribute
WHERE attrelid = 'lagodb_connectors_regress.csv_inferred'::regclass
  AND attnum > 0 AND NOT attisdropped
ORDER BY attnum;
SELECT count(*) AS inferred_rows FROM lagodb_connectors_regress.csv_inferred;

-- A prefix scan reads every object under the prefix.
\set csv_prefix_path :csv_base 'prefix/'
\set csv_part_path :csv_prefix_path 'part-a.csv'
COPY (SELECT * FROM lagodb_connectors_regress.common_source WHERE id = 1)
TO :'csv_part_path'
WITH (server 'lagodb_connectors_regress_s3', format 'csv');
\set csv_part_path :csv_prefix_path 'part-b.csv'
COPY (SELECT * FROM lagodb_connectors_regress.common_source WHERE id = 2)
TO :'csv_part_path'
WITH (server 'lagodb_connectors_regress_s3', format 'csv');
CREATE FOREIGN TABLE lagodb_connectors_regress.csv_prefix (:common_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'csv_prefix_path', format 'csv');
SELECT * FROM lagodb_connectors_regress.csv_prefix ORDER BY id;

-- Distinguish a quoted literal NULL marker from SQL NULL for force_null/force_not_null.
CREATE TABLE lagodb_connectors_regress.csv_option_source (
    id integer,
    force_null_col text,
    force_not_null_col text,
    payload text
);
INSERT INTO lagodb_connectors_regress.csv_option_source
VALUES (1, '<NULL>', NULL, 'delimiter;quote"value'),
       (2, 'ordinary', 'ordinary', E'escape\\value');
\set csv_options_path :csv_base 'options.csv'
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

-- Prefix foreign INSERT uses the CSV writer.
\set csv_write_path :csv_base 'write/'
CREATE FOREIGN TABLE lagodb_connectors_regress.csv_write (:common_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'csv_write_path', format 'csv');
INSERT INTO lagodb_connectors_regress.csv_write
SELECT * FROM lagodb_connectors_regress.common_source;
SELECT * FROM lagodb_connectors_regress.csv_write ORDER BY id;

-- One common malformed-row error protects CSV field-count validation.
\set csv_malformed_path :csv_base 'malformed-width.csv'
\setenv OBJECT_STORAGE_FILE data/malformed_csv_width.csv
\setenv OBJECT_STORAGE_KEY 'lagodb-connectors/csv/malformed-width.csv'
\! sh bin/object_storage_tool put
CREATE FOREIGN TABLE lagodb_connectors_regress.csv_malformed (id integer, payload text)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'csv_malformed_path', format 'csv');
\set VERBOSITY sqlstate
SELECT count(*) FROM lagodb_connectors_regress.csv_malformed;
\set VERBOSITY default

-- Each prefix member resolves its own codec and header: the INSERTs write
-- gzip, zstd, and plain objects with a header line.
\set csv_mixed_path :csv_base 'mixed/'
CREATE FOREIGN TABLE lagodb_connectors_regress.csv_mixed (id integer, payload text)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'csv_mixed_path', format 'csv', compression 'gzip', header 'true');
INSERT INTO lagodb_connectors_regress.csv_mixed VALUES (1, 'gzip');
ALTER FOREIGN TABLE lagodb_connectors_regress.csv_mixed OPTIONS (SET compression 'zstd');
INSERT INTO lagodb_connectors_regress.csv_mixed VALUES (2, 'zstd');
ALTER FOREIGN TABLE lagodb_connectors_regress.csv_mixed OPTIONS (DROP compression);
INSERT INTO lagodb_connectors_regress.csv_mixed VALUES (3, 'plain');
SELECT * FROM lagodb_connectors_regress.csv_mixed ORDER BY id;
