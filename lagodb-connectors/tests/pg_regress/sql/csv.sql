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
WITH (server 'lagodb_connectors_regress_s3', format 'csv', header);
COPY lagodb_connectors_regress.common_source
TO :'csv_custom_path'
WITH (
    server 'lagodb_connectors_regress_s3',
    format 'csv',
    delimiter ';',
    null '<NULL>'
);
COPY lagodb_connectors_regress.common_source
TO :'csv_compressed_path'
WITH (
    server 'lagodb_connectors_regress_s3',
    format 'csv',
    compression 'gzip'
);

-- Compare every column in both directions; ALL also checks duplicate counts.
CREATE TABLE lagodb_connectors_regress.csv_copy_sink (:common_columns);
COPY lagodb_connectors_regress.csv_copy_sink
FROM :'csv_exact_path'
WITH (server 'lagodb_connectors_regress_s3');
(SELECT * FROM lagodb_connectors_regress.csv_copy_sink
 EXCEPT ALL SELECT * FROM lagodb_connectors_regress.common_source)
UNION ALL
(SELECT * FROM lagodb_connectors_regress.common_source
 EXCEPT ALL SELECT * FROM lagodb_connectors_regress.csv_copy_sink);

TRUNCATE lagodb_connectors_regress.csv_copy_sink;
COPY lagodb_connectors_regress.csv_copy_sink
FROM :'csv_header_path'
WITH (server 'lagodb_connectors_regress_s3', format 'csv', header true);
(SELECT * FROM lagodb_connectors_regress.csv_copy_sink
 EXCEPT ALL SELECT * FROM lagodb_connectors_regress.common_source)
UNION ALL
(SELECT * FROM lagodb_connectors_regress.common_source
 EXCEPT ALL SELECT * FROM lagodb_connectors_regress.csv_copy_sink);

TRUNCATE lagodb_connectors_regress.csv_copy_sink;
COPY lagodb_connectors_regress.csv_copy_sink
FROM :'csv_custom_path'
WITH (
    server 'lagodb_connectors_regress_s3',
    format 'csv',
    delimiter ';',
    null '<NULL>'
);
(SELECT * FROM lagodb_connectors_regress.csv_copy_sink
 EXCEPT ALL SELECT * FROM lagodb_connectors_regress.common_source)
UNION ALL
(SELECT * FROM lagodb_connectors_regress.common_source
 EXCEPT ALL SELECT * FROM lagodb_connectors_regress.csv_copy_sink);

TRUNCATE lagodb_connectors_regress.csv_copy_sink;
COPY lagodb_connectors_regress.csv_copy_sink
FROM :'csv_compressed_path'
WITH (server 'lagodb_connectors_regress_s3');
(SELECT * FROM lagodb_connectors_regress.csv_copy_sink
 EXCEPT ALL SELECT * FROM lagodb_connectors_regress.common_source)
UNION ALL
(SELECT * FROM lagodb_connectors_regress.common_source
 EXCEPT ALL SELECT * FROM lagodb_connectors_regress.csv_copy_sink);

-- CSV quoting must preserve PostgreSQL JSON and array datum representations.
COPY lagodb_connectors_regress.stream_extra_source
TO :'csv_extra_path'
WITH (server 'lagodb_connectors_regress_s3', format 'csv');
CREATE TABLE lagodb_connectors_regress.csv_extra_sink
    (:stream_extra_columns);
COPY lagodb_connectors_regress.csv_extra_sink
FROM :'csv_extra_path'
WITH (server 'lagodb_connectors_regress_s3', format 'csv');
SELECT * FROM lagodb_connectors_regress.csv_extra_sink ORDER BY id;

-- An empty exact object is valid CSV input.
COPY (
    SELECT * FROM lagodb_connectors_regress.common_source WHERE false
) TO :'csv_empty_path'
WITH (server 'lagodb_connectors_regress_s3', format 'csv');
TRUNCATE lagodb_connectors_regress.csv_copy_sink;
COPY lagodb_connectors_regress.csv_copy_sink
FROM :'csv_empty_path'
WITH (server 'lagodb_connectors_regress_s3', format 'csv');
SELECT count(*) AS empty_rows
FROM lagodb_connectors_regress.csv_copy_sink;

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

(SELECT * FROM lagodb_connectors_regress.csv_exact
 EXCEPT ALL SELECT * FROM lagodb_connectors_regress.common_source)
UNION ALL
(SELECT * FROM lagodb_connectors_regress.common_source
 EXCEPT ALL SELECT * FROM lagodb_connectors_regress.csv_exact);

(SELECT * FROM lagodb_connectors_regress.csv_prefix
 EXCEPT ALL SELECT * FROM lagodb_connectors_regress.common_source)
UNION ALL
(SELECT * FROM lagodb_connectors_regress.common_source
 EXCEPT ALL SELECT * FROM lagodb_connectors_regress.csv_prefix);

SELECT attname, format_type(atttypid, atttypmod) AS type
FROM pg_attribute
WHERE attrelid = 'lagodb_connectors_regress.csv_inferred'::regclass
  AND attnum > 0 AND NOT attisdropped
ORDER BY attnum;
SELECT count(*) AS inferred_rows
FROM lagodb_connectors_regress.csv_inferred;

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
(SELECT * FROM lagodb_connectors_regress.csv_write
 EXCEPT ALL SELECT * FROM lagodb_connectors_regress.common_source)
UNION ALL
(SELECT * FROM lagodb_connectors_regress.common_source
 EXCEPT ALL SELECT * FROM lagodb_connectors_regress.csv_write);

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

-- Mixed gzip/zstd/plain csv members resolve their codec per file.
-- Include local and remote membership; CSV must consume each member's header.
BEGIN;
CREATE SERVER lagodb_connectors_regress_mixed_local FOREIGN DATA WRAPPER lagodb_connectors;
CREATE USER MAPPING FOR PUBLIC SERVER lagodb_connectors_regress_mixed_local;
SELECT current_setting('data_directory') || '/lagodb-mixed-csv-' || pg_backend_pid()
           || '-' || txid_current() || '/' AS mixed_local_path,
       's3://' || bucket || '/lagodb-connectors/mixed-csv-' || pg_backend_pid()
           || '-' || txid_current() || '/' AS mixed_remote_path
FROM lagodb_regress.object_storage_fixture
\gset

CREATE TABLE lagodb_connectors_regress.mixed_source (id integer, payload text);
INSERT INTO lagodb_connectors_regress.mixed_source
VALUES (1, E'comma,value "quoted"\nline'), (2, NULL), (3, ''), (4, '中文');

CREATE FOREIGN TABLE lagodb_connectors_regress.mixed_local (id integer, payload text)
SERVER lagodb_connectors_regress_mixed_local
OPTIONS (path :'mixed_local_path', format 'csv', compression 'gzip', header 'true');
INSERT INTO lagodb_connectors_regress.mixed_local SELECT * FROM lagodb_connectors_regress.mixed_source;
ALTER FOREIGN TABLE lagodb_connectors_regress.mixed_local OPTIONS (SET compression 'zstd');
INSERT INTO lagodb_connectors_regress.mixed_local SELECT * FROM lagodb_connectors_regress.mixed_source;
ALTER FOREIGN TABLE lagodb_connectors_regress.mixed_local OPTIONS (DROP compression);
INSERT INTO lagodb_connectors_regress.mixed_local SELECT * FROM lagodb_connectors_regress.mixed_source;
(SELECT source.* FROM lagodb_connectors_regress.mixed_source AS source, generate_series(1, 3)
 EXCEPT ALL TABLE lagodb_connectors_regress.mixed_local)
UNION ALL
(TABLE lagodb_connectors_regress.mixed_local
 EXCEPT ALL SELECT source.* FROM lagodb_connectors_regress.mixed_source AS source, generate_series(1, 3));

-- Parameterized scans must reopen all members with their individual codecs.
SELECT sum(matched.n) AS rescan_rows
FROM (VALUES (1), (3)) AS thresholds(id)
CROSS JOIN LATERAL (
    SELECT count(*) AS n FROM lagodb_connectors_regress.mixed_local AS files
    WHERE files.id >= thresholds.id OFFSET 0
) AS matched;

CREATE FOREIGN TABLE lagodb_connectors_regress.mixed_local_inferred ()
SERVER lagodb_connectors_regress_mixed_local
OPTIONS (path :'mixed_local_path', format 'csv', header 'true');
SELECT count(*) AS inferred_rows
FROM lagodb_connectors_regress.mixed_local_inferred;

CREATE FOREIGN TABLE lagodb_connectors_regress.mixed_remote (id integer, payload text)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'mixed_remote_path', format 'csv', compression 'gzip', header 'true');
INSERT INTO lagodb_connectors_regress.mixed_remote SELECT * FROM lagodb_connectors_regress.mixed_source;
ALTER FOREIGN TABLE lagodb_connectors_regress.mixed_remote OPTIONS (SET compression 'zstd');
INSERT INTO lagodb_connectors_regress.mixed_remote SELECT * FROM lagodb_connectors_regress.mixed_source;
ALTER FOREIGN TABLE lagodb_connectors_regress.mixed_remote OPTIONS (DROP compression);
INSERT INTO lagodb_connectors_regress.mixed_remote SELECT * FROM lagodb_connectors_regress.mixed_source;
(SELECT source.* FROM lagodb_connectors_regress.mixed_source AS source, generate_series(1, 3)
 EXCEPT ALL TABLE lagodb_connectors_regress.mixed_remote)
UNION ALL
(TABLE lagodb_connectors_regress.mixed_remote
 EXCEPT ALL SELECT source.* FROM lagodb_connectors_regress.mixed_source AS source, generate_series(1, 3));

-- Parameterized scans must reopen all members with their individual codecs.
SELECT sum(matched.n) AS rescan_rows
FROM (VALUES (1), (3)) AS thresholds(id)
CROSS JOIN LATERAL (
    SELECT count(*) AS n FROM lagodb_connectors_regress.mixed_remote AS files
    WHERE files.id >= thresholds.id OFFSET 0
) AS matched;

CREATE FOREIGN TABLE lagodb_connectors_regress.mixed_remote_inferred ()
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'mixed_remote_path', format 'csv', header 'true');
SELECT count(*) AS inferred_rows
FROM lagodb_connectors_regress.mixed_remote_inferred;

ROLLBACK;
