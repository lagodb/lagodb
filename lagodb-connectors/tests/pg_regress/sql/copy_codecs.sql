\i include/column_definitions.sql

-- Stream compression inference, explicit overrides, and malformed input.

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

SELECT format('s3://%s/lagodb-connectors/codecs/plain-suffix.txt.gz',
              :'storage_bucket') AS plain_gz_path,
       format('s3://%s/lagodb-connectors/codecs/plain-suffix.txt.zst',
              :'storage_bucket') AS plain_zst_path,
       format('s3://%s/lagodb-connectors/codecs/alias.txt.gzip',
              :'storage_bucket') AS gzip_alias_path,
       format('s3://%s/lagodb-connectors/codecs/alias.txt.zstd',
              :'storage_bucket') AS zstd_alias_path,
       format('s3://%s/lagodb-connectors/codecs/member-1.txt.gz',
              :'storage_bucket') AS gzip_member_1_path,
       format('s3://%s/lagodb-connectors/codecs/member-2.txt.gz',
              :'storage_bucket') AS gzip_member_2_path,
       format('s3://%s/lagodb-connectors/codecs/concatenated.txt.gz',
              :'storage_bucket') AS gzip_concatenated_path,
       format('s3://%s/lagodb-connectors/codecs/truncated.txt.gz',
              :'storage_bucket') AS truncated_gzip_path,
       format('s3://%s/lagodb-connectors/codecs/truncated.txt.zst',
              :'storage_bucket') AS truncated_zstd_path,
       format('s3://%s/lagodb-connectors/codecs/corrupt.txt.gz',
              :'storage_bucket') AS corrupt_gzip_path,
       format('s3://%s/lagodb-connectors/codecs/corrupt.txt.zst',
              :'storage_bucket') AS corrupt_zstd_path,
       format('s3://%s/lagodb-connectors/codecs/malformed-framing.csv',
              :'storage_bucket') AS malformed_framing_path,
       format('s3://%s/lagodb-connectors/codecs/malformed-width.csv',
              :'storage_bucket') AS malformed_width_path,
       'lagodb-connectors/codecs/member-1.txt.gz' AS gzip_member_1_key,
       'lagodb-connectors/codecs/member-2.txt.gz' AS gzip_member_2_key,
       'lagodb-connectors/codecs/concatenated.txt.gz' AS gzip_concatenated_key,
       'lagodb-connectors/codecs/truncated.txt.gz' AS truncated_gzip_key,
       'lagodb-connectors/codecs/truncated.txt.zst' AS truncated_zstd_key,
       'lagodb-connectors/codecs/corrupt.txt.gz' AS corrupt_gzip_key,
       'lagodb-connectors/codecs/corrupt.txt.zst' AS corrupt_zstd_key,
       'lagodb-connectors/codecs/malformed-framing.csv' AS malformed_framing_key,
       'lagodb-connectors/codecs/malformed-width.csv' AS malformed_width_key
\gset codec_

-- An explicit compression option takes precedence over a compression-looking
-- suffix in both COPY and foreign-table option resolution.
COPY lagodb_connectors_regress.common_source
TO :'codec_plain_gz_path'
WITH (
    server 'lagodb_connectors_regress_s3',
    compression 'none'
);
COPY lagodb_connectors_regress.common_source
TO :'codec_plain_zst_path'
WITH (
    server 'lagodb_connectors_regress_s3',
    compression 'none'
);

DROP TABLE IF EXISTS lagodb_connectors_regress.codec_plain_gz;
CREATE TABLE lagodb_connectors_regress.codec_plain_gz
    (:common_columns);
COPY lagodb_connectors_regress.codec_plain_gz
FROM :'codec_plain_gz_path'
WITH (
    server 'lagodb_connectors_regress_s3',
    compression 'none'
);

CREATE FOREIGN TABLE lagodb_connectors_regress.codec_plain_zst
    (:common_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'codec_plain_zst_path', compression 'none');

SELECT relation, rows
FROM (
    SELECT 'copy-none-gz' AS relation, count(*) AS rows
    FROM lagodb_connectors_regress.codec_plain_gz
    UNION ALL
    SELECT 'foreign-none-zst', count(*)
    FROM lagodb_connectors_regress.codec_plain_zst
) AS explicit_none_results
ORDER BY relation;

-- Long compression suffix aliases participate in format/compression inference.
COPY lagodb_connectors_regress.common_source
TO :'codec_gzip_alias_path'
WITH (server 'lagodb_connectors_regress_s3');
COPY lagodb_connectors_regress.common_source
TO :'codec_zstd_alias_path'
WITH (server 'lagodb_connectors_regress_s3');

DROP TABLE IF EXISTS lagodb_connectors_regress.codec_gzip_alias;
CREATE TABLE lagodb_connectors_regress.codec_gzip_alias
    (:common_columns);
COPY lagodb_connectors_regress.codec_gzip_alias
FROM :'codec_gzip_alias_path'
WITH (server 'lagodb_connectors_regress_s3');
CREATE FOREIGN TABLE lagodb_connectors_regress.codec_zstd_alias
    (:common_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'codec_zstd_alias_path');

SELECT relation, rows
FROM (
    SELECT 'gzip-alias' AS relation, count(*) AS rows
    FROM lagodb_connectors_regress.codec_gzip_alias
    UNION ALL
    SELECT 'zstd-alias', count(*)
    FROM lagodb_connectors_regress.codec_zstd_alias
) AS alias_results
ORDER BY relation;

-- RFC 1952 permits concatenated gzip members. The helper concatenates the raw
-- compressed objects without decoding or recompressing either member.
COPY (
    SELECT * FROM lagodb_connectors_regress.common_source WHERE id = 1
) TO :'codec_gzip_member_1_path'
WITH (server 'lagodb_connectors_regress_s3');
COPY (
    SELECT * FROM lagodb_connectors_regress.common_source WHERE id = 2
) TO :'codec_gzip_member_2_path'
WITH (server 'lagodb_connectors_regress_s3');

\setenv OBJECT_STORAGE_SOURCE_KEY_1 :codec_gzip_member_1_key
\setenv OBJECT_STORAGE_SOURCE_KEY_2 :codec_gzip_member_2_key
\setenv OBJECT_STORAGE_KEY :codec_gzip_concatenated_key
\! sh bin/object_storage_tool concatenate

DROP TABLE IF EXISTS lagodb_connectors_regress.codec_concatenated_gzip;
CREATE TABLE lagodb_connectors_regress.codec_concatenated_gzip
    (:common_columns);
COPY lagodb_connectors_regress.codec_concatenated_gzip
FROM :'codec_gzip_concatenated_path'
WITH (server 'lagodb_connectors_regress_s3');
SELECT count(*) AS concatenated_gzip_rows,
       string_agg(id::text, ',' ORDER BY id) AS concatenated_gzip_ids
FROM lagodb_connectors_regress.codec_concatenated_gzip;

-- Corruption and truncation must surface a codec I/O SQLSTATE rather than
-- successful EOF. Header corruption deterministically invalidates the format
-- magic; truncation exercises final frame/trailer validation.
COPY lagodb_connectors_regress.common_source
TO :'codec_corrupt_gzip_path'
WITH (server 'lagodb_connectors_regress_s3');
COPY lagodb_connectors_regress.common_source
TO :'codec_corrupt_zstd_path'
WITH (server 'lagodb_connectors_regress_s3');
COPY lagodb_connectors_regress.common_source
TO :'codec_truncated_gzip_path'
WITH (server 'lagodb_connectors_regress_s3');
COPY lagodb_connectors_regress.common_source
TO :'codec_truncated_zstd_path'
WITH (server 'lagodb_connectors_regress_s3');

\setenv OBJECT_STORAGE_KEY :codec_corrupt_gzip_key
\! sh bin/object_storage_tool corrupt
\setenv OBJECT_STORAGE_KEY :codec_corrupt_zstd_key
\! sh bin/object_storage_tool corrupt
\setenv OBJECT_STORAGE_TRUNCATE_BYTES 8
\setenv OBJECT_STORAGE_KEY :codec_truncated_gzip_key
\! sh bin/object_storage_tool truncate
\setenv OBJECT_STORAGE_KEY :codec_truncated_zstd_key
\! sh bin/object_storage_tool truncate

SELECT lagodb.invalidate_object_cache(
           :'codec_corrupt_gzip_path', 'lagodb_connectors_regress_s3'
       ) AS cache_invalidated
\gset
SELECT lagodb.invalidate_object_cache(
           :'codec_corrupt_zstd_path', 'lagodb_connectors_regress_s3'
       ) AS cache_invalidated
\gset
SELECT lagodb.invalidate_object_cache(
           :'codec_truncated_gzip_path', 'lagodb_connectors_regress_s3'
       ) AS cache_invalidated
\gset
SELECT lagodb.invalidate_object_cache(
           :'codec_truncated_zstd_path', 'lagodb_connectors_regress_s3'
       ) AS cache_invalidated
\gset

DROP TABLE IF EXISTS lagodb_connectors_regress.codec_error_sink;
CREATE TABLE lagodb_connectors_regress.codec_error_sink
    (:common_columns);

\set VERBOSITY sqlstate
COPY lagodb_connectors_regress.codec_error_sink
FROM :'codec_corrupt_gzip_path'
WITH (server 'lagodb_connectors_regress_s3');
COPY lagodb_connectors_regress.codec_error_sink
FROM :'codec_corrupt_zstd_path'
WITH (server 'lagodb_connectors_regress_s3');
COPY lagodb_connectors_regress.codec_error_sink
FROM :'codec_truncated_gzip_path'
WITH (server 'lagodb_connectors_regress_s3');
COPY lagodb_connectors_regress.codec_error_sink
FROM :'codec_truncated_zstd_path'
WITH (server 'lagodb_connectors_regress_s3');
\set VERBOSITY default

-- Raw fixtures reach PostgreSQL's CSV framing and field-count validation
-- through both the direct COPY adapter and the shared text/CSV FDW reader.
\setenv OBJECT_STORAGE_FILE data/malformed_csv_framing.csv
\setenv OBJECT_STORAGE_KEY :codec_malformed_framing_key
\! sh bin/object_storage_tool put
\setenv OBJECT_STORAGE_FILE data/malformed_csv_width.csv
\setenv OBJECT_STORAGE_KEY :codec_malformed_width_key
\! sh bin/object_storage_tool put

DROP TABLE IF EXISTS lagodb_connectors_regress.codec_csv_sink;
CREATE TABLE lagodb_connectors_regress.codec_csv_sink (
    id integer,
    payload text
);
CREATE FOREIGN TABLE lagodb_connectors_regress.codec_csv_framing (
    id integer,
    payload text
)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'codec_malformed_framing_path', format 'csv');
CREATE FOREIGN TABLE lagodb_connectors_regress.codec_csv_width (
    id integer,
    payload text
)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'codec_malformed_width_path', format 'csv');

\set VERBOSITY sqlstate
COPY lagodb_connectors_regress.codec_csv_sink
FROM :'codec_malformed_framing_path'
WITH (server 'lagodb_connectors_regress_s3', format 'csv');
COPY lagodb_connectors_regress.codec_csv_sink
FROM :'codec_malformed_width_path'
WITH (server 'lagodb_connectors_regress_s3', format 'csv');
SELECT count(*) FROM lagodb_connectors_regress.codec_csv_framing;
SELECT count(*) FROM lagodb_connectors_regress.codec_csv_width;
\set VERBOSITY default

RESET client_min_messages;
