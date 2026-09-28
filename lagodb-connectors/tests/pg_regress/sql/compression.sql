\i include/column_definitions.sql

-- Shared stream-compression inference, overrides, and codec boundaries.

SELECT endpoint,
       bucket,
       region,
       access_key_id,
       secret_access_key
FROM lagodb_regress.object_storage_fixture
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
       format('s3://%s/lagodb-connectors/codecs/truncated.txt.zst',
              :'storage_bucket') AS truncated_zstd_path,
       format('s3://%s/lagodb-connectors/codecs/corrupt.txt.gz',
              :'storage_bucket') AS corrupt_gzip_path,
       'lagodb-connectors/codecs/member-1.txt.gz' AS gzip_member_1_key,
       'lagodb-connectors/codecs/member-2.txt.gz' AS gzip_member_2_key,
       'lagodb-connectors/codecs/concatenated.txt.gz' AS gzip_concatenated_key,
       'lagodb-connectors/codecs/truncated.txt.zst' AS truncated_zstd_key,
       'lagodb-connectors/codecs/corrupt.txt.gz' AS corrupt_gzip_key
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

SELECT count(*) AS copy_none_gz_rows
FROM lagodb_connectors_regress.codec_plain_gz;
SELECT count(*) AS foreign_none_zst_rows
FROM lagodb_connectors_regress.codec_plain_zst;

-- Long compression suffix aliases participate in format/compression inference.
COPY lagodb_connectors_regress.common_source
TO :'codec_gzip_alias_path'
WITH (server 'lagodb_connectors_regress_s3');
COPY lagodb_connectors_regress.common_source
TO :'codec_zstd_alias_path'
WITH (server 'lagodb_connectors_regress_s3');

CREATE TABLE lagodb_connectors_regress.codec_gzip_alias
    (:common_columns);
COPY lagodb_connectors_regress.codec_gzip_alias
FROM :'codec_gzip_alias_path'
WITH (server 'lagodb_connectors_regress_s3');
CREATE FOREIGN TABLE lagodb_connectors_regress.codec_zstd_alias
    (:common_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'codec_zstd_alias_path');

SELECT count(*) AS gzip_alias_rows
FROM lagodb_connectors_regress.codec_gzip_alias;
SELECT count(*) AS zstd_alias_rows
FROM lagodb_connectors_regress.codec_zstd_alias;

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

CREATE TABLE lagodb_connectors_regress.codec_concatenated_gzip
    (:common_columns);
COPY lagodb_connectors_regress.codec_concatenated_gzip
FROM :'codec_gzip_concatenated_path'
WITH (server 'lagodb_connectors_regress_s3');
SELECT id FROM lagodb_connectors_regress.codec_concatenated_gzip ORDER BY id;

-- One representative failure per codec protects error propagation without
-- multiplying corruption and truncation across every stream format.
COPY lagodb_connectors_regress.common_source
TO :'codec_corrupt_gzip_path'
WITH (server 'lagodb_connectors_regress_s3');
COPY lagodb_connectors_regress.common_source
TO :'codec_truncated_zstd_path'
WITH (server 'lagodb_connectors_regress_s3');

\setenv OBJECT_STORAGE_KEY :codec_corrupt_gzip_key
\! sh bin/object_storage_tool corrupt
\setenv OBJECT_STORAGE_TRUNCATE_BYTES 8
\setenv OBJECT_STORAGE_KEY :codec_truncated_zstd_key
\! sh bin/object_storage_tool truncate

SELECT lagodb.invalidate_object_cache(
           :'codec_corrupt_gzip_path', 'lagodb_connectors_regress_s3'
       ) AS cache_invalidated
\gset
SELECT lagodb.invalidate_object_cache(
           :'codec_truncated_zstd_path', 'lagodb_connectors_regress_s3'
       ) AS cache_invalidated
\gset

CREATE TABLE lagodb_connectors_regress.codec_error_sink
    (:common_columns);

\set VERBOSITY sqlstate
COPY lagodb_connectors_regress.codec_error_sink
FROM :'codec_corrupt_gzip_path'
WITH (server 'lagodb_connectors_regress_s3');
COPY lagodb_connectors_regress.codec_error_sink
FROM :'codec_truncated_zstd_path'
WITH (server 'lagodb_connectors_regress_s3');
\set VERBOSITY default
