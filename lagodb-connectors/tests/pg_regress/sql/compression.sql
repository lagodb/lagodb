\i include/column_definitions.sql

-- Shared stream-compression inference, overrides, and codec boundaries.

SELECT endpoint, bucket, region, access_key_id, secret_access_key
FROM lagodb_regress.object_storage_fixture
\gset storage_

\setenv OBJECT_STORAGE_ENDPOINT :storage_endpoint
\setenv OBJECT_STORAGE_BUCKET :storage_bucket
\setenv OBJECT_STORAGE_REGION :storage_region
\setenv OBJECT_STORAGE_ACCESS_KEY_ID :storage_access_key_id
\setenv OBJECT_STORAGE_SECRET_ACCESS_KEY :storage_secret_access_key

\set codec_base 's3://' :storage_bucket '/lagodb-connectors/codecs/'
\pset null _null_

-- An explicit compression option takes precedence over a compression-looking
-- suffix in both COPY and foreign-table option resolution.
\set codec_plain_gz_path :codec_base 'plain-suffix.txt.gz'
COPY lagodb_connectors_regress.common_source TO :'codec_plain_gz_path'
WITH (server 'lagodb_connectors_regress_s3', compression 'none');
CREATE TABLE lagodb_connectors_regress.codec_plain_gz (:common_columns);
COPY lagodb_connectors_regress.codec_plain_gz FROM :'codec_plain_gz_path'
WITH (server 'lagodb_connectors_regress_s3', compression 'none');
SELECT count(*) AS copy_none_gz_rows FROM lagodb_connectors_regress.codec_plain_gz;

\set codec_plain_zst_path :codec_base 'plain-suffix.txt.zst'
COPY lagodb_connectors_regress.common_source TO :'codec_plain_zst_path'
WITH (server 'lagodb_connectors_regress_s3', compression 'none');
CREATE FOREIGN TABLE lagodb_connectors_regress.codec_plain_zst (:common_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'codec_plain_zst_path', compression 'none');
SELECT count(*) AS foreign_none_zst_rows FROM lagodb_connectors_regress.codec_plain_zst;

-- Long compression suffix aliases participate in format/compression inference.
\set codec_gzip_alias_path :codec_base 'alias.txt.gzip'
COPY lagodb_connectors_regress.common_source TO :'codec_gzip_alias_path'
WITH (server 'lagodb_connectors_regress_s3');
CREATE TABLE lagodb_connectors_regress.codec_gzip_alias (:common_columns);
COPY lagodb_connectors_regress.codec_gzip_alias FROM :'codec_gzip_alias_path'
WITH (server 'lagodb_connectors_regress_s3');
SELECT count(*) AS gzip_alias_rows FROM lagodb_connectors_regress.codec_gzip_alias;

\set codec_zstd_alias_path :codec_base 'alias.txt.zstd'
COPY lagodb_connectors_regress.common_source TO :'codec_zstd_alias_path'
WITH (server 'lagodb_connectors_regress_s3');
CREATE FOREIGN TABLE lagodb_connectors_regress.codec_zstd_alias (:common_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'codec_zstd_alias_path');
SELECT count(*) AS zstd_alias_rows FROM lagodb_connectors_regress.codec_zstd_alias;

-- RFC 1952 permits concatenated gzip members. The helper concatenates the raw
-- compressed objects without decoding or recompressing either member.
\set codec_member_path :codec_base 'member-1.txt.gz'
COPY (SELECT * FROM lagodb_connectors_regress.common_source WHERE id = 1)
TO :'codec_member_path'
WITH (server 'lagodb_connectors_regress_s3');
\set codec_member_path :codec_base 'member-2.txt.gz'
COPY (SELECT * FROM lagodb_connectors_regress.common_source WHERE id = 2)
TO :'codec_member_path'
WITH (server 'lagodb_connectors_regress_s3');
\setenv OBJECT_STORAGE_SOURCE_KEY_1 'lagodb-connectors/codecs/member-1.txt.gz'
\setenv OBJECT_STORAGE_SOURCE_KEY_2 'lagodb-connectors/codecs/member-2.txt.gz'
\setenv OBJECT_STORAGE_KEY 'lagodb-connectors/codecs/concatenated.txt.gz'
\! sh bin/object_storage_tool concatenate
\set codec_concatenated_path :codec_base 'concatenated.txt.gz'
CREATE TABLE lagodb_connectors_regress.codec_concatenated_gzip (:common_columns);
COPY lagodb_connectors_regress.codec_concatenated_gzip FROM :'codec_concatenated_path'
WITH (server 'lagodb_connectors_regress_s3');
SELECT id FROM lagodb_connectors_regress.codec_concatenated_gzip ORDER BY id;

-- One representative failure per codec protects error propagation without
-- multiplying corruption and truncation across every stream format.
CREATE TABLE lagodb_connectors_regress.codec_error_sink (:common_columns);

\set codec_corrupt_path :codec_base 'corrupt.txt.gz'
COPY lagodb_connectors_regress.common_source TO :'codec_corrupt_path'
WITH (server 'lagodb_connectors_regress_s3');
\setenv OBJECT_STORAGE_KEY 'lagodb-connectors/codecs/corrupt.txt.gz'
\! sh bin/object_storage_tool corrupt
SELECT lagodb.invalidate_object_cache(
           :'codec_corrupt_path', 'lagodb_connectors_regress_s3'
       )
\g /dev/null
\set VERBOSITY sqlstate
COPY lagodb_connectors_regress.codec_error_sink FROM :'codec_corrupt_path'
WITH (server 'lagodb_connectors_regress_s3');
\set VERBOSITY default

\set codec_truncated_path :codec_base 'truncated.txt.zst'
COPY lagodb_connectors_regress.common_source TO :'codec_truncated_path'
WITH (server 'lagodb_connectors_regress_s3');
\setenv OBJECT_STORAGE_KEY 'lagodb-connectors/codecs/truncated.txt.zst'
\setenv OBJECT_STORAGE_TRUNCATE_BYTES 8
\! sh bin/object_storage_tool truncate
SELECT lagodb.invalidate_object_cache(
           :'codec_truncated_path', 'lagodb_connectors_regress_s3'
       )
\g /dev/null
\set VERBOSITY sqlstate
COPY lagodb_connectors_regress.codec_error_sink FROM :'codec_truncated_path'
WITH (server 'lagodb_connectors_regress_s3');
\set VERBOSITY default
