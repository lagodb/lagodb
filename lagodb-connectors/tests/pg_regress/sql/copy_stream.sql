\i include/column_definitions.sql

-- Direct COPY paths for Text, CSV, and JSON stream formats.
--
-- Exact outputs exercise suffix inference. Prefix outputs require an explicit
-- format because the prefix itself does not carry a format suffix.

SET TIME ZONE 'UTC';
SET client_min_messages = warning;

SELECT bucket AS lakebase_regress_bucket
FROM lakebase_regress.object_storage_fixture
\gset

SELECT format('s3://%s/lagodb-connectors/copy/text/exact.txt',
              :'lakebase_regress_bucket') AS copy_text_exact,
       format('s3://%s/lagodb-connectors/copy/text/prefix/',
              :'lakebase_regress_bucket') AS copy_text_prefix,
       format('s3://%s/lagodb-connectors/copy/csv/exact.csv',
              :'lakebase_regress_bucket') AS copy_csv_exact,
       format('s3://%s/lagodb-connectors/copy/csv/prefix/',
              :'lakebase_regress_bucket') AS copy_csv_prefix,
       format('s3://%s/lagodb-connectors/copy/csv/header.csv',
              :'lakebase_regress_bucket') AS copy_csv_header,
       format('s3://%s/lagodb-connectors/copy/csv/custom.csv',
              :'lakebase_regress_bucket') AS copy_csv_custom,
       format('s3://%s/lagodb-connectors/copy/csv/compressed.csv.gz',
              :'lakebase_regress_bucket') AS copy_csv_compressed,
       format('s3://%s/lagodb-connectors/copy/text/compressed.txt.zst',
              :'lakebase_regress_bucket') AS copy_text_compressed,
       format('s3://%s/lagodb-connectors/copy/aliases/data.text',
              :'lakebase_regress_bucket') AS copy_text_alias,
       format('s3://%s/lagodb-connectors/copy/json/exact.json',
              :'lakebase_regress_bucket') AS copy_json_exact,
       format('s3://%s/lagodb-connectors/copy/json/prefix/',
              :'lakebase_regress_bucket') AS copy_json_prefix,
       format('s3://%s/lagodb-connectors/copy/json/compressed.json.gz',
              :'lakebase_regress_bucket') AS copy_json_compressed,
       format('s3://%s/lagodb-connectors/copy/aliases/data.ndjson',
              :'lakebase_regress_bucket') AS copy_json_alias,
       format('s3://%s/lagodb-connectors/copy/extra/text.txt',
              :'lakebase_regress_bucket') AS copy_extra_text,
       format('s3://%s/lagodb-connectors/copy/extra/csv.csv',
              :'lakebase_regress_bucket') AS copy_extra_csv
\gset

-- Text: scalar types, NULLs, escaping, exact COPY TO/FROM, and prefix COPY TO.
COPY lagodb_connectors_regress.common_source
TO :'copy_text_exact'
WITH (server 'lagodb_connectors_regress_s3');
COPY lagodb_connectors_regress.common_source
TO :'copy_text_prefix'
WITH (server 'lagodb_connectors_regress_s3', format 'text');

DROP TABLE IF EXISTS lagodb_connectors_regress.copy_text_from;
CREATE TABLE lagodb_connectors_regress.copy_text_from
    (:common_columns);
COPY lagodb_connectors_regress.copy_text_from
FROM :'copy_text_exact'
WITH (server 'lagodb_connectors_regress_s3');

SELECT 'common_source' AS relation,
       count(*) AS rows,
       md5(string_agg(row_to_json(source)::text, E'\n' ORDER BY source.id)) AS digest
FROM lagodb_connectors_regress.common_source AS source
UNION ALL
SELECT 'copy_text_from',
       count(*),
       md5(string_agg(row_to_json(source)::text, E'\n' ORDER BY source.id))
FROM lagodb_connectors_regress.copy_text_from AS source
ORDER BY relation;

COPY lagodb_connectors_regress.common_source
TO :'copy_text_alias'
WITH (server 'lagodb_connectors_regress_s3');
DROP TABLE IF EXISTS lagodb_connectors_regress.copy_text_alias_from;
CREATE TABLE lagodb_connectors_regress.copy_text_alias_from
    (:common_columns);
COPY lagodb_connectors_regress.copy_text_alias_from
FROM :'copy_text_alias'
WITH (server 'lagodb_connectors_regress_s3');
SELECT count(*) AS text_alias_rows,
       md5(string_agg(row_to_json(source)::text, E'\n' ORDER BY source.id))
       AS text_alias_digest
FROM lagodb_connectors_regress.copy_text_alias_from AS source;

-- CSV: exact/prefix paths, header handling, and PostgreSQL CSV options.
COPY lagodb_connectors_regress.common_source
TO :'copy_csv_exact'
WITH (server 'lagodb_connectors_regress_s3');
COPY lagodb_connectors_regress.common_source
TO :'copy_csv_prefix'
WITH (server 'lagodb_connectors_regress_s3', format 'csv');
COPY lagodb_connectors_regress.common_source
TO :'copy_csv_header'
WITH (server 'lagodb_connectors_regress_s3', format 'csv', header true);
COPY lagodb_connectors_regress.common_source
TO :'copy_csv_custom'
WITH (
    server 'lagodb_connectors_regress_s3',
    format 'csv',
    delimiter ';',
    null '<NULL>',
    quote '"',
    escape '"'
);
COPY lagodb_connectors_regress.common_source
TO :'copy_csv_compressed'
WITH (
    server 'lagodb_connectors_regress_s3',
    format 'csv',
    compression 'gzip'
);

DROP TABLE IF EXISTS lagodb_connectors_regress.copy_csv_from;
CREATE TABLE lagodb_connectors_regress.copy_csv_from
    (:common_columns);
COPY lagodb_connectors_regress.copy_csv_from
FROM :'copy_csv_exact'
WITH (server 'lagodb_connectors_regress_s3');

DROP TABLE IF EXISTS lagodb_connectors_regress.copy_csv_header_from;
CREATE TABLE lagodb_connectors_regress.copy_csv_header_from
    (:common_columns);
COPY lagodb_connectors_regress.copy_csv_header_from
FROM :'copy_csv_header'
WITH (server 'lagodb_connectors_regress_s3', header true);

DROP TABLE IF EXISTS lagodb_connectors_regress.copy_csv_custom_from;
CREATE TABLE lagodb_connectors_regress.copy_csv_custom_from
    (:common_columns);
COPY lagodb_connectors_regress.copy_csv_custom_from
FROM :'copy_csv_custom'
WITH (
    server 'lagodb_connectors_regress_s3',
    delimiter ';',
    null '<NULL>',
    quote '"',
    escape '"'
);

DROP TABLE IF EXISTS lagodb_connectors_regress.copy_csv_compressed_from;
CREATE TABLE lagodb_connectors_regress.copy_csv_compressed_from
    (:common_columns);
COPY lagodb_connectors_regress.copy_csv_compressed_from
FROM :'copy_csv_compressed'
WITH (server 'lagodb_connectors_regress_s3');

SELECT relation, rows, digest
FROM (
    SELECT 'csv' AS relation,
           count(*) AS rows,
           md5(string_agg(row_to_json(source)::text, E'\n' ORDER BY source.id)) AS digest
    FROM lagodb_connectors_regress.copy_csv_from AS source
    UNION ALL
    SELECT 'csv-header', count(*),
           md5(string_agg(row_to_json(source)::text, E'\n' ORDER BY source.id))
    FROM lagodb_connectors_regress.copy_csv_header_from AS source
    UNION ALL
    SELECT 'csv-custom', count(*),
           md5(string_agg(row_to_json(source)::text, E'\n' ORDER BY source.id))
    FROM lagodb_connectors_regress.copy_csv_custom_from AS source
    UNION ALL
    SELECT 'csv-gzip', count(*),
           md5(string_agg(row_to_json(source)::text, E'\n' ORDER BY source.id))
    FROM lagodb_connectors_regress.copy_csv_compressed_from AS source
) AS results
ORDER BY relation;

-- Text zstd is read back with compression inferred from the suffix.
COPY lagodb_connectors_regress.common_source
TO :'copy_text_compressed'
WITH (
    server 'lagodb_connectors_regress_s3',
    format 'text',
    compression 'zstd'
);
DROP TABLE IF EXISTS lagodb_connectors_regress.copy_text_compressed_from;
CREATE TABLE lagodb_connectors_regress.copy_text_compressed_from
    (:common_columns);
COPY lagodb_connectors_regress.copy_text_compressed_from
FROM :'copy_text_compressed'
WITH (server 'lagodb_connectors_regress_s3');
SELECT count(*) AS text_zstd_rows,
       md5(string_agg(row_to_json(source)::text, E'\n' ORDER BY source.id))
       AS text_zstd_digest
FROM lagodb_connectors_regress.copy_text_compressed_from AS source;

-- JSON: json/jsonb values are exercised in addition to the common scalar set.
COPY lagodb_connectors_regress.json_source
TO :'copy_json_exact'
WITH (server 'lagodb_connectors_regress_s3');
COPY lagodb_connectors_regress.json_source
TO :'copy_json_prefix'
WITH (server 'lagodb_connectors_regress_s3', format 'json');
COPY lagodb_connectors_regress.json_source
TO :'copy_json_compressed'
WITH (
    server 'lagodb_connectors_regress_s3',
    format 'json',
    compression 'gzip'
);

DROP TABLE IF EXISTS lagodb_connectors_regress.copy_json_from;
CREATE TABLE lagodb_connectors_regress.copy_json_from
    (:json_columns);
COPY lagodb_connectors_regress.copy_json_from
FROM :'copy_json_exact'
WITH (server 'lagodb_connectors_regress_s3');

DROP TABLE IF EXISTS lagodb_connectors_regress.copy_json_compressed_from;
CREATE TABLE lagodb_connectors_regress.copy_json_compressed_from
    (:json_columns);
COPY lagodb_connectors_regress.copy_json_compressed_from
FROM :'copy_json_compressed'
WITH (server 'lagodb_connectors_regress_s3');

SELECT relation, rows, digest
FROM (
    SELECT 'json' AS relation,
           count(*) AS rows,
           md5(string_agg(row_to_json(source)::text, E'\n' ORDER BY source.id)) AS digest
    FROM lagodb_connectors_regress.copy_json_from AS source
    UNION ALL
    SELECT 'json-gzip', count(*),
           md5(string_agg(row_to_json(source)::text, E'\n' ORDER BY source.id))
    FROM lagodb_connectors_regress.copy_json_compressed_from AS source
) AS results
ORDER BY relation;

COPY lagodb_connectors_regress.json_source
TO :'copy_json_alias'
WITH (server 'lagodb_connectors_regress_s3');
DROP TABLE IF EXISTS lagodb_connectors_regress.copy_json_alias_from;
CREATE TABLE lagodb_connectors_regress.copy_json_alias_from
    (:json_columns);
COPY lagodb_connectors_regress.copy_json_alias_from
FROM :'copy_json_alias'
WITH (server 'lagodb_connectors_regress_s3');
SELECT count(*) AS json_alias_rows,
       md5(string_agg(row_to_json(source)::text, E'\n' ORDER BY source.id))
       AS json_alias_digest
FROM lagodb_connectors_regress.copy_json_alias_from AS source;

-- Stream formats retain PostgreSQL COPY's array and JSON datum semantics.
COPY lagodb_connectors_regress.stream_extra_source
TO :'copy_extra_text'
WITH (server 'lagodb_connectors_regress_s3', format 'text');
COPY lagodb_connectors_regress.stream_extra_source
TO :'copy_extra_csv'
WITH (server 'lagodb_connectors_regress_s3', format 'csv');

DROP TABLE IF EXISTS lagodb_connectors_regress.copy_extra_text_from;
CREATE TABLE lagodb_connectors_regress.copy_extra_text_from
    (:stream_extra_columns);
COPY lagodb_connectors_regress.copy_extra_text_from
FROM :'copy_extra_text'
WITH (server 'lagodb_connectors_regress_s3', format 'text');

DROP TABLE IF EXISTS lagodb_connectors_regress.copy_extra_csv_from;
CREATE TABLE lagodb_connectors_regress.copy_extra_csv_from
    (:stream_extra_columns);
COPY lagodb_connectors_regress.copy_extra_csv_from
FROM :'copy_extra_csv'
WITH (server 'lagodb_connectors_regress_s3', format 'csv');

SELECT relation, rows, digest
FROM (
    SELECT 'extra-text' AS relation,
           count(*) AS rows,
           md5(string_agg(row_to_json(source)::text, E'\n' ORDER BY source.id)) AS digest
    FROM lagodb_connectors_regress.copy_extra_text_from AS source
    UNION ALL
    SELECT 'extra-csv', count(*),
           md5(string_agg(row_to_json(source)::text, E'\n' ORDER BY source.id))
    FROM lagodb_connectors_regress.copy_extra_csv_from AS source
) AS results
ORDER BY relation;

RESET client_min_messages;
