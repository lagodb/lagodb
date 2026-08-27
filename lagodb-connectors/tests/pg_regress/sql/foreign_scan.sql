\i include/column_definitions.sql

-- Foreign-table exact/prefix scan coverage.
--
-- Exact and prefix scans consume the seed objects created by setup.sql.

SET TIME ZONE 'UTC';
SET client_min_messages = warning;

SELECT bucket AS lagodb_regress_bucket
FROM lagodb_regress.object_storage_fixture
\gset

SELECT format('s3://%s/lagodb-connectors/seed/common-exact.txt',
              :'lagodb_regress_bucket') AS seed_text_exact,
       format('s3://%s/lagodb-connectors/seed/common-prefix/',
              :'lagodb_regress_bucket') AS seed_text_prefix,
       format('s3://%s/lagodb-connectors/seed/common-exact.csv',
              :'lagodb_regress_bucket') AS seed_csv_exact,
       format('s3://%s/lagodb-connectors/seed/common-prefix-csv/',
              :'lagodb_regress_bucket') AS seed_csv_prefix,
       format('s3://%s/lagodb-connectors/seed/common-header.csv',
              :'lagodb_regress_bucket') AS seed_csv_header,
       format('s3://%s/lagodb-connectors/seed/common-exact.avro',
              :'lagodb_regress_bucket') AS seed_avro_exact,
       format('s3://%s/lagodb-connectors/seed/common-prefix-avro/',
              :'lagodb_regress_bucket') AS seed_avro_prefix,
       format('s3://%s/lagodb-connectors/seed/json-exact.json',
              :'lagodb_regress_bucket') AS seed_json_exact,
       format('s3://%s/lagodb-connectors/seed/json-prefix/',
              :'lagodb_regress_bucket') AS seed_json_prefix,
       format('s3://%s/lagodb-connectors/seed/json-compressed.json.gz',
              :'lagodb_regress_bucket') AS seed_json_compressed,
       format('s3://%s/lagodb-connectors/seed/parquet-exact.parquet',
              :'lagodb_regress_bucket') AS seed_parquet_exact,
       format('s3://%s/lagodb-connectors/seed/parquet-prefix/',
              :'lagodb_regress_bucket') AS seed_parquet_prefix,
       format('s3://%s/lagodb-connectors/seed/extra-exact.txt',
              :'lagodb_regress_bucket') AS seed_extra_text_exact,
       format('s3://%s/lagodb-connectors/seed/extra-exact.csv',
              :'lagodb_regress_bucket') AS seed_extra_csv_exact
\gset

CREATE FOREIGN TABLE lagodb_connectors_regress.foreign_text_exact
    (:common_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'seed_text_exact');

CREATE FOREIGN TABLE lagodb_connectors_regress.foreign_text_prefix
    (:common_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'seed_text_prefix', format 'text');

CREATE FOREIGN TABLE lagodb_connectors_regress.foreign_csv_exact
    (:common_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'seed_csv_exact');

CREATE FOREIGN TABLE lagodb_connectors_regress.foreign_csv_prefix
    (:common_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'seed_csv_prefix', format 'csv');

CREATE FOREIGN TABLE lagodb_connectors_regress.foreign_csv_header
    (:common_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'seed_csv_header', format 'csv', header 'match');

CREATE FOREIGN TABLE lagodb_connectors_regress.foreign_avro_exact
    (:common_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'seed_avro_exact');

CREATE FOREIGN TABLE lagodb_connectors_regress.foreign_avro_prefix
    (:common_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'seed_avro_prefix', format 'avro');

CREATE FOREIGN TABLE lagodb_connectors_regress.foreign_json_exact
    (:json_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'seed_json_exact');

CREATE FOREIGN TABLE lagodb_connectors_regress.foreign_json_prefix
    (:json_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'seed_json_prefix', format 'json');

CREATE FOREIGN TABLE lagodb_connectors_regress.foreign_json_compressed
    (:json_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'seed_json_compressed');

CREATE FOREIGN TABLE lagodb_connectors_regress.foreign_parquet_exact
    (:parquet_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'seed_parquet_exact');

CREATE FOREIGN TABLE lagodb_connectors_regress.foreign_parquet_prefix
    (:parquet_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'seed_parquet_prefix', format 'parquet');

CREATE FOREIGN TABLE lagodb_connectors_regress.foreign_extra_text
    (:stream_extra_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'seed_extra_text_exact');

CREATE FOREIGN TABLE lagodb_connectors_regress.foreign_extra_csv
    (:stream_extra_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'seed_extra_csv_exact');

-- SELECT exact/prefix for every format, with stable row digests.
SELECT relation, rows, digest
FROM (
    SELECT 'text-exact' AS relation, count(*) AS rows,
           md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id)) AS digest
    FROM lagodb_connectors_regress.foreign_text_exact AS value
    UNION ALL
    SELECT 'text-prefix', count(*),
           md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id))
    FROM lagodb_connectors_regress.foreign_text_prefix AS value
    UNION ALL
    SELECT 'csv-exact', count(*),
           md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id))
    FROM lagodb_connectors_regress.foreign_csv_exact AS value
    UNION ALL
    SELECT 'csv-prefix', count(*),
           md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id))
    FROM lagodb_connectors_regress.foreign_csv_prefix AS value
    UNION ALL
    SELECT 'csv-header', count(*),
           md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id))
    FROM lagodb_connectors_regress.foreign_csv_header AS value
    UNION ALL
    SELECT 'avro-exact', count(*),
           md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id))
    FROM lagodb_connectors_regress.foreign_avro_exact AS value
    UNION ALL
    SELECT 'avro-prefix', count(*),
           md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id))
    FROM lagodb_connectors_regress.foreign_avro_prefix AS value
    UNION ALL
    SELECT 'json-exact', count(*),
           md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id))
    FROM lagodb_connectors_regress.foreign_json_exact AS value
    UNION ALL
    SELECT 'json-prefix', count(*),
           md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id))
    FROM lagodb_connectors_regress.foreign_json_prefix AS value
    UNION ALL
    SELECT 'json-compressed', count(*),
           md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id))
    FROM lagodb_connectors_regress.foreign_json_compressed AS value
    UNION ALL
    SELECT 'parquet-exact', count(*),
           md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id))
    FROM lagodb_connectors_regress.foreign_parquet_exact AS value
    UNION ALL
    SELECT 'parquet-prefix', count(*),
           md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id))
    FROM lagodb_connectors_regress.foreign_parquet_prefix AS value
    UNION ALL
    SELECT 'extra-text', count(*),
           md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id))
    FROM lagodb_connectors_regress.foreign_extra_text AS value
    UNION ALL
    SELECT 'extra-csv', count(*),
           md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id))
    FROM lagodb_connectors_regress.foreign_extra_csv AS value
) AS results
ORDER BY relation;

RESET client_min_messages;
