-- Cross-path round trips: foreign scan -> direct COPY TO -> direct COPY FROM.
-- The foreign input is a prefix for every format, while the final COPY FROM
-- uses exact objects for formats whose direct input contract is exact-only.

SET TIME ZONE 'UTC';
SET client_min_messages = warning;

SELECT bucket AS lakebase_regress_bucket
FROM lakebase_regress.object_storage_fixture
\gset

SELECT format('s3://%s/lagodb-connectors/seed/common-prefix/',
              :'lakebase_regress_bucket') AS seed_text_prefix,
       format('s3://%s/lagodb-connectors/seed/common-prefix-csv/',
              :'lakebase_regress_bucket') AS seed_csv_prefix,
       format('s3://%s/lagodb-connectors/seed/json-prefix/',
              :'lakebase_regress_bucket') AS seed_json_prefix,
       format('s3://%s/lagodb-connectors/seed/common-prefix-avro/',
              :'lakebase_regress_bucket') AS seed_avro_prefix,
       format('s3://%s/lagodb-connectors/seed/parquet-prefix/',
              :'lakebase_regress_bucket') AS seed_parquet_prefix
\gset

SELECT format('s3://%s/lagodb-connectors/round-trip/text.txt',
              :'lakebase_regress_bucket') AS round_text,
       format('s3://%s/lagodb-connectors/round-trip/csv.csv',
              :'lakebase_regress_bucket') AS round_csv,
       format('s3://%s/lagodb-connectors/round-trip/json.json',
              :'lakebase_regress_bucket') AS round_json,
       format('s3://%s/lagodb-connectors/round-trip/avro.avro',
              :'lakebase_regress_bucket') AS round_avro,
       format('s3://%s/lagodb-connectors/round-trip/parquet/',
              :'lakebase_regress_bucket') AS round_parquet
\gset

CREATE FOREIGN TABLE lagodb_connectors_regress.round_text_input
    (LIKE lagodb_connectors_regress.common_source)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'seed_text_prefix', format 'text');

CREATE FOREIGN TABLE lagodb_connectors_regress.round_csv_input
    (LIKE lagodb_connectors_regress.common_source)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'seed_csv_prefix', format 'csv');

CREATE FOREIGN TABLE lagodb_connectors_regress.round_json_input
    (LIKE lagodb_connectors_regress.json_source)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'seed_json_prefix', format 'json');

CREATE FOREIGN TABLE lagodb_connectors_regress.round_avro_input
    (LIKE lagodb_connectors_regress.common_source)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'seed_avro_prefix', format 'avro');

CREATE FOREIGN TABLE lagodb_connectors_regress.round_parquet_input
    (LIKE lagodb_connectors_regress.parquet_source)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'seed_parquet_prefix', format 'parquet');

COPY (
    SELECT *
    FROM lagodb_connectors_regress.round_text_input
    ORDER BY id
) TO :'round_text'
WITH (storage_server 'lagodb_connectors_regress_s3', format 'text');

COPY (
    SELECT *
    FROM lagodb_connectors_regress.round_csv_input
    ORDER BY id
) TO :'round_csv'
WITH (storage_server 'lagodb_connectors_regress_s3', format 'csv');

COPY (
    SELECT *
    FROM lagodb_connectors_regress.round_json_input
    ORDER BY id
) TO :'round_json'
WITH (storage_server 'lagodb_connectors_regress_s3', format 'json');

COPY (
    SELECT *
    FROM lagodb_connectors_regress.round_avro_input
    ORDER BY id
) TO :'round_avro'
WITH (storage_server 'lagodb_connectors_regress_s3', format 'avro');

COPY (
    SELECT *
    FROM lagodb_connectors_regress.round_parquet_input
    ORDER BY id
) TO :'round_parquet'
WITH (storage_server 'lagodb_connectors_regress_s3', format 'parquet');

CREATE TABLE lagodb_connectors_regress.round_text_sink
    (LIKE lagodb_connectors_regress.common_source);
COPY lagodb_connectors_regress.round_text_sink
FROM :'round_text'
WITH (storage_server 'lagodb_connectors_regress_s3', format 'text');

CREATE TABLE lagodb_connectors_regress.round_csv_sink
    (LIKE lagodb_connectors_regress.common_source);
COPY lagodb_connectors_regress.round_csv_sink
FROM :'round_csv'
WITH (storage_server 'lagodb_connectors_regress_s3', format 'csv');

CREATE TABLE lagodb_connectors_regress.round_json_sink
    (LIKE lagodb_connectors_regress.json_source);
COPY lagodb_connectors_regress.round_json_sink
FROM :'round_json'
WITH (storage_server 'lagodb_connectors_regress_s3', format 'json');

CREATE TABLE lagodb_connectors_regress.round_avro_sink
    (LIKE lagodb_connectors_regress.common_source);
COPY lagodb_connectors_regress.round_avro_sink
FROM :'round_avro'
WITH (storage_server 'lagodb_connectors_regress_s3', format 'avro');

CREATE TABLE lagodb_connectors_regress.round_parquet_sink
    (LIKE lagodb_connectors_regress.parquet_source);
COPY lagodb_connectors_regress.round_parquet_sink
FROM :'round_parquet'
WITH (
    storage_server 'lagodb_connectors_regress_s3',
    format 'parquet'
);

SELECT relation, source_rows, sink_rows, source_digest = sink_digest AS ok
FROM (
    SELECT 'text' AS relation,
           (SELECT count(*) FROM lagodb_connectors_regress.round_text_input) AS source_rows,
           (SELECT count(*) FROM lagodb_connectors_regress.round_text_sink) AS sink_rows,
           (SELECT md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id))
            FROM lagodb_connectors_regress.round_text_input AS value) AS source_digest,
           (SELECT md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id))
            FROM lagodb_connectors_regress.round_text_sink AS value) AS sink_digest
    UNION ALL
    SELECT 'csv',
           (SELECT count(*) FROM lagodb_connectors_regress.round_csv_input),
           (SELECT count(*) FROM lagodb_connectors_regress.round_csv_sink),
           (SELECT md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id))
            FROM lagodb_connectors_regress.round_csv_input AS value),
           (SELECT md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id))
            FROM lagodb_connectors_regress.round_csv_sink AS value)
    UNION ALL
    SELECT 'json',
           (SELECT count(*) FROM lagodb_connectors_regress.round_json_input),
           (SELECT count(*) FROM lagodb_connectors_regress.round_json_sink),
           (SELECT md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id))
            FROM lagodb_connectors_regress.round_json_input AS value),
           (SELECT md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id))
            FROM lagodb_connectors_regress.round_json_sink AS value)
    UNION ALL
    SELECT 'avro',
           (SELECT count(*) FROM lagodb_connectors_regress.round_avro_input),
           (SELECT count(*) FROM lagodb_connectors_regress.round_avro_sink),
           (SELECT md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id))
            FROM lagodb_connectors_regress.round_avro_input AS value),
           (SELECT md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id))
            FROM lagodb_connectors_regress.round_avro_sink AS value)
    UNION ALL
    SELECT 'parquet',
           (SELECT count(*) FROM lagodb_connectors_regress.round_parquet_input),
           (SELECT count(*) FROM lagodb_connectors_regress.round_parquet_sink),
           (SELECT md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id))
            FROM lagodb_connectors_regress.round_parquet_input AS value),
           (SELECT md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id))
            FROM lagodb_connectors_regress.round_parquet_sink AS value)
) AS round_trips
ORDER BY relation;

RESET client_min_messages;
