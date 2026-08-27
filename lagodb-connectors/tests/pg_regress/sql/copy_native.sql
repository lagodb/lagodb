\i include/column_definitions.sql

-- Direct COPY paths for Avro and Parquet container formats.

SET TIME ZONE 'UTC';
SET client_min_messages = warning;

SELECT bucket AS lagodb_regress_bucket
FROM lagodb_regress.object_storage_fixture
\gset

SELECT format('s3://%s/lagodb-connectors/copy/avro/exact.avro',
              :'lagodb_regress_bucket') AS avro_exact,
       format('s3://%s/lagodb-connectors/copy/avro/prefix/',
              :'lagodb_regress_bucket') AS avro_prefix,
       format('s3://%s/lagodb-connectors/copy/avro/snappy.avro',
              :'lagodb_regress_bucket') AS avro_snappy,
       format('s3://%s/lagodb-connectors/copy/parquet/exact.parquet',
              :'lagodb_regress_bucket') AS parquet_exact,
       format('s3://%s/lagodb-connectors/copy/parquet/prefix/',
              :'lagodb_regress_bucket') AS parquet_prefix,
       format('s3://%s/lagodb-connectors/copy/parquet/zstd.parquet',
              :'lagodb_regress_bucket') AS parquet_zstd,
       format('s3://%s/lagodb-connectors/copy/native-bridge/sentinels.json',
              :'lagodb_regress_bucket') AS bridge_json,
       format('s3://%s/lagodb-connectors/copy/native-bridge/sentinels.avro',
              :'lagodb_regress_bucket') AS bridge_avro,
       format('s3://%s/lagodb-connectors/copy/native-bridge/sentinels.parquet',
              :'lagodb_regress_bucket') AS bridge_parquet
\gset native_

COPY lagodb_connectors_regress.common_source
TO :'native_avro_exact';
COPY lagodb_connectors_regress.common_source
TO :'native_avro_prefix'
WITH (server 'lagodb_connectors_regress_s3', format 'avro');
COPY lagodb_connectors_regress.common_source
TO :'native_avro_snappy'
WITH (
    server 'lagodb_connectors_regress_s3',
    format 'avro',
    compression 'snappy'
);

DROP TABLE IF EXISTS lagodb_connectors_regress.copy_avro_exact;
CREATE TABLE lagodb_connectors_regress.copy_avro_exact
    (:common_columns);
COPY lagodb_connectors_regress.copy_avro_exact
FROM :'native_avro_exact';
DROP TABLE IF EXISTS lagodb_connectors_regress.copy_avro_snappy;
CREATE TABLE lagodb_connectors_regress.copy_avro_snappy
    (:common_columns);
COPY lagodb_connectors_regress.copy_avro_snappy
FROM :'native_avro_snappy'
WITH (server 'lagodb_connectors_regress_s3');

SELECT relation, rows, digest
FROM (
    SELECT 'avro' AS relation, count(*) AS rows,
           md5(string_agg(row_to_json(source)::text, E'\n' ORDER BY source.id)) AS digest
    FROM lagodb_connectors_regress.copy_avro_exact AS source
    UNION ALL
    SELECT 'avro-snappy', count(*),
           md5(string_agg(row_to_json(source)::text, E'\n' ORDER BY source.id))
    FROM lagodb_connectors_regress.copy_avro_snappy AS source
) AS avro_results
ORDER BY relation;

COPY lagodb_connectors_regress.parquet_source
TO :'native_parquet_exact'
WITH (server 'lagodb_connectors_regress_s3');
COPY lagodb_connectors_regress.parquet_source
TO :'native_parquet_prefix'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');
COPY lagodb_connectors_regress.parquet_source
TO :'native_parquet_zstd'
WITH (
    server 'lagodb_connectors_regress_s3',
    format 'parquet',
    compression 'zstd'
);

DROP TABLE IF EXISTS lagodb_connectors_regress.copy_parquet_exact;
CREATE TABLE lagodb_connectors_regress.copy_parquet_exact
    (:parquet_columns);
COPY lagodb_connectors_regress.copy_parquet_exact
FROM :'native_parquet_exact'
WITH (server 'lagodb_connectors_regress_s3');
DROP TABLE IF EXISTS lagodb_connectors_regress.copy_parquet_zstd;
CREATE TABLE lagodb_connectors_regress.copy_parquet_zstd
    (:parquet_columns);
COPY lagodb_connectors_regress.copy_parquet_zstd
FROM :'native_parquet_zstd'
WITH (server 'lagodb_connectors_regress_s3');
DROP TABLE IF EXISTS lagodb_connectors_regress.copy_parquet_prefix;
CREATE TABLE lagodb_connectors_regress.copy_parquet_prefix
    (:parquet_columns);
COPY lagodb_connectors_regress.copy_parquet_prefix
FROM :'native_parquet_prefix'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');

SELECT relation, rows, digest
FROM (
    SELECT 'parquet' AS relation, count(*) AS rows,
           md5(string_agg(row_to_json(source)::text, E'\n' ORDER BY source.id)) AS digest
    FROM lagodb_connectors_regress.copy_parquet_exact AS source
    UNION ALL
    SELECT 'parquet-zstd', count(*),
           md5(string_agg(row_to_json(source)::text, E'\n' ORDER BY source.id))
    FROM lagodb_connectors_regress.copy_parquet_zstd AS source
    UNION ALL
    SELECT 'parquet-prefix', count(*),
           md5(string_agg(row_to_json(source)::text, E'\n' ORDER BY source.id))
    FROM lagodb_connectors_regress.copy_parquet_prefix AS source
) AS parquet_results
ORDER BY relation;

-- Native formats exchange rows with PostgreSQL through the connector's
-- canonical CSV bridge. These values distinguish protocol sentinels from
-- actual NULL and exercise quoting, escaping, and row-buffer reuse.
DROP TABLE IF EXISTS lagodb_connectors_regress.native_bridge_source;
CREATE TABLE lagodb_connectors_regress.native_bridge_source (
    id integer,
    payload text
);
INSERT INTO lagodb_connectors_regress.native_bridge_source
VALUES (1, NULL),
       (2, ''),
       (3, E'\\N'),
       (4, E'\\.'),
       (5, 'comma,value'),
       (6, 'quote"value'),
       (7, E'line\nbreak');

COPY lagodb_connectors_regress.native_bridge_source
TO :'native_bridge_json'
WITH (server 'lagodb_connectors_regress_s3', format 'json');
COPY lagodb_connectors_regress.native_bridge_source
TO :'native_bridge_avro'
WITH (server 'lagodb_connectors_regress_s3', format 'avro');
COPY lagodb_connectors_regress.native_bridge_source
TO :'native_bridge_parquet'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');

CREATE TABLE lagodb_connectors_regress.native_bridge_json
    (:id_payload_columns);
COPY lagodb_connectors_regress.native_bridge_json
FROM :'native_bridge_json'
WITH (server 'lagodb_connectors_regress_s3', format 'json');
CREATE TABLE lagodb_connectors_regress.native_bridge_avro
    (:id_payload_columns);
COPY lagodb_connectors_regress.native_bridge_avro
FROM :'native_bridge_avro'
WITH (server 'lagodb_connectors_regress_s3', format 'avro');
CREATE TABLE lagodb_connectors_regress.native_bridge_parquet
    (:id_payload_columns);
COPY lagodb_connectors_regress.native_bridge_parquet
FROM :'native_bridge_parquet'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');

SELECT relation, rows, source_digest = sink_digest AS round_trip
FROM (
    SELECT 'json' AS relation,
           count(*) AS rows,
           (SELECT md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id))
            FROM lagodb_connectors_regress.native_bridge_source AS value) AS source_digest,
           md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id)) AS sink_digest
    FROM lagodb_connectors_regress.native_bridge_json AS value
    UNION ALL
    SELECT 'avro', count(*),
           (SELECT md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id))
            FROM lagodb_connectors_regress.native_bridge_source AS value),
           md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id))
    FROM lagodb_connectors_regress.native_bridge_avro AS value
    UNION ALL
    SELECT 'parquet', count(*),
           (SELECT md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id))
            FROM lagodb_connectors_regress.native_bridge_source AS value),
           md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id))
    FROM lagodb_connectors_regress.native_bridge_parquet AS value
) AS bridge_results
ORDER BY relation;

RESET client_min_messages;
