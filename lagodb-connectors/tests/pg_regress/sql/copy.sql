\i include/column_definitions.sql

-- Common COPY routing, canonical bridge, and output-contract coverage.

SELECT bucket AS lagodb_regress_bucket
FROM lagodb_regress.object_storage_fixture
\gset

SELECT format('s3://%s/lagodb-connectors/copy/bridge/sentinels.json',
              :'lagodb_regress_bucket') AS bridge_json,
       format('s3://%s/lagodb-connectors/copy/bridge/sentinels.avro',
              :'lagodb_regress_bucket') AS bridge_avro,
       format('s3://%s/lagodb-connectors/copy/bridge/sentinels.parquet',
              :'lagodb_regress_bucket') AS bridge_parquet,
       format('s3://%s/lagodb-connectors/copy/empty-prefix/',
              :'lagodb_regress_bucket') AS empty_prefix,
       format('s3://%s/lagodb-connectors/copy/missing.txt',
              :'lagodb_regress_bucket') AS missing_exact
\gset copy_

-- JSON, Avro, and Parquet share the canonical CSV bridge between PostgreSQL's
-- byte-oriented COPY callbacks and native typed encoders. The values separate
-- protocol sentinels from SQL NULL and exercise quoting and row-buffer reuse.
CREATE TABLE lagodb_connectors_regress.copy_bridge_source (
    id integer,
    payload text
);
INSERT INTO lagodb_connectors_regress.copy_bridge_source
VALUES (1, NULL),
       (2, ''),
       (3, E'\\N'),
       (4, E'\\.'),
       (5, 'comma,value'),
       (6, 'quote"value'),
       (7, E'line\nbreak');

COPY lagodb_connectors_regress.copy_bridge_source
TO :'copy_bridge_json';
COPY lagodb_connectors_regress.copy_bridge_source
TO :'copy_bridge_avro'
WITH (server 'lagodb_connectors_regress_s3', format 'avro');
COPY lagodb_connectors_regress.copy_bridge_source
TO :'copy_bridge_parquet'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');

CREATE TABLE lagodb_connectors_regress.copy_bridge_json
    (:id_payload_columns);
COPY lagodb_connectors_regress.copy_bridge_json
FROM :'copy_bridge_json';
CREATE TABLE lagodb_connectors_regress.copy_bridge_avro
    (:id_payload_columns);
COPY lagodb_connectors_regress.copy_bridge_avro
FROM :'copy_bridge_avro'
WITH (server 'lagodb_connectors_regress_s3', format 'avro');
CREATE TABLE lagodb_connectors_regress.copy_bridge_parquet
    (:id_payload_columns);
COPY lagodb_connectors_regress.copy_bridge_parquet
FROM :'copy_bridge_parquet'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');

SELECT relation, rows, source_digest = sink_digest AS round_trip
FROM (
    SELECT 'json' AS relation,
           count(*) AS rows,
           (SELECT md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id))
            FROM lagodb_connectors_regress.copy_bridge_source AS value) AS source_digest,
           md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id)) AS sink_digest
    FROM lagodb_connectors_regress.copy_bridge_json AS value
    UNION ALL
    SELECT 'avro', count(*),
           (SELECT md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id))
            FROM lagodb_connectors_regress.copy_bridge_source AS value),
           md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id))
    FROM lagodb_connectors_regress.copy_bridge_avro AS value
    UNION ALL
    SELECT 'parquet', count(*),
           (SELECT md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id))
            FROM lagodb_connectors_regress.copy_bridge_source AS value),
           md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id))
    FROM lagodb_connectors_regress.copy_bridge_parquet AS value
) AS results
ORDER BY relation;

-- Direct COPY TO emits one readable empty object for a prefix. The per-format
-- empty exact-object contract is exercised by each format suite.
COPY (
    SELECT * FROM lagodb_connectors_regress.common_source WHERE false
) TO :'copy_empty_prefix'
WITH (server 'lagodb_connectors_regress_s3', format 'text');
CREATE FOREIGN TABLE lagodb_connectors_regress.copy_empty_prefix
    (:common_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'copy_empty_prefix', format 'text');
SELECT count(*) AS empty_prefix_rows
FROM lagodb_connectors_regress.copy_empty_prefix;

-- Representative common routing failures: no matching accessible server and
-- a missing exact object at the COPY FFI boundary.
CREATE TABLE lagodb_connectors_regress.copy_error_sink
    (:common_columns);
\set VERBOSITY sqlstate
COPY lagodb_connectors_regress.common_source
TO 's3://invalid-bucket/lagodb-connectors/copy/no-default.txt';
COPY lagodb_connectors_regress.copy_error_sink
FROM :'copy_missing_exact'
WITH (server 'lagodb_connectors_regress_s3', format 'text');
\set VERBOSITY default
