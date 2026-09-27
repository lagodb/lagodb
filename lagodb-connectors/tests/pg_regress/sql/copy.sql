\i include/column_definitions.sql

-- Common COPY routing, typed native-format paths, and output-contract coverage.

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

-- JSON, Avro, and Parquet use the typed Datum/slot COPY boundary. The values
-- separate protocol-looking sentinels from SQL NULL and exercise direct native
-- encoding, decoding, quoting, and row-buffer reuse.
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

-- Native COPY follows PG query routing and target assignment semantics.
-- Display payloads as JSON to distinguish NULL, empty strings, and escapes.
CREATE TABLE lagodb_connectors_regress.copy_native_sink (payload text, id integer);

SELECT format('s3://%s/lagodb-connectors/copy/query.json',
              :'lagodb_regress_bucket') AS json,
       format('s3://%s/lagodb-connectors/copy/query.avro',
              :'lagodb_regress_bucket') AS avro,
       format('s3://%s/lagodb-connectors/copy/query.parquet',
              :'lagodb_regress_bucket') AS parquet
\gset copy_

-- Query slots: reordered columns, an expression, and NULL.
COPY (
    SELECT payload, id + 10 AS id FROM lagodb_connectors_regress.copy_bridge_source
) TO :'copy_json'
WITH (server 'lagodb_connectors_regress_s3', format 'json');
COPY lagodb_connectors_regress.copy_native_sink
FROM :'copy_json'
WITH (server 'lagodb_connectors_regress_s3', format 'json');
SELECT id, to_json(payload) AS payload
FROM lagodb_connectors_regress.copy_native_sink ORDER BY id;
TRUNCATE lagodb_connectors_regress.copy_native_sink;
COPY (
    SELECT payload, id + 10 AS id FROM lagodb_connectors_regress.copy_bridge_source
) TO :'copy_avro'
WITH (server 'lagodb_connectors_regress_s3', format 'avro');
COPY lagodb_connectors_regress.copy_native_sink
FROM :'copy_avro'
WITH (server 'lagodb_connectors_regress_s3', format 'avro');
SELECT id, to_json(payload) AS payload
FROM lagodb_connectors_regress.copy_native_sink ORDER BY id;
TRUNCATE lagodb_connectors_regress.copy_native_sink;
COPY (
    SELECT payload, id + 10 AS id FROM lagodb_connectors_regress.copy_bridge_source
) TO :'copy_parquet'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');
COPY lagodb_connectors_regress.copy_native_sink
FROM :'copy_parquet'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');
SELECT id, to_json(payload) AS payload
FROM lagodb_connectors_regress.copy_native_sink ORDER BY id;
TRUNCATE lagodb_connectors_regress.copy_native_sink;

-- Typed input must still use PostgreSQL column assignment and execution.
CREATE TABLE lagodb_connectors_regress.copy_assignment_sink (
    id integer,
    payload text,
    marker integer DEFAULT 42,
    total integer GENERATED ALWAYS AS (id + marker) STORED,
    CHECK (marker = 43)
);
CREATE FUNCTION lagodb_connectors_regress.copy_assignment_before()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.marker := NEW.marker + 1;
    RETURN NEW;
END;
$$;
CREATE TRIGGER copy_assignment_before
BEFORE INSERT ON lagodb_connectors_regress.copy_assignment_sink
FOR EACH ROW EXECUTE FUNCTION lagodb_connectors_regress.copy_assignment_before();

-- Explicit column order, WHERE, defaults, BEFORE triggers, generated columns,
-- and CHECK constraints must use PostgreSQL assignment and execution.
COPY lagodb_connectors_regress.copy_assignment_sink (payload, id)
FROM :'copy_json'
WITH (server 'lagodb_connectors_regress_s3', format 'json') WHERE id >= 15;
SELECT id, to_json(payload) AS payload, marker, total
FROM lagodb_connectors_regress.copy_assignment_sink ORDER BY id;
TRUNCATE lagodb_connectors_regress.copy_assignment_sink;
COPY lagodb_connectors_regress.copy_assignment_sink (payload, id)
FROM :'copy_avro'
WITH (server 'lagodb_connectors_regress_s3', format 'avro') WHERE id >= 15;
SELECT id, to_json(payload) AS payload, marker, total
FROM lagodb_connectors_regress.copy_assignment_sink ORDER BY id;
TRUNCATE lagodb_connectors_regress.copy_assignment_sink;
COPY lagodb_connectors_regress.copy_assignment_sink (payload, id)
FROM :'copy_parquet'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet') WHERE id >= 15;
SELECT id, to_json(payload) AS payload, marker, total
FROM lagodb_connectors_regress.copy_assignment_sink ORDER BY id;

SELECT format('s3://%s/lagodb-connectors/copy/foreign.json',
              :'lagodb_regress_bucket') AS json,
       format('s3://%s/lagodb-connectors/copy/foreign.avro',
              :'lagodb_regress_bucket') AS avro,
       format('s3://%s/lagodb-connectors/copy/foreign.parquet',
              :'lagodb_regress_bucket') AS parquet
\gset copy_

-- Relation-form COPY of a foreign table is prepared as a PG query.
CREATE FOREIGN TABLE lagodb_connectors_regress.copy_native_foreign (:id_payload_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'copy_bridge_json', format 'json');
COPY lagodb_connectors_regress.copy_native_foreign TO :'copy_json'
WITH (server 'lagodb_connectors_regress_s3', format 'json');
COPY lagodb_connectors_regress.copy_native_sink
FROM :'copy_json'
WITH (server 'lagodb_connectors_regress_s3', format 'json');
SELECT id, to_json(payload) AS payload
FROM lagodb_connectors_regress.copy_native_sink ORDER BY id;
TRUNCATE lagodb_connectors_regress.copy_native_sink;
DROP FOREIGN TABLE lagodb_connectors_regress.copy_native_foreign;
CREATE FOREIGN TABLE lagodb_connectors_regress.copy_native_foreign (:id_payload_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'copy_bridge_avro', format 'avro');
COPY lagodb_connectors_regress.copy_native_foreign TO :'copy_avro'
WITH (server 'lagodb_connectors_regress_s3', format 'avro');
COPY lagodb_connectors_regress.copy_native_sink
FROM :'copy_avro'
WITH (server 'lagodb_connectors_regress_s3', format 'avro');
SELECT id, to_json(payload) AS payload
FROM lagodb_connectors_regress.copy_native_sink ORDER BY id;
TRUNCATE lagodb_connectors_regress.copy_native_sink;
DROP FOREIGN TABLE lagodb_connectors_regress.copy_native_foreign;
CREATE FOREIGN TABLE lagodb_connectors_regress.copy_native_foreign (:id_payload_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'copy_bridge_parquet', format 'parquet');
COPY lagodb_connectors_regress.copy_native_foreign TO :'copy_parquet'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');
COPY lagodb_connectors_regress.copy_native_sink
FROM :'copy_parquet'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');
SELECT id, to_json(payload) AS payload
FROM lagodb_connectors_regress.copy_native_sink ORDER BY id;
TRUNCATE lagodb_connectors_regress.copy_native_sink;
DROP FOREIGN TABLE lagodb_connectors_regress.copy_native_foreign;

SELECT format('s3://%s/lagodb-connectors/copy/typmods.json',
              :'lagodb_regress_bucket') AS json,
       format('s3://%s/lagodb-connectors/copy/typmods.avro',
              :'lagodb_regress_bucket') AS avro,
       format('s3://%s/lagodb-connectors/copy/typmods.parquet',
              :'lagodb_regress_bucket') AS parquet
\gset copy_

-- Native datums must honor target typmods: varchar trimming, char padding,
-- and numeric/time/timestamp rounding.
CREATE TABLE lagodb_connectors_regress.copy_typmod_source (
    id integer, payload text, padded text, amount numeric(8, 4),
    clock time, happened timestamp
);
INSERT INTO lagodb_connectors_regress.copy_typmod_source VALUES
    (1, 'abc   ', 'x', 12.3450, '12:34:56.1236', '1999-12-31 23:59:59.9996'),
    (2, 'overflow', 'x', 999.9950, '12:34:56.1236', '1999-12-31 23:59:59.9996');
CREATE TABLE lagodb_connectors_regress.copy_typmod_sink (
    id integer, payload varchar(3), padded char(3), amount numeric(4, 2),
    clock time(3), happened timestamp(3)
);
COPY lagodb_connectors_regress.copy_typmod_source TO :'copy_json'
WITH (server 'lagodb_connectors_regress_s3', format 'json');
COPY lagodb_connectors_regress.copy_typmod_source TO :'copy_avro'
WITH (server 'lagodb_connectors_regress_s3', format 'avro');
COPY lagodb_connectors_regress.copy_typmod_source TO :'copy_parquet'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');

-- A STOP error must leave the target empty; one format covers the common
-- statement rollback path. Each decoder retains ON_ERROR IGNORE coverage.
\set VERBOSITY sqlstate
COPY lagodb_connectors_regress.copy_typmod_sink FROM :'copy_json'
WITH (server 'lagodb_connectors_regress_s3', format 'json');
\set VERBOSITY default
SELECT count(*) AS rows_after_error FROM lagodb_connectors_regress.copy_typmod_sink;
SET client_min_messages = warning;
COPY lagodb_connectors_regress.copy_typmod_sink FROM :'copy_json'
WITH (server 'lagodb_connectors_regress_s3', format 'json', on_error ignore);
SELECT id, payload, octet_length(padded) AS padded_bytes, amount,
       clock = time '12:34:56.124' AS time_rounded,
       happened = timestamp '2000-01-01' AS timestamp_rounded
FROM lagodb_connectors_regress.copy_typmod_sink ORDER BY id;
TRUNCATE lagodb_connectors_regress.copy_typmod_sink;
COPY lagodb_connectors_regress.copy_typmod_sink FROM :'copy_avro'
WITH (server 'lagodb_connectors_regress_s3', format 'avro', on_error ignore);
SELECT id, payload, octet_length(padded) AS padded_bytes, amount,
       clock = time '12:34:56.124' AS time_rounded,
       happened = timestamp '2000-01-01' AS timestamp_rounded
FROM lagodb_connectors_regress.copy_typmod_sink ORDER BY id;
TRUNCATE lagodb_connectors_regress.copy_typmod_sink;
COPY lagodb_connectors_regress.copy_typmod_sink FROM :'copy_parquet'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet', on_error ignore);
SELECT id, payload, octet_length(padded) AS padded_bytes, amount,
       clock = time '12:34:56.124' AS time_rounded,
       happened = timestamp '2000-01-01' AS timestamp_rounded
FROM lagodb_connectors_regress.copy_typmod_sink ORDER BY id;
RESET client_min_messages;

SELECT format('s3://%s/lagodb-connectors/copy/rls.json',
              :'lagodb_regress_bucket') AS json,
       format('s3://%s/lagodb-connectors/copy/rls.avro',
              :'lagodb_regress_bucket') AS avro,
       format('s3://%s/lagodb-connectors/copy/rls.parquet',
              :'lagodb_regress_bucket') AS parquet
\gset copy_

-- Relation-form COPY must export only rows visible to the current role.
CREATE ROLE lagodb_copy_reader;
GRANT USAGE ON SCHEMA lagodb_connectors_regress TO lagodb_copy_reader;
GRANT USAGE ON FOREIGN SERVER lagodb_connectors_regress_s3 TO lagodb_copy_reader;
GRANT SELECT ON lagodb_connectors_regress.copy_bridge_source TO lagodb_copy_reader;
ALTER TABLE lagodb_connectors_regress.copy_bridge_source ENABLE ROW LEVEL SECURITY;
CREATE POLICY copy_reader ON lagodb_connectors_regress.copy_bridge_source
    TO lagodb_copy_reader USING (id <= 2);
SET ROLE lagodb_copy_reader;
COPY lagodb_connectors_regress.copy_bridge_source TO :'copy_json'
WITH (server 'lagodb_connectors_regress_s3', format 'json');
COPY lagodb_connectors_regress.copy_bridge_source TO :'copy_avro'
WITH (server 'lagodb_connectors_regress_s3', format 'avro');
COPY lagodb_connectors_regress.copy_bridge_source TO :'copy_parquet'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');
RESET ROLE;
COPY lagodb_connectors_regress.copy_native_sink
FROM :'copy_json'
WITH (server 'lagodb_connectors_regress_s3', format 'json');
SELECT id, to_json(payload) AS payload
FROM lagodb_connectors_regress.copy_native_sink ORDER BY id;
TRUNCATE lagodb_connectors_regress.copy_native_sink;
COPY lagodb_connectors_regress.copy_native_sink
FROM :'copy_avro'
WITH (server 'lagodb_connectors_regress_s3', format 'avro');
SELECT id, to_json(payload) AS payload
FROM lagodb_connectors_regress.copy_native_sink ORDER BY id;
TRUNCATE lagodb_connectors_regress.copy_native_sink;
COPY lagodb_connectors_regress.copy_native_sink
FROM :'copy_parquet'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');
SELECT id, to_json(payload) AS payload
FROM lagodb_connectors_regress.copy_native_sink ORDER BY id;
DROP POLICY copy_reader ON lagodb_connectors_regress.copy_bridge_source;
ALTER TABLE lagodb_connectors_regress.copy_bridge_source DISABLE ROW LEVEL SECURITY;
DROP OWNED BY lagodb_copy_reader;
DROP ROLE lagodb_copy_reader;
