-- Writable Iceberg FDW lifecycle against consumer-owned REST/MinIO tables.

\setenv PGDATABASE :DBNAME
\set iceberg_fixture fdw-writes
\i include/iceberg_fixture.sql

SELECT rest_uri AS regress_rest_uri,
       fallback_rest_uri AS regress_fallback_rest_uri,
       failure_rest_uri AS regress_failure_rest_uri,
       endpoint AS regress_s3_endpoint,
       fallback_bucket AS regress_fallback_bucket,
       fallback_second_bucket AS regress_fallback_second_bucket,
       region AS regress_s3_region,
       access_key_id AS regress_s3_access_key_id,
       secret_access_key AS regress_s3_secret_access_key
FROM lagodb_regress.object_storage_fixture
\gset
\set regress_bucket_a_scope 's3://' :regress_fallback_bucket '/'
\set regress_bucket_b_scope 's3://' :regress_fallback_second_bucket '/'

SET client_min_messages = warning;
DROP EXTENSION IF EXISTS dblink;
DROP EXTENSION IF EXISTS lagodb_iceberg CASCADE;
RESET client_min_messages;
CREATE EXTENSION lagodb_iceberg;
CREATE EXTENSION dblink;
CREATE SCHEMA iceberg_fdw_regress;
CREATE SERVER iceberg_rest
TYPE 'rest'
FOREIGN DATA WRAPPER lagodb_iceberg
OPTIONS (
    uri :'regress_rest_uri'
);
CREATE SERVER iceberg_rest_failure
TYPE 'rest'
FOREIGN DATA WRAPPER lagodb_iceberg
OPTIONS (
    uri :'regress_failure_rest_uri'
);
CREATE SERVER iceberg_rest_second
TYPE 'rest'
FOREIGN DATA WRAPPER lagodb_iceberg
OPTIONS (
    uri :'regress_rest_uri'
);
CREATE SERVER iceberg_rest_fallback
TYPE 'rest'
FOREIGN DATA WRAPPER lagodb_iceberg
OPTIONS (
    uri :'regress_fallback_rest_uri',
    request_vended_credentials 'false'
);
CREATE SERVER bucket_a_storage
TYPE 'storage'
FOREIGN DATA WRAPPER lagodb_iceberg
OPTIONS (
    provider 's3_compatible',
    endpoint :'regress_s3_endpoint',
    region :'regress_s3_region',
    scope :'regress_bucket_a_scope',
    allow_http 'true',
    virtual_hosted_style_request 'false'
);
CREATE SERVER bucket_b_storage
TYPE 'storage'
FOREIGN DATA WRAPPER lagodb_iceberg
OPTIONS (
    provider 's3_compatible',
    endpoint :'regress_s3_endpoint',
    region :'regress_s3_region',
    scope :'regress_bucket_b_scope',
    allow_http 'true',
    virtual_hosted_style_request 'false'
);
CREATE USER MAPPING FOR CURRENT_USER SERVER iceberg_rest;
CREATE USER MAPPING FOR CURRENT_USER SERVER iceberg_rest_failure;
CREATE USER MAPPING FOR CURRENT_USER SERVER iceberg_rest_second;
CREATE USER MAPPING FOR CURRENT_USER SERVER iceberg_rest_fallback;
CREATE USER MAPPING FOR CURRENT_USER SERVER bucket_a_storage
OPTIONS (
    access_key_id :'regress_s3_access_key_id',
    secret_access_key :'regress_s3_secret_access_key'
);
CREATE USER MAPPING FOR CURRENT_USER SERVER bucket_b_storage
OPTIONS (
    access_key_id :'regress_s3_access_key_id',
    secret_access_key :'regress_s3_secret_access_key'
);

-- CREATE checks declared names and types; the write entry point checks the
-- complete writable contract after PostgreSQL has processed local columns.
CREATE FOREIGN TABLE iceberg_fdw_regress.writable (
    id integer,
    payload text
)
SERVER iceberg_rest
OPTIONS (
    table 'fdw_writes.rows_v2'
);

SELECT * FROM iceberg_fdw_regress.writable ORDER BY id;

-- JSONB writes use the remote schema, including recursive values and NULLs.
SET lagodb.query_offload_mode = 'off';
CREATE FOREIGN TABLE iceberg_fdw_regress.nested (
    id integer, record jsonb, items jsonb, labels jsonb,
    indexed jsonb, structured jsonb, numbers integer[]
)
SERVER iceberg_rest
OPTIONS (
    table 'fdw_writes.nested_rows_v2'
);
CREATE TEMP TABLE nested_before AS SELECT * FROM iceberg_fdw_regress.nested;
INSERT INTO iceberg_fdw_regress.nested
SELECT id + 10, record, items, labels, indexed, structured, numbers
FROM nested_before;

SELECT count(*) = 3 AND bool_and(
           ROW(a.record, a.items, a.labels, a.indexed, a.structured, a.numbers)
           IS NOT DISTINCT FROM
           ROW(b.record, b.items, b.labels, b.indexed, b.structured, b.numbers)) AS roundtrip
FROM iceberg_fdw_regress.nested a JOIN nested_before b ON a.id = b.id + 10;

-- Display precision and Numeric zero normalization must not alter float leaves.
-- Both float widths retain signed zero through INSERT and id-only UPDATE.
SET extra_float_digits = 0;
SELECT record->'special' = '[1.2345678901234567,"NaN","Infinity","-Infinity","-0.0",0]'::jsonb
       AND record->'single' = '[1.25,"-0.0",0]'::jsonb
       AS exact_read
FROM iceberg_fdw_regress.nested WHERE id = 11;
UPDATE iceberg_fdw_regress.nested SET id = 21 WHERE id = 11;
RESET extra_float_digits;
SELECT ROW(a.record, a.items, a.labels, a.indexed, a.structured, a.numbers)
       IS NOT DISTINCT FROM
       ROW(b.record, b.items, b.labels, b.indexed, b.structured, b.numbers)
       AND a.record->'special' = '[1.2345678901234567,"NaN","Infinity","-Infinity","-0.0",0]'::jsonb
       AND a.record->'single' = '[1.25,"-0.0",0]'::jsonb
       AS exact_update
FROM iceberg_fdw_regress.nested a JOIN nested_before b ON b.id = 1
WHERE a.id = 21;

-- Updating one field also rewrites the other nested columns without loss.
UPDATE iceberg_fdw_regress.nested
SET id = 11, record = jsonb_set(record, '{label}', '"updated"') WHERE id = 21;

SELECT record->>'label' = 'updated'
       AND record->'amount' = '123456789012345678901234567890.12345678'::jsonb
       AND indexed = '[{"key":7,"value":["x",null]},{"key":8,"value":null}]'::jsonb
       AND numbers = ARRAY[1,NULL,2] AS updated
FROM iceberg_fdw_regress.nested WHERE id = 11;

-- Alternate populated, empty and NULL containers across FDW insert batches.
INSERT INTO iceberg_fdw_regress.nested
SELECT 100 + n, record, items, labels, indexed, structured, numbers
FROM generate_series(0, 1099) AS n
JOIN nested_before ON id = n % 3 + 1
ORDER BY n;

SELECT count(*) = 1100 AND count(DISTINCT a.id) = 1100 AND bool_and(
           ROW(a.record, a.items, a.labels, a.indexed, a.structured, a.numbers)
           IS NOT DISTINCT FROM
           ROW(b.record, b.items, b.labels, b.indexed, b.structured, b.numbers))
       AS bulk_values
FROM iceberg_fdw_regress.nested a
LEFT JOIN nested_before b ON b.id = (a.id - 100) % 3 + 1
WHERE a.id >= 100;

CREATE FOREIGN TABLE iceberg_fdw_regress.nanos ()
SERVER iceberg_rest
OPTIONS (
    table 'fdw_writes.nanos_v3'
);
CREATE TEMP TABLE nanos_before AS SELECT id, detail FROM iceberg_fdw_regress.nanos;
INSERT INTO iceberg_fdw_regress.nanos (id, detail)
SELECT id + 20, detail FROM nanos_before;

SELECT count(*) = 9 AND bool_and(a.detail IS NOT DISTINCT FROM b.detail) AS nanos_roundtrip
FROM iceberg_fdw_regress.nanos a JOIN nanos_before b ON a.id = b.id + 20;

-- Missing fields use write-default, not initial-default; explicit null stays null.
INSERT INTO iceberg_fdw_regress.nanos (id, detail) VALUES
    (40, '{"ts":"1969-12-31T23:59:59.999999999"}'),
    (41, '{"ts":"1970-01-01T00:00:00.000000000","label":null}');

SELECT count(*) = 2 AND bool_and(detail IS NOT DISTINCT FROM CASE id
           WHEN 40 THEN '{"ts":"1969-12-31T23:59:59.999999999","tz":null,"label":"written"}'::jsonb
           WHEN 41 THEN '{"ts":"1970-01-01T00:00:00.000000000","tz":null,"label":null}'::jsonb
       END) AS defaults
FROM iceberg_fdw_regress.nanos WHERE id IN (40, 41);

-- Invalid shapes, required fields, and typed duplicate map keys are errors.
\set VERBOSITY sqlstate
-- UPDATE rewrites native nanosecond columns even when only id is assigned.
UPDATE iceberg_fdw_regress.nanos SET id = 100 WHERE id = 1;

INSERT INTO iceberg_fdw_regress.nested (id, record) VALUES (50, '{"unknown":1}');

-- Special float strings do not make arbitrary numeric strings valid input.
INSERT INTO iceberg_fdw_regress.nested (id, record) VALUES (50, '{"single":["0.0"]}');

INSERT INTO iceberg_fdw_regress.nanos (id, detail) VALUES (50, '{"ts":null}');

-- Removing the nanosecond fraction must not make an invalid time valid.
INSERT INTO iceberg_fdw_regress.nanos (id, detail)
VALUES (50, '{"ts":"1970-01-01T24:00:00.000000001"}');

INSERT INTO iceberg_fdw_regress.nested (id, indexed)
VALUES (50, '[{"key":1,"value":[]},{"key":1.0,"value":[]}]');

\set VERBOSITY default
-- DELETE does not rewrite timestamp values and remains supported.
DELETE FROM iceberg_fdw_regress.nanos WHERE id = 1 RETURNING id;

-- A failed append must not leave a partial row visible to the next statement.
INSERT INTO iceberg_fdw_regress.nested (id, numbers) VALUES (60, ARRAY[3,NULL,4]);

SELECT count(*) = 1 AND bool_and(id = 60 AND numbers = ARRAY[3,NULL,4]) AS recovered
FROM iceberg_fdw_regress.nested WHERE id IN (50, 60);

DROP TABLE nested_before, nanos_before;
RESET lagodb.query_offload_mode;

-- The provisioned format-v3 table already contains Spark deletion vectors.
-- Exercise FDW reads of that state, then create FDW deletion vectors through
-- UPDATE/DELETE while preserving read-own-writes semantics.
CREATE FOREIGN TABLE iceberg_fdw_regress.v3_mutations (
    id integer,
    payload text
)
SERVER iceberg_rest
OPTIONS (
    table 'fdw_writes.deletion_vectors_v3'
);
SELECT * FROM iceberg_fdw_regress.v3_mutations ORDER BY id;
BEGIN;
INSERT INTO iceberg_fdw_regress.v3_mutations VALUES (5, 'five');
UPDATE iceberg_fdw_regress.v3_mutations
SET payload = 'one-pg' WHERE id = 1
RETURNING id, payload;
DELETE FROM iceberg_fdw_regress.v3_mutations
WHERE id = 4
RETURNING id, payload;
SELECT * FROM iceberg_fdw_regress.v3_mutations ORDER BY id;
COMMIT;
SELECT * FROM iceberg_fdw_regress.v3_mutations ORDER BY id;

-- The foreign table consumes the remote default spec directly. Existing data
-- spans two specs; UPDATE writes new data with the current spec while v2
-- position deletes retain each target file's original spec and partition.
CREATE FOREIGN TABLE iceberg_fdw_regress.partition_evolution_v2 ()
SERVER iceberg_rest
OPTIONS (
    table 'fdw_writes.partition_evolution_v2'
);
SELECT array_agg(id ORDER BY id) AS initial_partitioned_rows
FROM iceberg_fdw_regress.partition_evolution_v2;
BEGIN;
INSERT INTO iceberg_fdw_regress.partition_evolution_v2
VALUES (7, NULL, NULL, 'seven');
UPDATE iceberg_fdw_regress.partition_evolution_v2
SET category = 'delta', event_date = DATE '2024-02-01', payload = 'one-updated'
WHERE id = 1
RETURNING id, category, payload;
WITH deleted AS (
    DELETE FROM iceberg_fdw_regress.partition_evolution_v2
    WHERE id IN (2, 5)
    RETURNING id
)
SELECT array_agg(id ORDER BY id) AS deleted_partitioned_rows FROM deleted;
SELECT array_agg(id ORDER BY id) AS transaction_partitioned_rows,
       max(payload) FILTER (WHERE id = 1) AS updated_partitioned_payload
FROM iceberg_fdw_regress.partition_evolution_v2;
COMMIT;
COPY iceberg_fdw_regress.partition_evolution_v2 FROM stdin WITH (FORMAT csv);
8,theta,2024-03-01,copy-eight
9,iota,2024-03-02,copy-nine
\.
SELECT array_agg(id ORDER BY id) AS committed_partitioned_rows
FROM iceberg_fdw_regress.partition_evolution_v2;

-- An empty local column list is populated from the remote Iceberg schema.
CREATE FOREIGN TABLE iceberg_fdw_regress.inferred ()
SERVER iceberg_rest
OPTIONS (
    table 'fdw_writes.import_rows_v2'
);
SELECT * FROM iceberg_fdw_regress.inferred ORDER BY id;

-- A catalog that does not vend storage access uses independently scoped
-- Iceberg-owned storage profiles; the REST server is not bound to one bucket.
CREATE FOREIGN TABLE iceberg_fdw_regress.fallback_writable (
    id integer,
    payload text
)
SERVER iceberg_rest_fallback
OPTIONS (
    table 'fdw_writes.rows_v2'
);
SELECT * FROM iceberg_fdw_regress.fallback_writable ORDER BY id;

CREATE FOREIGN TABLE iceberg_fdw_regress.fallback_second_bucket ()
SERVER iceberg_rest_fallback
OPTIONS (
    table 'fdw_reads.second_bucket_v2'
);
SELECT * FROM iceberg_fdw_regress.fallback_second_bucket ORDER BY id;
INSERT INTO iceberg_fdw_regress.fallback_writable
VALUES (101, 'client-fallback');
SELECT * FROM iceberg_fdw_regress.fallback_writable ORDER BY id;

CREATE SCHEMA iceberg_fdw_import;
IMPORT FOREIGN SCHEMA fdw_writes
LIMIT TO (import_rows_v2)
FROM SERVER iceberg_rest
INTO iceberg_fdw_import;
INSERT INTO iceberg_fdw_import.import_rows_v2 VALUES (15, 'fifteen');
SELECT array_agg(id ORDER BY id) AS imported_write_rows
FROM iceberg_fdw_import.import_rows_v2;

-- Incompatible explicit types are rejected during CREATE.
CREATE FOREIGN TABLE iceberg_fdw_regress.invalid_schema (
    id bigint,
    payload text
)
SERVER iceberg_rest
OPTIONS (
    table 'fdw_writes.rows_v2'
);

-- Inferred definitions are writable without an opt-in mode.
INSERT INTO iceberg_fdw_regress.inferred VALUES (11, 'eleven');
SELECT array_agg(id ORDER BY id) AS inferred_write_rows
FROM iceberg_fdw_regress.inferred;
TRUNCATE iceberg_fdw_regress.writable;
VACUUM iceberg_fdw_regress.writable;
VACUUM (ANALYZE) iceberg_fdw_regress.writable;

-- INSERT/UPDATE/DELETE are visible to later statements in the same PostgreSQL
-- transaction, and savepoint rollback removes only its own staged actions.
BEGIN;
INSERT INTO iceberg_fdw_regress.writable VALUES (4, 'four');
SELECT array_agg(id ORDER BY id) AS ids_after_insert
FROM iceberg_fdw_regress.writable;
UPDATE iceberg_fdw_regress.writable
SET payload = 'two-updated'
WHERE id = 2
RETURNING id, payload;
SELECT payload AS payload_after_update
FROM iceberg_fdw_regress.writable WHERE id = 2;
SAVEPOINT delete_three;
DELETE FROM iceberg_fdw_regress.writable
WHERE id = 3
RETURNING id, payload;
SELECT array_agg(id ORDER BY id) AS ids_after_delete
FROM iceberg_fdw_regress.writable;
ROLLBACK TO SAVEPOINT delete_three;
SELECT array_agg(id ORDER BY id) AS ids_after_savepoint_rollback
FROM iceberg_fdw_regress.writable;
COMMIT;

SELECT * FROM iceberg_fdw_regress.writable ORDER BY id;

COPY iceberg_fdw_regress.writable FROM stdin WITH (FORMAT csv);
5,five
6,six
\.
SELECT array_agg(id ORDER BY id) AS ids_after_copy
FROM iceberg_fdw_regress.writable;

-- One PostgreSQL transaction publishes changes for multiple remote tables in
-- one REST transaction request.
CREATE FOREIGN TABLE iceberg_fdw_regress.second (
    id integer,
    payload text
)
SERVER iceberg_rest
OPTIONS (
    table 'fdw_writes.import_rows_v2'
);
BEGIN;
INSERT INTO iceberg_fdw_regress.writable VALUES (7, 'seven');
INSERT INTO iceberg_fdw_regress.second VALUES (20, 'twenty');
COMMIT;
SELECT array_agg(id ORDER BY id) AS writable_after_multi_table_commit
FROM iceberg_fdw_regress.writable;
SELECT array_agg(id ORDER BY id) AS second_after_multi_table_commit
FROM iceberg_fdw_regress.second;

-- Distinct immutable REST catalog transaction states publish independently
-- in one PostgreSQL transaction.
CREATE FOREIGN TABLE iceberg_fdw_regress.second_server_table (
    id integer,
    payload text
)
SERVER iceberg_rest_second
OPTIONS (
    table 'fdw_writes.import_rows_v2'
);
BEGIN;
INSERT INTO iceberg_fdw_regress.writable VALUES (30, 'thirty');
INSERT INTO iceberg_fdw_regress.second_server_table
VALUES (31, 'thirty-one');
COMMIT;
SELECT count(*) AS first_catalog_committed
FROM iceberg_fdw_regress.writable WHERE id = 30;
SELECT count(*) AS second_catalog_committed
FROM iceberg_fdw_regress.second_server_table WHERE id = 31;

-- Catalog identity is fixed when the server is created. Runtime and
-- authentication behavior remains configurable.
ALTER SERVER iceberg_rest OPTIONS (SET uri :'regress_failure_rest_uri');

-- Once a server/effective-user pair participates in a writable transaction,
-- changing a mutable option is rejected before another write or scan can
-- attach to the frozen transaction view.
BEGIN;
INSERT INTO iceberg_fdw_regress.writable VALUES (40, 'forty');
ALTER SERVER iceberg_rest OPTIONS (ADD request_vended_credentials 'false');
INSERT INTO iceberg_fdw_regress.writable VALUES (41, 'forty-one');
ROLLBACK;
SELECT count(*) AS changed_catalog_rows
FROM iceberg_fdw_regress.writable WHERE id IN (40, 41);

-- A change after the final write must also block publication at pre-commit.
BEGIN;
INSERT INTO iceberg_fdw_regress.writable VALUES (44, 'forty-four');
ALTER SERVER iceberg_rest OPTIONS (ADD request_vended_credentials 'false');
COMMIT;
SELECT count(*) AS invalidated_precommit_rows
FROM iceberg_fdw_regress.writable WHERE id = 44;

-- User-mapping invalidation reaches the same transaction admission boundary.
BEGIN;
INSERT INTO iceberg_fdw_regress.writable VALUES (45, 'forty-five');
ALTER USER MAPPING FOR CURRENT_USER SERVER iceberg_rest
    OPTIONS (ADD scope 'changed');
COMMIT;
SELECT count(*) AS invalidated_mapping_rows
FROM iceberg_fdw_regress.writable WHERE id = 45;

BEGIN;
INSERT INTO iceberg_fdw_regress.writable VALUES (50, 'fifty');
PREPARE TRANSACTION 'iceberg_fdw_must_not_prepare';
SELECT count(*) AS unprepared_remote_rows
FROM iceberg_fdw_regress.writable WHERE id = 50;

-- Two real PostgreSQL backends update disjoint rows from the same Iceberg
-- snapshot. Serializable Iceberg validation rejects the stale committer.
SELECT dblink_connect(
    'iceberg_concurrent',
    format(
        'host=localhost port=%s dbname=%I',
        current_setting('port'),
        current_database()
    )
);
SELECT dblink_exec('iceberg_concurrent', 'BEGIN');
SELECT dblink_exec(
    'iceberg_concurrent',
    $$UPDATE iceberg_fdw_regress.writable
      SET payload = 'backend-b' WHERE id = 1$$
);
BEGIN;
UPDATE iceberg_fdw_regress.writable
SET payload = 'backend-a' WHERE id = 2;
SELECT dblink_exec('iceberg_concurrent', 'COMMIT');
COMMIT;
SELECT id, payload FROM iceberg_fdw_regress.writable
WHERE id IN (1, 2) ORDER BY id;
SELECT dblink_disconnect('iceberg_concurrent');

UPDATE iceberg_fdw_regress.writable SET payload = NULL WHERE id = 7;
ANALYZE iceberg_fdw_regress.writable;
SELECT reltuples::integer AS analyzed_live_rows
FROM pg_class
WHERE oid = 'iceberg_fdw_regress.writable'::regclass;
SELECT null_frac = 0
       AND n_distinct = -1
       AND most_common_vals IS NULL
       AND histogram_bounds::text = '{1,2,3,4,5,6,7,30}'
       AS analyzed_id_statistics_complete
FROM pg_stats
WHERE schemaname = 'iceberg_fdw_regress'
  AND tablename = 'writable'
  AND attname = 'id';
SELECT abs(null_frac - 0.125) < 0.001
       AND abs(n_distinct + 0.875) < 0.001
       AND most_common_vals IS NULL
       AND histogram_bounds IS NOT NULL
       AS analyzed_payload_statistics_complete
FROM pg_stats
WHERE schemaname = 'iceberg_fdw_regress'
  AND tablename = 'writable'
  AND attname = 'payload';

-- DROP removes only PostgreSQL's local foreign-table metadata. Rebinding the
-- same remote identifier exposes the unchanged remote table and its data.
DROP FOREIGN TABLE iceberg_fdw_regress.writable;
SELECT to_regclass('iceberg_fdw_regress.writable') IS NULL
       AS local_definition_dropped;
CREATE FOREIGN TABLE iceberg_fdw_regress.rebound ()
SERVER iceberg_rest
OPTIONS (
    table 'fdw_writes.rows_v2'
);
SELECT array_agg(id ORDER BY id) AS remote_rows_survive_local_drop
FROM iceberg_fdw_regress.rebound;

-- The proxy forwards all reads and preparation requests but rejects only the
-- post-commit REST publication. PostgreSQL COMMIT therefore succeeds and emits
-- a warning; the uncommitted remote row is absent through the normal server.
CREATE FOREIGN TABLE iceberg_fdw_regress.failure_writable (
    id integer,
    payload text
)
SERVER iceberg_rest_failure
OPTIONS (
    table 'fdw_writes.rows_v2'
);
BEGIN;
INSERT INTO iceberg_fdw_regress.failure_writable VALUES (99, 'not-published');
COMMIT;
SELECT count(*) AS unpublished_remote_rows
FROM iceberg_fdw_regress.rebound WHERE id = 99;

-- Retargeting affects later statements, not writes already staged for A.
CREATE FOREIGN TABLE iceberg_fdw_regress.retargeted (id integer, payload text)
SERVER iceberg_rest OPTIONS (table 'fdw_writes.rows_v2');
BEGIN;
INSERT INTO iceberg_fdw_regress.retargeted VALUES (60, 'target-a');
ALTER FOREIGN TABLE iceberg_fdw_regress.retargeted
    OPTIONS (SET table 'fdw_writes.import_rows_v2');
INSERT INTO iceberg_fdw_regress.retargeted VALUES (61, 'target-b');
COMMIT;
SELECT array_agg(id ORDER BY id) AS target_a
FROM iceberg_fdw_regress.rebound WHERE id IN (60, 61);
SELECT array_agg(id ORDER BY id) AS target_b
FROM iceberg_fdw_regress.retargeted WHERE id IN (60, 61);

-- Rollback removes staged writes to both targets and restores local options.
BEGIN;
INSERT INTO iceberg_fdw_regress.retargeted VALUES (62, 'rollback-b');
ALTER FOREIGN TABLE iceberg_fdw_regress.retargeted
    OPTIONS (SET table 'fdw_writes.rows_v2');
INSERT INTO iceberg_fdw_regress.retargeted VALUES (63, 'rollback-a');
ROLLBACK;
SELECT array_agg(id ORDER BY id) AS target_a
FROM iceberg_fdw_regress.rebound WHERE id BETWEEN 60 AND 63;
SELECT array_agg(id ORDER BY id) AS target_b
FROM iceberg_fdw_regress.retargeted WHERE id BETWEEN 60 AND 63;
SELECT option_name, option_value
FROM pg_foreign_table, LATERAL pg_options_to_table(ftoptions)
WHERE ftrelid = 'iceberg_fdw_regress.retargeted'::regclass
ORDER BY option_name;

SET client_min_messages = warning;
DROP SCHEMA iceberg_fdw_regress CASCADE;
DROP SCHEMA iceberg_fdw_import CASCADE;
DROP SERVER iceberg_rest CASCADE;
DROP SERVER iceberg_rest_failure CASCADE;
DROP SERVER iceberg_rest_second CASCADE;
DROP SERVER iceberg_rest_fallback CASCADE;
DROP SERVER bucket_a_storage CASCADE;
DROP SERVER bucket_b_storage CASCADE;
DROP EXTENSION dblink;
RESET client_min_messages;
