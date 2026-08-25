-- Writable Iceberg FDW lifecycle against the shared REST/MinIO fixture.

\set ECHO none
\setenv PGDATABASE :DBNAME

SELECT rest_uri AS regress_rest_uri,
       fallback_rest_uri AS regress_fallback_rest_uri,
       failure_rest_uri AS regress_failure_rest_uri,
       endpoint AS regress_s3_endpoint,
       fallback_bucket AS regress_fallback_bucket,
       fallback_second_bucket AS regress_fallback_second_bucket,
       region AS regress_s3_region,
       access_key_id AS regress_s3_access_key_id,
       secret_access_key AS regress_s3_secret_access_key
FROM lakebase_regress.object_storage_fixture
\gset
\set regress_bucket_a_scope 's3://' :regress_fallback_bucket '/'
\set regress_bucket_b_scope 's3://' :regress_fallback_second_bucket '/'

SET client_min_messages = warning;
DROP EXTENSION IF EXISTS dblink;
DROP EXTENSION IF EXISTS lagodb_iceberg CASCADE;
CREATE EXTENSION lagodb_iceberg;
CREATE EXTENSION dblink;
CREATE SCHEMA iceberg_fdw_regress;
CREATE SERVER iceberg_rest
TYPE 'rest'
FOREIGN DATA WRAPPER iceberg_fdw
OPTIONS (
    uri :'regress_rest_uri'
);
CREATE SERVER iceberg_rest_failure
TYPE 'rest'
FOREIGN DATA WRAPPER iceberg_fdw
OPTIONS (
    uri :'regress_failure_rest_uri'
);
CREATE SERVER iceberg_rest_second
TYPE 'rest'
FOREIGN DATA WRAPPER iceberg_fdw
OPTIONS (
    uri :'regress_rest_uri'
);
CREATE SERVER iceberg_rest_fallback
TYPE 'rest'
FOREIGN DATA WRAPPER iceberg_fdw
OPTIONS (
    uri :'regress_fallback_rest_uri',
    enable_vended_credentials 'false'
);
CREATE SERVER bucket_a_storage
TYPE 'storage'
FOREIGN DATA WRAPPER iceberg_fdw
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
FOREIGN DATA WRAPPER iceberg_fdw
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
RESET client_min_messages;

\set ECHO all
-- Explicit CREATE FOREIGN TABLE validates the complete remote schema before
-- PostgreSQL creates the local relation.
CREATE FOREIGN TABLE iceberg_fdw_regress.writable (
    id integer,
    payload text
)
SERVER iceberg_rest
OPTIONS (
    catalog_name 'regress',
    catalog_namespace 'fdw_regress',
    catalog_table_name 'writable',
    mode 'read_write'
);

SELECT * FROM iceberg_fdw_regress.writable ORDER BY id;

-- The provisioned format-v3 table already contains Spark deletion vectors.
-- Exercise FDW reads of that state, then create FDW deletion vectors through
-- UPDATE/DELETE while preserving read-own-writes semantics.
CREATE FOREIGN TABLE iceberg_fdw_regress.v3_mutations (
    id integer,
    payload text
)
SERVER iceberg_rest
OPTIONS (
    catalog_name 'regress',
    catalog_namespace 'fdw_regress',
    catalog_table_name 'v3_mutations',
    mode 'read_write'
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

-- An empty local column list is populated from the remote Iceberg schema.
CREATE FOREIGN TABLE iceberg_fdw_regress.inferred ()
SERVER iceberg_rest
OPTIONS (
    catalog_name 'regress',
    catalog_namespace 'fdw_regress',
    catalog_table_name 'second',
    mode 'read_only'
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
    catalog_name 'regress',
    catalog_namespace 'fdw_regress',
    catalog_table_name 'writable',
    mode 'read_write'
);
SELECT * FROM iceberg_fdw_regress.fallback_writable ORDER BY id;

CREATE FOREIGN TABLE iceberg_fdw_regress.fallback_second_bucket ()
SERVER iceberg_rest_fallback
OPTIONS (
    catalog_name 'regress',
    catalog_namespace 'fdw_regress',
    catalog_table_name 'second_bucket',
    mode 'read_only'
);
SELECT * FROM iceberg_fdw_regress.fallback_second_bucket ORDER BY id;
INSERT INTO iceberg_fdw_regress.fallback_writable
VALUES (101, 'client-fallback');
SELECT * FROM iceberg_fdw_regress.fallback_writable ORDER BY id;

CREATE SCHEMA iceberg_fdw_import;
IMPORT FOREIGN SCHEMA fdw_regress
LIMIT TO (second)
FROM SERVER iceberg_rest
INTO iceberg_fdw_import
OPTIONS (mode 'read_write');
INSERT INTO iceberg_fdw_import.second VALUES (15, 'fifteen');
SELECT array_agg(id ORDER BY id) AS imported_write_rows
FROM iceberg_fdw_import.second;

-- Mismatched explicit columns are rejected and leave no local catalog entry.
\set VERBOSITY terse
CREATE FOREIGN TABLE iceberg_fdw_regress.invalid_schema (
    id bigint,
    payload text
)
SERVER iceberg_rest
OPTIONS (
    catalog_name 'regress',
    catalog_namespace 'fdw_regress',
    catalog_table_name 'writable',
    mode 'read_write'
);
SELECT to_regclass('iceberg_fdw_regress.invalid_schema') IS NULL
       AS invalid_schema_not_created;

INSERT INTO iceberg_fdw_regress.inferred VALUES (11, 'eleven');
ALTER FOREIGN TABLE iceberg_fdw_regress.writable ADD COLUMN extra integer;
ALTER FOREIGN TABLE iceberg_fdw_regress.writable RENAME TO renamed;
TRUNCATE iceberg_fdw_regress.writable;
VACUUM iceberg_fdw_regress.writable;
VACUUM (ANALYZE) iceberg_fdw_regress.writable;
\set VERBOSITY default

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
    catalog_name 'regress',
    catalog_namespace 'fdw_regress',
    catalog_table_name 'second',
    mode 'read_write'
);
BEGIN;
INSERT INTO iceberg_fdw_regress.writable VALUES (7, 'seven');
INSERT INTO iceberg_fdw_regress.second VALUES (20, 'twenty');
COMMIT;
SELECT array_agg(id ORDER BY id) AS writable_after_multi_table_commit
FROM iceberg_fdw_regress.writable;
SELECT array_agg(id ORDER BY id) AS second_after_multi_table_commit
FROM iceberg_fdw_regress.second;

-- Distinct immutable REST catalog bindings publish independently in one
-- PostgreSQL transaction.
CREATE FOREIGN TABLE iceberg_fdw_regress.second_server_table (
    id integer,
    payload text
)
SERVER iceberg_rest_second
OPTIONS (
    catalog_name 'regress',
    catalog_namespace 'fdw_regress',
    catalog_table_name 'second',
    mode 'read_write'
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

-- Once a server/effective-user pair participates in a writable transaction,
-- changing its server configuration is rejected before another write or scan
-- can attach to the frozen transaction view.
\set VERBOSITY terse
BEGIN;
INSERT INTO iceberg_fdw_regress.writable VALUES (40, 'forty');
ALTER SERVER iceberg_rest OPTIONS (SET uri :'regress_failure_rest_uri');
INSERT INTO iceberg_fdw_regress.writable VALUES (41, 'forty-one');
ROLLBACK;
\set VERBOSITY default
SELECT count(*) AS changed_binding_rows
FROM iceberg_fdw_regress.writable WHERE id IN (40, 41);

\set VERBOSITY terse
BEGIN;
INSERT INTO iceberg_fdw_regress.writable VALUES (50, 'fifty');
PREPARE TRANSACTION 'iceberg_fdw_must_not_prepare';
\set VERBOSITY default
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
\set VERBOSITY terse
COMMIT;
\set VERBOSITY default
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
    catalog_name 'regress',
    catalog_namespace 'fdw_regress',
    catalog_table_name 'writable',
    mode 'read_only'
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
    catalog_name 'regress',
    catalog_namespace 'fdw_regress',
    catalog_table_name 'writable',
    mode 'read_write'
);
BEGIN;
INSERT INTO iceberg_fdw_regress.failure_writable VALUES (99, 'not-published');
COMMIT;
SELECT count(*) AS unpublished_remote_rows
FROM iceberg_fdw_regress.rebound WHERE id = 99;

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
