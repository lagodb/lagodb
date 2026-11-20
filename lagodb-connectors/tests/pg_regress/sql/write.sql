\i include/column_definitions.sql

-- Shared writer transaction, statement-abort, and rollover lifecycle.

SELECT endpoint, bucket, region, access_key_id, secret_access_key
FROM lagodb_regress.object_storage_fixture
\gset storage_

\setenv OBJECT_STORAGE_ENDPOINT :storage_endpoint
\setenv OBJECT_STORAGE_BUCKET :storage_bucket
\setenv OBJECT_STORAGE_REGION :storage_region
\setenv OBJECT_STORAGE_ACCESS_KEY_ID :storage_access_key_id
\setenv OBJECT_STORAGE_SECRET_ACCESS_KEY :storage_secret_access_key

\set lifecycle_base 's3://' :storage_bucket '/lagodb-connectors/lifecycle/'

-- Rollback removes published objects.
\set lifecycle_rollback_text_path :lifecycle_base 'rollback/text/'
CREATE FOREIGN TABLE lagodb_connectors_regress.lifecycle_rollback_text
    (:common_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'lifecycle_rollback_text_path', format 'text');

BEGIN;
INSERT INTO lagodb_connectors_regress.lifecycle_rollback_text
SELECT * FROM lagodb_connectors_regress.common_source WHERE id = 1;
SELECT count(*) AS text_rows_before_rollback
FROM lagodb_connectors_regress.lifecycle_rollback_text;
ROLLBACK;

SELECT count(*) AS rows_after_rollback
FROM lagodb_connectors_regress.lifecycle_rollback_text;

-- A committed prefix object remains visible after the transaction callback.
\set lifecycle_commit_path :lifecycle_base 'commit/'
CREATE FOREIGN TABLE lagodb_connectors_regress.lifecycle_commit
    (:json_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'lifecycle_commit_path', format 'json');
BEGIN;
INSERT INTO lagodb_connectors_regress.lifecycle_commit
SELECT * FROM lagodb_connectors_regress.json_source WHERE id = 1;
SELECT count(*) AS rows_before_commit
FROM lagodb_connectors_regress.lifecycle_commit;
COMMIT;
SELECT count(*) AS rows_after_commit
FROM lagodb_connectors_regress.lifecycle_commit;

-- A savepoint abort deletes only objects registered by the subtransaction.
\set lifecycle_savepoint_path :lifecycle_base 'savepoint/'
CREATE FOREIGN TABLE lagodb_connectors_regress.lifecycle_savepoint
    (:common_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'lifecycle_savepoint_path', format 'text');
BEGIN;
INSERT INTO lagodb_connectors_regress.lifecycle_savepoint
SELECT * FROM lagodb_connectors_regress.common_source WHERE id = 1;
SAVEPOINT writer_savepoint;
INSERT INTO lagodb_connectors_regress.lifecycle_savepoint
SELECT * FROM lagodb_connectors_regress.common_source WHERE id = 2;
SELECT id FROM lagodb_connectors_regress.lifecycle_savepoint ORDER BY id;
ROLLBACK TO SAVEPOINT writer_savepoint;
SELECT id FROM lagodb_connectors_regress.lifecycle_savepoint ORDER BY id;
COMMIT;
SELECT id FROM lagodb_connectors_regress.lifecycle_savepoint ORDER BY id;

-- Each INSERT statement allocates a new object under the same prefix.
\set lifecycle_append_path :lifecycle_base 'append/'
CREATE FOREIGN TABLE lagodb_connectors_regress.lifecycle_append
    (:avro_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'lifecycle_append_path', format 'avro');
INSERT INTO lagodb_connectors_regress.lifecycle_append
SELECT * FROM lagodb_connectors_regress.avro_source WHERE id = 1;
INSERT INTO lagodb_connectors_regress.lifecycle_append
SELECT * FROM lagodb_connectors_regress.avro_source WHERE id = 2;
SELECT id FROM lagodb_connectors_regress.lifecycle_append ORDER BY id;

-- Empty foreign INSERT leaves no object; direct COPY TO has a separate
-- empty-object contract. Each FDW writer chooses this policy (text and CSV
-- share one writer).
\set lifecycle_empty_path :lifecycle_base 'empty/text/'
CREATE FOREIGN TABLE lagodb_connectors_regress.lifecycle_empty_text (:common_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'lifecycle_empty_path', format 'text');
INSERT INTO lagodb_connectors_regress.lifecycle_empty_text
SELECT * FROM lagodb_connectors_regress.common_source WHERE false;
\setenv OBJECT_STORAGE_PREFIX 'lagodb-connectors/lifecycle/empty/text/'
\! sh bin/object_storage_tool assert-prefix-empty
\set lifecycle_empty_path :lifecycle_base 'empty/json/'
CREATE FOREIGN TABLE lagodb_connectors_regress.lifecycle_empty_json (:json_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'lifecycle_empty_path', format 'json');
INSERT INTO lagodb_connectors_regress.lifecycle_empty_json
SELECT * FROM lagodb_connectors_regress.json_source WHERE false;
\setenv OBJECT_STORAGE_PREFIX 'lagodb-connectors/lifecycle/empty/json/'
\! sh bin/object_storage_tool assert-prefix-empty
\set lifecycle_empty_path :lifecycle_base 'empty/avro/'
CREATE FOREIGN TABLE lagodb_connectors_regress.lifecycle_empty_avro (:avro_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'lifecycle_empty_path', format 'avro');
INSERT INTO lagodb_connectors_regress.lifecycle_empty_avro
SELECT * FROM lagodb_connectors_regress.avro_source WHERE false;
\setenv OBJECT_STORAGE_PREFIX 'lagodb-connectors/lifecycle/empty/avro/'
\! sh bin/object_storage_tool assert-prefix-empty
\set lifecycle_empty_path :lifecycle_base 'empty/parquet/'
CREATE FOREIGN TABLE lagodb_connectors_regress.lifecycle_empty_parquet (:parquet_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'lifecycle_empty_path', format 'parquet');
INSERT INTO lagodb_connectors_regress.lifecycle_empty_parquet
SELECT * FROM lagodb_connectors_regress.parquet_source WHERE false;
\setenv OBJECT_STORAGE_PREFIX 'lagodb-connectors/lifecycle/empty/parquet/'
\! sh bin/object_storage_tool assert-prefix-empty

-- Force rollover before a later row raises division_by_zero. Prefix writers
-- have already uploaded objects and must remove them during statement abort;
-- foreign INSERT and prefix COPY register them for cleanup the same way.
SET lagodb_connectors.target_file_size_mb = 1;
\set lifecycle_failure_foreign_path :lifecycle_base 'failure/foreign/'
CREATE FOREIGN TABLE lagodb_connectors_regress.lifecycle_failure_foreign (
    id integer,
    payload text
)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'lifecycle_failure_foreign_path', format 'text');

\set VERBOSITY sqlstate
INSERT INTO lagodb_connectors_regress.lifecycle_failure_foreign
SELECT id,
       repeat(md5(id::text), 40000 + 0 / (4 - id))
FROM generate_series(1, 4) AS rows(id);
\setenv OBJECT_STORAGE_PREFIX 'lagodb-connectors/lifecycle/failure/foreign/'
\! sh bin/object_storage_tool assert-prefix-empty

-- Exact COPY keeps bytes in local staging until successful completion, so a
-- statement error cannot publish a partial exact object.
\set lifecycle_failure_copy_exact_path :lifecycle_base 'failure/copy-exact.txt'
COPY (
    SELECT id,
           repeat(md5(id::text), 40000 + 0 / (4 - id)) AS payload
    FROM generate_series(1, 4) AS rows(id)
) TO :'lifecycle_failure_copy_exact_path'
WITH (server 'lagodb_connectors_regress_s3', format 'text');
\setenv OBJECT_STORAGE_PREFIX 'lagodb-connectors/lifecycle/failure/copy-exact.txt'
\! sh bin/object_storage_tool assert-prefix-empty
\set VERBOSITY default

-- Prefix rollover for every encoder's size estimate (text and CSV share one
-- encoder). Each COPY must split its output into several objects, and a
-- foreign table reads all of them back.
\set rollover_base 's3://' :storage_bucket '/lagodb-connectors/rollover/'
CREATE TABLE lagodb_connectors_regress.rollover_source (
    id integer,
    payload text
);
INSERT INTO lagodb_connectors_regress.rollover_source
SELECT row_id,
       string_agg(md5(row_id::text || ':' || chunk_id::text), '' ORDER BY chunk_id)
FROM generate_series(1, 800) AS row_values(row_id)
CROSS JOIN generate_series(1, 800) AS chunk_values(chunk_id)
GROUP BY row_id
ORDER BY row_id;

\set rollover_path :rollover_base 'text/'
COPY lagodb_connectors_regress.rollover_source TO :'rollover_path'
WITH (server 'lagodb_connectors_regress_s3', format 'text');
\setenv OBJECT_STORAGE_PREFIX 'lagodb-connectors/rollover/text/'
\! sh bin/object_storage_tool assert-prefix-rollover
CREATE FOREIGN TABLE lagodb_connectors_regress.rollover_text (:id_payload_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'rollover_path', format 'text');

\set rollover_path :rollover_base 'json/'
COPY lagodb_connectors_regress.rollover_source TO :'rollover_path'
WITH (server 'lagodb_connectors_regress_s3', format 'json');
\setenv OBJECT_STORAGE_PREFIX 'lagodb-connectors/rollover/json/'
\! sh bin/object_storage_tool assert-prefix-rollover
CREATE FOREIGN TABLE lagodb_connectors_regress.rollover_json (:id_payload_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'rollover_path', format 'json');

\set rollover_path :rollover_base 'avro/'
COPY lagodb_connectors_regress.rollover_source TO :'rollover_path'
WITH (server 'lagodb_connectors_regress_s3', format 'avro');
\setenv OBJECT_STORAGE_PREFIX 'lagodb-connectors/rollover/avro/'
\! sh bin/object_storage_tool assert-prefix-rollover
CREATE FOREIGN TABLE lagodb_connectors_regress.rollover_avro (:id_payload_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'rollover_path', format 'avro');

\set rollover_path :rollover_base 'parquet/'
COPY lagodb_connectors_regress.rollover_source TO :'rollover_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet', compression 'none');
\setenv OBJECT_STORAGE_PREFIX 'lagodb-connectors/rollover/parquet/'
\! sh bin/object_storage_tool assert-prefix-rollover
CREATE FOREIGN TABLE lagodb_connectors_regress.rollover_parquet (:id_payload_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'rollover_path', format 'parquet');

-- Rollover switches objects only between complete rows, so values use the
-- same encoders as the format round trips. A lost or repeated row changes the
-- count, and an incomplete object fails to read.
SELECT (SELECT count(*) FROM lagodb_connectors_regress.rollover_text) AS text,
       (SELECT count(*) FROM lagodb_connectors_regress.rollover_json) AS json,
       (SELECT count(*) FROM lagodb_connectors_regress.rollover_avro) AS avro,
       (SELECT count(*) FROM lagodb_connectors_regress.rollover_parquet) AS parquet;

RESET lagodb_connectors.target_file_size_mb;
