\i include/column_definitions.sql

-- Shared writer transaction, statement-abort, and rollover lifecycle.

SELECT endpoint,
       bucket,
       region,
       access_key_id,
       secret_access_key
FROM lagodb_regress.object_storage_fixture
\gset storage_

\setenv OBJECT_STORAGE_ENDPOINT :storage_endpoint
\setenv OBJECT_STORAGE_BUCKET :storage_bucket
\setenv OBJECT_STORAGE_REGION :storage_region
\setenv OBJECT_STORAGE_ACCESS_KEY_ID :storage_access_key_id
\setenv OBJECT_STORAGE_SECRET_ACCESS_KEY :storage_secret_access_key

SELECT format('s3://%s/lagodb-connectors/lifecycle/rollback/text/',
              :'storage_bucket') AS rollback_text_path,
       format('s3://%s/lagodb-connectors/lifecycle/commit/',
              :'storage_bucket') AS commit_path,
       format('s3://%s/lagodb-connectors/lifecycle/savepoint/',
              :'storage_bucket') AS savepoint_path,
       format('s3://%s/lagodb-connectors/lifecycle/append/',
              :'storage_bucket') AS append_path,
       format('s3://%s/lagodb-connectors/lifecycle/empty/text/',
              :'storage_bucket') AS empty_text_path,
       format('s3://%s/lagodb-connectors/lifecycle/empty/csv/',
              :'storage_bucket') AS empty_csv_path,
       format('s3://%s/lagodb-connectors/lifecycle/empty/json/',
              :'storage_bucket') AS empty_json_path,
       format('s3://%s/lagodb-connectors/lifecycle/empty/avro/',
              :'storage_bucket') AS empty_avro_path,
       format('s3://%s/lagodb-connectors/lifecycle/empty/parquet/',
              :'storage_bucket') AS empty_parquet_path,
       format('s3://%s/lagodb-connectors/lifecycle/failure/foreign/',
              :'storage_bucket') AS failure_foreign_path,
       format('s3://%s/lagodb-connectors/lifecycle/failure/copy-prefix/',
              :'storage_bucket') AS failure_copy_prefix_path,
       format('s3://%s/lagodb-connectors/lifecycle/failure/copy-exact.txt',
              :'storage_bucket') AS failure_copy_exact_path,
       format('s3://%s/lagodb-connectors/lifecycle/abort/copy-prefix/',
              :'storage_bucket') AS abort_copy_prefix_path,
       'lagodb-connectors/lifecycle/empty/text/' AS empty_text_key,
       'lagodb-connectors/lifecycle/empty/csv/' AS empty_csv_key,
       'lagodb-connectors/lifecycle/empty/json/' AS empty_json_key,
       'lagodb-connectors/lifecycle/empty/avro/' AS empty_avro_key,
       'lagodb-connectors/lifecycle/empty/parquet/' AS empty_parquet_key,
       'lagodb-connectors/lifecycle/failure/foreign/' AS failure_foreign_key,
       'lagodb-connectors/lifecycle/failure/copy-prefix/' AS failure_copy_prefix_key,
       'lagodb-connectors/lifecycle/failure/copy-exact.txt' AS failure_copy_exact_key,
       'lagodb-connectors/lifecycle/abort/copy-prefix/' AS abort_copy_prefix_key
\gset lifecycle_

CREATE FOREIGN TABLE lagodb_connectors_regress.lifecycle_rollback_text
    (:common_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'lifecycle_rollback_text_path', format 'text');

-- Rollback removes published objects; empty-output and rollover cases cover each writer.
BEGIN;
INSERT INTO lagodb_connectors_regress.lifecycle_rollback_text
SELECT * FROM lagodb_connectors_regress.common_source WHERE id = 1;
SELECT count(*) AS text_rows_before_rollback
FROM lagodb_connectors_regress.lifecycle_rollback_text;
ROLLBACK;

SELECT count(*) AS rows_after_rollback
FROM lagodb_connectors_regress.lifecycle_rollback_text;

-- A committed prefix object remains visible after the transaction callback.
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
CREATE FOREIGN TABLE lagodb_connectors_regress.lifecycle_append
    (:common_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'lifecycle_append_path', format 'avro');
INSERT INTO lagodb_connectors_regress.lifecycle_append
SELECT * FROM lagodb_connectors_regress.common_source WHERE id = 1;
INSERT INTO lagodb_connectors_regress.lifecycle_append
SELECT * FROM lagodb_connectors_regress.common_source WHERE id = 2;
SELECT id FROM lagodb_connectors_regress.lifecycle_append ORDER BY id;

-- Empty foreign INSERT leaves no object; direct COPY TO has a separate empty-object contract.
CREATE FOREIGN TABLE lagodb_connectors_regress.lifecycle_empty_text
    (:common_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'lifecycle_empty_text_path', format 'text');
CREATE FOREIGN TABLE lagodb_connectors_regress.lifecycle_empty_csv
    (:common_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'lifecycle_empty_csv_path', format 'csv');
CREATE FOREIGN TABLE lagodb_connectors_regress.lifecycle_empty_json
    (:json_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'lifecycle_empty_json_path', format 'json');
CREATE FOREIGN TABLE lagodb_connectors_regress.lifecycle_empty_avro
    (:common_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'lifecycle_empty_avro_path', format 'avro');
CREATE FOREIGN TABLE lagodb_connectors_regress.lifecycle_empty_parquet
    (:parquet_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'lifecycle_empty_parquet_path', format 'parquet');

INSERT INTO lagodb_connectors_regress.lifecycle_empty_text
SELECT * FROM lagodb_connectors_regress.common_source WHERE false;
INSERT INTO lagodb_connectors_regress.lifecycle_empty_csv
SELECT * FROM lagodb_connectors_regress.common_source WHERE false;
INSERT INTO lagodb_connectors_regress.lifecycle_empty_json
SELECT * FROM lagodb_connectors_regress.json_source WHERE false;
INSERT INTO lagodb_connectors_regress.lifecycle_empty_avro
SELECT * FROM lagodb_connectors_regress.common_source WHERE false;
INSERT INTO lagodb_connectors_regress.lifecycle_empty_parquet
SELECT * FROM lagodb_connectors_regress.parquet_source WHERE false;

\setenv OBJECT_STORAGE_PREFIX :lifecycle_empty_text_key
\! sh bin/object_storage_tool assert-prefix-empty
\setenv OBJECT_STORAGE_PREFIX :lifecycle_empty_csv_key
\! sh bin/object_storage_tool assert-prefix-empty
\setenv OBJECT_STORAGE_PREFIX :lifecycle_empty_json_key
\! sh bin/object_storage_tool assert-prefix-empty
\setenv OBJECT_STORAGE_PREFIX :lifecycle_empty_avro_key
\! sh bin/object_storage_tool assert-prefix-empty
\setenv OBJECT_STORAGE_PREFIX :lifecycle_empty_parquet_key
\! sh bin/object_storage_tool assert-prefix-empty

-- Force rollover before a later row raises division_by_zero. Prefix writers
-- have already uploaded objects and must remove them during statement abort.
SET lagodb_connectors.target_file_size_mb = 1;
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
\setenv OBJECT_STORAGE_PREFIX :lifecycle_failure_foreign_key
\! sh bin/object_storage_tool assert-prefix-empty

-- Exact COPY keeps bytes in local staging until successful completion, so a
-- statement error cannot publish a partial exact object.
COPY (
    SELECT id,
           repeat(md5(id::text), 40000 + 0 / (4 - id)) AS payload
    FROM generate_series(1, 4) AS rows(id)
) TO :'lifecycle_failure_copy_exact_path'
WITH (server 'lagodb_connectors_regress_s3', format 'text');
\setenv OBJECT_STORAGE_PREFIX :lifecycle_failure_copy_exact_key
\! sh bin/object_storage_tool assert-prefix-empty

-- Prefix COPY can publish rolled objects before a later row fails; abort
-- cleanup removes every object allocated by the failed statement.
COPY (
    SELECT id,
           repeat(md5(id::text), 40000 + 0 / (4 - id)) AS payload
    FROM generate_series(1, 4) AS rows(id)
) TO :'lifecycle_failure_copy_prefix_path'
WITH (server 'lagodb_connectors_regress_s3', format 'text');
\setenv OBJECT_STORAGE_PREFIX :lifecycle_failure_copy_prefix_key
\! sh bin/object_storage_tool assert-prefix-empty
\set VERBOSITY default

-- A successful prefix COPY is visible before commit and removed on an
-- explicit top-level abort.
CREATE FOREIGN TABLE lagodb_connectors_regress.lifecycle_abort_copy_prefix (
    id integer,
    payload text
)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'lifecycle_abort_copy_prefix_path', format 'text');
BEGIN;
COPY (SELECT 1 AS id, 'abort'::text AS payload)
TO :'lifecycle_abort_copy_prefix_path'
WITH (server 'lagodb_connectors_regress_s3', format 'text');
SELECT count(*) AS copy_prefix_rows_before_rollback
FROM lagodb_connectors_regress.lifecycle_abort_copy_prefix;
ROLLBACK;
\setenv OBJECT_STORAGE_PREFIX :lifecycle_abort_copy_prefix_key
\! sh bin/object_storage_tool assert-prefix-empty

-- Prefix rollover for every writer implementation.

SELECT format('s3://%s/lagodb-connectors/rollover/text/',
              :'storage_bucket') AS text_path,
       format('s3://%s/lagodb-connectors/rollover/csv/',
              :'storage_bucket') AS csv_path,
       format('s3://%s/lagodb-connectors/rollover/json/',
              :'storage_bucket') AS json_path,
       format('s3://%s/lagodb-connectors/rollover/avro/',
              :'storage_bucket') AS avro_path,
       format('s3://%s/lagodb-connectors/rollover/parquet/',
              :'storage_bucket') AS parquet_path,
       'lagodb-connectors/rollover/text/' AS text_key,
       'lagodb-connectors/rollover/csv/' AS csv_key,
       'lagodb-connectors/rollover/json/' AS json_key,
       'lagodb-connectors/rollover/avro/' AS avro_key,
       'lagodb-connectors/rollover/parquet/' AS parquet_key
\gset rollover_

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

COPY lagodb_connectors_regress.rollover_source
TO :'rollover_text_path'
WITH (server 'lagodb_connectors_regress_s3', format 'text');
COPY lagodb_connectors_regress.rollover_source
TO :'rollover_csv_path'
WITH (server 'lagodb_connectors_regress_s3', format 'csv');
COPY lagodb_connectors_regress.rollover_source
TO :'rollover_json_path'
WITH (server 'lagodb_connectors_regress_s3', format 'json');
COPY lagodb_connectors_regress.rollover_source
TO :'rollover_avro_path'
WITH (server 'lagodb_connectors_regress_s3', format 'avro');
COPY lagodb_connectors_regress.rollover_source
TO :'rollover_parquet_path'
WITH (
    server 'lagodb_connectors_regress_s3',
    format 'parquet',
    compression 'none'
);

\setenv OBJECT_STORAGE_PREFIX :rollover_text_key
\! sh bin/object_storage_tool assert-prefix-rollover
\setenv OBJECT_STORAGE_PREFIX :rollover_csv_key
\! sh bin/object_storage_tool assert-prefix-rollover
\setenv OBJECT_STORAGE_PREFIX :rollover_json_key
\! sh bin/object_storage_tool assert-prefix-rollover
\setenv OBJECT_STORAGE_PREFIX :rollover_avro_key
\! sh bin/object_storage_tool assert-prefix-rollover
\setenv OBJECT_STORAGE_PREFIX :rollover_parquet_key
\! sh bin/object_storage_tool assert-prefix-rollover

CREATE FOREIGN TABLE lagodb_connectors_regress.rollover_text
    (:id_payload_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'rollover_text_path', format 'text');
CREATE FOREIGN TABLE lagodb_connectors_regress.rollover_csv
    (:id_payload_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'rollover_csv_path', format 'csv');
CREATE FOREIGN TABLE lagodb_connectors_regress.rollover_json
    (:id_payload_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'rollover_json_path', format 'json');
CREATE FOREIGN TABLE lagodb_connectors_regress.rollover_avro
    (:id_payload_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'rollover_avro_path', format 'avro');
CREATE FOREIGN TABLE lagodb_connectors_regress.rollover_parquet
    (:id_payload_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'rollover_parquet_path', format 'parquet');

CREATE TABLE lagodb_connectors_regress.rollover_parquet_copy
    (:id_payload_columns);
COPY lagodb_connectors_regress.rollover_parquet_copy
FROM :'rollover_parquet_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');

-- Compare complete rows and duplicate counts across all rolled objects.
(SELECT * FROM lagodb_connectors_regress.rollover_text
 EXCEPT ALL SELECT * FROM lagodb_connectors_regress.rollover_source)
UNION ALL
(SELECT * FROM lagodb_connectors_regress.rollover_source
 EXCEPT ALL SELECT * FROM lagodb_connectors_regress.rollover_text);

(SELECT * FROM lagodb_connectors_regress.rollover_csv
 EXCEPT ALL SELECT * FROM lagodb_connectors_regress.rollover_source)
UNION ALL
(SELECT * FROM lagodb_connectors_regress.rollover_source
 EXCEPT ALL SELECT * FROM lagodb_connectors_regress.rollover_csv);

(SELECT * FROM lagodb_connectors_regress.rollover_json
 EXCEPT ALL SELECT * FROM lagodb_connectors_regress.rollover_source)
UNION ALL
(SELECT * FROM lagodb_connectors_regress.rollover_source
 EXCEPT ALL SELECT * FROM lagodb_connectors_regress.rollover_json);

(SELECT * FROM lagodb_connectors_regress.rollover_avro
 EXCEPT ALL SELECT * FROM lagodb_connectors_regress.rollover_source)
UNION ALL
(SELECT * FROM lagodb_connectors_regress.rollover_source
 EXCEPT ALL SELECT * FROM lagodb_connectors_regress.rollover_avro);

(SELECT * FROM lagodb_connectors_regress.rollover_parquet
 EXCEPT ALL SELECT * FROM lagodb_connectors_regress.rollover_source)
UNION ALL
(SELECT * FROM lagodb_connectors_regress.rollover_source
 EXCEPT ALL SELECT * FROM lagodb_connectors_regress.rollover_parquet);

(SELECT * FROM lagodb_connectors_regress.rollover_parquet_copy
 EXCEPT ALL SELECT * FROM lagodb_connectors_regress.rollover_source)
UNION ALL
(SELECT * FROM lagodb_connectors_regress.rollover_source
 EXCEPT ALL SELECT * FROM lagodb_connectors_regress.rollover_parquet_copy);

RESET lagodb_connectors.target_file_size_mb;
