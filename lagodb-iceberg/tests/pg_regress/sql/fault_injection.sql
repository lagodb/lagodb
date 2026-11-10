-- PostgreSQL 17 introduced injection points; _1.out is the PostgreSQL 15 and
-- 16 output.
SELECT current_setting('server_version_num')::int < 170000 AS skip_test \gset
\if :skip_test
\quit
\endif

-- Failure paths that need a PostgreSQL injection point and own their fixtures.
\setenv PGDATABASE :DBNAME
DROP EXTENSION IF EXISTS lagodb_iceberg CASCADE;
CREATE EXTENSION lagodb_iceberg;
CREATE EXTENSION injection_points;

-- A failed rewrite must leave the old snapshot queryable, clean its attempt
-- artifacts, and permit a later VACUUM to succeed.
CREATE TABLE vacuum_failure_recovery_t (id integer, payload text)
USING iceberg;
INSERT INTO vacuum_failure_recovery_t VALUES (1, 'one');
INSERT INTO vacuum_failure_recovery_t VALUES (2, 'two');
INSERT INTO vacuum_failure_recovery_t VALUES (3, 'three');
INSERT INTO vacuum_failure_recovery_t VALUES (4, 'four');
INSERT INTO vacuum_failure_recovery_t VALUES (5, 'five');
INSERT INTO vacuum_failure_recovery_t VALUES (6, 'six');

SELECT pg_relation_filepath('vacuum_failure_recovery_t'::regclass) || '_iceberg' AS failure_root
\gset
SELECT count(*) AS objects_before_failure
FROM pg_ls_dir(:'failure_root', true, false)
\gset

SELECT injection_points_set_local();
SELECT injection_points_attach('lagodb-iceberg-vacuum-after-rewrite', 'error');
VACUUM vacuum_failure_recovery_t;
SELECT injection_points_detach('lagodb-iceberg-vacuum-after-rewrite');

SELECT * FROM vacuum_failure_recovery_t ORDER BY id;
SELECT current_data_objects AS failed_attempt_data_objects
FROM lagodb.table_maintenance_stats('vacuum_failure_recovery_t');
SELECT count(*) = :objects_before_failure::bigint
       AS failed_attempt_artifacts_cleaned
FROM pg_ls_dir(:'failure_root', true, false);

VACUUM vacuum_failure_recovery_t;
SELECT * FROM vacuum_failure_recovery_t ORDER BY id;
SELECT current_data_objects AS retry_data_objects
FROM lagodb.table_maintenance_stats('vacuum_failure_recovery_t');

DROP TABLE vacuum_failure_recovery_t;

-- Verify query cancellation during storage I/O and post-cancel recovery:
-- cleanup defers PostgreSQL ERROR until Drop finishes, while a foreground
-- response wait processes cancel immediately and poisons its connection.
\set object_tablespace regress_socket_cancel
\i include/object_tablespace.sql

CREATE TABLE storage_socket_cancel_contexts_t (id integer)
USING iceberg
TABLESPACE :"object_tablespace";
INSERT INTO storage_socket_cancel_contexts_t VALUES (1);

\! sh bin/storage_socket_cancel_contexts

SELECT count(*) AS rows_after_cancel FROM storage_socket_cancel_contexts_t;

DROP TABLE storage_socket_cancel_contexts_t;
DROP TABLESPACE :"object_tablespace";
DROP EXTENSION lagodb_iceberg CASCADE;
DROP EXTENSION injection_points;
\! rm -rf "$LAGODB_REGRESS_TABLESPACE_PATH"/*
