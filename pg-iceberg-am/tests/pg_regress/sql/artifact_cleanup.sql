-- artifact_cleanup.sql
-- Tests for transactional storage artifact cleanup on local tablespaces.
-- Verifies that abort cleans up data files and savepoint semantics work.

DROP EXTENSION IF EXISTS pg_iceberg_am CASCADE;
CREATE EXTENSION pg_iceberg_am;

CREATE SCHEMA artifact_cleanup;

--
-- Test 1: Local DML abort deletes new data files
--
-- After a rolled-back INSERT, newly created data files must be removed.
CREATE TABLE artifact_cleanup.abort_insert (id integer) USING iceberg;
SELECT pg_relation_filepath('artifact_cleanup.abort_insert') || '_iceberg' AS tbl_dir \gset

-- Verify table dir exists but data dir has no parquet files yet
\! find :"tbl_dir" -name '*.parquet' 2>/dev/null | wc -l | tr -d ' '

BEGIN;
INSERT INTO artifact_cleanup.abort_insert SELECT g FROM generate_series(1, 100) AS g;
ROLLBACK;

-- After ROLLBACK, data files must be cleaned up
\! find :"tbl_dir" -name '*.parquet' 2>/dev/null | wc -l | tr -d ' '

-- Table should have zero rows
SELECT count(*) AS rows_after_abort FROM artifact_cleanup.abort_insert;


--
-- Test 2: Committed INSERT preserves data files
--
INSERT INTO artifact_cleanup.abort_insert SELECT g FROM generate_series(1, 10) AS g;
SELECT count(*) AS rows_after_commit FROM artifact_cleanup.abort_insert;


--
-- Test 3: Savepoint rollback cleans only sub-transaction files
--
CREATE TABLE artifact_cleanup.savepoint_cleanup (id integer) USING iceberg;

BEGIN;
-- Outer INSERT (nest level 1)
INSERT INTO artifact_cleanup.savepoint_cleanup VALUES (1), (2), (3);
SAVEPOINT s1;
-- Inner INSERT (nest level 2)
INSERT INTO artifact_cleanup.savepoint_cleanup VALUES (10), (20), (30);
-- Rollback inner savepoint - inner data files should be cleaned
ROLLBACK TO SAVEPOINT s1;
-- Commit outer
COMMIT;

-- Only outer rows survive
SELECT count(*) AS rows_after_partial_rollback FROM artifact_cleanup.savepoint_cleanup;
SELECT * FROM artifact_cleanup.savepoint_cleanup ORDER BY id;


--
-- Test 4: Savepoint release + outer abort cleans all merged entries
--
CREATE TABLE artifact_cleanup.release_abort (id integer) USING iceberg;
SELECT pg_relation_filepath('artifact_cleanup.release_abort') || '_iceberg' AS ra_dir \gset

BEGIN;
SAVEPOINT s2;
INSERT INTO artifact_cleanup.release_abort VALUES (100), (200);
-- Release promotes sub-xact artifacts to parent
RELEASE SAVEPOINT s2;
-- Outer abort should clean all (including promoted) artifacts
ROLLBACK;

-- Zero rows, data files cleaned
SELECT count(*) AS rows_after_release_abort FROM artifact_cleanup.release_abort;
\! find :"ra_dir" -name '*.parquet' 2>/dev/null | wc -l | tr -d ' '


--
-- Test 5: Nested savepoints - only innermost is cleaned on rollback
--
CREATE TABLE artifact_cleanup.nested_sp (id integer) USING iceberg;

BEGIN;
INSERT INTO artifact_cleanup.nested_sp VALUES (1);
SAVEPOINT sp_outer;
INSERT INTO artifact_cleanup.nested_sp VALUES (2);
SAVEPOINT sp_inner;
INSERT INTO artifact_cleanup.nested_sp VALUES (3);
-- Rollback only the innermost
ROLLBACK TO SAVEPOINT sp_inner;
-- Row 3 is gone, rows 1 and 2 survive
INSERT INTO artifact_cleanup.nested_sp VALUES (4);
COMMIT;

SELECT * FROM artifact_cleanup.nested_sp ORDER BY id;


SET client_min_messages = warning;
DROP SCHEMA artifact_cleanup CASCADE;
RESET client_min_messages;
