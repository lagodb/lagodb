-- Transactional lifecycle of Iceberg write operations.

DROP EXTENSION IF EXISTS pg_iceberg_am CASCADE;
CREATE EXTENSION IF NOT EXISTS pg_iceberg_am;
CREATE SCHEMA dml_lifecycle;

-- ROLLBACK TO SAVEPOINT must abort the frame and allow a new frame later.
CREATE TABLE dml_lifecycle.savepoint_t (
    id integer
) USING iceberg;

BEGIN;
SAVEPOINT s1;
INSERT INTO dml_lifecycle.savepoint_t VALUES (1);
ROLLBACK TO SAVEPOINT s1;
INSERT INTO dml_lifecycle.savepoint_t VALUES (2);
COMMIT;

SELECT * FROM dml_lifecycle.savepoint_t ORDER BY id;

-- RELEASE SAVEPOINT smoke test. This DML frame finishes before RELEASE, so
-- it does not directly exercise ResourceOwner reparent; it guards against
-- accidental leak warnings on the release path.
BEGIN;
SAVEPOINT s2;
INSERT INTO dml_lifecycle.savepoint_t VALUES (3);
RELEASE SAVEPOINT s2;
COMMIT;

SELECT * FROM dml_lifecycle.savepoint_t ORDER BY id;

-- Nested subtransaction abort must only discard the inner frame.
BEGIN;
SAVEPOINT outer_sp;
INSERT INTO dml_lifecycle.savepoint_t VALUES (10);
SAVEPOINT inner_sp;
INSERT INTO dml_lifecycle.savepoint_t VALUES (11);
ROLLBACK TO SAVEPOINT inner_sp;
INSERT INTO dml_lifecycle.savepoint_t VALUES (12);
COMMIT;

SELECT * FROM dml_lifecycle.savepoint_t WHERE id >= 10 ORDER BY id;

-- COPY FROM failure must abort the COPY frame and leave no inserted rows.
CREATE TABLE dml_lifecycle.copy_t (
    id integer
) USING iceberg;

\set VERBOSITY terse
COPY dml_lifecycle.copy_t FROM stdin;
1
bad
\.
\set VERBOSITY default

SELECT 'no resource leak' AS check;
SELECT count(*) AS rows_after_failed_copy FROM dml_lifecycle.copy_t;

COPY dml_lifecycle.copy_t FROM stdin;
2
3
\.

SELECT * FROM dml_lifecycle.copy_t ORDER BY id;

-- RowDelta validation must use the final transaction-local view. In
-- particular, position deletes may reference data files appended by an
-- earlier statement in the same PostgreSQL transaction.
CREATE TABLE dml_lifecycle.same_tx_dml_t (
    id integer,
    label text
) USING iceberg;

BEGIN;
INSERT INTO dml_lifecycle.same_tx_dml_t VALUES
    (1, 'one'),
    (2, 'two'),
    (3, 'three');
UPDATE dml_lifecycle.same_tx_dml_t
SET label = 'two_updated'
WHERE id = 2;
DELETE FROM dml_lifecycle.same_tx_dml_t
WHERE id IN (1, 2);
COPY (
    SELECT id, label FROM dml_lifecycle.same_tx_dml_t ORDER BY id
) TO STDOUT WITH (FORMAT csv);
COMMIT;

COPY (
    SELECT id, label FROM dml_lifecycle.same_tx_dml_t ORDER BY id
) TO STDOUT WITH (FORMAT csv);

SET client_min_messages = warning;
DROP SCHEMA dml_lifecycle CASCADE;
RESET client_min_messages;
