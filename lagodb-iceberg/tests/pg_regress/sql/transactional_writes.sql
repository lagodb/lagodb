-- Transactional lifecycle of Iceberg write operations.

DROP EXTENSION IF EXISTS lagodb_iceberg CASCADE;
CREATE EXTENSION IF NOT EXISTS lagodb_iceberg;
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
SELECT * FROM dml_lifecycle.savepoint_t ORDER BY id;
COMMIT;

SELECT * FROM dml_lifecycle.savepoint_t ORDER BY id;

-- RELEASE SAVEPOINT smoke test. This DML frame finishes before RELEASE, so
-- it does not directly exercise ResourceOwner reparent; it guards against
-- accidental leak warnings on the release path.
BEGIN;
SAVEPOINT s2;
INSERT INTO dml_lifecycle.savepoint_t VALUES (3);
RELEASE SAVEPOINT s2;
SELECT * FROM dml_lifecycle.savepoint_t ORDER BY id;
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

COPY dml_lifecycle.copy_t FROM stdin;
1
bad
\.

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
SELECT id, label FROM dml_lifecycle.same_tx_dml_t ORDER BY id;
COMMIT;

SELECT id, label FROM dml_lifecycle.same_tx_dml_t ORDER BY id;

-- A cursor keeps its command-visible overlay even when its second Append
-- input starts after a later INSERT. Use the ordinary AM's lazy scan binding.
SET lagodb.query_offload_mode = 'off';
SET lagodb.customscan_mode = 'off';
SET max_parallel_workers_per_gather = 0;
CREATE TABLE dml_lifecycle.cursor_t (id integer) USING iceberg;
BEGIN;
INSERT INTO dml_lifecycle.cursor_t VALUES (1);
DECLARE visible_rows NO SCROLL CURSOR FOR
SELECT id FROM dml_lifecycle.cursor_t
UNION ALL
SELECT id FROM dml_lifecycle.cursor_t;
FETCH 1 FROM visible_rows;
INSERT INTO dml_lifecycle.cursor_t VALUES (2);
FETCH ALL FROM visible_rows;
CLOSE visible_rows;
SELECT id FROM dml_lifecycle.cursor_t ORDER BY id;
ROLLBACK;

-- SPI advances the CID before the outer writer publishes its files. The
-- cursor must include the outer command and exclude the later nested INSERT,
-- even though that nested write reaches the action log first.
CREATE TABLE dml_lifecycle.command_t (id integer, marker integer) USING iceberg;
CREATE FUNCTION dml_lifecycle.capture_command() RETURNS integer
LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    c refcursor := 'command_rows';
BEGIN
    OPEN c FOR SELECT id FROM dml_lifecycle.command_t ORDER BY id;
    INSERT INTO dml_lifecycle.command_t (id) VALUES (2);
    RETURN 1;
END;
$$;
BEGIN;
INSERT INTO dml_lifecycle.command_t (id) VALUES (1)
RETURNING dml_lifecycle.capture_command() AS result;
FETCH ALL FROM command_rows;
SELECT id FROM dml_lifecycle.command_t ORDER BY id;
ROLLBACK;

-- COPY captures its output CID before running triggers. The same nested
-- command case covers COPY without depending on a ModifyTable executor CID.
CREATE FUNCTION dml_lifecycle.capture_copy() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    NEW.id := dml_lifecycle.capture_command();
    RETURN NEW;
END;
$$;
CREATE TRIGGER capture_copy BEFORE INSERT ON dml_lifecycle.command_t
FOR EACH ROW WHEN (NEW.marker = 10)
EXECUTE FUNCTION dml_lifecycle.capture_copy();
BEGIN;
COPY dml_lifecycle.command_t (marker) FROM stdin;
10
\.
FETCH ALL FROM command_rows;
SELECT id FROM dml_lifecycle.command_t ORDER BY id;
ROLLBACK;
RESET max_parallel_workers_per_gather;
RESET lagodb.customscan_mode;
RESET lagodb.query_offload_mode;

SET client_min_messages = warning;
DROP SCHEMA dml_lifecycle CASCADE;
RESET client_min_messages;
