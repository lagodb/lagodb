-- Local SQLite catalog: SQL import, publication and transaction lifecycle.
\setenv PGDATABASE :DBNAME
\set iceberg_fixture sqlite
\i include/iceberg_fixture.sql

SELECT '/tmp/lagodb_object_regress_' || current_setting('port') || '/sqlite' AS sqlite_root
\gset
\set sqlite_uri 'sqlite:' :sqlite_root '/catalog.db'
\set sqlite_warehouse 'file://' :sqlite_root
SET client_min_messages = warning;
DROP EXTENSION IF EXISTS lagodb_iceberg CASCADE;
RESET client_min_messages;
CREATE EXTENSION lagodb_iceberg;
CREATE SCHEMA iceberg_fdw_sqlite;

-- Configuring a local-file catalog requires both server-file privileges.
CREATE ROLE regress_sqlite_unprivileged;
GRANT USAGE ON FOREIGN DATA WRAPPER lagodb_iceberg TO regress_sqlite_unprivileged;
SET ROLE regress_sqlite_unprivileged;
CREATE SERVER iceberg_sqlite_denied TYPE 'sqlite'
FOREIGN DATA WRAPPER lagodb_iceberg
OPTIONS (uri :'sqlite_uri', warehouse :'sqlite_warehouse', catalog_name 'regress');

RESET ROLE;
REVOKE USAGE ON FOREIGN DATA WRAPPER lagodb_iceberg FROM regress_sqlite_unprivileged;
CREATE SERVER iceberg_sqlite TYPE 'sqlite'
FOREIGN DATA WRAPPER lagodb_iceberg
OPTIONS (uri :'sqlite_uri', warehouse :'sqlite_warehouse', catalog_name 'regress');
SET ROLE regress_sqlite_unprivileged;
ALTER SERVER iceberg_sqlite OPTIONS (SET uri :'sqlite_uri');
RESET ROLE;
DROP ROLE regress_sqlite_unprivileged;
ALTER SERVER iceberg_sqlite OPTIONS (SET catalog_name 'other');
IMPORT FOREIGN SCHEMA fdw_sqlite
FROM SERVER iceberg_sqlite INTO iceberg_fdw_sqlite;
SELECT count(*) = 0 AS initially_empty FROM iceberg_fdw_sqlite.items;

-- A staged definition is visible to the creating transaction. Rolling back a
-- rejected DDL statement must preserve the staged table and its data.
BEGIN;
CREATE FOREIGN TABLE iceberg_fdw_sqlite.created_by_lagodb (
    id integer NOT NULL,
    payload text
)
SERVER iceberg_sqlite
OPTIONS (table 'fdw_sqlite.created_by_lagodb', create_remote_if_missing 'true');
INSERT INTO iceberg_fdw_sqlite.created_by_lagodb
VALUES (7, 'created');
UPDATE iceberg_fdw_sqlite.created_by_lagodb
SET payload = 'updated' WHERE id = 7;
SAVEPOINT staged_ddl;
\set VERBOSITY terse
ALTER FOREIGN TABLE iceberg_fdw_sqlite.created_by_lagodb ADD COLUMN extra integer;
ROLLBACK TO SAVEPOINT staged_ddl;
ALTER FOREIGN TABLE iceberg_fdw_sqlite.created_by_lagodb
    OPTIONS (SET table 'fdw_sqlite.retargeted_while_staged');
ROLLBACK TO SAVEPOINT staged_ddl;
SELECT * FROM iceberg_fdw_sqlite.created_by_lagodb;
COMMIT;

-- One live staged relation owns its remote identity until publication.
BEGIN;
CREATE FOREIGN TABLE iceberg_fdw_sqlite.first_amount_mapping (
    amount numeric(8,2)
)
SERVER iceberg_sqlite
OPTIONS (table 'fdw_sqlite.amounts', create_remote_if_missing 'true');
CREATE FOREIGN TABLE iceberg_fdw_sqlite.read_amount_mapping (amount numeric)
SERVER iceberg_sqlite
OPTIONS (table 'fdw_sqlite.amounts', create_remote_if_missing 'true');
ROLLBACK;
\set VERBOSITY default

-- CREATE+DROP collapses to no catalog publication.
BEGIN;
CREATE FOREIGN TABLE iceberg_fdw_sqlite.canceled_create (id integer)
SERVER iceberg_sqlite
OPTIONS (table 'fdw_sqlite.canceled_create', create_remote_if_missing 'true');
INSERT INTO iceberg_fdw_sqlite.canceled_create VALUES (1);
DROP FOREIGN TABLE iceberg_fdw_sqlite.canceled_create;
COMMIT;
CREATE FOREIGN TABLE iceberg_fdw_sqlite.canceled_probe (id integer)
SERVER iceberg_sqlite OPTIONS (table 'fdw_sqlite.canceled_create');

-- Rolling back a replacement restores the prior generation and its actions.
BEGIN;
CREATE FOREIGN TABLE iceberg_fdw_sqlite.restored_create (id integer)
SERVER iceberg_sqlite
OPTIONS (table 'fdw_sqlite.restored_create', create_remote_if_missing 'true');
INSERT INTO iceberg_fdw_sqlite.restored_create VALUES (10);
SAVEPOINT replace_generation;
DROP FOREIGN TABLE iceberg_fdw_sqlite.restored_create;
CREATE FOREIGN TABLE iceberg_fdw_sqlite.replacement_create (
    id integer,
    generation text
)
SERVER iceberg_sqlite
OPTIONS (table 'fdw_sqlite.restored_create', create_remote_if_missing 'true');
INSERT INTO iceberg_fdw_sqlite.replacement_create VALUES (20, 'discard');
ROLLBACK TO SAVEPOINT replace_generation;
SELECT * FROM iceberg_fdw_sqlite.restored_create;
COMMIT;
SELECT * FROM iceberg_fdw_sqlite.restored_create;

-- Subtransaction abort discards staged creation; RELEASE promotes it.
BEGIN;
SAVEPOINT staged_create;
CREATE FOREIGN TABLE iceberg_fdw_sqlite.rolled_back_create (id integer)
SERVER iceberg_sqlite
OPTIONS (table 'fdw_sqlite.rolled_back_create', create_remote_if_missing 'true');
ROLLBACK TO SAVEPOINT staged_create;
COMMIT;
CREATE FOREIGN TABLE iceberg_fdw_sqlite.rolled_back_probe (id integer)
SERVER iceberg_sqlite OPTIONS (table 'fdw_sqlite.rolled_back_create');

BEGIN;
SAVEPOINT staged_create;
CREATE FOREIGN TABLE iceberg_fdw_sqlite.released_create (id integer)
SERVER iceberg_sqlite
OPTIONS (table 'fdw_sqlite.released_create', create_remote_if_missing 'true');
RELEASE SAVEPOINT staged_create;
COMMIT;
CREATE FOREIGN TABLE iceberg_fdw_sqlite.released_probe ()
SERVER iceberg_sqlite OPTIONS (table 'fdw_sqlite.released_create');
SELECT attname
FROM pg_attribute
WHERE attrelid = 'iceberg_fdw_sqlite.released_probe'::regclass
  AND attnum > 0 AND NOT attisdropped;

-- A creation schema cannot select a definition from a table that does not exist.
CREATE FOREIGN TABLE iceberg_fdw_sqlite.invalid_creation_schema (id integer)
SERVER iceberg_sqlite
OPTIONS (table 'fdw_sqlite.invalid_creation_schema',
         create_remote_if_missing 'true', schema_id '0');

-- The same SQLite catalog publishes both tables at commit.
BEGIN;
INSERT INTO iceberg_fdw_sqlite.items VALUES (1, 'one'), (2, 'two');
INSERT INTO iceberg_fdw_sqlite.other_items VALUES (10, 'ten');
SELECT array_agg(id ORDER BY id) AS staged_ids FROM iceberg_fdw_sqlite.items;

COMMIT;
-- A new backend must reopen the catalog and read the published local files.
\connect :DBNAME
SELECT (SELECT array_agg(id ORDER BY id) FROM iceberg_fdw_sqlite.items) AS items,
       (SELECT array_agg(id ORDER BY id) FROM iceberg_fdw_sqlite.other_items) AS other_items;
SELECT * FROM iceberg_fdw_sqlite.created_by_lagodb;

UPDATE iceberg_fdw_sqlite.items SET payload = 'changed' WHERE id = 1;
DELETE FROM iceberg_fdw_sqlite.items WHERE id = 2;
-- Rollback leaves both published pointers unchanged.
BEGIN;
INSERT INTO iceberg_fdw_sqlite.items VALUES (3, 'discard');
INSERT INTO iceberg_fdw_sqlite.other_items VALUES (11, 'discard');
ROLLBACK;
SELECT (SELECT array_agg(id::text || ':' || payload ORDER BY id)
        FROM iceberg_fdw_sqlite.items) AS items,
       (SELECT array_agg(id::text || ':' || payload ORDER BY id)
        FROM iceberg_fdw_sqlite.other_items) AS other_items;

DROP FOREIGN TABLE iceberg_fdw_sqlite.items, iceberg_fdw_sqlite.other_items,
    iceberg_fdw_sqlite.created_by_lagodb,
    iceberg_fdw_sqlite.restored_create,
    iceberg_fdw_sqlite.released_create,
    iceberg_fdw_sqlite.released_probe;
DROP SERVER iceberg_sqlite;
DROP SCHEMA iceberg_fdw_sqlite;
