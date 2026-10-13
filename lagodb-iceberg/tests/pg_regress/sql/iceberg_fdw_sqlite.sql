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
DROP ROLE regress_sqlite_unprivileged;
CREATE SERVER iceberg_sqlite TYPE 'sqlite'
FOREIGN DATA WRAPPER lagodb_iceberg
OPTIONS (uri :'sqlite_uri', warehouse :'sqlite_warehouse', catalog_name 'regress');
IMPORT FOREIGN SCHEMA fdw_sqlite
FROM SERVER iceberg_sqlite INTO iceberg_fdw_sqlite;
SELECT count(*) = 0 AS initially_empty FROM iceberg_fdw_sqlite.items;

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

DROP FOREIGN TABLE iceberg_fdw_sqlite.items, iceberg_fdw_sqlite.other_items;
DROP SERVER iceberg_sqlite;
DROP SCHEMA iceberg_fdw_sqlite;
