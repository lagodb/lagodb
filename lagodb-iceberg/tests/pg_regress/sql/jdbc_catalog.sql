-- JDBC v1 discovery and read-only publication over the managed metadata row.
-- Object storage is provisioned by setup.sql; no external query engine runs here.
\setenv PGDATABASE :DBNAME
SELECT endpoint AS jdbc_endpoint, bucket AS jdbc_bucket, region AS jdbc_region,
       access_key_id AS jdbc_access_key, secret_access_key AS jdbc_secret_key
FROM lagodb_regress.object_storage_fixture
\gset

SET client_min_messages = warning;
DROP EXTENSION IF EXISTS lagodb_iceberg CASCADE;
RESET client_min_messages;
CREATE EXTENSION lagodb_iceberg;

\! mkdir -p /tmp/iceberg_regress_jdbc
CREATE TABLESPACE regress_jdbc LOCATION '/tmp/iceberg_regress_jdbc';
SELECT 'regress-jdbc-' || gen_random_uuid() AS jdbc_volume \gset
SELECT lagodb.create_storage_volume(
    :'jdbc_volume', 'regress_jdbc', format('s3://%s', :'jdbc_bucket'),
    jsonb_build_object('type', 's3_access_key',
                       'access_key_id', :'jdbc_access_key',
                       'secret_access_key', :'jdbc_secret_key'),
    jsonb_build_object('region', :'jdbc_region', 'endpoint', :'jdbc_endpoint',
                       'allow_http', true)
) AS jdbc_created_volume \gset
SELECT internal_volume_id AS jdbc_volume_id
FROM lagodb.storage_volumes WHERE storage_volume_name = :'jdbc_volume'
\gset
\setenv LAGODB_REGRESS_VOLUME_ID :jdbc_volume_id
\setenv LAGODB_REGRESS_OBJECT_NAMESPACE :jdbc_bucket
\! bin/wait_for_object_store 30

CREATE SCHEMA "JDBC Catalog";
CREATE TABLE "JDBC Catalog"."Events Mixed" (id integer)
USING iceberg TABLESPACE regress_jdbc;
CREATE TABLE "JDBC Catalog".partitioned (id integer)
PARTITION BY LIST (id) USING iceberg TABLESPACE regress_jdbc;
CREATE TABLE "JDBC Catalog".local_table (id integer) USING iceberg;
CREATE TABLE "JDBC Catalog".heap_table (id integer) USING heap;

-- Empty object-backed tables, including Iceberg partitioned roots, are published.
SELECT table_name, iceberg_type, metadata_location IS NOT NULL AS has_metadata
FROM iceberg_tables
WHERE catalog_name = current_database() AND table_namespace = 'JDBC Catalog'
ORDER BY table_name;

SELECT namespace, property_key, property_value
FROM iceberg_namespace_properties
WHERE catalog_name = current_database() AND namespace = 'JDBC Catalog';

-- The JDBC pointer is the existing catalog pointer, not a second publication.
SELECT metadata_location AS jdbc_initial
FROM iceberg_tables
WHERE table_namespace = 'JDBC Catalog' AND table_name = 'Events Mixed'
\gset
BEGIN;
INSERT INTO "JDBC Catalog"."Events Mixed" VALUES (1);
SELECT metadata_location = :'jdbc_initial' AS pointer_unchanged
FROM iceberg_tables
WHERE table_namespace = 'JDBC Catalog' AND table_name = 'Events Mixed';

COMMIT;
-- JDBC binds string parameters when loading a table by its native PG names.
PREPARE jdbc_load_table (varchar, varchar) AS
SELECT metadata_location <> :'jdbc_initial' AS pointer_advanced,
       previous_metadata_location = :'jdbc_initial' AS previous_matches
FROM iceberg_tables
WHERE table_namespace = $1 AND table_name = $2;
EXECUTE jdbc_load_table('JDBC Catalog', 'Events Mixed');
DEALLOCATE jdbc_load_table;

SELECT metadata_location AS jdbc_committed
FROM iceberg_tables
WHERE table_namespace = 'JDBC Catalog' AND table_name = 'Events Mixed'
\gset
BEGIN;
INSERT INTO "JDBC Catalog"."Events Mixed" VALUES (2);
ROLLBACK;
SELECT metadata_location = :'jdbc_committed' AS rollback_preserves_pointer
FROM iceberg_tables
WHERE table_namespace = 'JDBC Catalog' AND table_name = 'Events Mixed';

-- Table SELECT alone does not expose a table or namespace without schema USAGE.
CREATE ROLE regress_jdbc_reader;
GRANT SELECT ON pg_catalog.iceberg_tables,
                pg_catalog.iceberg_namespace_properties TO regress_jdbc_reader;
GRANT SELECT ON "JDBC Catalog"."Events Mixed" TO regress_jdbc_reader;
SET ROLE regress_jdbc_reader;
SELECT count(*) AS visible_tables
FROM iceberg_tables WHERE table_namespace = 'JDBC Catalog';

SELECT count(*) AS visible_namespaces
FROM iceberg_namespace_properties WHERE namespace = 'JDBC Catalog';

RESET ROLE;
-- With schema USAGE, only the table granted SELECT is exposed.
GRANT USAGE ON SCHEMA "JDBC Catalog" TO regress_jdbc_reader;
SET ROLE regress_jdbc_reader;
SELECT table_name FROM iceberg_tables
WHERE table_namespace = 'JDBC Catalog';

SELECT namespace FROM iceberg_namespace_properties
WHERE namespace = 'JDBC Catalog';

SELECT has_table_privilege('pg_catalog.iceberg_tables', 'INSERT,UPDATE,DELETE')
           AS catalog_writable;

RESET ROLE;
-- Even the view owner cannot mutate this projection.
\set VERBOSITY default
UPDATE iceberg_tables SET metadata_location = 's3://invalid/metadata.json'
WHERE table_namespace = 'JDBC Catalog' AND table_name = 'Events Mixed';
\set VERBOSITY terse

-- PostgreSQL renames change discovery names without changing the pointer.
ALTER TABLE "JDBC Catalog"."Events Mixed" RENAME TO renamed;
ALTER SCHEMA "JDBC Catalog" RENAME TO jdbc_renamed;
SELECT metadata_location = :'jdbc_committed' AS rename_preserves_pointer
FROM iceberg_tables
WHERE table_namespace = 'jdbc_renamed' AND table_name = 'renamed';

SELECT count(*) AS old_namespace_entries
FROM iceberg_namespace_properties WHERE namespace = 'JDBC Catalog';

DROP TABLE jdbc_renamed.renamed;
DROP TABLE jdbc_renamed.partitioned;
SELECT count(*) AS remaining_catalog_tables
FROM iceberg_tables WHERE table_namespace = 'jdbc_renamed';

SELECT count(*) AS remaining_catalog_namespaces
FROM iceberg_namespace_properties WHERE namespace = 'jdbc_renamed';

DROP TABLE jdbc_renamed.local_table;
DROP TABLE jdbc_renamed.heap_table;
DROP SCHEMA jdbc_renamed;
DROP OWNED BY regress_jdbc_reader;
DROP ROLE regress_jdbc_reader;
DROP TABLESPACE regress_jdbc;
