-- Release the object-storage fixture after object-backed pg_regress tests.
SET client_min_messages = warning;
DROP TABLE IF EXISTS maintenance_remote_drop;
DROP TABLE IF EXISTS maintenance_remote_rollback;
DROP TABLESPACE IF EXISTS regress_object;
DROP TABLE IF EXISTS lakebase_regress.object_storage_fixture;
DROP SCHEMA IF EXISTS lakebase_regress;
RESET client_min_messages;
\! bin/object_storage_fixture teardown
