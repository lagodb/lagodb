-- Release the database objects and the shared MinIO fixture.

SET client_min_messages = warning;
DROP SCHEMA IF EXISTS lagodb_connectors_regress CASCADE;
DROP SERVER IF EXISTS lagodb_connectors_regress_scope CASCADE;
DROP SERVER IF EXISTS lagodb_connectors_regress_missing_mapping CASCADE;
DROP SERVER IF EXISTS lagodb_connectors_regress_s3 CASCADE;
DROP EXTENSION IF EXISTS lagodb_connectors CASCADE;
DROP EXTENSION IF EXISTS lagodb_base CASCADE;
DROP TABLE IF EXISTS lagodb_regress.object_storage_fixture;
DROP SCHEMA IF EXISTS lagodb_regress;
RESET client_min_messages;

\setenv PGDATABASE :DBNAME
\! python3 ../../../scripts/pg_regress/regress_fixture.py teardown
\set ECHO none
\i ../../../scripts/pg_regress/fixture_command_result.sql
\set ECHO all
