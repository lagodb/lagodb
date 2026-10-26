-- Spark/LagoDB interoperability for format-v2 position deletes.
\setenv PGDATABASE :DBNAME
SELECT rest_uri
FROM lagodb_regress.object_storage_fixture
\gset

SET client_min_messages = warning;
DROP EXTENSION IF EXISTS lagodb_iceberg CASCADE;
RESET client_min_messages;
CREATE EXTENSION lagodb_iceberg;

\set object_tablespace regress_spark_v2
\i include/object_tablespace.sql

CREATE SCHEMA spark_interop_v2;
CREATE TABLE spark_interop_v2.mutations (
    id integer,
    region text,
    payload text
) USING iceberg WITH ("format-version" = 2) TABLESPACE :"object_tablespace";
INSERT INTO spark_interop_v2.mutations VALUES
    (1, 'east', 'one'), (2, 'west', 'two'), (3, 'east', 'three'),
    (4, 'west', 'four'), (5, 'east', 'five');

-- The JDBC catalog must retain PostgreSQL transaction visibility. A separate
-- Spark connection sees the committed metadata pointer until COMMIT.
BEGIN;
UPDATE spark_interop_v2.mutations SET payload = 'two-updated' WHERE id = 2;
DELETE FROM spark_interop_v2.mutations WHERE id = 3;

\set spark_run_schema spark_interop_v2
\set spark_run_step uncommitted
\i include/spark_run.sql

COMMIT;

\set spark_run_step updated
\i include/spark_run.sql

CREATE TABLE spark_interop_v2.partitioned_mutations (
    id integer,
    region text,
    payload text
) PARTITION BY LIST (region) USING iceberg
WITH ("format-version" = 2) TABLESPACE :"object_tablespace";
INSERT INTO spark_interop_v2.partitioned_mutations VALUES
    (1, 'east', 'one'), (2, 'west', 'two'),
    (3, 'east', 'three'), (4, 'west', 'four');
UPDATE spark_interop_v2.partitioned_mutations
SET payload = 'two-updated' WHERE id = 2;
DELETE FROM spark_interop_v2.partitioned_mutations WHERE id = 3;

-- Both engines add position deletes for the same Spark data file. The final
-- Spark read must apply their combined state.
\set spark_run_step external_seed
\i include/spark_run.sql

CREATE SERVER spark_v2_rest
TYPE 'rest'
FOREIGN DATA WRAPPER lagodb_iceberg
OPTIONS (uri :'rest_uri');
CREATE USER MAPPING FOR CURRENT_USER SERVER spark_v2_rest;
CREATE FOREIGN TABLE spark_interop_v2.external (id integer, payload text)
SERVER spark_v2_rest OPTIONS (table 'spark_interop_v2.external');
SELECT * FROM spark_interop_v2.external ORDER BY id;
UPDATE spark_interop_v2.external
SET payload = 'one-lagodb' WHERE id = 1;
DELETE FROM spark_interop_v2.external WHERE id = 4;
INSERT INTO spark_interop_v2.external VALUES (5, 'five-lagodb');

\set spark_run_step final
\i include/spark_run.sql

SET client_min_messages = warning;
DROP SCHEMA spark_interop_v2 CASCADE;
DROP SERVER spark_v2_rest CASCADE;
DROP TABLESPACE :"object_tablespace";
RESET client_min_messages;
\! rm -rf "$LAGODB_REGRESS_TABLESPACE_PATH"/*
