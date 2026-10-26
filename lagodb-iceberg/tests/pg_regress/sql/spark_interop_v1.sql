-- Spark/LagoDB interoperability for format-v1 managed and external tables.
\setenv PGDATABASE :DBNAME
SELECT rest_uri
FROM lagodb_regress.object_storage_fixture
\gset

SET client_min_messages = warning;
DROP EXTENSION IF EXISTS lagodb_iceberg CASCADE;
RESET client_min_messages;
CREATE EXTENSION lagodb_iceberg;

\set object_tablespace regress_spark_v1
\i include/object_tablespace.sql

CREATE SCHEMA spark_interop_v1;
CREATE TYPE spark_interop_v1.details AS (score integer, label text);
CREATE TABLE spark_interop_v1.all_types (
    id integer,
    b boolean,
    si smallint,
    i integer,
    bi bigint,
    r real,
    dp double precision,
    num numeric(12, 4),
    t text,
    vc varchar(20),
    c char(10),
    n name,
    j json,
    jb jsonb,
    d date,
    ts timestamp,
    tstz timestamptz,
    u uuid,
    by bytea,
    details spark_interop_v1.details,
    numbers integer[]
) USING iceberg WITH ("format-version" = 1) TABLESPACE :"object_tablespace";
INSERT INTO spark_interop_v1.all_types VALUES (
    1, true, 32767, 2147483647, 9223372036854775807,
    3.5, 2.25, 12345678.1250,
    'text value', 'varchar value', 'char', 'name_value',
    '{"json_key":"value"}', '{"jsonb_key":"value"}',
    DATE '2024-01-02', TIMESTAMP '2024-01-02 03:04:05.123456',
    TIMESTAMPTZ '2024-01-02 03:04:05.123456+00',
    'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '\xdeadbeef',
    ROW(7, 'nested'), ARRAY[1, 2, 3]
);
INSERT INTO spark_interop_v1.all_types (id) VALUES (2);

CREATE TABLE spark_interop_v1.list_partitioned (
    id integer,
    region text
) PARTITION BY LIST (region) USING iceberg
WITH ("format-version" = 1) TABLESPACE :"object_tablespace";
INSERT INTO spark_interop_v1.list_partitioned VALUES
    (1, 'east'), (2, 'west'), (3, 'east');

CREATE TABLE spark_interop_v1.range_partitioned (
    id integer,
    event_date date
) PARTITION BY RANGE (event_date) USING iceberg
WITH ("format-version" = 1) TABLESPACE :"object_tablespace";
INSERT INTO spark_interop_v1.range_partitioned VALUES
    (1, DATE '2024-01-02'), (2, DATE '2024-01-03'), (3, DATE '2024-01-02');

-- Spark owns the initial external-table state; LagoDB must see Spark's
-- copy-on-write changes, expose its own append back to Spark, and reject
-- UPDATE for format v1.
\set spark_run_schema spark_interop_v1
\set spark_run_step external_seed
\i include/spark_run.sql

CREATE SERVER spark_v1_rest
TYPE 'rest'
FOREIGN DATA WRAPPER lagodb_iceberg
OPTIONS (uri :'rest_uri');
CREATE USER MAPPING FOR CURRENT_USER SERVER spark_v1_rest;
CREATE FOREIGN TABLE spark_interop_v1.external (id integer, payload text)
SERVER spark_v1_rest OPTIONS (table 'spark_interop_v1.external');
SELECT * FROM spark_interop_v1.external ORDER BY id;
INSERT INTO spark_interop_v1.external VALUES (5, 'five-lagodb');
UPDATE spark_interop_v1.external
SET payload = 'one-lagodb' WHERE id = 1;

\set spark_run_step final
\i include/spark_run.sql

SET client_min_messages = warning;
DROP SCHEMA spark_interop_v1 CASCADE;
DROP SERVER spark_v1_rest CASCADE;
DROP TABLESPACE :"object_tablespace";
RESET client_min_messages;
\! rm -rf "$LAGODB_REGRESS_TABLESPACE_PATH"/*
