\i include/column_definitions.sql

-- Shared LagoDB connector regression fixture.
--
-- Keep only infrastructure and relational source data here. Each format test
-- owns its object-storage fixtures so failures are attributed to that format
-- and the individual tests remain safe to run in any order after setup.

\setenv PGDATABASE :DBNAME
\! python3 ../../../scripts/pg_regress/regress_fixture.py setup
\i ../../../scripts/pg_regress/fixture_command_result.sql

CREATE EXTENSION lagodb_base;
CREATE EXTENSION lagodb_connectors;
\! python3 ../../../scripts/pg_regress/regress_fixture.py wait-storage
\i ../../../scripts/pg_regress/fixture_command_result.sql

SELECT endpoint AS lagodb_regress_endpoint,
       bucket AS lagodb_regress_bucket,
       format('s3://%s/', bucket) AS lagodb_regress_scope,
       region AS lagodb_regress_region,
       access_key_id AS lagodb_regress_access_key_id,
       secret_access_key AS lagodb_regress_secret_access_key
FROM lagodb_regress.object_storage_fixture
\gset

CREATE SCHEMA lagodb_connectors_regress;

CREATE SERVER lagodb_connectors_regress_s3
    FOREIGN DATA WRAPPER lagodb_connectors
    OPTIONS (
        provider 's3_compatible',
        endpoint :'lagodb_regress_endpoint',
        region :'lagodb_regress_region',
        scope :'lagodb_regress_scope',
        allow_http 'true',
        virtual_hosted_style_request 'false'
    );

CREATE USER MAPPING FOR PUBLIC
    SERVER lagodb_connectors_regress_s3
    OPTIONS (
        access_key_id :'lagodb_regress_access_key_id',
        secret_access_key :'lagodb_regress_secret_access_key'
    );

CREATE TABLE lagodb_connectors_regress.common_source (
    id integer,
    bool_col boolean,
    smallint_col smallint,
    integer_col integer,
    bigint_col bigint,
    real_col real,
    double_col double precision,
    numeric_col numeric(12, 3),
    text_col text,
    varchar_col varchar(20),
    char_col character(5),
    name_col name,
    bytea_col bytea,
    uuid_col uuid,
    date_col date,
    time_col time without time zone,
    timestamp_col timestamp without time zone,
    timestamptz_col timestamp with time zone
);

INSERT INTO lagodb_connectors_regress.common_source
VALUES
    (
        1,
        true,
        7,
        11,
        9000000000,
        1.25::real,
        2.5::double precision,
        12345.678,
        E'comma,value "quoted"\nline',
        'varchar-one',
        'abc',
        'name-one',
        decode('000102ff', 'hex'),
        '00000000-0000-0000-0000-000000000001',
        '2024-01-02',
        '03:04:05.123456',
        '2024-01-02 03:04:05.123456',
        '2024-01-02 03:04:05.123456+00'
    ),
    (
        2,
        false,
        NULL,
        -12,
        -9000000000,
        NULL,
        -2.5::double precision,
        -123.450,
        NULL,
        '',
        'xy',
        NULL,
        NULL,
        '00000000-0000-0000-0000-000000000002',
        NULL,
        '23:59:59',
        NULL,
        '2024-06-07 08:09:10+00'
    );

CREATE TABLE lagodb_connectors_regress.stream_extra_source (
    id integer,
    json_col json,
    jsonb_col jsonb,
    bool_array boolean[],
    int_array integer[],
    text_array text[]
);

INSERT INTO lagodb_connectors_regress.stream_extra_source
VALUES
    (
        1,
        '{"nested":[1,true],"text":"value"}',
        '{"nested":[1,true],"text":"value"}',
        ARRAY[true, false],
        ARRAY[1, 2, 3],
        ARRAY['a', 'comma,value', 'quote"value']
    ),
    (
        2,
        NULL,
        NULL,
        ARRAY[]::boolean[],
        NULL,
        ARRAY['']
    );

CREATE TABLE lagodb_connectors_regress.json_source
    (:common_columns);
ALTER TABLE lagodb_connectors_regress.json_source
    ADD COLUMN json_col json,
    ADD COLUMN jsonb_col jsonb;

INSERT INTO lagodb_connectors_regress.json_source
SELECT source.*,
       CASE WHEN source.id = 1
            THEN '{"nested":[1,true],"text":"value"}'::json
            ELSE NULL
       END,
       CASE WHEN source.id = 1
            THEN '{"nested":[1,true],"text":"value"}'::jsonb
            ELSE NULL
       END
FROM lagodb_connectors_regress.common_source AS source
ORDER BY source.id;

CREATE TABLE lagodb_connectors_regress.parquet_source
    (:common_columns);
ALTER TABLE lagodb_connectors_regress.parquet_source
    ADD COLUMN json_col json,
    ADD COLUMN bool_array boolean[],
    ADD COLUMN smallint_array smallint[],
    ADD COLUMN integer_array integer[],
    ADD COLUMN bigint_array bigint[],
    ADD COLUMN real_array real[],
    ADD COLUMN double_array double precision[],
    ADD COLUMN text_array text[],
    ADD COLUMN varchar_array varchar(20)[],
    ADD COLUMN bpchar_array character(5)[],
    ADD COLUMN name_array name[],
    ADD COLUMN json_array json[];

INSERT INTO lagodb_connectors_regress.parquet_source
SELECT source.*,
       CASE WHEN source.id = 1
            THEN '{"nested":[1,true],"text":"value"}'::json
            ELSE NULL
       END,
       CASE WHEN source.id = 1
            THEN ARRAY[true, false]
            ELSE ARRAY[]::boolean[]
       END,
       CASE WHEN source.id = 1
            THEN ARRAY[7::smallint, -2::smallint]
            ELSE NULL
       END,
       CASE WHEN source.id = 1
            THEN ARRAY[11, -12]
            ELSE ARRAY[]::integer[]
       END,
       CASE WHEN source.id = 1
            THEN ARRAY[9000000000::bigint, -9000000000::bigint]
            ELSE NULL
       END,
       CASE WHEN source.id = 1
            THEN ARRAY[1.25::real, -2.5::real]
            ELSE ARRAY[]::real[]
       END,
       CASE WHEN source.id = 1
            THEN ARRAY[2.5::double precision, -2.5::double precision]
            ELSE NULL
       END,
       CASE WHEN source.id = 1
            THEN ARRAY['a', 'comma,value']
            ELSE ARRAY[]::text[]
       END,
       CASE WHEN source.id = 1
            THEN ARRAY['varchar-one'::varchar(20), 'two'::varchar(20)]
            ELSE NULL
       END,
       CASE WHEN source.id = 1
            THEN ARRAY['abc'::character(5), 'xy'::character(5)]
            ELSE ARRAY[]::character(5)[]
       END,
       CASE WHEN source.id = 1
            THEN ARRAY['name-one'::name, 'name-two'::name]
            ELSE NULL
       END,
       CASE WHEN source.id = 1
            THEN ARRAY['{"a":1}'::json, '{"b":2}'::json]
            ELSE ARRAY[]::json[]
       END
FROM lagodb_connectors_regress.common_source AS source
ORDER BY source.id;
