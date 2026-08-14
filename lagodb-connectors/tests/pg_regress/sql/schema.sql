-- Foreign-table schema inference and option-boundary coverage.

SET TIME ZONE 'UTC';
SET client_min_messages = warning;

SELECT bucket AS lakebase_regress_bucket
FROM lakebase_regress.object_storage_fixture
\gset

SELECT format('s3://%s/lagodb-connectors/seed/common-exact.txt',
              :'lakebase_regress_bucket') AS schema_text_exact,
       format('s3://%s/lagodb-connectors/seed/common-prefix/',
              :'lakebase_regress_bucket') AS schema_text_prefix,
       format('s3://%s/lagodb-connectors/seed/common-exact.csv',
              :'lakebase_regress_bucket') AS schema_csv_exact,
       format('s3://%s/lagodb-connectors/seed/common-prefix-csv/',
              :'lakebase_regress_bucket') AS schema_csv_prefix,
       format('s3://%s/lagodb-connectors/seed/common-header.csv',
              :'lakebase_regress_bucket') AS schema_csv_header,
       format('s3://%s/lagodb-connectors/seed/json-exact.json',
              :'lakebase_regress_bucket') AS schema_json_exact,
       format('s3://%s/lagodb-connectors/seed/json-prefix/',
              :'lakebase_regress_bucket') AS schema_json_prefix,
       format('s3://%s/lagodb-connectors/seed/json-compressed.json.gz',
              :'lakebase_regress_bucket') AS schema_json_compressed,
       format('s3://%s/lagodb-connectors/seed/common-exact.avro',
              :'lakebase_regress_bucket') AS schema_avro_exact,
       format('s3://%s/lagodb-connectors/seed/common-prefix-avro/',
              :'lakebase_regress_bucket') AS schema_avro_prefix,
       format('s3://%s/lagodb-connectors/seed/parquet-exact.parquet',
              :'lakebase_regress_bucket') AS schema_parquet_exact,
       format('s3://%s/lagodb-connectors/seed/parquet-prefix/',
              :'lakebase_regress_bucket') AS schema_parquet_prefix,
       format('s3://%s/lagodb-connectors/seed/parquet-exact.parquet.gz',
              :'lakebase_regress_bucket') AS invalid_parquet_path
\gset

-- An empty column list invokes the connector DDL hook. Text/CSV infer scalar
-- COPY types, while JSON, Avro, and Parquet infer their native schemas.
CREATE FOREIGN TABLE lagodb_connectors_regress.inferred_text_exact ()
SERVER lagodb_connectors_regress_s3
OPTIONS (
    path :'schema_text_exact'
);

CREATE FOREIGN TABLE lagodb_connectors_regress.inferred_text_prefix ()
SERVER lagodb_connectors_regress_s3
OPTIONS (
    path :'schema_text_prefix',
    format 'text'
);

CREATE FOREIGN TABLE lagodb_connectors_regress.inferred_csv_exact ()
SERVER lagodb_connectors_regress_s3
OPTIONS (
    path :'schema_csv_exact'
);

CREATE FOREIGN TABLE lagodb_connectors_regress.inferred_csv_prefix ()
SERVER lagodb_connectors_regress_s3
OPTIONS (
    path :'schema_csv_prefix',
    format 'csv'
);

CREATE FOREIGN TABLE lagodb_connectors_regress.inferred_csv_header ()
SERVER lagodb_connectors_regress_s3
OPTIONS (
    path :'schema_csv_header',
    format 'csv',
    header 'match'
);

CREATE FOREIGN TABLE lagodb_connectors_regress.inferred_json_exact ()
SERVER lagodb_connectors_regress_s3
OPTIONS (
    path :'schema_json_exact'
);

CREATE FOREIGN TABLE lagodb_connectors_regress.inferred_json_prefix ()
SERVER lagodb_connectors_regress_s3
OPTIONS (
    path :'schema_json_prefix',
    format 'json'
);

CREATE FOREIGN TABLE lagodb_connectors_regress.inferred_json_compressed ()
SERVER lagodb_connectors_regress_s3
OPTIONS (
    path :'schema_json_compressed'
);

CREATE FOREIGN TABLE lagodb_connectors_regress.inferred_avro_exact ()
SERVER lagodb_connectors_regress_s3
OPTIONS (
    path :'schema_avro_exact'
);

CREATE FOREIGN TABLE lagodb_connectors_regress.inferred_avro_prefix ()
SERVER lagodb_connectors_regress_s3
OPTIONS (
    path :'schema_avro_prefix',
    format 'avro'
);

CREATE FOREIGN TABLE lagodb_connectors_regress.inferred_parquet_exact ()
SERVER lagodb_connectors_regress_s3
OPTIONS (
    path :'schema_parquet_exact'
);

CREATE FOREIGN TABLE lagodb_connectors_regress.inferred_parquet_prefix ()
SERVER lagodb_connectors_regress_s3
OPTIONS (
    path :'schema_parquet_prefix',
    format 'parquet'
);

SELECT table_name, column_count, column_types
FROM (
    SELECT c.relname AS table_name,
           count(a.attname)::integer AS column_count,
           string_agg(
               format_type(a.atttypid, a.atttypmod),
               ', ' ORDER BY a.attnum
           ) AS column_types
    FROM pg_catalog.pg_class AS c
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
    JOIN pg_catalog.pg_attribute AS a ON a.attrelid = c.oid
    WHERE n.nspname = 'lagodb_connectors_regress'
      AND c.relname LIKE 'inferred_%'
      AND a.attnum > 0
      AND NOT a.attisdropped
    GROUP BY c.relname
) AS inferred
ORDER BY table_name;

SELECT table_name, rows
FROM (
    SELECT 'text-exact' AS table_name, count(*) AS rows
    FROM lagodb_connectors_regress.inferred_text_exact
    UNION ALL
    SELECT 'text-prefix', count(*)
    FROM lagodb_connectors_regress.inferred_text_prefix
    UNION ALL
    SELECT 'csv-exact', count(*)
    FROM lagodb_connectors_regress.inferred_csv_exact
    UNION ALL
    SELECT 'csv-prefix', count(*)
    FROM lagodb_connectors_regress.inferred_csv_prefix
    UNION ALL
    SELECT 'csv-header', count(*)
    FROM lagodb_connectors_regress.inferred_csv_header
    UNION ALL
    SELECT 'json-exact', count(*)
    FROM lagodb_connectors_regress.inferred_json_exact
    UNION ALL
    SELECT 'json-prefix', count(*)
    FROM lagodb_connectors_regress.inferred_json_prefix
    UNION ALL
    SELECT 'json-compressed', count(*)
    FROM lagodb_connectors_regress.inferred_json_compressed
    UNION ALL
    SELECT 'avro-exact', count(*)
    FROM lagodb_connectors_regress.inferred_avro_exact
    UNION ALL
    SELECT 'avro-prefix', count(*)
    FROM lagodb_connectors_regress.inferred_avro_prefix
    UNION ALL
    SELECT 'parquet-exact', count(*)
    FROM lagodb_connectors_regress.inferred_parquet_exact
    UNION ALL
    SELECT 'parquet-prefix', count(*)
    FROM lagodb_connectors_regress.inferred_parquet_prefix
) AS counts
ORDER BY table_name;

-- CSV relation options and column-level force flags are valid only for CSV.
CREATE FOREIGN TABLE lagodb_connectors_regress.foreign_csv_options (
    id integer OPTIONS (force_not_null 'true'),
    bool_col boolean,
    smallint_col smallint,
    integer_col integer,
    bigint_col bigint,
    real_col real,
    double_col double precision,
    numeric_col numeric(12, 3),
    text_col text OPTIONS (force_null 'true'),
    varchar_col varchar(20),
    char_col character(5),
    name_col name,
    bytea_col bytea,
    uuid_col uuid,
    date_col date,
    time_col time without time zone,
    timestamp_col timestamp without time zone,
    timestamptz_col timestamp with time zone
)
SERVER lagodb_connectors_regress_s3
OPTIONS (
    path :'schema_csv_exact',
    format 'csv',
    header 'false',
    delimiter ',',
    quote '"',
    escape '"'
);

SELECT count(*) AS csv_option_rows
FROM lagodb_connectors_regress.foreign_csv_options;

-- These cold-path errors protect format-specific option ownership and object
-- location classification. The commands are intentionally expected errors.
\set VERBOSITY sqlstate
CREATE FOREIGN TABLE lagodb_connectors_regress.invalid_parquet_compression ()
SERVER lagodb_connectors_regress_s3
OPTIONS (
    path :'invalid_parquet_path',
    format 'parquet'
);

CREATE FOREIGN TABLE lagodb_connectors_regress.invalid_avro_column_option (
    id integer OPTIONS (force_null 'true')
)
SERVER lagodb_connectors_regress_s3
OPTIONS (
    path :'schema_avro_exact',
    format 'avro'
);

\set VERBOSITY default
RESET client_min_messages;
