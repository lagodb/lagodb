\i include/column_definitions.sql

-- Text format COPY, foreign-table, inference, and scan-state coverage.

SELECT bucket AS lagodb_regress_bucket
FROM lagodb_regress.object_storage_fixture
\gset

SELECT format('s3://%s/lagodb-connectors/text/exact.txt',
              :'lagodb_regress_bucket') AS exact_path,
       format('s3://%s/lagodb-connectors/text/prefix/',
              :'lagodb_regress_bucket') AS prefix_path,
       format('s3://%s/lagodb-connectors/text/prefix/part-a.txt',
              :'lagodb_regress_bucket') AS part_a_path,
       format('s3://%s/lagodb-connectors/text/prefix/part-b.txt',
              :'lagodb_regress_bucket') AS part_b_path,
       format('s3://%s/lagodb-connectors/text/extra.txt',
              :'lagodb_regress_bucket') AS extra_path,
       format('s3://%s/lagodb-connectors/text/alias.text',
              :'lagodb_regress_bucket') AS alias_path,
       format('s3://%s/lagodb-connectors/text/empty.txt',
              :'lagodb_regress_bucket') AS empty_path,
       format('s3://%s/lagodb-connectors/text/write/',
              :'lagodb_regress_bucket') AS write_path
\gset text_

-- Scalar values, NULLs, escaping, exact COPY FROM, and prefix COPY TO.
COPY lagodb_connectors_regress.common_source
TO :'text_exact_path'
WITH (server 'lagodb_connectors_regress_s3');
COPY (
    SELECT * FROM lagodb_connectors_regress.common_source WHERE id = 1
) TO :'text_part_a_path'
WITH (server 'lagodb_connectors_regress_s3', format 'text');
COPY (
    SELECT * FROM lagodb_connectors_regress.common_source WHERE id = 2
) TO :'text_part_b_path'
WITH (server 'lagodb_connectors_regress_s3', format 'text');

CREATE TABLE lagodb_connectors_regress.text_copy_sink
    (:common_columns);
COPY lagodb_connectors_regress.text_copy_sink
FROM :'text_exact_path'
WITH (server 'lagodb_connectors_regress_s3');

SELECT count(*) AS rows,
       array_agg(to_jsonb(value) ORDER BY to_jsonb(value)) =
           (SELECT array_agg(to_jsonb(source) ORDER BY to_jsonb(source))
            FROM lagodb_connectors_regress.common_source AS source)
           AS round_trip
FROM lagodb_connectors_regress.text_copy_sink AS value;

COPY lagodb_connectors_regress.common_source
TO :'text_alias_path'
WITH (server 'lagodb_connectors_regress_s3');
CREATE TABLE lagodb_connectors_regress.text_alias_sink
    (:common_columns);
COPY lagodb_connectors_regress.text_alias_sink
FROM :'text_alias_path'
WITH (server 'lagodb_connectors_regress_s3');
SELECT count(*) AS alias_rows
FROM lagodb_connectors_regress.text_alias_sink;

-- PostgreSQL text datum semantics include JSON values and arrays.
COPY lagodb_connectors_regress.stream_extra_source
TO :'text_extra_path'
WITH (server 'lagodb_connectors_regress_s3', format 'text');
CREATE TABLE lagodb_connectors_regress.text_extra_sink
    (:stream_extra_columns);
COPY lagodb_connectors_regress.text_extra_sink
FROM :'text_extra_path'
WITH (server 'lagodb_connectors_regress_s3', format 'text');
SELECT count(*) AS extra_rows,
       md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id))
           AS extra_digest
FROM lagodb_connectors_regress.text_extra_sink AS value;

-- An empty exact object is a valid Text object.
COPY (
    SELECT * FROM lagodb_connectors_regress.common_source WHERE false
) TO :'text_empty_path'
WITH (server 'lagodb_connectors_regress_s3', format 'text');
CREATE TABLE lagodb_connectors_regress.text_empty_sink
    (:common_columns);
COPY lagodb_connectors_regress.text_empty_sink
FROM :'text_empty_path'
WITH (server 'lagodb_connectors_regress_s3', format 'text');
SELECT count(*) AS empty_rows
FROM lagodb_connectors_regress.text_empty_sink;

-- Exact and prefix foreign scans plus Text-owned schema inference.
CREATE FOREIGN TABLE lagodb_connectors_regress.text_exact
    (:common_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'text_exact_path');
CREATE FOREIGN TABLE lagodb_connectors_regress.text_prefix
    (:common_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'text_prefix_path', format 'text');
CREATE FOREIGN TABLE lagodb_connectors_regress.text_inferred ()
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'text_exact_path', format 'text');

SELECT relation, rows, matches_source
FROM (
    SELECT 'exact' AS relation, count(*) AS rows,
           array_agg(to_jsonb(value) ORDER BY to_jsonb(value)) =
               (SELECT array_agg(to_jsonb(source) ORDER BY to_jsonb(source))
                FROM lagodb_connectors_regress.common_source AS source)
               AS matches_source
    FROM lagodb_connectors_regress.text_exact AS value
    UNION ALL
    SELECT 'prefix', count(*),
           array_agg(to_jsonb(value) ORDER BY to_jsonb(value)) =
               (SELECT array_agg(to_jsonb(source) ORDER BY to_jsonb(source))
                FROM lagodb_connectors_regress.common_source AS source)
    FROM lagodb_connectors_regress.text_prefix AS value
) AS results
ORDER BY relation;
SELECT count(*) AS inferred_columns,
       string_agg(format_type(atttypid, atttypmod), ', ' ORDER BY attnum)
           AS inferred_types
FROM pg_attribute
WHERE attrelid = 'lagodb_connectors_regress.text_inferred'::regclass
  AND attnum > 0 AND NOT attisdropped;
SELECT count(*) AS inferred_rows
FROM lagodb_connectors_regress.text_inferred;

-- Prefix foreign INSERT uses the Text writer.
CREATE FOREIGN TABLE lagodb_connectors_regress.text_write
    (:common_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'text_write_path', format 'text');
INSERT INTO lagodb_connectors_regress.text_write
SELECT * FROM lagodb_connectors_regress.common_source;
SELECT count(*) AS written_rows,
       array_agg(to_jsonb(value) ORDER BY to_jsonb(value)) =
           (SELECT array_agg(to_jsonb(source) ORDER BY to_jsonb(source))
            FROM lagodb_connectors_regress.common_source AS source)
           AS matches_source
FROM lagodb_connectors_regress.text_write AS value;

-- Text represents the shared Text/CSV DelimitedScanState rescan path.
SET enable_hashjoin = off;
SET enable_mergejoin = off;
SET enable_material = off;
SET enable_nestloop = on;
SELECT outer_rel.id AS outer_id, inner_rel.id AS inner_id
FROM (VALUES (1), (2), (999)) AS outer_rel(id)
LEFT JOIN LATERAL (
    SELECT id
    FROM lagodb_connectors_regress.text_prefix AS inner_rel
    WHERE inner_rel.id = outer_rel.id
    OFFSET 0
) AS inner_rel ON true
ORDER BY outer_rel.id;
RESET enable_hashjoin;
RESET enable_mergejoin;
RESET enable_material;
RESET enable_nestloop;

-- Direct Text COPY FROM accepts an exact object, not a prefix.
\set VERBOSITY sqlstate
COPY lagodb_connectors_regress.text_copy_sink
FROM :'text_prefix_path'
WITH (server 'lagodb_connectors_regress_s3', format 'text');
\set VERBOSITY default
