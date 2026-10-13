-- Parquet declared types, checked COPY input and recursive schema binding.
-- This file owns its objects and can run independently after setup.sql.
SELECT bucket AS lagodb_regress_bucket
FROM lagodb_regress.object_storage_fixture
\gset
\set parquet_base 's3://' :lagodb_regress_bucket '/lagodb-connectors/parquet-schema/'
\set parquet_exact_path :parquet_base 'scalars.parquet'
COPY (
    SELECT numeric_col, numeric_array
    FROM lagodb_connectors_regress.parquet_source
) TO :'parquet_exact_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');

-- Declared typmods are producer guarantees, not per-value read conversions.
\set declared_path :parquet_base 'declared/'
\set declared_copy_path :parquet_base 'declared-copy.parquet'
CREATE TYPE lagodb_connectors_regress.parquet_declared_value AS (
    label varchar(20), code char(5), observed timestamp(2), labels varchar(20)[]
);
CREATE FOREIGN TABLE lagodb_connectors_regress.parquet_declared (
    id integer, label varchar(20), code char(5), clock time(2),
    zoned timestamptz(2), clocks time(2)[],
    record lagodb_connectors_regress.parquet_declared_value,
    items lagodb_connectors_regress.parquet_declared_value[]
)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'declared_path', format 'parquet');
CREATE TEMP TABLE parquet_declared_expected
    (LIKE lagodb_connectors_regress.parquet_declared);
INSERT INTO parquet_declared_expected VALUES
    (1, repeat('界', 20), '中', '12:34:56.126', '2024-01-02 12:34:56.126+00',
     ARRAY['12:34:56.126'::time, NULL],
     ROW(repeat('界', 20), '中', '2024-01-02 12:34:56.126'::timestamp,
         ARRAY[repeat('界', 20), NULL]),
     ARRAY[ROW(repeat('界', 20), '中', '2024-01-02 12:34:56.126'::timestamp,
               ARRAY[repeat('界', 20), NULL])::lagodb_connectors_regress.parquet_declared_value,
           NULL]),
    (2, NULL, NULL, NULL, NULL, ARRAY[]::time[],
     ROW(NULL, NULL, NULL, ARRAY[]::varchar[]),
     ARRAY[]::lagodb_connectors_regress.parquet_declared_value[]),
    (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
INSERT INTO lagodb_connectors_regress.parquet_declared
SELECT * FROM parquet_declared_expected;
SELECT count(*) AS rows,
       bool_and(to_jsonb(actual) IS NOT DISTINCT FROM to_jsonb(expected)) AS matches
FROM lagodb_connectors_regress.parquet_declared AS actual
FULL JOIN parquet_declared_expected AS expected USING (id);
SELECT char_length(label) = 20 AND octet_length(label) = 60 AS multibyte,
       octet_length(code) = 7 AND octet_length((record).code) = 7 AS padding
FROM lagodb_connectors_regress.parquet_declared WHERE id = 1;
ANALYZE lagodb_connectors_regress.parquet_declared;

-- COPY TO preserves the same declarations; the foreign reader restores them.
COPY parquet_declared_expected TO :'declared_copy_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');
ALTER FOREIGN TABLE lagodb_connectors_regress.parquet_declared
OPTIONS (SET path :'declared_copy_path');
SELECT count(*) AS rows,
       bool_and(to_jsonb(actual) IS NOT DISTINCT FROM to_jsonb(expected)) AS matches
FROM lagodb_connectors_regress.parquet_declared AS actual
FULL JOIN parquet_declared_expected AS expected USING (id);

-- COPY scalar/array targets still perform assignment rather than trusting lengths.
CREATE TEMP TABLE parquet_declared_copy (
    label varchar(20), code char(5), clock time(2),
    zoned timestamptz(2), clocks time(2)[]
);
COPY parquet_declared_copy FROM :'declared_copy_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');
(SELECT * FROM parquet_declared_copy
 EXCEPT ALL SELECT label, code, clock, zoned, clocks FROM parquet_declared_expected)
UNION ALL
(SELECT label, code, clock, zoned, clocks FROM parquet_declared_expected
 EXCEPT ALL SELECT * FROM parquet_declared_copy);
CREATE TEMP TABLE parquet_short_copy (label varchar(3));
-- Unlike declared scans, COPY cannot assign typmods inside composite arrays.
CREATE TEMP TABLE parquet_declared_member_copy (
    items lagodb_connectors_regress.parquet_declared_value[]
);
\set VERBOSITY sqlstate
COPY parquet_short_copy FROM :'declared_copy_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');
COPY parquet_declared_member_copy FROM :'declared_copy_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');
\set VERBOSITY default

-- Domain admission is recursive and independent of the values in the file.
CREATE DOMAIN lagodb_connectors_regress.parquet_positive AS numeric
    CHECK (VALUE > 0) NOT NULL;
CREATE TEMP TABLE parquet_domain_sink (
    numeric_col lagodb_connectors_regress.parquet_positive
);
CREATE FOREIGN TABLE lagodb_connectors_regress.parquet_read_limits (
    numeric_array lagodb_connectors_regress.parquet_positive[]
)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'parquet_exact_path', format 'parquet');
\set VERBOSITY sqlstate
SET client_min_messages = notice;
COPY parquet_domain_sink FROM :'parquet_exact_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet', on_error ignore);
RESET client_min_messages;
SELECT numeric_array FROM lagodb_connectors_regress.parquet_read_limits;
\set VERBOSITY default

-- COPY validates ordinary Parquet strings without the foreign-table producer contract.
\set json_input_path :parquet_base 'json-input.parquet'
CREATE TYPE lagodb_connectors_regress.parquet_json_text AS (payload text, items text[]);
CREATE TYPE lagodb_connectors_regress.parquet_json_value AS (payload json, items json[]);
CREATE TEMP TABLE parquet_json_source (
    id integer, payload text, items text[],
    nested lagodb_connectors_regress.parquet_json_text
);
INSERT INTO parquet_json_source VALUES
    (1, '{"ok":true}', ARRAY['null', NULL], ROW('1', ARRAY['[]', NULL])),
    (2, 'not-json', ARRAY['null', NULL], ROW('1', ARRAY['[]', NULL])),
    (3, '{"ok":true}', ARRAY['null', 'not-json'], ROW('1', ARRAY['[]', NULL])),
    (4, '{"ok":true}', ARRAY['null', NULL], ROW('not-json', ARRAY['[]', NULL])),
    (5, '{"ok":true}', ARRAY['null', NULL], ROW('1', ARRAY['[]', 'not-json'])),
    (6, '{"ok":true}', ARRAY['null', NULL], ROW('1', ARRAY['[]', NULL])),
    (7, NULL, ARRAY[]::text[], ROW(NULL, ARRAY[]::text[])),
    (8, NULL, NULL, NULL);
COPY (SELECT * FROM parquet_json_source ORDER BY id) TO :'json_input_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');

CREATE TEMP TABLE parquet_json_sink (
    id integer, payload json, items json[],
    nested lagodb_connectors_regress.parquet_json_value
);
\set VERBOSITY sqlstate
COPY parquet_json_sink FROM :'json_input_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');
\set VERBOSITY default
SELECT count(*) AS rows_after_error FROM parquet_json_sink;

-- Four bad rows exercise scalar, array and recursive JSON conversion.
-- Valid rows after the failures also check reuse of partially filled buffers.
SET client_min_messages = notice;
COPY parquet_json_sink FROM :'json_input_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet', on_error ignore);
RESET client_min_messages;
SELECT coalesce(actual.id, expected.id) AS id,
       actual.id IS NOT DISTINCT FROM expected.id
       AND actual.payload::text IS NOT DISTINCT FROM expected.payload
       AND actual.items::text[] IS NOT DISTINCT FROM expected.items
       AND ((actual.nested)::text IS NULL) = ((expected.nested)::text IS NULL)
       AND (actual.nested).payload::text IS NOT DISTINCT FROM (expected.nested).payload
       AND (actual.nested).items::text[] IS NOT DISTINCT FROM (expected.nested).items AS matches
FROM parquet_json_sink AS actual
FULL JOIN (SELECT * FROM parquet_json_source WHERE id IN (1, 6, 7, 8)) AS expected
USING (id)
ORDER BY id;

-- JSONB uses the same semantic input contract, also inside arrays/composites.
CREATE TYPE lagodb_connectors_regress.parquet_jsonb_value AS (payload jsonb, items jsonb[]);
CREATE TEMP TABLE parquet_jsonb_sink (
    id integer, payload jsonb, items jsonb[],
    nested lagodb_connectors_regress.parquet_jsonb_value
);
SET client_min_messages = notice;
COPY parquet_jsonb_sink FROM :'json_input_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet', on_error ignore);
RESET client_min_messages;
SELECT coalesce(actual.id, expected.id) AS id,
       to_jsonb(actual) IS NOT DISTINCT FROM to_jsonb(expected) AS matches
FROM parquet_jsonb_sink AS actual
FULL JOIN parquet_json_sink AS expected USING (id)
ORDER BY id;

-- Named PG types determine nested storage, including dropped attribute positions.
\set nested_path :parquet_base 'nested/'
\set nested_copy_path :parquet_base 'nested-copy.parquet'
\set nested_batch_path :parquet_base 'nested-batch.parquet'

CREATE TYPE lagodb_connectors_regress.parquet_leaf AS (
    removed text, amount numeric(12, 2), tags text[], document jsonb
);
ALTER TYPE lagodb_connectors_regress.parquet_leaf DROP ATTRIBUTE removed;
CREATE TYPE lagodb_connectors_regress.parquet_branch AS (
    leaf lagodb_connectors_regress.parquet_leaf,
    leaves lagodb_connectors_regress.parquet_leaf[]
);
CREATE TEMP TABLE parquet_nested_expected (
    id integer, record lagodb_connectors_regress.parquet_branch,
    items lagodb_connectors_regress.parquet_leaf[], document jsonb, documents jsonb[]
);
INSERT INTO parquet_nested_expected VALUES
    (1, ROW(ROW(12.35, ARRAY['x', NULL], '{"ok":true}'),
            ARRAY[ROW(42, ARRAY[]::text[], 'null')::lagodb_connectors_regress.parquet_leaf, NULL]),
     ARRAY[ROW(12.35, ARRAY['x', NULL], '{"ok":true}')::lagodb_connectors_regress.parquet_leaf, NULL],
     '{"nested":[1,null,[]]}', ARRAY['null'::jsonb, NULL, '{"ok":true}'::jsonb]),
    (2, ROW(ROW(NULL, NULL, NULL), ARRAY[]::lagodb_connectors_regress.parquet_leaf[]),
     ARRAY[]::lagodb_connectors_regress.parquet_leaf[], 'null', ARRAY[]::jsonb[]);
INSERT INTO parquet_nested_expected (id) VALUES (3);

CREATE FOREIGN TABLE lagodb_connectors_regress.parquet_nested (
    id integer, record lagodb_connectors_regress.parquet_branch,
    items lagodb_connectors_regress.parquet_leaf[], document jsonb, documents jsonb[]
)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'nested_path', format 'parquet');
-- First INSERT needs no seed file; subsequent INSERT uses exactly the same schema.
INSERT INTO lagodb_connectors_regress.parquet_nested
SELECT * FROM parquet_nested_expected WHERE id = 1;
INSERT INTO lagodb_connectors_regress.parquet_nested
SELECT * FROM parquet_nested_expected WHERE id > 1;
SELECT coalesce(actual.id, expected.id) AS id,
       to_jsonb(actual) IS NOT DISTINCT FROM to_jsonb(expected) AS matches
FROM lagodb_connectors_regress.parquet_nested AS actual
FULL JOIN parquet_nested_expected AS expected USING (id)
ORDER BY id;

-- COPY restores INSERT output; COPY TO also supplies the member-binding cases.
COPY parquet_nested_expected TO :'nested_copy_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');
CREATE TEMP TABLE parquet_nested_copy (LIKE parquet_nested_expected);
COPY parquet_nested_copy FROM :'nested_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');
SELECT coalesce(actual.id, expected.id) AS id,
       to_jsonb(actual) IS NOT DISTINCT FROM to_jsonb(expected) AS matches
FROM parquet_nested_copy AS actual
FULL JOIN parquet_nested_expected AS expected USING (id)
ORDER BY id;

ANALYZE lagodb_connectors_regress.parquet_nested;
SELECT reltuples::integer AS nested_rows
FROM pg_class
WHERE oid = 'lagodb_connectors_regress.parquet_nested'::regclass;

-- Exact member numeric typmods were exercised above. Unconstrained numeric
-- also needs no assignment, including inside a composite array.
CREATE TYPE lagodb_connectors_regress.parquet_leaf_numeric AS (
    amount numeric, tags text[], document jsonb
);
CREATE TEMP TABLE parquet_numeric_members (
    items lagodb_connectors_regress.parquet_leaf_numeric[]
);
COPY parquet_numeric_members FROM :'nested_copy_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');
SELECT count(*) AS rows,
       count(*) FILTER (WHERE (items[1]).amount = 12.35) AS populated
FROM parquet_numeric_members;

-- COPY's top-level assignment does not reach composite members.
CREATE TYPE lagodb_connectors_regress.parquet_leaf_scale AS (
    amount numeric(12, 1), tags text[], document jsonb
);
CREATE TEMP TABLE parquet_scale_members (
    items lagodb_connectors_regress.parquet_leaf_scale[]
);
\set VERBOSITY sqlstate
COPY parquet_scale_members FROM :'nested_copy_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');
\set VERBOSITY default

-- Cross the 8192-row reader batch boundary with populated, empty and NULL values.
COPY (
    SELECT n AS id, e.record, e.items, e.document, e.documents
    FROM generate_series(1, 8193) AS n
    JOIN parquet_nested_expected AS e ON e.id = (n - 1) % 3 + 1
    ORDER BY n
) TO :'nested_batch_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');
TRUNCATE parquet_nested_copy;
COPY parquet_nested_copy FROM :'nested_batch_path'
WITH (server 'lagodb_connectors_regress_s3', format 'parquet');
SELECT count(*) AS rows,
       bool_and((to_jsonb(actual) - 'id') IS NOT DISTINCT FROM
                (to_jsonb(expected) - 'id')) AS matches
FROM parquet_nested_copy AS actual
JOIN parquet_nested_expected AS expected ON expected.id = (actual.id - 1) % 3 + 1;
ALTER FOREIGN TABLE lagodb_connectors_regress.parquet_nested
OPTIONS (SET path :'nested_batch_path');

-- Equal field counts are insufficient: composite member names must also match.
CREATE TYPE lagodb_connectors_regress.parquet_leaf_renamed AS (
    amount numeric(12, 2), renamed text[], document jsonb
);
CREATE FOREIGN TABLE lagodb_connectors_regress.parquet_bad_struct (
    items lagodb_connectors_regress.parquet_leaf_renamed[]
)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'nested_batch_path', format 'parquet');
CREATE FOREIGN TABLE lagodb_connectors_regress.parquet_struct_jsonb (record jsonb)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'nested_batch_path', format 'parquet');

-- Force reader rebinding rather than retaining a materialized inner scan.
SET enable_material = off;
-- The same decoder state is rebound across batches and correlated rescans.
SELECT expected.id, actual.rows, actual.matches
FROM parquet_nested_expected AS expected
CROSS JOIN LATERAL (
    SELECT count(*) AS rows,
           bool_and((to_jsonb(value) - 'id') IS NOT DISTINCT FROM
                    (to_jsonb(expected) - 'id')) AS matches
    FROM lagodb_connectors_regress.parquet_nested AS value
    WHERE (value.id - 1) % 3 + 1 = expected.id
) AS actual
ORDER BY expected.id;

RESET enable_material;

\set VERBOSITY sqlstate
-- Containers cannot silently fall back to JSONB or mismatched composites.
SELECT items FROM lagodb_connectors_regress.parquet_bad_struct LIMIT 1;
SELECT record FROM lagodb_connectors_regress.parquet_struct_jsonb LIMIT 1;
CREATE FOREIGN TABLE lagodb_connectors_regress.parquet_infer_struct ()
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'nested_batch_path', format 'parquet');
\set VERBOSITY default
