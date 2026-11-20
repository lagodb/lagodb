\i include/column_definitions.sql

-- Text format COPY, foreign-table, inference, and scan-state coverage.

SELECT bucket AS lagodb_regress_bucket
FROM lagodb_regress.object_storage_fixture
\gset
\set text_base 's3://' :lagodb_regress_bucket '/lagodb-connectors/text/'
\pset null _null_

-- COPY round trip of scalar values, NULLs, and escapes through an exact object.
\set text_exact_path :text_base 'exact.txt'
COPY lagodb_connectors_regress.common_source TO :'text_exact_path'
WITH (server 'lagodb_connectors_regress_s3');
CREATE TABLE lagodb_connectors_regress.text_copy_sink (:common_columns);
COPY lagodb_connectors_regress.text_copy_sink FROM :'text_exact_path'
WITH (server 'lagodb_connectors_regress_s3');
SELECT * FROM lagodb_connectors_regress.text_copy_sink ORDER BY id;

-- The .text suffix is an alias for the text format.
\set text_alias_path :text_base 'alias.text'
COPY lagodb_connectors_regress.common_source TO :'text_alias_path'
WITH (server 'lagodb_connectors_regress_s3');
TRUNCATE lagodb_connectors_regress.text_copy_sink;
COPY lagodb_connectors_regress.text_copy_sink FROM :'text_alias_path'
WITH (server 'lagodb_connectors_regress_s3');
SELECT * FROM lagodb_connectors_regress.text_copy_sink ORDER BY id;

-- PostgreSQL text datum semantics include JSON values and arrays.
\set text_extra_path :text_base 'extra.txt'
COPY lagodb_connectors_regress.stream_extra_source TO :'text_extra_path'
WITH (server 'lagodb_connectors_regress_s3', format 'text');
CREATE TABLE lagodb_connectors_regress.text_extra_sink (:stream_extra_columns);
COPY lagodb_connectors_regress.text_extra_sink FROM :'text_extra_path'
WITH (server 'lagodb_connectors_regress_s3', format 'text');
SELECT * FROM lagodb_connectors_regress.text_extra_sink ORDER BY id;

-- An empty exact object is valid text input.
\set text_empty_path :text_base 'empty.txt'
COPY (SELECT * FROM lagodb_connectors_regress.common_source WHERE false)
TO :'text_empty_path'
WITH (server 'lagodb_connectors_regress_s3', format 'text');
TRUNCATE lagodb_connectors_regress.text_copy_sink;
COPY lagodb_connectors_regress.text_copy_sink FROM :'text_empty_path'
WITH (server 'lagodb_connectors_regress_s3', format 'text');
SELECT count(*) AS empty_rows FROM lagodb_connectors_regress.text_copy_sink;

-- Exact foreign scan and text-owned schema inference.
CREATE FOREIGN TABLE lagodb_connectors_regress.text_exact (:common_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'text_exact_path');
SELECT * FROM lagodb_connectors_regress.text_exact ORDER BY id;
CREATE FOREIGN TABLE lagodb_connectors_regress.text_inferred ()
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'text_exact_path', format 'text');
SELECT attname, format_type(atttypid, atttypmod) AS type
FROM pg_attribute
WHERE attrelid = 'lagodb_connectors_regress.text_inferred'::regclass
  AND attnum > 0 AND NOT attisdropped
ORDER BY attnum;
SELECT count(*) AS inferred_rows FROM lagodb_connectors_regress.text_inferred;

-- A prefix scan reads every object under the prefix.
\set text_prefix_path :text_base 'prefix/'
\set text_part_path :text_prefix_path 'part-a.txt'
COPY (SELECT * FROM lagodb_connectors_regress.common_source WHERE id = 1)
TO :'text_part_path'
WITH (server 'lagodb_connectors_regress_s3', format 'text');
\set text_part_path :text_prefix_path 'part-b.txt'
COPY (SELECT * FROM lagodb_connectors_regress.common_source WHERE id = 2)
TO :'text_part_path'
WITH (server 'lagodb_connectors_regress_s3', format 'text');
CREATE FOREIGN TABLE lagodb_connectors_regress.text_prefix (:common_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'text_prefix_path', format 'text');
SELECT * FROM lagodb_connectors_regress.text_prefix ORDER BY id;

-- Text represents the shared text/CSV DelimitedScanState rescan path.
SELECT outer_rel.id AS outer_id, inner_rel.id AS inner_id
FROM (VALUES (1), (2), (999)) AS outer_rel(id)
LEFT JOIN LATERAL (
    SELECT id
    FROM lagodb_connectors_regress.text_prefix AS inner_rel
    WHERE inner_rel.id = outer_rel.id
    OFFSET 0
) AS inner_rel ON true
ORDER BY outer_rel.id;

-- Direct text COPY FROM accepts an exact object, not a prefix.
\set VERBOSITY sqlstate
COPY lagodb_connectors_regress.text_copy_sink FROM :'text_prefix_path'
WITH (server 'lagodb_connectors_regress_s3', format 'text');
\set VERBOSITY default

-- Prefix foreign INSERT uses the text writer.
\set text_write_path :text_base 'write/'
CREATE FOREIGN TABLE lagodb_connectors_regress.text_write (:common_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'text_write_path', format 'text');
INSERT INTO lagodb_connectors_regress.text_write
SELECT * FROM lagodb_connectors_regress.common_source;
SELECT * FROM lagodb_connectors_regress.text_write ORDER BY id;

-- PostgreSQL's COPY source callback has no context argument, so each delimited
-- parser call installs its own input. Objects larger than PG's 64 KiB raw
-- buffer force refills while a nested-loop rescan interleaves two parsers.
\set scan_text_path :text_base 'interleaved.txt'
COPY (SELECT id, repeat('a', 4096) FROM generate_series(1, 40) AS rows(id))
TO :'scan_text_path'
WITH (server 'lagodb_connectors_regress_s3', format 'text');
CREATE FOREIGN TABLE lagodb_connectors_regress.scan_text (id integer, payload text)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'scan_text_path', format 'text');

\set scan_csv_path :text_base 'interleaved.csv'
COPY (SELECT id, repeat('b', 4096) FROM generate_series(1, 40) AS rows(id))
TO :'scan_csv_path'
WITH (server 'lagodb_connectors_regress_s3', format 'csv', header true);
CREATE FOREIGN TABLE lagodb_connectors_regress.scan_csv (id integer, payload text)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'scan_csv_path', format 'csv', header 'true');

SELECT count(*) AS matched_rows
FROM lagodb_connectors_regress.scan_text AS t
CROSS JOIN LATERAL (
    SELECT payload FROM lagodb_connectors_regress.scan_csv AS c
    WHERE c.id = t.id OFFSET 0
) AS c
WHERE t.payload = repeat('a', 4096) AND c.payload = repeat('b', 4096);

-- A foreign scan inside a COPY FROM trigger must restore the enclosing COPY input.
CREATE TABLE lagodb_connectors_regress.scan_copy_sink (
    id integer, payload text, matched boolean
);
CREATE FUNCTION lagodb_connectors_regress.scan_copy_match() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    NEW.matched := EXISTS (
        SELECT 1 FROM lagodb_connectors_regress.scan_csv
        WHERE id = NEW.id AND payload = repeat('b', 4096)
    );
    RETURN NEW;
END;
$$;
CREATE TRIGGER scan_copy_match BEFORE INSERT
ON lagodb_connectors_regress.scan_copy_sink
FOR EACH ROW EXECUTE FUNCTION lagodb_connectors_regress.scan_copy_match();

COPY lagodb_connectors_regress.scan_copy_sink (id, payload) FROM :'scan_text_path'
WITH (server 'lagodb_connectors_regress_s3', format 'text');
SELECT count(*) AS copied_rows,
       count(*) FILTER (WHERE matched AND payload = repeat('a', 4096)) AS matched_rows
FROM lagodb_connectors_regress.scan_copy_sink;
