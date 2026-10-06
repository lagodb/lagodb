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

-- Scalar values, NULLs, escaping, and exact/prefix objects.
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

-- Compare every column in both directions; ALL also checks duplicate counts.
(SELECT * FROM lagodb_connectors_regress.text_copy_sink
 EXCEPT ALL SELECT * FROM lagodb_connectors_regress.common_source)
UNION ALL
(SELECT * FROM lagodb_connectors_regress.common_source
 EXCEPT ALL SELECT * FROM lagodb_connectors_regress.text_copy_sink);

COPY lagodb_connectors_regress.common_source
TO :'text_alias_path'
WITH (server 'lagodb_connectors_regress_s3');
TRUNCATE lagodb_connectors_regress.text_copy_sink;
COPY lagodb_connectors_regress.text_copy_sink
FROM :'text_alias_path'
WITH (server 'lagodb_connectors_regress_s3');
(SELECT * FROM lagodb_connectors_regress.text_copy_sink
 EXCEPT ALL SELECT * FROM lagodb_connectors_regress.common_source)
UNION ALL
(SELECT * FROM lagodb_connectors_regress.common_source
 EXCEPT ALL SELECT * FROM lagodb_connectors_regress.text_copy_sink);

-- PostgreSQL text datum semantics include JSON values and arrays.
COPY lagodb_connectors_regress.stream_extra_source
TO :'text_extra_path'
WITH (server 'lagodb_connectors_regress_s3', format 'text');
CREATE TABLE lagodb_connectors_regress.text_extra_sink
    (:stream_extra_columns);
COPY lagodb_connectors_regress.text_extra_sink
FROM :'text_extra_path'
WITH (server 'lagodb_connectors_regress_s3', format 'text');
SELECT * FROM lagodb_connectors_regress.text_extra_sink ORDER BY id;

-- An empty exact object is a valid Text object.
COPY (
    SELECT * FROM lagodb_connectors_regress.common_source WHERE false
) TO :'text_empty_path'
WITH (server 'lagodb_connectors_regress_s3', format 'text');
TRUNCATE lagodb_connectors_regress.text_copy_sink;
COPY lagodb_connectors_regress.text_copy_sink
FROM :'text_empty_path'
WITH (server 'lagodb_connectors_regress_s3', format 'text');
SELECT count(*) AS empty_rows
FROM lagodb_connectors_regress.text_copy_sink;

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

(SELECT * FROM lagodb_connectors_regress.text_exact
 EXCEPT ALL SELECT * FROM lagodb_connectors_regress.common_source)
UNION ALL
(SELECT * FROM lagodb_connectors_regress.common_source
 EXCEPT ALL SELECT * FROM lagodb_connectors_regress.text_exact);

(SELECT * FROM lagodb_connectors_regress.text_prefix
 EXCEPT ALL SELECT * FROM lagodb_connectors_regress.common_source)
UNION ALL
(SELECT * FROM lagodb_connectors_regress.common_source
 EXCEPT ALL SELECT * FROM lagodb_connectors_regress.text_prefix);

SELECT attname, format_type(atttypid, atttypmod) AS type
FROM pg_attribute
WHERE attrelid = 'lagodb_connectors_regress.text_inferred'::regclass
  AND attnum > 0 AND NOT attisdropped
ORDER BY attnum;
SELECT count(*) AS inferred_rows
FROM lagodb_connectors_regress.text_inferred;

-- Prefix foreign INSERT uses the Text writer.
CREATE FOREIGN TABLE lagodb_connectors_regress.text_write
    (:common_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'text_write_path', format 'text');
INSERT INTO lagodb_connectors_regress.text_write
SELECT * FROM lagodb_connectors_regress.common_source;
(SELECT * FROM lagodb_connectors_regress.text_write
 EXCEPT ALL SELECT * FROM lagodb_connectors_regress.common_source)
UNION ALL
(SELECT * FROM lagodb_connectors_regress.common_source
 EXCEPT ALL SELECT * FROM lagodb_connectors_regress.text_write);

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

-- Mixed gzip/zstd/plain text members resolve their codec per file.
-- Scan and infer local and remote collections, including parameterized rescans.
BEGIN;
CREATE SERVER lagodb_connectors_regress_mixed_local FOREIGN DATA WRAPPER lagodb_connectors;
CREATE USER MAPPING FOR PUBLIC SERVER lagodb_connectors_regress_mixed_local;
SELECT current_setting('data_directory') || '/lagodb-mixed-text-' || pg_backend_pid()
           || '-' || txid_current() || '/' AS mixed_local_path,
       's3://' || bucket || '/lagodb-connectors/mixed-text-' || pg_backend_pid()
           || '-' || txid_current() || '/' AS mixed_remote_path
FROM lagodb_regress.object_storage_fixture
\gset

CREATE TABLE lagodb_connectors_regress.mixed_source (id integer, payload text);
INSERT INTO lagodb_connectors_regress.mixed_source
VALUES (1, E'comma,value "quoted"\nline'), (2, NULL), (3, ''), (4, '中文');

CREATE FOREIGN TABLE lagodb_connectors_regress.mixed_local (id integer, payload text)
SERVER lagodb_connectors_regress_mixed_local
OPTIONS (path :'mixed_local_path', format 'text', compression 'gzip');
INSERT INTO lagodb_connectors_regress.mixed_local SELECT * FROM lagodb_connectors_regress.mixed_source;
ALTER FOREIGN TABLE lagodb_connectors_regress.mixed_local OPTIONS (SET compression 'zstd');
INSERT INTO lagodb_connectors_regress.mixed_local SELECT * FROM lagodb_connectors_regress.mixed_source;
ALTER FOREIGN TABLE lagodb_connectors_regress.mixed_local OPTIONS (DROP compression);
INSERT INTO lagodb_connectors_regress.mixed_local SELECT * FROM lagodb_connectors_regress.mixed_source;
(SELECT source.* FROM lagodb_connectors_regress.mixed_source AS source, generate_series(1, 3)
 EXCEPT ALL TABLE lagodb_connectors_regress.mixed_local)
UNION ALL
(TABLE lagodb_connectors_regress.mixed_local
 EXCEPT ALL SELECT source.* FROM lagodb_connectors_regress.mixed_source AS source, generate_series(1, 3));

-- Parameterized scans must reopen all members with their individual codecs.
SELECT sum(matched.n) AS rescan_rows
FROM (VALUES (1), (3)) AS thresholds(id)
CROSS JOIN LATERAL (
    SELECT count(*) AS n FROM lagodb_connectors_regress.mixed_local AS files
    WHERE files.id >= thresholds.id OFFSET 0
) AS matched;

CREATE FOREIGN TABLE lagodb_connectors_regress.mixed_local_inferred ()
SERVER lagodb_connectors_regress_mixed_local
OPTIONS (path :'mixed_local_path', format 'text');
SELECT count(*) AS inferred_rows
FROM lagodb_connectors_regress.mixed_local_inferred;

CREATE FOREIGN TABLE lagodb_connectors_regress.mixed_remote (id integer, payload text)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'mixed_remote_path', format 'text', compression 'gzip');
INSERT INTO lagodb_connectors_regress.mixed_remote SELECT * FROM lagodb_connectors_regress.mixed_source;
ALTER FOREIGN TABLE lagodb_connectors_regress.mixed_remote OPTIONS (SET compression 'zstd');
INSERT INTO lagodb_connectors_regress.mixed_remote SELECT * FROM lagodb_connectors_regress.mixed_source;
ALTER FOREIGN TABLE lagodb_connectors_regress.mixed_remote OPTIONS (DROP compression);
INSERT INTO lagodb_connectors_regress.mixed_remote SELECT * FROM lagodb_connectors_regress.mixed_source;
(SELECT source.* FROM lagodb_connectors_regress.mixed_source AS source, generate_series(1, 3)
 EXCEPT ALL TABLE lagodb_connectors_regress.mixed_remote)
UNION ALL
(TABLE lagodb_connectors_regress.mixed_remote
 EXCEPT ALL SELECT source.* FROM lagodb_connectors_regress.mixed_source AS source, generate_series(1, 3));

-- Parameterized scans must reopen all members with their individual codecs.
SELECT sum(matched.n) AS rescan_rows
FROM (VALUES (1), (3)) AS thresholds(id)
CROSS JOIN LATERAL (
    SELECT count(*) AS n FROM lagodb_connectors_regress.mixed_remote AS files
    WHERE files.id >= thresholds.id OFFSET 0
) AS matched;

CREATE FOREIGN TABLE lagodb_connectors_regress.mixed_remote_inferred ()
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'mixed_remote_path', format 'text');
SELECT count(*) AS inferred_rows
FROM lagodb_connectors_regress.mixed_remote_inferred;

-- Two live Text/CSV parsers must retain their own inputs across refills and rescans.
-- Use provider COPY input; each object exceeds PG's 64 KiB raw buffer.
\set scan_text_path :mixed_remote_path 'interleaved.txt'
\set scan_csv_path :mixed_remote_path 'interleaved.csv'
COPY (SELECT id, repeat('a', 4096) FROM generate_series(1, 40) AS rows(id))
TO :'scan_text_path'
WITH (server 'lagodb_connectors_regress_s3', format 'text');
COPY (SELECT id, repeat('b', 4096) FROM generate_series(1, 40) AS rows(id))
TO :'scan_csv_path'
WITH (server 'lagodb_connectors_regress_s3', format 'csv', header true);
CREATE FOREIGN TABLE lagodb_connectors_regress.scan_text (id integer, payload text)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'scan_text_path', format 'text');
CREATE FOREIGN TABLE lagodb_connectors_regress.scan_csv (id integer, payload text)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'scan_csv_path', format 'csv', header 'true');
SET LOCAL enable_hashjoin = off;
SET LOCAL enable_mergejoin = off;
SET LOCAL enable_material = off;
SET LOCAL enable_nestloop = on;
SELECT count(*) AS matched_rows
FROM lagodb_connectors_regress.scan_text AS outer_rel
CROSS JOIN LATERAL (
    SELECT inner_rel.id
    FROM lagodb_connectors_regress.scan_csv AS inner_rel
    WHERE inner_rel.id = outer_rel.id AND inner_rel.payload = repeat('b', 4096)
    OFFSET 0
) AS inner_rel
WHERE outer_rel.payload = repeat('a', 4096);

-- Early termination must also allow scans to end in executor order.
SELECT true AS own_input
FROM lagodb_connectors_regress.scan_text AS outer_rel
CROSS JOIN LATERAL (
    SELECT inner_rel.id
    FROM lagodb_connectors_regress.scan_csv AS inner_rel
    WHERE inner_rel.id = outer_rel.id AND inner_rel.payload = repeat('b', 4096)
    OFFSET 0
) AS inner_rel
WHERE outer_rel.payload = repeat('a', 4096)
LIMIT 1;

-- A foreign scan in a COPY FROM trigger must restore the enclosing COPY input.
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

ROLLBACK;
