\i include/column_definitions.sql

-- Avro format COPY, foreign-table, inference, and scan-state coverage.

SELECT bucket AS lagodb_regress_bucket
FROM lagodb_regress.object_storage_fixture
\gset
\set avro_base 's3://' :lagodb_regress_bucket '/lagodb-connectors/avro/'
\pset null _null_

-- COPY round trip through an exact object; the suffix selects the format.
\set avro_exact_path :avro_base 'exact.avro'
COPY lagodb_connectors_regress.avro_source TO :'avro_exact_path'
WITH (server 'lagodb_connectors_regress_s3');
CREATE TABLE lagodb_connectors_regress.avro_copy (:avro_columns);
COPY lagodb_connectors_regress.avro_copy FROM :'avro_exact_path'
WITH (server 'lagodb_connectors_regress_s3');
SELECT * FROM lagodb_connectors_regress.avro_copy ORDER BY id;

-- A Snappy-compressed container.
\set avro_snappy_path :avro_base 'snappy.avro'
COPY lagodb_connectors_regress.avro_source TO :'avro_snappy_path'
WITH (server 'lagodb_connectors_regress_s3', format 'avro', compression 'snappy');
TRUNCATE lagodb_connectors_regress.avro_copy;
COPY lagodb_connectors_regress.avro_copy FROM :'avro_snappy_path'
WITH (server 'lagodb_connectors_regress_s3', format 'avro');
SELECT * FROM lagodb_connectors_regress.avro_copy ORDER BY id;

-- An empty container is valid input.
\set avro_empty_path :avro_base 'empty.avro'
COPY (SELECT * FROM lagodb_connectors_regress.avro_source WHERE false)
TO :'avro_empty_path'
WITH (server 'lagodb_connectors_regress_s3', format 'avro');
TRUNCATE lagodb_connectors_regress.avro_copy;
COPY lagodb_connectors_regress.avro_copy FROM :'avro_empty_path'
WITH (server 'lagodb_connectors_regress_s3', format 'avro');
SELECT count(*) AS empty_rows FROM lagodb_connectors_regress.avro_copy;

-- Exact foreign scan and Avro-owned schema inference.
CREATE FOREIGN TABLE lagodb_connectors_regress.avro_exact (:avro_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'avro_exact_path');
SELECT * FROM lagodb_connectors_regress.avro_exact ORDER BY id;
CREATE FOREIGN TABLE lagodb_connectors_regress.avro_inferred ()
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'avro_exact_path', format 'avro');
SELECT attname, format_type(atttypid, atttypmod) AS type
FROM pg_attribute
WHERE attrelid = 'lagodb_connectors_regress.avro_inferred'::regclass
  AND attnum > 0 AND NOT attisdropped
ORDER BY attnum;
SELECT count(*) AS inferred_rows FROM lagodb_connectors_regress.avro_inferred;

-- A prefix scan reads every object under the prefix.
\set avro_prefix_path :avro_base 'prefix/'
\set avro_part_path :avro_prefix_path 'part-a.avro'
COPY (SELECT * FROM lagodb_connectors_regress.avro_source WHERE id = 1)
TO :'avro_part_path'
WITH (server 'lagodb_connectors_regress_s3', format 'avro');
\set avro_part_path :avro_prefix_path 'part-b.avro'
COPY (SELECT * FROM lagodb_connectors_regress.avro_source WHERE id = 2)
TO :'avro_part_path'
WITH (server 'lagodb_connectors_regress_s3', format 'avro');
CREATE FOREIGN TABLE lagodb_connectors_regress.avro_prefix (:avro_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'avro_prefix_path', format 'avro');
SELECT * FROM lagodb_connectors_regress.avro_prefix ORDER BY id;

-- AvroScanState must restart for parameterized nested-loop rescans.
SELECT outer_rel.id AS outer_id, inner_rel.id AS inner_id
FROM (VALUES (1), (2), (999)) AS outer_rel(id)
LEFT JOIN LATERAL (
    SELECT id
    FROM lagodb_connectors_regress.avro_prefix AS inner_rel
    WHERE inner_rel.id = outer_rel.id
    OFFSET 0
) AS inner_rel ON true
ORDER BY outer_rel.id;

-- Prefix foreign INSERT uses the Avro writer.
\set avro_write_path :avro_base 'write/'
CREATE FOREIGN TABLE lagodb_connectors_regress.avro_write (:avro_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'avro_write_path', format 'avro');
INSERT INTO lagodb_connectors_regress.avro_write
SELECT * FROM lagodb_connectors_regress.avro_source;
SELECT * FROM lagodb_connectors_regress.avro_write ORDER BY id;

-- A local container from an external writer
-- (scripts/pg_regress/avro_nested_fixture.py). The scan skips unprojected
-- nested records, arrays and maps with byte-sized blocks, an enum, fixed
-- values, a fixed(16) uuid, and named and recursive references; ref_id reads a
-- fixed(16) uuid, and amount is a union whose non-null branch comes first.
\getenv abs_srcdir PG_ABS_SRCDIR
\set avro_nested_path :abs_srcdir '/data/avro_nested.avro'
CREATE SERVER lagodb_connectors_regress_avro_local FOREIGN DATA WRAPPER lagodb_connectors;
CREATE FOREIGN TABLE lagodb_connectors_regress.avro_nested (
    id integer,
    label text,
    ref_id uuid,
    amount bigint,
    flag boolean
)
SERVER lagodb_connectors_regress_avro_local
OPTIONS (path :'avro_nested_path');
SELECT * FROM lagodb_connectors_regress.avro_nested ORDER BY id;
DROP FOREIGN TABLE lagodb_connectors_regress.avro_nested;
DROP SERVER lagodb_connectors_regress_avro_local;

-- Avro integer widths are fixed: a reader cannot narrow long to integer, and
-- smallint has no Avro representation.
CREATE FOREIGN TABLE lagodb_connectors_regress.avro_narrow (bigint_col integer)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'avro_exact_path', format 'avro');
\set avro_unsupported_path :avro_base 'unsupported.avro'
\set VERBOSITY sqlstate
SELECT bigint_col FROM lagodb_connectors_regress.avro_narrow;
COPY (SELECT 1::smallint AS id) TO :'avro_unsupported_path'
WITH (server 'lagodb_connectors_regress_s3', format 'avro');
\set VERBOSITY default

-- Prefix objects must share one writer schema.
\set avro_drift_path :avro_base 'drift/'
\set avro_part_path :avro_drift_path 'part-a.avro'
COPY (SELECT id, text_col FROM lagodb_connectors_regress.avro_source WHERE id = 1)
TO :'avro_part_path'
WITH (server 'lagodb_connectors_regress_s3', format 'avro');
\set avro_part_path :avro_drift_path 'part-b.avro'
COPY (SELECT id, integer_col FROM lagodb_connectors_regress.avro_source WHERE id = 2)
TO :'avro_part_path'
WITH (server 'lagodb_connectors_regress_s3', format 'avro');
CREATE FOREIGN TABLE lagodb_connectors_regress.avro_drift (id integer, text_col text)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'avro_drift_path', format 'avro');
\set VERBOSITY sqlstate
SELECT count(*) FROM lagodb_connectors_regress.avro_drift;
\set VERBOSITY default
