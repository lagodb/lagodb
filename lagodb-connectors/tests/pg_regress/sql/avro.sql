\i include/column_definitions.sql

-- Avro format COPY, foreign-table, inference, and scan-state coverage.

SELECT bucket AS lagodb_regress_bucket
FROM lagodb_regress.object_storage_fixture
\gset

\set avro_base 's3://' :lagodb_regress_bucket '/lagodb-connectors/avro/'
\set avro_exact_path :avro_base 'exact.avro'
\set avro_prefix_path :avro_base 'prefix/'
\set avro_part_a_path :avro_base 'prefix/part-a.avro'
\set avro_part_b_path :avro_base 'prefix/part-b.avro'
\set avro_snappy_path :avro_base 'snappy.avro'
\set avro_empty_path :avro_base 'empty.avro'
\set avro_write_path :avro_base 'write/'
\set avro_drift_path :avro_base 'drift/'
\set avro_drift_a_path :avro_base 'drift/part-a.avro'
\set avro_drift_b_path :avro_base 'drift/part-b.avro'
\set avro_unsupported_path :avro_base 'unsupported.avro'

-- Exact, prefix, Snappy, and empty Avro containers.
COPY lagodb_connectors_regress.avro_source
TO :'avro_exact_path'
WITH (server 'lagodb_connectors_regress_s3');
COPY (
    SELECT * FROM lagodb_connectors_regress.avro_source WHERE id = 1
) TO :'avro_part_a_path'
WITH (server 'lagodb_connectors_regress_s3', format 'avro');
COPY (
    SELECT * FROM lagodb_connectors_regress.avro_source WHERE id = 2
) TO :'avro_part_b_path'
WITH (server 'lagodb_connectors_regress_s3', format 'avro');
COPY lagodb_connectors_regress.avro_source
TO :'avro_snappy_path'
WITH (
    server 'lagodb_connectors_regress_s3',
    format 'avro',
    compression 'snappy'
);
COPY (
    SELECT * FROM lagodb_connectors_regress.avro_source WHERE false
) TO :'avro_empty_path'
WITH (server 'lagodb_connectors_regress_s3', format 'avro');

-- Compare every column in both directions; ALL also checks duplicate counts.
CREATE TABLE lagodb_connectors_regress.avro_copy (:avro_columns);
COPY lagodb_connectors_regress.avro_copy
FROM :'avro_exact_path'
WITH (server 'lagodb_connectors_regress_s3');
(SELECT * FROM lagodb_connectors_regress.avro_copy
 EXCEPT ALL SELECT * FROM lagodb_connectors_regress.avro_source)
UNION ALL
(SELECT * FROM lagodb_connectors_regress.avro_source
 EXCEPT ALL SELECT * FROM lagodb_connectors_regress.avro_copy);

TRUNCATE lagodb_connectors_regress.avro_copy;
COPY lagodb_connectors_regress.avro_copy
FROM :'avro_snappy_path'
WITH (server 'lagodb_connectors_regress_s3', format 'avro');
(SELECT * FROM lagodb_connectors_regress.avro_copy
 EXCEPT ALL SELECT * FROM lagodb_connectors_regress.avro_source)
UNION ALL
(SELECT * FROM lagodb_connectors_regress.avro_source
 EXCEPT ALL SELECT * FROM lagodb_connectors_regress.avro_copy);

TRUNCATE lagodb_connectors_regress.avro_copy;
COPY lagodb_connectors_regress.avro_copy
FROM :'avro_empty_path'
WITH (server 'lagodb_connectors_regress_s3', format 'avro');
SELECT count(*) AS empty_rows FROM lagodb_connectors_regress.avro_copy;

-- Exact/prefix scans and Avro-owned schema inference.
CREATE FOREIGN TABLE lagodb_connectors_regress.avro_exact
    (:avro_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'avro_exact_path');
CREATE FOREIGN TABLE lagodb_connectors_regress.avro_prefix
    (:avro_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'avro_prefix_path', format 'avro');
CREATE FOREIGN TABLE lagodb_connectors_regress.avro_inferred ()
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'avro_exact_path', format 'avro');

(SELECT * FROM lagodb_connectors_regress.avro_exact
 EXCEPT ALL SELECT * FROM lagodb_connectors_regress.avro_source)
UNION ALL
(SELECT * FROM lagodb_connectors_regress.avro_source
 EXCEPT ALL SELECT * FROM lagodb_connectors_regress.avro_exact);

(SELECT * FROM lagodb_connectors_regress.avro_prefix
 EXCEPT ALL SELECT * FROM lagodb_connectors_regress.avro_source)
UNION ALL
(SELECT * FROM lagodb_connectors_regress.avro_source
 EXCEPT ALL SELECT * FROM lagodb_connectors_regress.avro_prefix);

SELECT attname, format_type(atttypid, atttypmod) AS type
FROM pg_attribute
WHERE attrelid = 'lagodb_connectors_regress.avro_inferred'::regclass
  AND attnum > 0 AND NOT attisdropped
ORDER BY attnum;
SELECT count(*) AS inferred_rows
FROM lagodb_connectors_regress.avro_inferred;

CREATE FOREIGN TABLE lagodb_connectors_regress.avro_write
    (:avro_columns)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'avro_write_path', format 'avro');
INSERT INTO lagodb_connectors_regress.avro_write
SELECT * FROM lagodb_connectors_regress.avro_source;
(SELECT * FROM lagodb_connectors_regress.avro_write
 EXCEPT ALL SELECT * FROM lagodb_connectors_regress.avro_source)
UNION ALL
(SELECT * FROM lagodb_connectors_regress.avro_source
 EXCEPT ALL SELECT * FROM lagodb_connectors_regress.avro_write);

-- Avro integer widths are fixed; smallint has no supported representation.
CREATE FOREIGN TABLE lagodb_connectors_regress.avro_narrow (
    bigint_col integer
)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'avro_exact_path', format 'avro');
CREATE TEMP TABLE avro_smallint_sink (integer_col smallint);
\set VERBOSITY sqlstate
SELECT bigint_col FROM lagodb_connectors_regress.avro_narrow;
COPY avro_smallint_sink FROM :'avro_exact_path'
WITH (server 'lagodb_connectors_regress_s3', format 'avro');
COPY (SELECT 1::smallint AS id) TO :'avro_unsupported_path'
WITH (server 'lagodb_connectors_regress_s3', format 'avro');
\set VERBOSITY default

-- AvroScanState must restart for parameterized nested-loop rescans.
SET enable_hashjoin = off;
SET enable_mergejoin = off;
SET enable_material = off;
SET enable_nestloop = on;
SELECT outer_rel.id AS outer_id, inner_rel.id AS inner_id
FROM (VALUES (1), (2), (999)) AS outer_rel(id)
LEFT JOIN LATERAL (
    SELECT id
    FROM lagodb_connectors_regress.avro_prefix AS inner_rel
    WHERE inner_rel.id = outer_rel.id
    OFFSET 0
) AS inner_rel ON true
ORDER BY outer_rel.id;
RESET enable_hashjoin;
RESET enable_mergejoin;
RESET enable_material;
RESET enable_nestloop;

-- Prefix objects must share one writer schema. JSON datum input is outside the
-- supported Avro type contract.
COPY (
    SELECT id, text_col
    FROM lagodb_connectors_regress.avro_source WHERE id = 1
) TO :'avro_drift_a_path'
WITH (server 'lagodb_connectors_regress_s3', format 'avro');
COPY (
    SELECT id, integer_col
    FROM lagodb_connectors_regress.avro_source WHERE id = 2
) TO :'avro_drift_b_path'
WITH (server 'lagodb_connectors_regress_s3', format 'avro');
CREATE FOREIGN TABLE lagodb_connectors_regress.avro_drift (
    id integer,
    text_col text
)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'avro_drift_path', format 'avro');

\set VERBOSITY sqlstate
SELECT count(*) FROM lagodb_connectors_regress.avro_drift;
COPY (SELECT json_col FROM lagodb_connectors_regress.json_source)
TO :'avro_unsupported_path'
WITH (server 'lagodb_connectors_regress_s3', format 'avro');
\set VERBOSITY default
