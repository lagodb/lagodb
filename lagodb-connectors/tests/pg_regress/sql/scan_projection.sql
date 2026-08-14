-- Native-reader projection and COPY target-column mapping.

SET TIME ZONE 'UTC';
SET client_min_messages = warning;

SELECT bucket AS lakebase_regress_bucket
FROM lakebase_regress.object_storage_fixture
\gset

SELECT format('s3://%s/lagodb-connectors/scan/reorder.parquet',
              :'lakebase_regress_bucket') AS reorder_path
\gset projection_

-- A small native Parquet object makes projection and column-order failures
-- visible without hiding them in the complete type matrix.
COPY (
    SELECT id, bool_col, text_col
    FROM lagodb_connectors_regress.common_source
    ORDER BY id
) TO :'projection_reorder_path'
WITH (storage_server 'lagodb_connectors_regress_s3', format 'parquet');

DROP TABLE IF EXISTS lagodb_connectors_regress.scan_projection_sink;
CREATE TABLE lagodb_connectors_regress.scan_projection_sink (
    id integer,
    text_col text
);
COPY lagodb_connectors_regress.scan_projection_sink (id, text_col)
FROM :'projection_reorder_path'
WITH (storage_server 'lagodb_connectors_regress_s3', format 'parquet');

DROP TABLE IF EXISTS lagodb_connectors_regress.scan_reordered_sink;
CREATE TABLE lagodb_connectors_regress.scan_reordered_sink (
    id integer,
    bool_col boolean,
    text_col text
);
COPY lagodb_connectors_regress.scan_reordered_sink (
    text_col,
    id,
    bool_col
)
FROM :'projection_reorder_path'
WITH (storage_server 'lagodb_connectors_regress_s3', format 'parquet');

SELECT relation, rows, digest
FROM (
    SELECT 'projection' AS relation,
           count(*) AS rows,
           md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id)) AS digest
    FROM lagodb_connectors_regress.scan_projection_sink AS value
    UNION ALL
    SELECT 'reordered', count(*),
           md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id))
    FROM lagodb_connectors_regress.scan_reordered_sink AS value
    UNION ALL
    SELECT 'source', count(*),
           md5(string_agg(row_to_json(value)::text, E'\n' ORDER BY value.id))
    FROM (
        SELECT id, bool_col, text_col
        FROM lagodb_connectors_regress.common_source
        ORDER BY id
    ) AS value
) AS mapping_results
ORDER BY relation;

CREATE FOREIGN TABLE lagodb_connectors_regress.scan_projection (
    id integer,
    bool_col boolean,
    text_col text
)
SERVER lagodb_connectors_regress_s3
OPTIONS (path :'projection_reorder_path', format 'parquet');

SELECT id, text_col
FROM lagodb_connectors_regress.scan_projection
ORDER BY id;

SELECT text_col, id, bool_col
FROM lagodb_connectors_regress.scan_projection
ORDER BY id;

RESET client_min_messages;
