-- End-to-end PostgreSQL statistics coverage for the Iceberg table AM.
DROP EXTENSION IF EXISTS lagodb_iceberg CASCADE;
CREATE EXTENSION lagodb_iceberg;
CREATE SCHEMA analyze_test;
SET search_path = analyze_test, public;

CREATE TABLE empty_t (id integer) USING iceberg;
ANALYZE empty_t;
SELECT reltuples::bigint = 0 AS empty_rows_estimated
FROM pg_class
WHERE oid = 'empty_t'::regclass;

CREATE TABLE stats_t (id integer, category text, optional integer) USING iceberg;
INSERT INTO stats_t
SELECT value,
       CASE WHEN value <= 750 THEN 'common' ELSE 'rare' END,
       CASE WHEN value % 10 = 0 THEN NULL ELSE value END
FROM generate_series(1, 1000) AS value;

ALTER TABLE stats_t ALTER COLUMN optional SET STATISTICS 1000;
ANALYZE stats_t (optional);
SELECT reltuples::bigint = 1000 AS column_analyze_updates_relation_rows
FROM pg_class
WHERE oid = 'stats_t'::regclass;
SELECT array_agg(attname::text ORDER BY attname) = ARRAY['optional']
       AS only_requested_column_analyzed
FROM pg_stats
WHERE schemaname = 'analyze_test' AND tablename = 'stats_t';
SELECT abs(null_frac - 0.1) < 0.001 AS null_fraction_ok,
       abs(n_distinct + 0.9) < 0.001 AS distinct_estimate_ok
FROM pg_stats
WHERE schemaname = 'analyze_test'
  AND tablename = 'stats_t'
  AND attname = 'optional';

ANALYZE stats_t;
SELECT most_common_vals::text = '{common,rare}' AS mcv_ok
FROM pg_stats
WHERE schemaname = 'analyze_test'
  AND tablename = 'stats_t'
  AND attname = 'category';
SELECT histogram_bounds::text LIKE '{1,%1000}' AS histogram_bounds_ok,
       abs(correlation - 1.0) < 0.001 AS ordered_correlation_ok
FROM pg_stats
WHERE schemaname = 'analyze_test'
  AND tablename = 'stats_t'
  AND attname = 'id';

-- Separate statements create separate Iceberg data files. With one selected
-- file, its 100 physical rows cannot supply the fixed 400-observation target
-- without replacement. The AM must reuse reader rows by multiplicity while
-- preserving the whole-population row estimate.
CREATE TABLE locality_t (id integer) USING iceberg;
INSERT INTO locality_t SELECT generate_series(1, 100);
INSERT INTO locality_t SELECT generate_series(101, 200);
INSERT INTO locality_t SELECT generate_series(201, 300);
INSERT INTO locality_t SELECT generate_series(301, 400);
SET lagodb_iceberg.analyze_max_data_files = 1;
ANALYZE locality_t;
SELECT reltuples::bigint = 400 AS locality_sample_estimates_all_files
FROM pg_class
WHERE oid = 'locality_t'::regclass;
RESET lagodb_iceberg.analyze_max_data_files;

CREATE TABLE deletes_t (id integer, visibility text) USING iceberg;
INSERT INTO deletes_t
SELECT value, CASE WHEN value <= 40 THEN 'deleted' ELSE 'live' END
FROM generate_series(1, 200) AS value;
DELETE FROM deletes_t WHERE id <= 40;
ANALYZE deletes_t;
SELECT reltuples::bigint = 160 AS deleted_rows_excluded
FROM pg_class
WHERE oid = 'deletes_t'::regclass;
SELECT most_common_vals::text = '{live}' AS deleted_values_excluded
FROM pg_stats
WHERE schemaname = 'analyze_test'
  AND tablename = 'deletes_t'
  AND attname = 'visibility';

-- A later snapshot with the same delete file must not replace the last
-- visibility-aware estimate with the physical manifest row count.
INSERT INTO deletes_t VALUES (201, 'live');
SET lagodb.customscan_mode = 'off';
CREATE FUNCTION explain_plan_rows(query text) RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE
    plan json;
BEGIN
    EXECUTE 'EXPLAIN (FORMAT JSON) ' || query INTO plan;
    RETURN (plan->0->'Plan'->>'Plan Rows')::bigint;
END;
$$;
SELECT explain_plan_rows('SELECT * FROM analyze_test.deletes_t') = 160
       AS stale_live_estimate_preserved;
DROP FUNCTION explain_plan_rows(text);
RESET lagodb.customscan_mode;

CREATE TABLE transaction_t (id integer) USING iceberg;
INSERT INTO transaction_t SELECT generate_series(1, 100);
BEGIN;
INSERT INTO transaction_t SELECT generate_series(101, 150);
DELETE FROM transaction_t WHERE id <= 10;
ANALYZE transaction_t;
SELECT reltuples::bigint = 140 AS transaction_delta_visible
FROM pg_class
WHERE oid = 'transaction_t'::regclass;
ROLLBACK;

VACUUM (ANALYZE) transaction_t;
SELECT reltuples::bigint = 100 AS vacuum_analyze_updates_rows
FROM pg_class
WHERE oid = 'transaction_t'::regclass;

RESET search_path;
DROP SCHEMA analyze_test CASCADE;
DROP EXTENSION lagodb_iceberg CASCADE;
