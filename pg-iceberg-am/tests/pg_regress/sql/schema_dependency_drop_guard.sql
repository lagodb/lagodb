-- Only the supported ALTER TABLE path may drop an Iceberg column.

DROP EXTENSION IF EXISTS pg_iceberg_am CASCADE;
CREATE EXTENSION pg_iceberg_am;
CREATE SCHEMA schema_dependency_drop_guard;

CREATE TABLE schema_dependency_drop_guard.controlled_t (
    id integer,
    value text
) USING iceberg;
ALTER TABLE schema_dependency_drop_guard.controlled_t DROP COLUMN value;
SELECT string_agg(attname, ',' ORDER BY attnum) AS live_columns
FROM pg_attribute
WHERE attrelid = 'schema_dependency_drop_guard.controlled_t'::regclass
  AND attnum > 0
  AND NOT attisdropped;

CREATE COLLATION schema_dependency_drop_guard.drop_me FROM "C";
CREATE TABLE schema_dependency_drop_guard.dependency_t (
    id integer,
    value text COLLATE schema_dependency_drop_guard.drop_me
) USING iceberg;
INSERT INTO schema_dependency_drop_guard.dependency_t VALUES (1, 'kept');

\set VERBOSITY terse
DROP COLLATION schema_dependency_drop_guard.drop_me CASCADE;
\set VERBOSITY default

SELECT string_agg(attname, ',' ORDER BY attnum) AS live_columns
FROM pg_attribute
WHERE attrelid = 'schema_dependency_drop_guard.dependency_t'::regclass
  AND attnum > 0
  AND NOT attisdropped;
SELECT count(*) AS rows_after_rejected_dependency_drop
FROM schema_dependency_drop_guard.dependency_t;

DROP SCHEMA schema_dependency_drop_guard CASCADE;
DROP EXTENSION pg_iceberg_am CASCADE;
