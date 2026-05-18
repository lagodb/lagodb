-- Guards on ALTER TABLESPACE for distributed (Lakebase) tablespaces:
--   * RENAME TO ... is rejected for distributed tablespaces because the
--     storage `store_id` is the tablespace name and cache/staging directories
--     hang off it.
--   * SET / RESET (...) is rejected for distributed tablespaces because
--     distributed tablespaces are immutable in this release.
--
-- Native tablespaces (no Lakebase options) keep the standard PostgreSQL
-- behaviour: rename and set/reset succeed.
--
-- The verification queries deliberately project booleans / counts so the
-- expected output does not depend on `psql` column-width formatting.

DROP EXTENSION IF EXISTS pg_iceberg_am CASCADE;

CREATE EXTENSION IF NOT EXISTS pg_iceberg_am;

\! mkdir -p /tmp/pg_iceberg_am_regress_guard_dist
\! rm -rf /tmp/pg_iceberg_am_regress_guard_dist/*
\! mkdir -p /tmp/pg_iceberg_am_regress_guard_native
\! rm -rf /tmp/pg_iceberg_am_regress_guard_native/*

SET client_min_messages = warning;
DROP TABLESPACE IF EXISTS iceberg_guard_dist;
DROP TABLESPACE IF EXISTS iceberg_guard_dist_renamed;
DROP TABLESPACE IF EXISTS iceberg_guard_native;
DROP TABLESPACE IF EXISTS iceberg_guard_native_renamed;
RESET client_min_messages;

CREATE TABLESPACE iceberg_guard_dist
    LOCATION '/tmp/pg_iceberg_am_regress_guard_dist'
    WITH (
        protocol = 's3',
        bucket = 'guard-bucket',
        region = 'us-east-1'
    );

CREATE TABLESPACE iceberg_guard_native
    LOCATION '/tmp/pg_iceberg_am_regress_guard_native';

\set VERBOSITY terse

-- Distributed: RENAME must be rejected.
ALTER TABLESPACE iceberg_guard_dist RENAME TO iceberg_guard_dist_renamed;

-- Distributed: SET / RESET must be rejected.
ALTER TABLESPACE iceberg_guard_dist SET (seq_page_cost = 1.5);
ALTER TABLESPACE iceberg_guard_dist RESET (seq_page_cost);

\set VERBOSITY default

-- Distributed: catalog must be unchanged: the original name still exists,
-- the renamed name does not, and the storage options were not mutated.
SELECT EXISTS (SELECT 1 FROM pg_tablespace WHERE spcname = 'iceberg_guard_dist')
    AS distributed_kept;
SELECT EXISTS (SELECT 1 FROM pg_tablespace WHERE spcname = 'iceberg_guard_dist_renamed')
    AS distributed_renamed_visible;
SELECT string_agg(x, ',' ORDER BY ord) AS distributed_options
FROM pg_tablespace, unnest(spcoptions) WITH ORDINALITY u(x, ord)
WHERE spcname = 'iceberg_guard_dist'
GROUP BY spcoptions;

-- Native: RENAME must succeed.
ALTER TABLESPACE iceberg_guard_native RENAME TO iceberg_guard_native_renamed;
SELECT EXISTS (SELECT 1 FROM pg_tablespace WHERE spcname = 'iceberg_guard_native')
    AS native_kept_old_name;
SELECT EXISTS (SELECT 1 FROM pg_tablespace WHERE spcname = 'iceberg_guard_native_renamed')
    AS native_renamed_visible;

-- Native: SET / RESET must succeed.
ALTER TABLESPACE iceberg_guard_native_renamed SET (seq_page_cost = 1.25);
SELECT (spcoptions = ARRAY['seq_page_cost=1.25']::text[]) AS native_set_applied
FROM pg_tablespace
WHERE spcname = 'iceberg_guard_native_renamed';

ALTER TABLESPACE iceberg_guard_native_renamed RESET (seq_page_cost);
SELECT spcoptions IS NULL AS native_reset_applied
FROM pg_tablespace
WHERE spcname = 'iceberg_guard_native_renamed';

DROP TABLESPACE iceberg_guard_dist;
DROP TABLESPACE iceberg_guard_native_renamed;
