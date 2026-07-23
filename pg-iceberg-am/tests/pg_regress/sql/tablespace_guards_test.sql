\set ECHO none

\! mkdir -p /tmp/pg_iceberg_am_regress_guard_dist
\! rm -rf /tmp/pg_iceberg_am_regress_guard_dist/*
\! mkdir -p /tmp/pg_iceberg_am_regress_guard_native
\! rm -rf /tmp/pg_iceberg_am_regress_guard_native/*

SET client_min_messages = warning;
DROP TABLESPACE IF EXISTS iceberg_guard_dist;
DROP TABLESPACE IF EXISTS iceberg_guard_dist_renamed;
DROP TABLESPACE IF EXISTS iceberg_guard_native;
RESET client_min_messages;

SELECT 'regress-guard-' || gen_random_uuid() AS volume_name
\gset
SELECT lakebase.create_storage_volume(
    :'volume_name',
    's3://tablespace-guard-regress/root',
    '{"type":"anonymous"}'::jsonb,
    '{"region":"us-east-1"}'::jsonb
) AS ignored
\gset

CREATE TABLESPACE iceberg_guard_dist
LOCATION '/tmp/pg_iceberg_am_regress_guard_dist'
WITH (lakebase_storage_volume = :'volume_name');
CREATE TABLESPACE iceberg_guard_native
LOCATION '/tmp/pg_iceberg_am_regress_guard_native';

-- Rename is allowed; every SET/RESET is rejected for a Lakebase tablespace.
ALTER TABLESPACE iceberg_guard_dist RENAME TO iceberg_guard_dist_renamed;
SELECT count(*) = 1 AS rename_allowed
FROM lakebase.storage_volumes AS volume
JOIN pg_tablespace AS tablespace
  ON tablespace.oid = volume.bound_tablespace_oid
WHERE volume.storage_volume_name = :'volume_name'
  AND tablespace.spcname = 'iceberg_guard_dist_renamed'
  AND EXISTS (
      SELECT 1 FROM unnest(tablespace.spcoptions) AS option
      WHERE option LIKE 'lakebase_volume_id=%'
  )
\gset
\echo rename_allowed: :rename_allowed

-- Public, internal and native options are all immutable after binding.
CREATE TEMP TABLE guard_results (
    public_alter_rejected boolean,
    internal_alter_rejected boolean,
    native_alter_rejected boolean,
    native_reset_rejected boolean
);
DO $guard$
DECLARE
    public_rejected boolean := false;
    internal_rejected boolean := false;
    native_rejected boolean := false;
    reset_rejected boolean := false;
BEGIN
    BEGIN
        EXECUTE 'ALTER TABLESPACE iceberg_guard_dist_renamed SET '
                '(lakebase_storage_volume = ''another-volume'')';
    EXCEPTION WHEN feature_not_supported THEN
        public_rejected := true;
    END;
    BEGIN
        EXECUTE 'ALTER TABLESPACE iceberg_guard_dist_renamed SET '
                '(lakebase_volume_id = 999)';
    EXCEPTION WHEN feature_not_supported THEN
        internal_rejected := true;
    END;
    BEGIN
        EXECUTE 'ALTER TABLESPACE iceberg_guard_dist_renamed SET '
                '(seq_page_cost = 1.25)';
    EXCEPTION WHEN feature_not_supported THEN
        native_rejected := true;
    END;
    BEGIN
        EXECUTE 'ALTER TABLESPACE iceberg_guard_dist_renamed RESET '
                '(seq_page_cost)';
    EXCEPTION WHEN feature_not_supported THEN
        reset_rejected := true;
    END;
    INSERT INTO guard_results VALUES (
        public_rejected,
        internal_rejected,
        native_rejected,
        reset_rejected
    );
END
$guard$;
SELECT public_alter_rejected AS public_binding_alter_rejected,
       internal_alter_rejected AS internal_binding_alter_rejected,
       native_alter_rejected,
       native_reset_rejected
FROM guard_results
\gset
\echo public_binding_alter_rejected: :public_binding_alter_rejected
\echo internal_binding_alter_rejected: :internal_binding_alter_rejected
\echo native_alter_rejected: :native_alter_rejected
\echo native_reset_rejected: :native_reset_rejected

-- Native tablespaces continue to use PostgreSQL's SET/RESET path.
ALTER TABLESPACE iceberg_guard_native SET (seq_page_cost = 1.25);
ALTER TABLESPACE iceberg_guard_native RESET (seq_page_cost);
SELECT count(*) = 1 AS internal_id_unchanged
FROM pg_tablespace
WHERE spcname = 'iceberg_guard_dist_renamed'
  AND array_length(spcoptions, 1) = 1
  AND EXISTS (
      SELECT 1 FROM unnest(spcoptions) AS option
      WHERE option LIKE 'lakebase_volume_id=%'
  )
\gset
\echo internal_id_unchanged: :internal_id_unchanged

DROP TABLESPACE iceberg_guard_dist_renamed;
DROP TABLESPACE iceberg_guard_native;
