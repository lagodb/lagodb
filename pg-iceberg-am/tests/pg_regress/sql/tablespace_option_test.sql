\set ECHO none

\! mkdir -p /tmp/pg_iceberg_am_regress_spc
\! rm -rf /tmp/pg_iceberg_am_regress_spc/*
SET client_min_messages = warning;
DROP TABLESPACE IF EXISTS iceberg_volume_test;
RESET client_min_messages;

SELECT 'regress-tablespace-' || gen_random_uuid() AS volume_name
\gset
SELECT lakebase.create_storage_volume(
    :'volume_name',
    's3://tablespace-option-regress/root',
    '{"type":"anonymous"}'::jsonb,
    '{"region":"us-east-1"}'::jsonb
) AS ignored
\gset

CREATE TABLESPACE iceberg_volume_test
LOCATION '/tmp/pg_iceberg_am_regress_spc'
WITH (lakebase_storage_volume = :'volume_name');

SELECT array_length(spcoptions, 1) = 1
       AND (SELECT count(*) FROM unnest(spcoptions) AS option
            WHERE option LIKE 'lakebase_volume_id=%') = 1
       AND NOT EXISTS (
           SELECT 1 FROM unnest(spcoptions) AS option
           WHERE option LIKE 'lakebase_storage_volume=%'
       ) AS internal_id_only
FROM pg_tablespace
WHERE spcname = 'iceberg_volume_test'
\gset
\echo internal_id_only: :internal_id_only

SELECT count(*) = 1 AS binding_visible
FROM lakebase.storage_volumes AS volume
JOIN pg_tablespace AS tablespace
  ON tablespace.oid = volume.bound_tablespace_oid
WHERE volume.storage_volume_name = :'volume_name'
  AND tablespace.spcname = 'iceberg_volume_test'
\gset
\echo binding_visible: :binding_visible

DROP TABLESPACE iceberg_volume_test;
SELECT count(*) = 1 AS binding_retained_after_drop
FROM lakebase.storage_volumes
WHERE storage_volume_name = :'volume_name'
  AND bound_tablespace_oid IS NOT NULL
  AND bound_tablespace_name IS NULL
\gset
\echo binding_retained_after_drop: :binding_retained_after_drop
