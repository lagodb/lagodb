# Orphan-only FULL has no metadata pointer CAS. Its no-op tuple validation must
# retry after a concurrent catalog tuple update before publishing deletion.

setup
{
  CREATE EXTENSION IF NOT EXISTS lagodb_iceberg;
  CREATE SCHEMA IF NOT EXISTS vacuum_orphan_iso;
  CREATE TABLE vacuum_orphan_iso.t (id integer) USING iceberg;
  INSERT INTO vacuum_orphan_iso.t VALUES (1);
  DO $$
  DECLARE
    relative_path text :=
      pg_relation_filepath('vacuum_orphan_iso.t') ||
      '_iceberg/data/orphan.parquet';
    absolute_path text :=
      current_setting('data_directory') || '/' || relative_path;
  BEGIN
    EXECUTE format(
      'COPY (SELECT %L) TO %L',
      'orphan',
      absolute_path
    );
    EXECUTE format(
      'COPY (SELECT '''') TO PROGRAM %L',
      'touch -t 200001010000 ' || absolute_path
    );
  END
  $$;
}

teardown
{
  DROP SCHEMA IF EXISTS vacuum_orphan_iso CASCADE;
}

session s1
step s1_begin { BEGIN; }
step s1_touch_metadata {
  UPDATE iceberg.iceberg_metadata
  SET maintenance_due_at = 'infinity'
  WHERE relid = 'vacuum_orphan_iso.t'::regclass;
}
step s1_commit { COMMIT; }

session s2
step s2_vacuum_full { VACUUM (FULL) vacuum_orphan_iso.t; }
step s2_verify {
  SELECT (SELECT array_agg(id ORDER BY id) FROM vacuum_orphan_iso.t)
           = ARRAY[1] AS rows_preserved,
         pg_stat_file(
           pg_relation_filepath('vacuum_orphan_iso.t') ||
           '_iceberg/data/orphan.parquet',
           true
         ) IS NULL AS orphan_deleted,
         (SELECT maintenance_due_at = 'infinity'
          FROM iceberg.iceberg_metadata
          WHERE relid = 'vacuum_orphan_iso.t'::regclass)
           AS concurrent_due_preserved;
}

permutation
  s1_begin s1_touch_metadata
  s2_vacuum_full
  s1_commit
  s2_verify
