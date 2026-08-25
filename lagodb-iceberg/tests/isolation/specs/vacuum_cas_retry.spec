# VACUUM must discard one attempt's metadata artifacts, reload the latest
# metadata, revalidate fixed rewrite inputs, and succeed after catalog CAS
# conflict.

setup
{
  CREATE EXTENSION IF NOT EXISTS lagodb_iceberg;
  CREATE SCHEMA IF NOT EXISTS vacuum_cas_iso;
  CREATE TABLE vacuum_cas_iso.t (id integer) USING iceberg;
  INSERT INTO vacuum_cas_iso.t VALUES (1);
  INSERT INTO vacuum_cas_iso.t VALUES (2);
  INSERT INTO vacuum_cas_iso.t VALUES (3);
  INSERT INTO vacuum_cas_iso.t VALUES (4);
  INSERT INTO vacuum_cas_iso.t VALUES (5);
  INSERT INTO vacuum_cas_iso.t VALUES (6);
}

teardown
{
  DROP SCHEMA IF EXISTS vacuum_cas_iso CASCADE;
}

session s1
step s1_begin { BEGIN; }
step s1_touch_metadata {
  UPDATE iceberg.iceberg_metadata
  SET maintenance_due_at = 'infinity'
  WHERE relid = 'vacuum_cas_iso.t'::regclass;
}
step s1_commit { COMMIT; }

session s2
step s2_vacuum { VACUUM vacuum_cas_iso.t; }
step s2_verify {
  SELECT (SELECT current_data_objects
          FROM lakebase.table_maintenance_stats('vacuum_cas_iso.t')) = 1
           AS compacted,
         (SELECT maintenance_due_at = 'infinity'
          FROM iceberg.iceberg_metadata
          WHERE relid = 'vacuum_cas_iso.t'::regclass)
           AS concurrent_due_preserved,
         (SELECT array_agg(id ORDER BY id) FROM vacuum_cas_iso.t)
           = ARRAY[1, 2, 3, 4, 5, 6] AS rows_preserved;
}

permutation
  s1_begin s1_touch_metadata
  s2_vacuum
  s1_commit
  s2_verify
