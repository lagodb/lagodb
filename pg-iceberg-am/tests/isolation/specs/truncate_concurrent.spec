# Strict truncate conflict and statement-time metadata visibility.

setup
{
  CREATE EXTENSION IF NOT EXISTS pg_iceberg_am;
  CREATE SCHEMA IF NOT EXISTS truncate_iso;
  CREATE TABLE truncate_iso.target_t (id int) USING iceberg;
  INSERT INTO truncate_iso.target_t VALUES (1);
  CREATE TABLE truncate_iso.shadow_t (id int) USING iceberg;
  INSERT INTO truncate_iso.shadow_t VALUES (2);
  CREATE TABLE truncate_iso.abort_t (id int) USING iceberg;
  INSERT INTO truncate_iso.abort_t VALUES (3);
  CREATE TABLE truncate_iso.multi_first_t (id int) USING iceberg;
  INSERT INTO truncate_iso.multi_first_t VALUES (4);
  CREATE TABLE truncate_iso.multi_conflict_t (id int) USING iceberg;
  INSERT INTO truncate_iso.multi_conflict_t VALUES (5);
  CREATE TABLE truncate_iso.multi_shadow_t (id int) USING iceberg;
  INSERT INTO truncate_iso.multi_shadow_t VALUES (6);
}

# Capture the baseline in a separate transaction. Iceberg metadata is only
# materialized at pre-commit, so the INSERTs above advance metadata_location
# when the first setup block commits. Reading it in the same transaction would
# capture the pre-INSERT CREATE-time location and make every comparison false.
setup
{
  CREATE TABLE truncate_iso.locations AS
  SELECT relid, metadata_location
  FROM lakebase.iceberg_metadata
  WHERE relid IN (
    'truncate_iso.target_t'::regclass,
    'truncate_iso.abort_t'::regclass,
    'truncate_iso.multi_first_t'::regclass,
    'truncate_iso.multi_conflict_t'::regclass
  );
}

teardown
{
  DROP SCHEMA IF EXISTS truncate_iso CASCADE;
}

session s1
step s1_begin { BEGIN; }
step s1_truncate { TRUNCATE truncate_iso.target_t; }
step s1_commit { COMMIT; }
step s1_begin_abort { BEGIN; }
step s1_truncate_abort { TRUNCATE truncate_iso.abort_t; }
step s1_abort { ROLLBACK; }
step s1_begin_multi { BEGIN; }
step s1_truncate_multi {
  TRUNCATE truncate_iso.multi_first_t, truncate_iso.multi_conflict_t;
}
step s1_commit_multi { COMMIT; }

session s2
step s2_location_unchanged {
  SELECT current.metadata_location = original.metadata_location AS unchanged
  FROM lakebase.iceberg_metadata AS current
  JOIN truncate_iso.locations AS original USING (relid)
  WHERE current.relid = 'truncate_iso.target_t'::regclass;
}
step s2_swap_metadata {
  UPDATE lakebase.iceberg_metadata AS target
  SET metadata_location = shadow.metadata_location,
      previous_metadata_location = target.metadata_location
  FROM lakebase.iceberg_metadata AS shadow
  WHERE target.relid = 'truncate_iso.target_t'::regclass
    AND shadow.relid = 'truncate_iso.shadow_t'::regclass;
}
step s2_target_rows { SELECT array_agg(id ORDER BY id) AS rows FROM truncate_iso.target_t; }
step s2_abort_location_unchanged {
  SELECT current.metadata_location = original.metadata_location AS unchanged
  FROM lakebase.iceberg_metadata AS current
  JOIN truncate_iso.locations AS original USING (relid)
  WHERE current.relid = 'truncate_iso.abort_t'::regclass;
}
step s2_abort_rows { SELECT array_agg(id ORDER BY id) AS rows FROM truncate_iso.abort_t; }
step s2_swap_multi_metadata {
  UPDATE lakebase.iceberg_metadata AS target
  SET metadata_location = shadow.metadata_location,
      previous_metadata_location = target.metadata_location
  FROM lakebase.iceberg_metadata AS shadow
  WHERE target.relid = 'truncate_iso.multi_conflict_t'::regclass
    AND shadow.relid = 'truncate_iso.multi_shadow_t'::regclass;
}
step s2_multi_first_location_unchanged {
  SELECT current.metadata_location = original.metadata_location AS unchanged
  FROM lakebase.iceberg_metadata AS current
  JOIN truncate_iso.locations AS original USING (relid)
  WHERE current.relid = 'truncate_iso.multi_first_t'::regclass;
}
step s2_multi_first_rows {
  SELECT array_agg(id ORDER BY id) AS rows FROM truncate_iso.multi_first_t;
}
step s2_multi_conflict_rows {
  SELECT array_agg(id ORDER BY id) AS rows FROM truncate_iso.multi_conflict_t;
}

permutation s1_begin s1_truncate s2_location_unchanged s2_swap_metadata s1_commit s2_target_rows
permutation s1_begin_abort s1_truncate_abort s2_abort_location_unchanged s1_abort s2_abort_location_unchanged s2_abort_rows
permutation s1_begin_multi s1_truncate_multi s2_swap_multi_metadata s1_commit_multi s2_multi_first_location_unchanged s2_multi_first_rows s2_multi_conflict_rows
