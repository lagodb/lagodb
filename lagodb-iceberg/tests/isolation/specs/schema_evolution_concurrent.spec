# Schema evolution conflict handling.
#
# PostgreSQL serializes real ALTER TABLE statements with AccessExclusiveLock,
# so this spec simulates an external Iceberg schema commit by swapping the
# target table's metadata location to a separately evolved shadow table.

setup
{
  CREATE EXTENSION IF NOT EXISTS lagodb_iceberg;
  CREATE SCHEMA IF NOT EXISTS schema_evolution_iso;
  CREATE TABLE schema_evolution_iso.concurrent_t (
    id int
  ) USING iceberg;
  CREATE TABLE schema_evolution_iso.shadow_t (
    id int
  ) USING iceberg;
  ALTER TABLE schema_evolution_iso.shadow_t ADD COLUMN external_col int;
  CREATE TABLE schema_evolution_iso.data_rebase_t (
    id int
  ) USING iceberg;
  CREATE TABLE schema_evolution_iso.data_shadow_t (
    id int
  ) USING iceberg;
  INSERT INTO schema_evolution_iso.data_shadow_t VALUES (10);
}

teardown
{
  DROP TABLE IF EXISTS schema_evolution_iso.concurrent_t CASCADE;
  DROP TABLE IF EXISTS schema_evolution_iso.shadow_t CASCADE;
  DROP TABLE IF EXISTS schema_evolution_iso.data_rebase_t CASCADE;
  DROP TABLE IF EXISTS schema_evolution_iso.data_shadow_t CASCADE;
  DROP SCHEMA IF EXISTS schema_evolution_iso CASCADE;
}

session s1
step s1_begin { BEGIN; }
step s1_add_local { ALTER TABLE schema_evolution_iso.concurrent_t ADD COLUMN local_col int; }
step s1_insert_local { INSERT INTO schema_evolution_iso.concurrent_t (id, local_col) VALUES (1, 10); }
step s1_commit { COMMIT; }
step s1_begin_rebase { BEGIN; }
step s1_add_rebase { ALTER TABLE schema_evolution_iso.data_rebase_t ADD COLUMN added_col int; }
step s1_commit_rebase { COMMIT; }

session s2
step s2_swap_metadata {
  UPDATE iceberg.iceberg_metadata AS target
  SET metadata_location = shadow.metadata_location,
      previous_metadata_location = target.metadata_location
  FROM iceberg.iceberg_metadata AS shadow
  WHERE target.relid = 'schema_evolution_iso.concurrent_t'::regclass
    AND shadow.relid = 'schema_evolution_iso.shadow_t'::regclass;
}
step s2_swap_data_metadata {
  UPDATE iceberg.iceberg_metadata AS target
  SET metadata_location = shadow.metadata_location,
      previous_metadata_location = target.metadata_location
  FROM iceberg.iceberg_metadata AS shadow
  WHERE target.relid = 'schema_evolution_iso.data_rebase_t'::regclass
    AND shadow.relid = 'schema_evolution_iso.data_shadow_t'::regclass;
}

session s3
step s3_columns {
  SELECT string_agg(attname, ',' ORDER BY attnum) AS cols
  FROM pg_attribute
  WHERE attrelid = 'schema_evolution_iso.concurrent_t'::regclass
    AND attnum > 0
    AND NOT attisdropped;
}
step s3_rows { SELECT count(*) AS rows FROM schema_evolution_iso.concurrent_t; }
step s3_rebase_columns {
  SELECT string_agg(attname, ',' ORDER BY attnum) AS cols
  FROM pg_attribute
  WHERE attrelid = 'schema_evolution_iso.data_rebase_t'::regclass
    AND attnum > 0
    AND NOT attisdropped;
}
step s3_rebase_rows {
  SELECT count(*) AS rows, count(added_col) AS added_values
  FROM schema_evolution_iso.data_rebase_t;
}

permutation s1_begin s1_add_local s1_insert_local s2_swap_metadata s1_commit s3_columns s3_rows
permutation s1_begin_rebase s1_add_rebase s2_swap_data_metadata s1_commit_rebase s3_rebase_columns s3_rebase_rows
