# Schema evolution conflict handling.
#
# PostgreSQL serializes real ALTER TABLE statements with AccessExclusiveLock,
# so this spec simulates an external Iceberg schema commit by swapping the
# target table's metadata location to a prepared external commit. Shadow tables
# supply the new schema/data, while the target UUID and storage root stay fixed.

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
  CREATE TABLE schema_evolution_iso.external_commits (relid oid, metadata_location text);

}

teardown
{
  DROP TABLE IF EXISTS schema_evolution_iso.concurrent_t CASCADE;
  DROP TABLE IF EXISTS schema_evolution_iso.shadow_t CASCADE;
  DROP TABLE IF EXISTS schema_evolution_iso.data_rebase_t CASCADE;
  DROP TABLE IF EXISTS schema_evolution_iso.data_shadow_t CASCADE;
  DROP TABLE schema_evolution_iso.external_commits;
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
# Global setup commits the shadow schema/data before their metadata is copied.
setup
{
  DO $$
  DECLARE
    pair record;
    target_path text;
    shadow_path text;
    target_json jsonb;
    external_json jsonb;
    new_path text;
    payload oid;
  BEGIN
    FOR pair IN SELECT * FROM (VALUES
      ('schema_evolution_iso.concurrent_t'::regclass, 'schema_evolution_iso.shadow_t'::regclass),
      ('schema_evolution_iso.data_rebase_t'::regclass, 'schema_evolution_iso.data_shadow_t'::regclass)
    ) AS sources(target_rel, shadow_rel)
    LOOP
      SELECT metadata_location INTO target_path FROM iceberg.iceberg_metadata WHERE relid = pair.target_rel;
      SELECT metadata_location INTO shadow_path FROM iceberg.iceberg_metadata WHERE relid = pair.shadow_rel;
      target_json := pg_read_file(target_path)::jsonb;
      external_json := pg_read_file(shadow_path)::jsonb;
      external_json := jsonb_set(external_json, ARRAY['location'], target_json->'location');
      external_json := jsonb_set(external_json, ARRAY['table-uuid'], target_json->'table-uuid');
      new_path := regexp_replace(target_path, '[^/]+$', '00001-' || gen_random_uuid() || '.metadata.json');
      -- Export exact JSON bytes; COPY text would escape the JSON payload.
      payload := lo_from_bytea(0, convert_to(external_json::text, 'UTF8'));
      PERFORM lo_export(payload, current_setting('data_directory') || '/' || new_path);
      PERFORM lo_unlink(payload);
      INSERT INTO schema_evolution_iso.external_commits VALUES (pair.target_rel, new_path);
    END LOOP;
  END
  $$;
}
step s2_swap_metadata {
  UPDATE iceberg.iceberg_metadata AS target
  SET metadata_location = external.metadata_location,
      previous_metadata_location = target.metadata_location
  FROM schema_evolution_iso.external_commits AS external
  WHERE target.relid = 'schema_evolution_iso.concurrent_t'::regclass
    AND external.relid = target.relid;
}
step s2_swap_data_metadata {
  UPDATE iceberg.iceberg_metadata AS target
  SET metadata_location = external.metadata_location,
      previous_metadata_location = target.metadata_location
  FROM schema_evolution_iso.external_commits AS external
  WHERE target.relid = 'schema_evolution_iso.data_rebase_t'::regclass
    AND external.relid = target.relid;
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
