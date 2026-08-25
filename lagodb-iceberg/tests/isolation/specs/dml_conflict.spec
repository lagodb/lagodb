# DML conflict detection: basic row-delta validation semantics.
#
# Permutations:
#   1. same_row_conflict    – two UPDATEs on the same row from the same scan
#                             snapshot; the second committer must be rejected
#                             (position-delete conflict)
#   2. unrelated_no_conflict – DML on rows in different data files must NOT
#                             conflict (position-delete check only fires when
#                             files overlap)
#   3. insert_only_merge     – two MERGEs scan an empty table, both choose
#                             NOT MATCHED, and add data without position-delete
#                             references; the second committer must still fail
#   4. merge_snapshot        – READ COMMITTED preserves the table's Snapshot
#                             policy, so both concurrent appends may commit
#   5. pg_serializable       – PostgreSQL SERIALIZABLE strengthens a table's
#                             Snapshot policy to Iceberg Serializable
#   6. dynamic_merge_scope   – a dynamic source join has no bounded static
#                             target predicate, so Serializable deliberately
#                             falls back to whole-table conflict detection

setup
{
  CREATE EXTENSION IF NOT EXISTS lagodb_iceberg;
  CREATE SCHEMA IF NOT EXISTS iceberg_dml_conflict;

  CREATE TABLE iceberg_dml_conflict.t (
    id int,
    label text
  ) USING iceberg;

  CREATE TABLE iceberg_dml_conflict.merge_t (
    id int,
    label text
  ) USING iceberg;

  CREATE TABLE iceberg_dml_conflict.merge_snapshot_t (
    id int,
    label text
  ) USING iceberg WITH (
    "write.merge.isolation-level" = 'snapshot'
  );

  CREATE TABLE iceberg_dml_conflict.merge_pg_serializable_t (
    id int,
    label text
  ) USING iceberg WITH (
    "write.merge.isolation-level" = 'snapshot'
  );

  CREATE TABLE iceberg_dml_conflict.merge_source_1 (
    id int,
    label text
  );

  CREATE TABLE iceberg_dml_conflict.merge_source_2 (
    id int,
    label text
  );

  INSERT INTO iceberg_dml_conflict.merge_source_1 VALUES (200, 's1');
  INSERT INTO iceberg_dml_conflict.merge_source_2 VALUES (201, 's2');
  INSERT INTO iceberg_dml_conflict.t VALUES (1, 'file_a');
  INSERT INTO iceberg_dml_conflict.t VALUES (100, 'file_b');
}

teardown
{
  DROP TABLE IF EXISTS iceberg_dml_conflict.merge_source_2 CASCADE;
  DROP TABLE IF EXISTS iceberg_dml_conflict.merge_source_1 CASCADE;
  DROP TABLE IF EXISTS iceberg_dml_conflict.merge_pg_serializable_t CASCADE;
  DROP TABLE IF EXISTS iceberg_dml_conflict.merge_snapshot_t CASCADE;
  DROP TABLE IF EXISTS iceberg_dml_conflict.merge_t CASCADE;
  DROP TABLE IF EXISTS iceberg_dml_conflict.t CASCADE;
  DROP SCHEMA IF EXISTS iceberg_dml_conflict CASCADE;
}

session s1
step s1_begin  { BEGIN; }
step s1_begin_serializable { BEGIN ISOLATION LEVEL SERIALIZABLE; }
step s1_update { UPDATE iceberg_dml_conflict.t SET label = 's1' WHERE id = 1; }
step s1_delete { DELETE FROM iceberg_dml_conflict.t WHERE id = 1; }
step s1_merge  { MERGE INTO iceberg_dml_conflict.merge_t AS target USING (VALUES (200, 's1')) AS source(id, label) ON target.id = source.id WHEN NOT MATCHED THEN INSERT (id, label) VALUES (source.id, source.label); }
step s1_merge_dynamic { MERGE INTO iceberg_dml_conflict.merge_t AS target USING iceberg_dml_conflict.merge_source_1 AS source ON target.id = source.id WHEN NOT MATCHED THEN INSERT (id, label) VALUES (source.id, source.label); }
step s1_merge_snapshot { MERGE INTO iceberg_dml_conflict.merge_snapshot_t AS target USING (VALUES (300, 's1')) AS source(id, label) ON target.id = source.id WHEN NOT MATCHED THEN INSERT (id, label) VALUES (source.id, source.label); }
step s1_merge_pg_serializable { MERGE INTO iceberg_dml_conflict.merge_pg_serializable_t AS target USING (VALUES (400, 's1')) AS source(id, label) ON target.id = source.id WHEN NOT MATCHED THEN INSERT (id, label) VALUES (source.id, source.label); }
step s1_commit { COMMIT; }

session s2
step s2_begin  { BEGIN; }
step s2_begin_serializable { BEGIN ISOLATION LEVEL SERIALIZABLE; }
step s2_update { UPDATE iceberg_dml_conflict.t SET label = 's2' WHERE id = 1; }
step s2_update_other { UPDATE iceberg_dml_conflict.t SET label = 's2' WHERE id = 100; }
step s2_merge { MERGE INTO iceberg_dml_conflict.merge_t AS target USING (VALUES (200, 's2')) AS source(id, label) ON target.id = source.id WHEN NOT MATCHED THEN INSERT (id, label) VALUES (source.id, source.label); }
step s2_merge_dynamic_other_key { MERGE INTO iceberg_dml_conflict.merge_t AS target USING iceberg_dml_conflict.merge_source_2 AS source ON target.id = source.id WHEN NOT MATCHED THEN INSERT (id, label) VALUES (source.id, source.label); }
step s2_merge_snapshot { MERGE INTO iceberg_dml_conflict.merge_snapshot_t AS target USING (VALUES (300, 's2')) AS source(id, label) ON target.id = source.id WHEN NOT MATCHED THEN INSERT (id, label) VALUES (source.id, source.label); }
step s2_merge_pg_serializable { MERGE INTO iceberg_dml_conflict.merge_pg_serializable_t AS target USING (VALUES (400, 's2')) AS source(id, label) ON target.id = source.id WHEN NOT MATCHED THEN INSERT (id, label) VALUES (source.id, source.label); }
step s2_commit { COMMIT; }

session observer
step verify { SELECT string_agg(id || ':' || label, ',' ORDER BY id) AS rows FROM iceberg_dml_conflict.t; }
step verify_merge { SELECT string_agg(id || ':' || label, ',' ORDER BY id) AS rows FROM iceberg_dml_conflict.merge_t; }
step verify_merge_snapshot { SELECT count(*) AS rows FROM iceberg_dml_conflict.merge_snapshot_t; }
step verify_merge_pg_serializable { SELECT count(*) AS rows FROM iceberg_dml_conflict.merge_pg_serializable_t; }

# 1. Same row updated by both txns → second committer MUST fail
permutation s1_begin s1_update s2_begin s2_update s1_commit s2_commit verify

# 2. Different rows in separate data files → no conflict
permutation s1_begin s1_delete s2_begin s2_update_other s1_commit s2_commit verify

# 3. Insert-only MERGE still depends on the target scan even with no references
permutation s1_begin s1_merge s2_begin s2_merge s1_commit s2_commit verify_merge

# 4. READ COMMITTED does not strengthen an explicit Snapshot table policy
permutation s1_begin s1_merge_snapshot s2_begin s2_merge_snapshot s1_commit s2_commit verify_merge_snapshot

# 5. PostgreSQL SERIALIZABLE strengthens Snapshot to Iceberg Serializable
permutation s1_begin_serializable s1_merge_pg_serializable s2_begin_serializable s2_merge_pg_serializable s1_commit s2_commit verify_merge_pg_serializable

# 6. Dynamic source keys are not accumulated into an unbounded predicate.
# Without a static target filter, Serializable uses a whole-table scope, so
# even unrelated keys conflict. This is conservative isolation, not key-level
# uniqueness enforcement.
permutation s1_begin s1_merge_dynamic s2_begin s2_merge_dynamic_other_key s1_commit s2_commit verify_merge
