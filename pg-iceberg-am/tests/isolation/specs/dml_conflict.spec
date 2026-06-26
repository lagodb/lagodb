# DML conflict detection: basic row-delta validation semantics.
#
# Permutations:
#   1. same_row_conflict    – two UPDATEs on the same row from the same scan
#                             snapshot; the second committer must be rejected
#                             (position-delete conflict)
#   2. unrelated_no_conflict – DML on rows in different data files must NOT
#                             conflict (position-delete check only fires when
#                             files overlap)

setup
{
  CREATE EXTENSION IF NOT EXISTS pg_iceberg_am;
  CREATE SCHEMA IF NOT EXISTS iceberg_dml_conflict;

  CREATE TABLE iceberg_dml_conflict.t (
    id int,
    label text
  ) USING iceberg;

  INSERT INTO iceberg_dml_conflict.t VALUES (1, 'file_a');
  INSERT INTO iceberg_dml_conflict.t VALUES (100, 'file_b');
}

teardown
{
  DROP TABLE IF EXISTS iceberg_dml_conflict.t CASCADE;
  DROP SCHEMA IF EXISTS iceberg_dml_conflict CASCADE;
}

session s1
step s1_begin  { BEGIN; }
step s1_update { UPDATE iceberg_dml_conflict.t SET label = 's1' WHERE id = 1; }
step s1_delete { DELETE FROM iceberg_dml_conflict.t WHERE id = 1; }
step s1_commit { COMMIT; }

session s2
step s2_begin  { BEGIN; }
step s2_update { UPDATE iceberg_dml_conflict.t SET label = 's2' WHERE id = 1; }
step s2_update_other { UPDATE iceberg_dml_conflict.t SET label = 's2' WHERE id = 100; }
step s2_commit { COMMIT; }

session observer
step verify { SELECT string_agg(id || ':' || label, ',' ORDER BY id) AS rows FROM iceberg_dml_conflict.t; }

# 1. Same row updated by both txns → second committer MUST fail
permutation s1_begin s1_update s2_begin s2_update s1_commit s2_commit verify

# 2. Different rows in separate data files → no conflict
permutation s1_begin s1_delete s2_begin s2_update_other s1_commit s2_commit verify
