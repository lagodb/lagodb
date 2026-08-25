# DML conflict filter: validates the serializable data-file conflict filter.
#
# The conflict filter narrows the set of concurrently-added data files that are
# considered conflicting. This file tests all aspects: DML operation
# combinations, overlapping writes, expression translation fallback, and
# special query patterns.
#
# Permutations (operations – filter narrows correctly):
#   1. delete_vs_insert   – DELETE id=1, INSERT id=200 → no conflict
#   2. update_vs_insert   – UPDATE id=1, INSERT id=200 → no conflict
#   3. delete_vs_update   – DELETE id=1, UPDATE id=100 → no conflict
#   4. update_vs_update   – UPDATE id=1, UPDATE id=100 → no conflict
#   5. overlap            – DELETE id=1, INSERT id=1 → MUST conflict
#
# Permutations (expression fallback – conservative safety):
#   6. untranslatable     – abs(id)=1 → AlwaysTrue → conflict
#   7. mixed_and          – id=1 AND abs(id)=1 → supported conjunct narrows → no conflict
#   8. unsupported_or     – id=1 OR abs(id)=1 → AlwaysTrue → conflict
#   9. generic_param      – prepared stmt with $1 → AlwaysTrue → conflict
#  10. self_join          – self-join UPDATE uses correct target RTI → no conflict

setup
{
  CREATE EXTENSION IF NOT EXISTS lagodb_iceberg;
  CREATE SCHEMA IF NOT EXISTS iceberg_cf;

  CREATE TABLE iceberg_cf.t (
    id int,
    label text
  ) USING iceberg;

  INSERT INTO iceberg_cf.t VALUES (1, 'base');
  INSERT INTO iceberg_cf.t VALUES (100, 'source');
}

teardown
{
  DROP TABLE IF EXISTS iceberg_cf.t CASCADE;
  DROP SCHEMA IF EXISTS iceberg_cf CASCADE;
}

session s1
step s1_begin          { BEGIN; }
step s1_insert         { INSERT INTO iceberg_cf.t VALUES (200, 'new'); }
step s1_update         { UPDATE iceberg_cf.t SET label = 's1' WHERE id = 100; }
step s1_insert_overlap { INSERT INTO iceberg_cf.t VALUES (1, 'inserted'); }
step s1_commit         { COMMIT; }

session s2
step s2_begin         { BEGIN; }
step s2_delete        { DELETE FROM iceberg_cf.t WHERE id = 1; }
step s2_update        { UPDATE iceberg_cf.t SET label = 's2' WHERE id = 1; }
step s2_del_abs       { DELETE FROM iceberg_cf.t WHERE abs(id) = 1; }
step s2_del_mixed_and { DELETE FROM iceberg_cf.t WHERE id = 1 AND abs(id) = 1; }
step s2_del_or        { DELETE FROM iceberg_cf.t WHERE id = 1 OR abs(id) = 1; }
step s2_update_self   { UPDATE iceberg_cf.t AS target SET label = 's2' FROM iceberg_cf.t AS source WHERE target.id = 1 AND source.id = 100; }
step s2_commit        { COMMIT; }

session s2_param
step s2p_generic { SET plan_cache_mode = force_generic_plan; }
step s2p_prepare { PREPARE delete_param(int) AS DELETE FROM iceberg_cf.t WHERE id = $1; }
step s2p_begin   { BEGIN; }
step s2p_delete  { EXECUTE delete_param(1); }
step s2p_commit  { COMMIT; }

session observer
step verify { SELECT string_agg(id || ':' || label, ',' ORDER BY id, label) AS rows FROM iceberg_cf.t; }

# --- Operations: filter narrows correctly ---

# 1. DELETE id=1 + INSERT id=200 → no conflict
permutation s2_begin s2_delete s1_begin s1_insert s1_commit s2_commit verify

# 2. UPDATE id=1 + INSERT id=200 → no conflict
permutation s2_begin s2_update s1_begin s1_insert s1_commit s2_commit verify

# 3. DELETE id=1 + UPDATE id=100 → no conflict
permutation s2_begin s2_delete s1_begin s1_update s1_commit s2_commit verify

# 4. UPDATE id=1 + UPDATE id=100 → no conflict
permutation s2_begin s2_update s1_begin s1_update s1_commit s2_commit verify

# 5. DELETE id=1 + INSERT id=1 → MUST conflict (overlapping range)
permutation s2_begin s2_delete s1_begin s1_insert_overlap s1_commit s2_commit verify

# --- Expression fallback: conservative safety ---

# 6. abs(id) is not translatable → AlwaysTrue → conflict
permutation s2_begin s2_del_abs s1_begin s1_insert s1_commit s2_commit verify

# 7. id=1 AND abs(id)=1 → supported conjunct narrows → no conflict
permutation s2_begin s2_del_mixed_and s1_begin s1_insert s1_commit s2_commit verify

# 8. id=1 OR abs(id)=1 → AlwaysTrue → conflict
permutation s2_begin s2_del_or s1_begin s1_insert s1_commit s2_commit verify

# 9. Generic plan param ($1) → AlwaysTrue → conflict
permutation s2p_generic s2p_prepare s2p_begin s2p_delete s1_begin s1_insert s1_commit s2p_commit verify

# 10. Self-join: target RTI identifies correct relation → no conflict
permutation s2_begin s2_update_self s1_begin s1_insert s1_commit s2_commit verify
