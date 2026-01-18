# Test subtransaction rollback with concurrent updates
# This tests the interaction between subtransactions (SAVEPOINTs)
# and concurrent metadata updates in metadata_tracking.rs

setup
{
  CREATE EXTENSION IF NOT EXISTS pg_am_iceberg;
  CREATE SCHEMA IF NOT EXISTS iceberg_test;
  
  CREATE TABLE iceberg_test.savepoint_test (
    id int,
    session_name text,
    savepoint_level int,
    value text
  ) USING iceberg;
}

teardown
{
  DROP TABLE IF EXISTS iceberg_test.savepoint_test CASCADE;
  DROP SCHEMA IF EXISTS iceberg_test CASCADE;
}

# Session 1: Uses savepoints with rollback
session s1
step s1_begin { BEGIN; }
step s1_insert_l1 { 
  INSERT INTO iceberg_test.savepoint_test 
  VALUES (1, 's1', 1, 'level1'); 
}
step s1_savepoint_sp1 { SAVEPOINT sp1; }
step s1_insert_l2 { 
  INSERT INTO iceberg_test.savepoint_test 
  VALUES (2, 's1', 2, 'level2'); 
}
step s1_savepoint_sp2 { SAVEPOINT sp2; }
step s1_insert_l3 { 
  INSERT INTO iceberg_test.savepoint_test 
  VALUES (3, 's1', 3, 'level3'); 
}
step s1_rollback_sp2 { ROLLBACK TO sp2; }
step s1_insert_l2b { 
  INSERT INTO iceberg_test.savepoint_test 
  VALUES (4, 's1', 2, 'level2_after_rollback'); 
}
step s1_commit { COMMIT; }
step s1_verify { 
  SELECT id, savepoint_level, value 
  FROM iceberg_test.savepoint_test 
  WHERE session_name = 's1'
  ORDER BY id; 
}

# Session 2: Concurrent writer
session s2
step s2_begin { BEGIN; }
step s2_insert { 
  INSERT INTO iceberg_test.savepoint_test 
  VALUES (100, 's2', 0, 'concurrent'); 
}
step s2_commit { COMMIT; }

# Session 3: Observer
session s3
step s3_count { 
  SELECT count(*) as total, count(DISTINCT session_name) as sessions
  FROM iceberg_test.savepoint_test; 
}

# Test 1: Savepoint rollback without concurrency
# Expected: Level 3 insert should be rolled back
# Tests rollback_to_level() at L398-L407
permutation s1_begin s1_insert_l1 s1_savepoint_sp1 s1_insert_l2 s1_savepoint_sp2 s1_insert_l3 s1_rollback_sp2 s1_insert_l2b s1_commit s1_verify s3_count

# Test 2: Savepoint with concurrent commit before rollback
# Expected: S2 commits first, S1 rebases after rollback, should see S2's changes
permutation s1_begin s1_insert_l1 s1_savepoint_sp1 s1_insert_l2 s2_begin s2_insert s2_commit s1_savepoint_sp2 s1_insert_l3 s1_rollback_sp2 s1_commit s1_verify s3_count

# Test 3: Concurrent commit during savepoint operations
# Expected: Tests that last_base_metadata_location is correctly managed
# across savepoints and concurrent updates (commit_all rebase logic)
permutation s1_begin s1_insert_l1 s1_savepoint_sp1 s2_begin s2_insert s1_insert_l2 s2_commit s1_commit s1_verify s3_count
