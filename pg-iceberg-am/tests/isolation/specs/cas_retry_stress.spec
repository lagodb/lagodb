# Advanced concurrent update test for metadata_tracker.rs commit_all().
# This targets the top-level SnapshotDelta materialization plus catalog CAS
# retry loop under several conflict scenarios.

setup
{
  CREATE EXTENSION IF NOT EXISTS pg_iceberg_am;
  CREATE SCHEMA IF NOT EXISTS iceberg_test;
  
  -- Create test table
  CREATE TABLE iceberg_test.txn_test (
    id int,
    session_id int,
    value text,
    updated_at timestamp DEFAULT now()
  ) USING iceberg;
}

teardown
{
  DROP TABLE IF EXISTS iceberg_test.txn_test CASCADE;
  DROP SCHEMA IF EXISTS iceberg_test CASCADE;
}

# Session 1: First concurrent writer
# Note: BEGIN is issued explicitly via s1_begin in each permutation rather
# than in setup. Putting BEGIN in setup leaks an open transaction across
# permutations that do not commit it, producing spurious
# "there is already a transaction in progress" warnings.
session s1
setup { SET application_name = 'session1'; }
step s1_begin { BEGIN; }
step s1_insert_batch { 
  INSERT INTO iceberg_test.txn_test (id, session_id, value) 
  SELECT i, 1, 'session1_row_' || i 
  FROM generate_series(1, 100) i; 
}
step s1_commit { COMMIT; }
step s1_verify { 
  SELECT session_id, count(*) as row_count 
  FROM iceberg_test.txn_test 
  GROUP BY session_id 
  ORDER BY session_id; 
}
step s1_lock_share { LOCK TABLE iceberg.iceberg_metadata IN SHARE MODE; }
step s1_update_touched {
  UPDATE iceberg.iceberg_metadata
  SET previous_metadata_location = previous_metadata_location
  WHERE relid = 'iceberg_test.txn_test'::regclass;
}

# Session 2: Second concurrent writer
session s2
setup { SET application_name = 'session2'; }
step s2_begin { BEGIN; }
step s2_insert_batch { 
  INSERT INTO iceberg_test.txn_test (id, session_id, value) 
  SELECT i, 2, 'session2_row_' || i 
  FROM generate_series(1001, 1100) i; 
}
step s2_commit { COMMIT; }
step s2_verify { 
  SELECT session_id, count(*) as row_count 
  FROM iceberg_test.txn_test 
  GROUP BY session_id 
  ORDER BY session_id; 
}

# Session 3: Third concurrent writer
session s3
setup { SET application_name = 'session3'; }
step s3_begin { BEGIN; }
step s3_insert_batch { 
  INSERT INTO iceberg_test.txn_test (id, session_id, value) 
  SELECT i, 3, 'session3_row_' || i 
  FROM generate_series(2001, 2100) i; 
}
step s3_commit { COMMIT; }

# Session 4: Fourth writer for maximum contention
session s4
setup { SET application_name = 'session4'; }
step s4_begin { BEGIN; }
step s4_insert_batch { 
  INSERT INTO iceberg_test.txn_test (id, session_id, value) 
  SELECT i, 4, 'session4_row_' || i 
  FROM generate_series(3001, 3100) i; 
}
step s4_commit { COMMIT; }

# Session 5: Read-only observer to check intermediate states
session s5
step s5_count { SELECT count(*) as total_rows FROM iceberg_test.txn_test; }
step s5_final_verify { 
  SELECT 
    count(*) as total_rows,
    count(DISTINCT session_id) as unique_sessions,
    min(id) as min_id,
    max(id) as max_id
  FROM iceberg_test.txn_test;
}

# ========================================
# TEST PERMUTATIONS
# ========================================

# Permutation 1: Two-way concurrent commit
# Expected: Both sessions insert, one commits first, the other
# detects conflict in commit_all(), rebases, and retries CAS
permutation s1_begin s1_insert_batch s2_begin s2_insert_batch s1_commit s2_commit s1_verify s2_verify s5_final_verify

# Permutation 2: Maximum contention - 4 concurrent sessions
# Expected: Multiple CAS retries, all should eventually succeed
# This is the most aggressive test of the retry loop
permutation s1_begin s1_insert_batch s2_begin s2_insert_batch s3_begin s3_insert_batch s4_begin s4_insert_batch s1_commit s2_commit s3_commit s4_commit s5_final_verify

# Permutation 3: Sequential commits (baseline for comparison)
# Expected: No conflicts, no retries
permutation s1_begin s1_insert_batch s1_commit s2_begin s2_insert_batch s2_commit s3_begin s3_insert_batch s3_commit s5_final_verify

# Permutation 4: Interleaved inserts with delayed commits
# Expected: Readers observe each committed transaction as it becomes visible,
# while later writers still publish through commit_all() CAS.
permutation s1_begin s1_insert_batch s2_begin s2_insert_batch s3_begin s3_insert_batch s1_commit s5_count s2_commit s5_count s3_commit s5_final_verify

# Permutation 5: Rapid-fire commits
# Expected: Stress test the CAS loop with minimal delay
permutation s1_begin s1_insert_batch s1_commit s2_begin s2_insert_batch s2_commit s3_begin s3_insert_batch s3_commit s4_begin s4_insert_batch s4_commit s5_final_verify

# Permutation 6: Force Conflict using Explicit Locking
# Purpose: Strictly enforce the race condition that triggers the CAS retry loop.
# Mechanism:
# 1. S1 takes a SHARE lock. This lock is:
#    - Compatible with AccessShareLock (used by S2 while reading the catalog row)
#    - Conflicting with RowExclusiveLock (used by S2 during Update/Write)
# 2. S2 materializes its SnapshotDelta on global V0 but BLOCKS when trying to Update.
# 3. S1 commits. This updates global to V1 and releases the lock.
# 4. S2 wakes up. It expects V0 but sees V1 from S1's commit.
# 5. S2 triggers MetadataCatalogConflict, reports "rebasing...", and retries.
permutation s1_begin s1_insert_batch s2_begin s2_insert_batch s1_lock_share s2_commit s1_commit s5_final_verify

# Permutation 7: Force Conflict using Tuple Lock
# Purpose: Trigger the optimistic catalog update failure path.
# Mechanism:
# 1. S1 performs a trivial update on the metadata tuple. S1 holds the Tuple Lock (Xmax) but keeps transaction open.
# 2. S2 inserts data (buffered).
# 3. S2 attempts to Commit.
#    - S2 opens table (RowExclusive incompatible with nothing S1 holds). OK.
#    - S2 reads tuple. Sees V0 because S1 is still uncommitted.
#    - S2 attempts Update. Blocks on S1's tuple lock.
# 4. S1 commits.
# 5. S2 wakes up and sees the tuple was updated.
# 6. S2 triggers conflict handling and retries on the latest metadata location.
permutation s1_begin s1_update_touched s2_begin s2_insert_batch s2_commit s1_commit s5_final_verify
