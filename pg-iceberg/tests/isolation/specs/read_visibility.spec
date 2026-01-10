# Basic read visibility test for pg-iceberg
# Verifies that uncommitted data is not visible to other sessions
# and becomes visible after commit.

setup
{
  CREATE EXTENSION IF NOT EXISTS pg_iceberg;
  CREATE SCHEMA IF NOT EXISTS visibility_test;
  CREATE TABLE visibility_test.read_viz (
    id int,
    value text
  ) USING iceberg;
}

teardown
{
  DROP TABLE IF EXISTS visibility_test.read_viz CASCADE;
  DROP SCHEMA IF EXISTS visibility_test CASCADE;
}

session s1
step s1_begin { BEGIN; }
step s1_insert { INSERT INTO visibility_test.read_viz VALUES (1, 's1_inserted'); }
step s1_commit { COMMIT; }

session s2
step s2_select { SELECT * FROM visibility_test.read_viz ORDER BY id; }

session s3
setup { BEGIN; }
step s3_insert_1 { INSERT INTO visibility_test.read_viz VALUES (10, 's3_v1'); }
step s3_insert_2 { INSERT INTO visibility_test.read_viz VALUES (20, 's3_v2'); }
step s3_select { SELECT * FROM visibility_test.read_viz ORDER BY id; }
step s3_commit { COMMIT; }

# Test 1: Standard Read Committed visibility
# Sequence:
# 1. S1 starts and inserts (not committed)
# 2. S2 selects (should see nothing)
# 3. S1 commits
# 4. S2 selects (should see S1's row)
permutation s1_begin s1_insert s2_select s1_commit s2_select

# Test 2: Multi-step insertion visibility
# Sequence:
# 1. S3 starts and inserts row 1
# 2. S2 query (sees nothing)
# 3. S3 inserts row 2
# 4. S2 query (sees nothing)
# 5. S3 commits
# 6. S2 query (sees 10 and 20)
permutation s3_insert_1 s2_select s3_insert_2 s2_select s3_commit s2_select

# Test 3: Read Committed mid-transaction visibility
# Sequence:
# 1. S3 starts (in-progress) and inserts row 10
# 2. S1 starts, inserts row 1 and commits (done)
# 3. S3 queries (should see ITS OWN row 10 AND S1's row 1)
# 4. S3 inserts row 20 and commits
# 5. S3 queries (should see 1, 10, 20)
permutation s3_insert_1 s1_begin s1_insert s1_commit s3_select s3_insert_2 s3_commit s3_select
