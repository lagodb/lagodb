-- metadata_tracking.sql
-- Test transaction-aware metadata location tracking
DROP EXTENSION IF EXISTS lagodb_iceberg CASCADE;
CREATE EXTENSION lagodb_iceberg;

-- Helper function to get metadata info without random UUIDs
CREATE OR REPLACE FUNCTION get_meta_info(relname text) 
RETURNS TABLE(has_meta boolean, has_prev boolean, meta_equals_prev boolean) AS $$
BEGIN
    RETURN QUERY 
    SELECT 
        metadata_location IS NOT NULL,
        previous_metadata_location IS NOT NULL,
        metadata_location = previous_metadata_location
    FROM iceberg.iceberg_metadata
    WHERE relid = relname::regclass;
END;
$$ LANGUAGE plpgsql;

--
-- Test 0: Setup
--
CREATE TABLE test_meta (id int) USING iceberg;
-- Initial state: metadata_location should exist, previous should be null
-- (Assuming CREATE TABLE sets an initial metadata file)
SELECT has_meta, has_prev FROM get_meta_info('test_meta');

-- Capture initial metadata location
SELECT metadata_location AS v0 FROM iceberg.iceberg_metadata WHERE relid = 'test_meta'::regclass \gset

--
-- Scenario 1: Subtransaction success commit
--
BEGIN;
  INSERT INTO test_meta VALUES (1);
  SELECT metadata_location AS v1 FROM iceberg.iceberg_metadata WHERE relid = 'test_meta'::regclass \gset
  
  -- Inside transaction, catalog should still show v0 (no update yet)
  SELECT metadata_location = :'v0' as still_v0 FROM iceberg.iceberg_metadata WHERE relid = 'test_meta'::regclass;
  
  -- But data should be visible!
  SELECT * FROM test_meta ORDER BY id;
  
  SAVEPOINT sp1;
    INSERT INTO test_meta VALUES (2);
    SELECT * FROM test_meta ORDER BY id;
    SELECT metadata_location AS v2 FROM iceberg.iceberg_metadata WHERE relid = 'test_meta'::regclass \gset
  RELEASE SAVEPOINT sp1;
COMMIT;

-- After commit, metadata should be updated to the latest (v2-ish, actually the one from second insert)
-- and previous should be the one from first insert (v1-ish)
SELECT 
    metadata_location != :'v0' as updated,
    previous_metadata_location != :'v0' as prev_updated,
    metadata_location != previous_metadata_location as meta_diff_prev
FROM iceberg.iceberg_metadata WHERE relid = 'test_meta'::regclass;

-- Capture current state as v_base
SELECT metadata_location AS v_base FROM iceberg.iceberg_metadata WHERE relid = 'test_meta'::regclass \gset

--
-- Scenario 2: Subtransaction rollback
--
BEGIN;
  INSERT INTO test_meta VALUES (3); -- metadata -> v_new
  SELECT metadata_location AS v_new FROM iceberg.iceberg_metadata WHERE relid = 'test_meta'::regclass \gset
  
  SAVEPOINT sp1;
    INSERT INTO test_meta VALUES (4); -- metadata -> v_rolled
  ROLLBACK TO SAVEPOINT sp1;
COMMIT;

-- After commit, metadata should be v_new, and previous should be v_base
SELECT 
    metadata_location != :'v_base' as updated,
    previous_metadata_location = :'v_base' as prev_matches_base
FROM iceberg.iceberg_metadata WHERE relid = 'test_meta'::regclass;

-- Capture current state as v_base2
SELECT metadata_location AS v_base2 FROM iceberg.iceberg_metadata WHERE relid = 'test_meta'::regclass \gset

--
-- Scenario 3: Subtransaction initial modification, then rollback
--
BEGIN;
  SAVEPOINT sp1;
    INSERT INTO test_meta VALUES (5);
  ROLLBACK TO SAVEPOINT sp1;
COMMIT;

-- After commit, metadata should still be v_base2
SELECT 
    metadata_location = :'v_base2' as still_base2
FROM iceberg.iceberg_metadata WHERE relid = 'test_meta'::regclass;


--
-- Scenario 4: Multi-level nested subtransactions
--
BEGIN;
  INSERT INTO test_meta VALUES (6); -- v_l1
  SELECT metadata_location AS v_l1_old FROM iceberg.iceberg_metadata WHERE relid = 'test_meta'::regclass \gset
  
  SAVEPOINT sp1;
    INSERT INTO test_meta VALUES (7); -- v_l2
    SELECT metadata_location AS v_l2_old FROM iceberg.iceberg_metadata WHERE relid = 'test_meta'::regclass \gset
    
    SAVEPOINT sp2;
      INSERT INTO test_meta VALUES (8); -- v_l3
    ROLLBACK TO SAVEPOINT sp2; -- Rollback to v_l2 state
    
  RELEASE SAVEPOINT sp1;
COMMIT;

-- After commit, metadata should be what it was after level 2 insert
SELECT 
    metadata_location != :'v_base2' as updated,
    previous_metadata_location != :'v_base2' as prev_updated
FROM iceberg.iceberg_metadata WHERE relid = 'test_meta'::regclass;

--
-- Scenario 5: Multiple tables in same transaction
--
CREATE TABLE test_meta_2 (id int) USING iceberg;
SELECT metadata_location AS v2_initial FROM iceberg.iceberg_metadata WHERE relid = 'test_meta_2'::regclass \gset
SELECT metadata_location AS v1_initial FROM iceberg.iceberg_metadata WHERE relid = 'test_meta'::regclass \gset

BEGIN;
  INSERT INTO test_meta VALUES (9);
  INSERT INTO test_meta_2 VALUES (10);
COMMIT;

SELECT 
    (SELECT metadata_location != :'v1_initial' FROM iceberg.iceberg_metadata WHERE relid = 'test_meta'::regclass) as t1_updated,
    (SELECT metadata_location != :'v2_initial' FROM iceberg.iceberg_metadata WHERE relid = 'test_meta_2'::regclass) as t2_updated;

--
-- Scenario 6: Main transaction rollback
--
SELECT metadata_location AS v1_before_rollback FROM iceberg.iceberg_metadata WHERE relid = 'test_meta'::regclass \gset

BEGIN;
  INSERT INTO test_meta VALUES (11);
ROLLBACK;

SELECT metadata_location = :'v1_before_rollback' as rollback_success
FROM iceberg.iceberg_metadata WHERE relid = 'test_meta'::regclass;

--
-- Scenario 7: Standard Multi-statement Transaction with Multiple Inserts
--
CREATE TABLE test_multi_insert (id int) USING iceberg;

BEGIN;
  INSERT INTO test_multi_insert VALUES (1);
  -- Check visibility of first insert
  SELECT * FROM test_multi_insert ORDER BY id;
  
  INSERT INTO test_multi_insert VALUES (2);
  -- Check visibility of both inserts
  SELECT * FROM test_multi_insert ORDER BY id;
  
  INSERT INTO test_multi_insert VALUES (3);
COMMIT;

-- Verify all data persists after commit
SELECT * FROM test_multi_insert ORDER BY id;

--
-- Scenario 8: Sibling savepoint after RELEASE, then ROLLBACK TO sibling
--
-- Exercises RELEASE-time promotion of per-table state to the parent
-- subtransaction. Without promotion, the released savepoint's writes
-- alias the sibling savepoint's nest level and get rolled back.
--
CREATE TABLE test_sibling_rb (id int) USING iceberg;
INSERT INTO test_sibling_rb VALUES (0); -- pre-existing baseline outside the txn

BEGIN;
  SAVEPOINT s1;
    INSERT INTO test_sibling_rb VALUES (100);
  RELEASE SAVEPOINT s1;        -- 100 must promote from level 2 to level 1

  SAVEPOINT s2;
    INSERT INTO test_sibling_rb VALUES (200);
  ROLLBACK TO SAVEPOINT s2;    -- must drop only 200, never touch 100
COMMIT;

-- Expected: {0, 100}; 200 was rolled back, 100 was already promoted out.
SELECT id FROM test_sibling_rb ORDER BY id;

--
-- Scenario 9: Table first registered inside a released savepoint
--
-- The table is touched for the first time inside s1 (so
-- `first_modified_at_level` would be 2 without promotion). After RELEASE s1
-- the state belongs to the parent. A sibling savepoint at level 2 must
-- not be able to delete the table state.
--
CREATE TABLE test_sibling_first (id int) USING iceberg;

BEGIN;
  SAVEPOINT s1;
    INSERT INTO test_sibling_first VALUES (1); -- first touch at level 2
  RELEASE SAVEPOINT s1;        -- promote first_modified_at_level to 1

  SAVEPOINT s2;
    INSERT INTO test_sibling_first VALUES (2);
  ROLLBACK TO SAVEPOINT s2;    -- must keep 1; without promotion, the
                               -- whole TableState would be discarded.
COMMIT;

-- Expected: {1}; 2 was rolled back, 1 must survive.
SELECT id FROM test_sibling_first ORDER BY id;

-- Cleanup
DROP TABLE test_multi_insert;
DROP TABLE test_meta;
DROP TABLE test_meta_2;
DROP TABLE test_sibling_rb;
DROP TABLE test_sibling_first;
DROP FUNCTION get_meta_info(text);
