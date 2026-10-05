# Local TRUNCATE publishes a new generation transactionally and retains PG locks.

setup
{
  CREATE EXTENSION IF NOT EXISTS lagodb_iceberg;
  CREATE SCHEMA IF NOT EXISTS truncate_iso;
  CREATE TABLE truncate_iso.target_t (id int) USING iceberg;
  INSERT INTO truncate_iso.target_t VALUES (1);
  CREATE TABLE truncate_iso.abort_t (id int) USING iceberg;
  INSERT INTO truncate_iso.abort_t VALUES (3);
  CREATE TABLE truncate_iso.multi_first_t (id int) USING iceberg;
  INSERT INTO truncate_iso.multi_first_t VALUES (4);
  CREATE TABLE truncate_iso.multi_second_t (id int) USING iceberg;
  INSERT INTO truncate_iso.multi_second_t VALUES (5);
}

# INSERT metadata is materialized when the first setup transaction commits.
setup
{
  CREATE TABLE truncate_iso.locations AS
  SELECT relid, metadata_location
  FROM iceberg.iceberg_metadata
  WHERE relid IN (
    'truncate_iso.target_t'::regclass,
    'truncate_iso.abort_t'::regclass,
    'truncate_iso.multi_first_t'::regclass,
    'truncate_iso.multi_second_t'::regclass
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
step s1_truncate_multi { TRUNCATE truncate_iso.multi_first_t, truncate_iso.multi_second_t; }
step s1_commit_multi { COMMIT; }

session s2
step s2_location_unchanged {
  SELECT current.metadata_location = original.metadata_location AS unchanged
  FROM iceberg.iceberg_metadata AS current
  JOIN truncate_iso.locations AS original USING (relid)
  WHERE current.relid = 'truncate_iso.target_t'::regclass;
}
step s2_insert { INSERT INTO truncate_iso.target_t VALUES (2); }
step s2_target_rows { SELECT array_agg(id ORDER BY id) AS rows FROM truncate_iso.target_t; }
step s2_abort_location_unchanged {
  SELECT current.metadata_location = original.metadata_location AS unchanged
  FROM iceberg.iceberg_metadata AS current
  JOIN truncate_iso.locations AS original USING (relid)
  WHERE current.relid = 'truncate_iso.abort_t'::regclass;
}
step s2_abort_rows { SELECT array_agg(id ORDER BY id) AS rows FROM truncate_iso.abort_t; }
step s2_multi_rows { SELECT (SELECT count(*) FROM truncate_iso.multi_first_t) = 0 AND (SELECT count(*) FROM truncate_iso.multi_second_t) = 0 AS both_empty; }
step s2_multi_locations_changed {
  SELECT bool_and(current.metadata_location <> original.metadata_location) AS changed
  FROM iceberg.iceberg_metadata AS current
  JOIN truncate_iso.locations AS original USING (relid)
  WHERE current.relid IN ('truncate_iso.multi_first_t'::regclass, 'truncate_iso.multi_second_t'::regclass);
}

permutation s1_begin s1_truncate s2_location_unchanged s2_insert s1_commit s2_target_rows
permutation s1_begin_abort s1_truncate_abort s2_abort_location_unchanged s1_abort s2_abort_location_unchanged s2_abort_rows
permutation s1_begin_multi s1_truncate_multi s2_multi_rows s1_commit_multi s2_multi_locations_changed
