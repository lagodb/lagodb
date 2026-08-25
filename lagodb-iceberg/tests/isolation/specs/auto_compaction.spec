# Automatic compaction must defer a locked due relation without blocking, then
# compact it after the lock is released.

setup
{
  CREATE EXTENSION IF NOT EXISTS lagodb_iceberg;
  CREATE SCHEMA IF NOT EXISTS automatic_compaction_iso;
  DO $$
  BEGIN
    EXECUTE format(
      'ALTER DATABASE %I SET lagodb_iceberg.auto_maintenance_enabled = off',
      current_database()
    );
    EXECUTE format(
      'ALTER DATABASE %I SET lagodb_iceberg.auto_maintenance_naptime_s = 10',
      current_database()
    );
  END
  $$;
  CREATE TABLE automatic_compaction_iso.t (id integer) USING iceberg;
  INSERT INTO automatic_compaction_iso.t VALUES (1);
  INSERT INTO automatic_compaction_iso.t VALUES (2);
  INSERT INTO automatic_compaction_iso.t VALUES (3);
  INSERT INTO automatic_compaction_iso.t VALUES (4);
  INSERT INTO automatic_compaction_iso.t VALUES (5);
  INSERT INTO automatic_compaction_iso.t VALUES (6);
}

# Iceberg metadata is published at pre-commit, so force the completed setup
# state due in a separate transaction. Updating the internal catalog in the
# first setup transaction would update the same tuple again from its pre-commit
# callback, which is not a production DML lifecycle.
setup
{
  UPDATE iceberg.iceberg_metadata
  SET maintenance_due_at = '-infinity'
  WHERE relid = 'automatic_compaction_iso.t'::regclass;
}

teardown
{
  DO $$
  BEGIN
    EXECUTE format(
      'ALTER DATABASE %I RESET lagodb_iceberg.auto_maintenance_enabled',
      current_database()
    );
    EXECUTE format(
      'ALTER DATABASE %I RESET lagodb_iceberg.auto_maintenance_naptime_s',
      current_database()
    );
  END
  $$;
  DROP SCHEMA IF EXISTS automatic_compaction_iso CASCADE;
}

session s1
step s1_begin { BEGIN; }
step s1_lock {
  LOCK TABLE automatic_compaction_iso.t IN SHARE UPDATE EXCLUSIVE MODE;
}
step s1_commit { COMMIT; }

session s2
step s2_enable_locked {
  DO $$
  BEGIN
    EXECUTE format(
      'ALTER DATABASE %I SET lagodb_iceberg.auto_maintenance_enabled = on',
      current_database()
    );
  END
  $$;
}
step s2_wait_deferred {
  DO $$
  DECLARE deadline timestamptz := clock_timestamp() + interval '30 seconds';
  BEGIN
    LOOP
      EXIT WHEN (
        SELECT maintenance_due_at > clock_timestamp()
        FROM iceberg.iceberg_metadata
        WHERE relid = 'automatic_compaction_iso.t'::regclass
      );
      IF clock_timestamp() >= deadline THEN
        RAISE EXCEPTION 'automatic compaction did not defer the locked relation';
      END IF;
      PERFORM pg_sleep(0.1);
    END LOOP;
  END
  $$;
}
step s2_retry {
  UPDATE iceberg.iceberg_metadata
  SET maintenance_due_at = '-infinity'
  WHERE relid = 'automatic_compaction_iso.t'::regclass;
  SELECT lakebase.request_worker_wakeup(
    'lagodb_iceberg', 'iceberg_maintenance'
  );
}
step s2_wait_maintained {
  DO $$
  DECLARE deadline timestamptz := clock_timestamp() + interval '30 seconds';
  BEGIN
    LOOP
      EXIT WHEN (
        SELECT current_data_objects = 1
        FROM lakebase.table_maintenance_stats('automatic_compaction_iso.t')
      );
      IF clock_timestamp() >= deadline THEN
        RAISE EXCEPTION 'automatic compaction did not compact unlocked relation';
      END IF;
      PERFORM pg_sleep(0.1);
    END LOOP;
  END
  $$;
}
step s2_verify {
  SELECT (SELECT current_data_objects
          FROM lakebase.table_maintenance_stats('automatic_compaction_iso.t')) = 1
           AS compacted,
         (SELECT maintenance_due_at IS NULL
          FROM iceberg.iceberg_metadata
          WHERE relid = 'automatic_compaction_iso.t'::regclass) AS due_cleared,
         (SELECT array_agg(id ORDER BY id) FROM automatic_compaction_iso.t)
           = ARRAY[1, 2, 3, 4, 5, 6] AS rows_preserved;
}

permutation
  s1_begin s1_lock
  s2_enable_locked s2_wait_deferred
  s1_commit
  s2_retry s2_wait_maintained s2_verify
