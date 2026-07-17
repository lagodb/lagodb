# The runtime-managed worker must skip a relation locked by manual maintenance,
# persist that outcome, and maintain it successfully after the lock is released.

setup
{
  CREATE EXTENSION IF NOT EXISTS pg_iceberg_am;
  CREATE SCHEMA IF NOT EXISTS automatic_worker_iso;
  CREATE TABLE automatic_worker_iso.t (id integer) USING iceberg;
  INSERT INTO automatic_worker_iso.t VALUES (1);
  INSERT INTO automatic_worker_iso.t VALUES (2);
  INSERT INTO automatic_worker_iso.t VALUES (3);
  INSERT INTO automatic_worker_iso.t VALUES (4);
  INSERT INTO automatic_worker_iso.t VALUES (5);
  INSERT INTO automatic_worker_iso.t VALUES (6);
  DO $$
  BEGIN
    EXECUTE format(
      'ALTER DATABASE %I SET pg_iceberg_am.auto_maintenance_enabled = on',
      current_database()
    );
    EXECUTE format(
      'ALTER DATABASE %I SET pg_iceberg_am.auto_maintenance_jitter_percent = 0',
      current_database()
    );
  END
  $$;
  DELETE FROM iceberg.automatic_maintenance_state
  WHERE relid = 'automatic_worker_iso.t'::regclass::oid;
}

teardown
{
  DO $$
  BEGIN
    EXECUTE format(
      'ALTER DATABASE %I RESET pg_iceberg_am.auto_maintenance_enabled',
      current_database()
    );
    EXECUTE format(
      'ALTER DATABASE %I RESET pg_iceberg_am.auto_maintenance_jitter_percent',
      current_database()
    );
  END
  $$;
  DROP SCHEMA IF EXISTS automatic_worker_iso CASCADE;
}

session s1
step s1_begin { BEGIN; }
step s1_lock {
  LOCK TABLE automatic_worker_iso.t IN SHARE UPDATE EXCLUSIVE MODE;
}
step s1_commit { COMMIT; }

session s2
step s2_wakeup_locked {
  SELECT lakebase.request_worker_wakeup(
    'pg_iceberg_am', 'iceberg_automatic_maintenance'
  );
}
step s2_wait_locked {
  DO $$
  DECLARE deadline timestamptz := clock_timestamp() + interval '30 seconds';
  BEGIN
    LOOP
      EXIT WHEN EXISTS (
        SELECT 1 FROM iceberg.automatic_maintenance_state
        WHERE relid = 'automatic_worker_iso.t'::regclass::oid
          AND last_outcome = 'lock-skipped'
      );
      IF clock_timestamp() >= deadline THEN
        RAISE EXCEPTION 'worker did not record lock skip';
      END IF;
      PERFORM pg_sleep(0.1);
    END LOOP;
  END
  $$;
}
step s2_unlock_retry {
  UPDATE iceberg.automatic_maintenance_state
  SET next_attempt_at = '-infinity'
  WHERE relid = 'automatic_worker_iso.t'::regclass::oid;
  SELECT lakebase.request_worker_wakeup(
    'pg_iceberg_am', 'iceberg_automatic_maintenance'
  );
}
step s2_wait_maintained {
  DO $$
  DECLARE deadline timestamptz := clock_timestamp() + interval '30 seconds';
  BEGIN
    LOOP
      EXIT WHEN EXISTS (
        SELECT 1 FROM iceberg.automatic_maintenance_state
        WHERE relid = 'automatic_worker_iso.t'::regclass::oid
          AND last_outcome = 'maintained'
      );
      IF clock_timestamp() >= deadline THEN
        RAISE EXCEPTION 'worker did not maintain unlocked relation';
      END IF;
      PERFORM pg_sleep(0.1);
    END LOOP;
  END
  $$;
}
step s2_verify {
  SELECT (SELECT current_data_objects
          FROM lakebase.table_maintenance_stats('automatic_worker_iso.t')) = 1
           AS compacted,
         (SELECT array_agg(id ORDER BY id) FROM automatic_worker_iso.t)
           = ARRAY[1, 2, 3, 4, 5, 6] AS rows_preserved;
}

permutation
  s1_begin s1_lock
  s2_wakeup_locked s2_wait_locked
  s1_commit
  s2_unlock_retry s2_wait_maintained s2_verify
