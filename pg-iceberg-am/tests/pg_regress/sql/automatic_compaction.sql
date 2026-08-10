-- Iceberg automatic compaction driven by per-table maintenance deadlines.
DROP EXTENSION IF EXISTS pg_iceberg_am CASCADE;
CREATE EXTENSION pg_iceberg_am;
CREATE SCHEMA automatic_compaction_test;

SELECT to_regclass('iceberg.automatic_maintenance_state') IS NULL
    AS has_no_scheduler_state_table;
SELECT to_regclass('iceberg.iceberg_metadata_maintenance_due_idx') IS NOT NULL
    AS has_due_index;
SELECT indexprs IS NULL AND indpred IS NULL
    AS due_index_is_catalog_compatible
FROM pg_index
WHERE indexrelid =
    'iceberg.iceberg_metadata_maintenance_due_idx'::regclass;

-- Database settings are required because the worker runs in another backend.
SELECT format(
    'ALTER DATABASE %I SET pg_iceberg_am.auto_maintenance_enabled = off',
    current_database()
)
\gexec
SELECT format(
    'ALTER DATABASE %I SET pg_iceberg_am.auto_maintenance_naptime_s = 10',
    current_database()
)
\gexec
SET pg_iceberg_am.auto_maintenance_enabled = off;
SET pg_iceberg_am.auto_maintenance_naptime_s = 10;
SELECT format(
    'ALTER DATABASE %I SET pg_iceberg_am.auto_maintenance_enabled = on',
    current_database()
)
\gexec

CREATE TABLE automatic_compaction_test.eligible (id integer) USING iceberg;
INSERT INTO automatic_compaction_test.eligible VALUES (1);
CREATE TEMP TABLE first_due AS
SELECT maintenance_due_at
FROM iceberg.iceberg_metadata
WHERE relid = 'automatic_compaction_test.eligible'::regclass;
INSERT INTO automatic_compaction_test.eligible VALUES (2);
INSERT INTO automatic_compaction_test.eligible VALUES (3);
INSERT INTO automatic_compaction_test.eligible VALUES (4);
INSERT INTO automatic_compaction_test.eligible VALUES (5);
INSERT INTO automatic_compaction_test.eligible VALUES (6);

SELECT metadata.maintenance_due_at = first_due.maintenance_due_at
    AS repeated_writes_kept_earliest_deadline
FROM iceberg.iceberg_metadata AS metadata
CROSS JOIN first_due
WHERE metadata.relid = 'automatic_compaction_test.eligible'::regclass;

DO $$
DECLARE
    deadline timestamptz := clock_timestamp() + interval '30 seconds';
BEGIN
    LOOP
        EXIT WHEN (
            SELECT current_data_objects < 6
            FROM lakebase.table_maintenance_stats(
                'automatic_compaction_test.eligible'
            )
        );
        IF clock_timestamp() >= deadline THEN
            RAISE EXCEPTION 'automatic compaction initial compaction timed out';
        END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
END
$$;

SELECT current_data_objects < 6 AS worker_compacted_eligible_table
FROM lakebase.table_maintenance_stats(
    'automatic_compaction_test.eligible'
);
SELECT maintenance_due_at IS NULL AS successful_attempt_cleared_due
FROM iceberg.iceberg_metadata
WHERE relid = 'automatic_compaction_test.eligible'::regclass;
SELECT array_agg(id ORDER BY id) = ARRAY[1, 2, 3, 4, 5, 6]
    AS rows_preserved_after_first_attempt
FROM automatic_compaction_test.eligible;

-- A registry row with no due timestamp must not be visited by a relation wake.
CREATE TABLE automatic_compaction_test.cold (id integer) USING iceberg;
INSERT INTO automatic_compaction_test.cold VALUES (1);
INSERT INTO automatic_compaction_test.cold VALUES (2);
INSERT INTO automatic_compaction_test.cold VALUES (3);
INSERT INTO automatic_compaction_test.cold VALUES (4);
INSERT INTO automatic_compaction_test.cold VALUES (5);
INSERT INTO automatic_compaction_test.cold VALUES (6);
UPDATE iceberg.iceberg_metadata
SET maintenance_due_at = NULL
WHERE relid = 'automatic_compaction_test.cold'::regclass;

INSERT INTO automatic_compaction_test.eligible VALUES (7);
INSERT INTO automatic_compaction_test.eligible VALUES (8);
INSERT INTO automatic_compaction_test.eligible VALUES (9);
INSERT INTO automatic_compaction_test.eligible VALUES (10);
INSERT INTO automatic_compaction_test.eligible VALUES (11);
INSERT INTO automatic_compaction_test.eligible VALUES (12);
DO $$
DECLARE
    deadline timestamptz := clock_timestamp() + interval '30 seconds';
BEGIN
    LOOP
        EXIT WHEN (
            SELECT current_data_objects < 7
            FROM lakebase.table_maintenance_stats(
                'automatic_compaction_test.eligible'
            )
        );
        IF clock_timestamp() >= deadline THEN
            RAISE EXCEPTION 'automatic compaction write scheduling timed out';
        END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
END
$$;

SELECT current_data_objects < 7 AS writes_scheduled_compaction
FROM lakebase.table_maintenance_stats(
    'automatic_compaction_test.eligible'
);
SELECT current_data_objects = 6 AS worker_did_not_scan_clean_registry_rows
FROM lakebase.table_maintenance_stats(
    'automatic_compaction_test.cold'
);
SELECT array_agg(id ORDER BY id) = ARRAY[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    AS rows_preserved_after_second_attempt
FROM automatic_compaction_test.eligible;

SELECT format(
    'ALTER DATABASE %I RESET pg_iceberg_am.auto_maintenance_enabled',
    current_database()
)
\gexec
SELECT format(
    'ALTER DATABASE %I RESET pg_iceberg_am.auto_maintenance_naptime_s',
    current_database()
)
\gexec
RESET pg_iceberg_am.auto_maintenance_enabled;
RESET pg_iceberg_am.auto_maintenance_naptime_s;

DROP SCHEMA automatic_compaction_test CASCADE;
DROP EXTENSION pg_iceberg_am CASCADE;
