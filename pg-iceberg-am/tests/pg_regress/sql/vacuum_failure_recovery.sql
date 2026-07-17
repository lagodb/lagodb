-- A failed rewrite must leave the old snapshot queryable, clean its attempt
-- artifacts, and permit a later VACUUM to succeed.
DROP EXTENSION IF EXISTS pg_iceberg_am CASCADE;
CREATE EXTENSION pg_iceberg_am;
\setenv PGDATABASE :DBNAME

CREATE TABLE vacuum_failure_recovery_t (id integer, payload text)
USING iceberg;
INSERT INTO vacuum_failure_recovery_t VALUES (1, 'one');
INSERT INTO vacuum_failure_recovery_t VALUES (2, 'two');
INSERT INTO vacuum_failure_recovery_t VALUES (3, 'three');
INSERT INTO vacuum_failure_recovery_t VALUES (4, 'four');
INSERT INTO vacuum_failure_recovery_t VALUES (5, 'five');
INSERT INTO vacuum_failure_recovery_t VALUES (6, 'six');

SELECT count(*) AS rows_before,
       md5(string_agg(id::text || ':' || payload, ',' ORDER BY id))
           AS digest_before
FROM vacuum_failure_recovery_t
\gset
SELECT pg_relation_filepath('vacuum_failure_recovery_t') || '_iceberg'
       AS failure_root
\gset
SELECT count(*) AS objects_before_failure
FROM pg_ls_dir(:'failure_root', true, false)
\gset

\! psql -XAtq -v ON_ERROR_STOP=1 -c "SET pg_iceberg_am.injection_point = 'panic_after_wal_write'" -c "VACUUM vacuum_failure_recovery_t" >/dev/null 2>&1; test $? -ne 0 && echo "injected_vacuum_failed: true"

SELECT count(*) = :rows_before::bigint AS rows_preserved_after_failure,
       md5(string_agg(id::text || ':' || payload, ',' ORDER BY id))
           = :'digest_before' AS content_preserved_after_failure
FROM vacuum_failure_recovery_t;
SELECT current_data_objects = 6 AS failed_attempt_not_published
FROM lakebase.table_maintenance_stats('vacuum_failure_recovery_t');
SELECT count(*) = :objects_before_failure::bigint
       AS failed_attempt_artifacts_cleaned
FROM pg_ls_dir(:'failure_root', true, false);

VACUUM vacuum_failure_recovery_t;
SELECT count(*) = :rows_before::bigint AS rows_preserved_after_retry,
       md5(string_agg(id::text || ':' || payload, ',' ORDER BY id))
           = :'digest_before' AS content_preserved_after_retry
FROM vacuum_failure_recovery_t;
SELECT current_data_objects = 1 AS retry_compacted
FROM lakebase.table_maintenance_stats('vacuum_failure_recovery_t');

DROP TABLE vacuum_failure_recovery_t;
DROP EXTENSION pg_iceberg_am CASCADE;
