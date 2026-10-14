-- Foreign schema contracts and local DDL. Execution-specific coverage belongs
-- in query_offload_foreign_tables.sql and native_parallel_scans.sql.
SELECT rest_uri AS regress_rest_uri
FROM lagodb_regress.object_storage_fixture
\gset

SET client_min_messages = warning;
DROP EXTENSION IF EXISTS lagodb_iceberg CASCADE;
DROP EXTENSION IF EXISTS dblink;
RESET client_min_messages;
CREATE EXTENSION lagodb_iceberg;
CREATE EXTENSION dblink;
CREATE SCHEMA iceberg_fdw_schema;
CREATE SERVER iceberg_schema_rest TYPE 'rest'
FOREIGN DATA WRAPPER lagodb_iceberg OPTIONS (uri :'regress_rest_uri');
SET lagodb.query_offload_mode = 'off';
SET max_parallel_workers_per_gather = 0;

-- Native DDL preserves PG's weaker index-rename lock,
-- including partitioned indexes and ALTER INDEX used to rename a plain table.
CREATE TABLE iceberg_fdw_schema.rename_target (id integer) USING heap;
CREATE INDEX rename_index ON iceberg_fdw_schema.rename_target (id);
CREATE TABLE iceberg_fdw_schema.rename_partitioned (id integer) PARTITION BY RANGE (id);
CREATE INDEX rename_partitioned_index ON iceberg_fdw_schema.rename_partitioned (id);
BEGIN;
ALTER INDEX iceberg_fdw_schema.rename_index RENAME TO renamed_index;
ALTER INDEX iceberg_fdw_schema.rename_partitioned_index RENAME TO renamed_partitioned_index;
SELECT bool_and(mode = 'ShareUpdateExclusiveLock') AS index_locks
FROM pg_locks
WHERE pid = pg_backend_pid() AND granted AND locktype = 'relation'
  AND relation IN ('iceberg_fdw_schema.renamed_index'::regclass,
                   'iceberg_fdw_schema.renamed_partitioned_index'::regclass);
ALTER INDEX iceberg_fdw_schema.rename_target RENAME TO renamed_target;
SELECT mode
FROM pg_locks
WHERE pid = pg_backend_pid() AND granted AND locktype = 'relation'
  AND relation = 'iceberg_fdw_schema.renamed_target'::regclass;
COMMIT;
DROP TABLE iceberg_fdw_schema.renamed_target, iceberg_fdw_schema.rename_partitioned;

-- A column subset accepts PG type aliases and a different column order.
CREATE FOREIGN TABLE iceberg_fdw_schema.evolving (id int4, body pg_catalog.text)
SERVER iceberg_schema_rest OPTIONS (table 'fdw_schema.evolving');
-- A reader holds AccessShareLock in another backend. Unauthorized ALTER and
-- RENAME must report ownership errors before waiting for an exclusive lock.
CREATE TABLE iceberg_fdw_schema.ddl_heap (id integer) USING heap;
CREATE ROLE regress_iceberg_ddl_nonowner;
GRANT USAGE ON SCHEMA iceberg_fdw_schema TO regress_iceberg_ddl_nonowner;
SELECT dblink_connect('ddl_reader', format(
    'host=localhost port=%s dbname=%I',
    current_setting('port'), current_database()));
SELECT dblink_exec('ddl_reader', 'BEGIN');
SELECT * FROM dblink('ddl_reader',
    'SELECT id FROM iceberg_fdw_schema.ddl_heap
     UNION ALL SELECT id FROM iceberg_fdw_schema.evolving LIMIT 0') AS held(id integer);
SET lock_timeout = '500ms';
SET ROLE regress_iceberg_ddl_nonowner;
ALTER TABLE iceberg_fdw_schema.ddl_heap ADD COLUMN extra integer;
ALTER TABLE iceberg_fdw_schema.ddl_heap RENAME TO ddl_heap_renamed;
ALTER FOREIGN TABLE iceberg_fdw_schema.evolving ADD COLUMN extra integer;
ALTER FOREIGN TABLE iceberg_fdw_schema.evolving RENAME TO evolving_renamed;
RESET ROLE;
RESET lock_timeout;
SELECT dblink_exec('ddl_reader', 'ROLLBACK');
SELECT dblink_disconnect('ddl_reader');
REVOKE USAGE ON SCHEMA iceberg_fdw_schema FROM regress_iceberg_ddl_nonowner;
DROP ROLE regress_iceberg_ddl_nonowner;
DROP TABLE iceberg_fdw_schema.ddl_heap;

SELECT * FROM iceberg_fdw_schema.evolving ORDER BY id;

-- Bind the historical definition while retaining the current data snapshot.
ALTER FOREIGN TABLE iceberg_fdw_schema.evolving OPTIONS (ADD schema_id '1');

-- DROP/ADD leaves a dropped attno; the local rename must not retarget the table.
ALTER FOREIGN TABLE iceberg_fdw_schema.evolving DROP COLUMN id;
ALTER FOREIGN TABLE iceberg_fdw_schema.evolving ADD COLUMN id integer;
ALTER FOREIGN TABLE iceberg_fdw_schema.evolving RENAME TO renamed;
SELECT * FROM iceberg_fdw_schema.renamed ORDER BY id;
ANALYZE iceberg_fdw_schema.renamed;
SELECT reltuples FROM pg_class WHERE oid = 'iceberg_fdw_schema.renamed'::regclass;

-- SET can select the current definition; DROP returns to the default.
ALTER FOREIGN TABLE iceberg_fdw_schema.renamed OPTIONS (SET schema_id '4');
SELECT * FROM iceberg_fdw_schema.renamed ORDER BY id;
ALTER FOREIGN TABLE iceberg_fdw_schema.renamed OPTIONS (DROP schema_id);
SELECT count(*) FROM iceberg_fdw_schema.renamed;

-- Changing table invalidates the cached plan and resolves the new target.
CREATE FOREIGN TABLE iceberg_fdw_schema.retargeted (id integer)
SERVER iceberg_schema_rest OPTIONS (table 'fdw_schema.evolving');
PREPARE iceberg_retarget AS
SELECT array_agg(id ORDER BY id) AS ids FROM iceberg_fdw_schema.retargeted;
EXECUTE iceberg_retarget;
ALTER FOREIGN TABLE iceberg_fdw_schema.retargeted
    OPTIONS (SET table 'fdw_schema.decimals');
EXECUTE iceberg_retarget;
DEALLOCATE iceberg_retarget;
DROP FOREIGN TABLE iceberg_fdw_schema.retargeted;

-- An unknown explicit column is rejected during CREATE.
CREATE FOREIGN TABLE iceberg_fdw_schema.invalid_column (missing integer)
SERVER iceberg_schema_rest OPTIONS (table 'fdw_schema.evolving');

-- IF NOT EXISTS leaves the existing definition intact without remote I/O.
CREATE FOREIGN TABLE IF NOT EXISTS iceberg_fdw_schema.renamed (invalid_column bigint)
SERVER iceberg_schema_rest OPTIONS (table 'absent.remote');

-- A same-name temporary table must not intercept inferred foreign CREATE.
SET search_path = iceberg_fdw_schema, public;
CREATE TEMP TABLE shadowed (local_only integer) USING heap;
CREATE FOREIGN TABLE shadowed ()
SERVER iceberg_schema_rest OPTIONS (table 'fdw_reads.filter_rows_v2');
SELECT relkind = 'f' AS foreign_created
FROM pg_class WHERE oid = 'iceberg_fdw_schema.shadowed'::regclass;
DROP FOREIGN TABLE iceberg_fdw_schema.shadowed;
DROP TABLE pg_temp.shadowed;
RESET search_path;

-- Writes bind the complete current schema even with an older definition ID
-- and reordered local columns. Roll back to leave the shared remote fixture intact.
CREATE FOREIGN TABLE iceberg_fdw_schema.writable (
    added integer, id integer, body text
)
SERVER iceberg_schema_rest
OPTIONS (table 'fdw_schema.evolving');
ALTER FOREIGN TABLE iceberg_fdw_schema.writable OPTIONS (ADD schema_id '1');
BEGIN;
INSERT INTO iceberg_fdw_schema.writable VALUES (30, 3, 'three');
SELECT body, id FROM iceberg_fdw_schema.writable WHERE id = 3;
ROLLBACK;

-- IMPORT splits namespace levels and preserves case without nested SQL quoting.
-- The generated table option also preserves the dotted table name.
-- Default import skips unsupported fields even when their names exceed PG
-- limits, and whole lists with unsupported elements. Inferred CREATE agrees.
-- Suppress the informational skip notice; inspect the resulting catalog.
SET client_min_messages = warning;
IMPORT FOREIGN SCHEMA "fdw_import.MixedCase" LIMIT TO ("rows.v3", unsupported_only)
FROM SERVER iceberg_schema_rest INTO iceberg_fdw_schema;
RESET client_min_messages;
SELECT to_regclass('iceberg_fdw_schema.unsupported_only') IS NULL AS unsupported_skipped;
CREATE FOREIGN TABLE iceberg_fdw_schema.inferred ()
SERVER iceberg_schema_rest OPTIONS (table 'FDW_IMPORT."MixedCase"."rows.v3"');
SELECT c.relname, a.attname, a.attnotnull
FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
WHERE a.attrelid IN ('iceberg_fdw_schema."rows.v3"'::regclass,
                    'iceberg_fdw_schema.inferred'::regclass)
  AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY c.relname, a.attnum;
SELECT count(*) FROM iceberg_fdw_schema."rows.v3";

-- Explicit subsets need not convert unsupported, undeclared remote fields;
-- a required remote field may be declared nullable for reads.
CREATE FOREIGN TABLE iceberg_fdw_schema.explicit (id integer)
SERVER iceberg_schema_rest OPTIONS (table 'fdw_import."MixedCase"."rows.v3"');
SELECT count(*) FROM iceberg_fdw_schema.explicit;

-- SERIAL is compared as integer; PG still creates its sequence and default.
CREATE FOREIGN TABLE iceberg_fdw_schema.serial_definition (id serial)
SERVER iceberg_schema_rest OPTIONS (table 'fdw_import."MixedCase"."rows.v3"');
SELECT pg_get_serial_sequence('iceberg_fdw_schema.serial_definition', 'id') IS NOT NULL AS has_sequence;
DROP FOREIGN TABLE iceberg_fdw_schema.serial_definition;

-- Strict import succeeds for supported schemas.
IMPORT FOREIGN SCHEMA fdw_schema LIMIT TO (evolving)
FROM SERVER iceberg_schema_rest INTO iceberg_fdw_schema OPTIONS (strict 'true');
SELECT option_value AS imported_schema_id
FROM pg_foreign_table, LATERAL pg_options_to_table(ftoptions)
WHERE ftrelid = 'iceberg_fdw_schema.evolving'::regclass
  AND option_name = 'schema_id';

-- Unused historical columns do not prevent reading valid columns.
CREATE FOREIGN TABLE iceberg_fdw_schema.old_name (id integer, payload text)
SERVER iceberg_schema_rest OPTIONS (table 'fdw_schema.evolving', schema_id '0');
SELECT id FROM iceberg_fdw_schema.old_name WHERE id > 0 ORDER BY id;
-- Actually reading the old name still fails; it does not follow the rename.
SELECT payload FROM iceberg_fdw_schema.old_name;

-- Local RENAME COLUMN and schema selection repair the definition.
ALTER FOREIGN TABLE iceberg_fdw_schema.old_name RENAME COLUMN payload TO body;
ALTER FOREIGN TABLE iceberg_fdw_schema.old_name OPTIONS (SET schema_id '1');
SELECT body FROM iceberg_fdw_schema.old_name ORDER BY body;

-- Reading a subset is allowed; writing still needs all columns.
CREATE FOREIGN TABLE iceberg_fdw_schema.incomplete (id integer)
SERVER iceberg_schema_rest
OPTIONS (table 'fdw_reads.filter_rows_v2');
SELECT id FROM iceberg_fdw_schema.incomplete WHERE id = 2;
INSERT INTO iceberg_fdw_schema.incomplete VALUES (5);
CREATE FOREIGN TABLE iceberg_fdw_schema.invalid_required (id integer NOT NULL)
SERVER iceberg_schema_rest OPTIONS (table 'fdw_reads.filter_rows_v2');
SELECT id FROM iceberg_fdw_schema.invalid_required;

-- Decimal declarations may differ in precision from the selected schema;
-- conversion uses the local numeric typmod as the remote precision evolves.
-- The promoted integer remains unused in these successful decimal reads.
CREATE FOREIGN TABLE iceberg_fdw_schema.decimals (
    id integer, amount numeric(6,2), promoted integer
)
SERVER iceberg_schema_rest OPTIONS (table 'fdw_schema.decimals', schema_id '0');
SELECT id, amount FROM iceberg_fdw_schema.decimals WHERE id < 3 ORDER BY id;
SELECT amount FROM iceberg_fdw_schema.decimals
WHERE id = 1 AND amount > 10 ORDER BY amount;
SELECT promoted FROM iceberg_fdw_schema.decimals WHERE id = 1;

-- ANALYZE reads every declared column. Remove the incompatible integer
-- before checking numeric overflow and MemoryContext cleanup in this backend.
ALTER FOREIGN TABLE iceberg_fdw_schema.decimals DROP COLUMN promoted;
\set VERBOSITY default
ANALYZE iceberg_fdw_schema.decimals;
\set VERBOSITY terse
-- Native local type ALTER repairs the definition without narrowing values.
ALTER FOREIGN TABLE iceberg_fdw_schema.decimals ALTER COLUMN amount TYPE numeric;
SELECT amount FROM iceberg_fdw_schema.decimals WHERE id = 3;
ANALYZE iceberg_fdw_schema.decimals;
SELECT reltuples = 3 AS analyzed
FROM pg_class WHERE oid = 'iceberg_fdw_schema.decimals'::regclass;

-- The removed mode option rejects the whole ALTER, including the ADD COLUMN.
ALTER FOREIGN TABLE iceberg_fdw_schema.renamed
    ADD COLUMN extra integer,
    OPTIONS (ADD mode 'read_write');
SELECT * FROM iceberg_fdw_schema.renamed ORDER BY id;

-- An absent schema ID must fail even on an empty table and COUNT(*).
ALTER FOREIGN TABLE iceberg_fdw_schema.explicit OPTIONS (ADD schema_id '2147483647');
SELECT count(*) FROM iceberg_fdw_schema.explicit;

-- Strict mode reports unsupported types instead of silently producing a subset.
IMPORT FOREIGN SCHEMA "fdw_import.MixedCase" LIMIT TO ("rows.v3")
FROM SERVER iceberg_schema_rest INTO iceberg_fdw_schema OPTIONS (strict 'true');

DROP FOREIGN TABLE iceberg_fdw_schema.evolving, iceberg_fdw_schema.renamed,
    iceberg_fdw_schema.writable, iceberg_fdw_schema."rows.v3",
    iceberg_fdw_schema.inferred, iceberg_fdw_schema.explicit,
    iceberg_fdw_schema.old_name, iceberg_fdw_schema.incomplete,
    iceberg_fdw_schema.invalid_required, iceberg_fdw_schema.decimals;
DROP SERVER iceberg_schema_rest;
DROP SCHEMA iceberg_fdw_schema;
DROP EXTENSION dblink;
RESET max_parallel_workers_per_gather;
RESET lagodb.query_offload_mode;
