-- Schema 0 is (id, payload, removed), schema 1 renames payload to body.
-- The current schema 4 is (body, id, added). Definitions selecting schema 1
-- can still bind id/body by name. Keep files written before and after evolution
-- so schema_id coverage reads both rows from the current snapshot.
-- This immutable source is shared by FDW, query-offload and native-parallel tests.
DROP TABLE IF EXISTS $table;
CREATE TABLE $table (
    id integer,
    payload string,
    removed integer
) USING iceberg
-- A 4 MiB target matches the default open-file cost: the two writes below
-- remain separate work groups for query-offload worker reconstruction.
TBLPROPERTIES ('format-version'='2', 'read.split.target-size'='4194304');
INSERT INTO $table VALUES (1, 'one', 10);
ALTER TABLE $table RENAME COLUMN payload TO body;
ALTER TABLE $table ALTER COLUMN body FIRST;
ALTER TABLE $table DROP COLUMN removed;
ALTER TABLE $table ADD COLUMN added integer;
INSERT INTO $table (id, body, added) VALUES (2, 'two', 20);
