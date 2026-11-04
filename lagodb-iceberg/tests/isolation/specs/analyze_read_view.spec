# ANALYZE reads what its snapshot sees, whatever commits after that snapshot.
# It takes page and row counts from separate callbacks, and both must stay on
# that population, including for a table with no baseline under the snapshot.
setup
{
  CREATE EXTENSION IF NOT EXISTS lagodb_iceberg;
  CREATE TABLE analyze_view_rows (id integer) USING iceberg;
  INSERT INTO analyze_view_rows SELECT generate_series(1, 100);
}

teardown
{
  DROP TABLE analyze_view_created;
  DROP TABLE analyze_view_rows;
}

session analyzer
step begin { BEGIN ISOLATION LEVEL SERIALIZABLE; }
step snapshot { SELECT count(*) AS visible_rows FROM analyze_view_rows; }
step analyze { ANALYZE analyze_view_rows, analyze_view_created; }
step commit { COMMIT; }

session writer
step write { INSERT INTO analyze_view_rows SELECT generate_series(101, 400); }
step create { CREATE TABLE analyze_view_created (id integer) USING iceberg; INSERT INTO analyze_view_created SELECT generate_series(1, 100); }
step estimates { SELECT relname, relpages > 0 AS has_pages, reltuples::bigint AS reltuples FROM pg_class WHERE relname LIKE 'analyze_view_%' ORDER BY relname; }

permutation begin snapshot write create analyze commit estimates
