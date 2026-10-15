# A transaction data snapshot does not freeze PG catalog definitions. Rename
# after the reader takes its snapshot without locking the Iceberg table; then
# read the old rows through the new name, including predicate binding.
setup
{
  CREATE EXTENSION IF NOT EXISTS lagodb_iceberg;
  CREATE TABLE schema_view_anchor (id integer);
  INSERT INTO schema_view_anchor VALUES (1);
  CREATE TABLE schema_read_view (id integer) USING iceberg;
  INSERT INTO schema_read_view VALUES (1);
}

teardown
{
  DROP TABLE schema_read_view;
  DROP TABLE schema_view_anchor;
}

session reader
setup { SET max_parallel_workers_per_gather = 0; }
step am { SET lagodb.customscan_mode = 'off'; SET lagodb.query_offload_mode = 'off'; }
step custom { SET lagodb.customscan_mode = 'force'; SET lagodb.query_offload_mode = 'off'; }
step offload { SET lagodb.customscan_mode = 'off'; SET lagodb.query_offload_mode = 'force'; }
step begin { BEGIN ISOLATION LEVEL SERIALIZABLE; }
step snapshot { SELECT id FROM schema_view_anchor; }
step read { SELECT renamed FROM schema_read_view WHERE renamed >= 1 ORDER BY renamed; }
step commit { COMMIT; }

session writer
step rename { BEGIN; ALTER TABLE schema_read_view RENAME COLUMN id TO renamed; INSERT INTO schema_read_view VALUES (2); COMMIT; }

permutation am begin snapshot rename read commit
permutation custom begin snapshot rename read commit
permutation offload begin snapshot rename read commit
