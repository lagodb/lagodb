# A local TRUNCATE changes the relation's storage even for an older data
# snapshot. The reader must not reopen the retired metadata or read the
# writer's post-TRUNCATE rows. Ordinary writes in the same root retain MVCC.
setup
{
  CREATE EXTENSION IF NOT EXISTS lagodb_iceberg;
  CREATE TABLE truncate_view_anchor (id integer);
  INSERT INTO truncate_view_anchor VALUES (1);
  CREATE TABLE truncate_view_t (id integer) USING iceberg;
  CREATE TABLE truncate_view_partitioned (id integer)
    PARTITION BY LIST (id) USING iceberg;
  CREATE TABLE truncate_view_control (id integer) USING iceberg;
  INSERT INTO truncate_view_t VALUES (1);
  INSERT INTO truncate_view_partitioned VALUES (1);
  INSERT INTO truncate_view_control VALUES (1);
}

teardown
{
  DROP TABLE truncate_view_t, truncate_view_partitioned, truncate_view_control;
  DROP TABLE truncate_view_anchor;
}

session reader
setup { SET max_parallel_workers_per_gather = 0; }
step am { SET lagodb.customscan_mode = 'off'; SET lagodb.query_offload_mode = 'off'; }
step custom { SET lagodb.customscan_mode = 'force'; SET lagodb.query_offload_mode = 'off'; }
step offload { SET lagodb.customscan_mode = 'off'; SET lagodb.query_offload_mode = 'force'; }
step begin { BEGIN ISOLATION LEVEL SERIALIZABLE; }
step snapshot { SELECT id FROM truncate_view_anchor; }
step read { SELECT id FROM truncate_view_t ORDER BY id; }
step read_partitioned { SELECT count(*) AS rows FROM truncate_view_partitioned; }
step read_control { SELECT id FROM truncate_view_control ORDER BY id; }
step own_write { INSERT INTO truncate_view_t VALUES (3); INSERT INTO truncate_view_partitioned VALUES (3); }
step rollback { ROLLBACK; }

session writer
step replace { TRUNCATE truncate_view_t, truncate_view_partitioned; INSERT INTO truncate_view_t VALUES (2); INSERT INTO truncate_view_partitioned VALUES (2); INSERT INTO truncate_view_control VALUES (2); }

permutation am begin snapshot replace read read_partitioned read_control own_write read read_partitioned rollback read read_partitioned
permutation custom begin snapshot replace read read_partitioned read_control own_write read read_partitioned rollback read read_partitioned
permutation offload begin snapshot replace read read_partitioned read_control own_write read read_partitioned rollback read read_partitioned
