# Current catalogs can resolve a table created after the reader's data snapshot.
# Its committed baseline is empty, but the reader's own later writes are visible.
setup
{
  CREATE EXTENSION IF NOT EXISTS lagodb_iceberg;
  CREATE TABLE creation_view_anchor (id integer);
  INSERT INTO creation_view_anchor VALUES (1);

  -- Read-only SPI retains the outer READ COMMITTED statement snapshot. The
  -- anchor lock lets the writer publish the new table before dynamic lookup.
  CREATE FUNCTION count_created_table() RETURNS bigint
  LANGUAGE plpgsql STABLE AS $$
  DECLARE
    rows bigint;
  BEGIN
    PERFORM id FROM creation_view_anchor;
    EXECUTE 'SELECT count(*) FROM created_read_view' INTO rows;
    RETURN rows;
  END;
  $$;
}

teardown
{
  DROP FUNCTION count_created_table();
  DROP TABLE created_read_view;
  DROP TABLE creation_view_anchor;
}

session reader
setup { SET max_parallel_workers_per_gather = 0; }
step am { SET lagodb.customscan_mode = 'off'; SET lagodb.query_offload_mode = 'off'; }
step custom { SET lagodb.customscan_mode = 'force'; SET lagodb.query_offload_mode = 'off'; }
step offload { SET lagodb.customscan_mode = 'off'; SET lagodb.query_offload_mode = 'force'; }
step begin { BEGIN ISOLATION LEVEL SERIALIZABLE; }
step snapshot { SELECT id FROM creation_view_anchor; }
step read { SELECT id FROM created_read_view WHERE id >= 1 ORDER BY id; }
step own_write { INSERT INTO created_read_view VALUES (2); }
step rollback { ROLLBACK; }
step stable_read { SELECT count_created_table() AS rows; }
step fresh_count { SELECT count(*) AS rows FROM created_read_view; }

session writer
step lock { BEGIN; LOCK TABLE creation_view_anchor IN ACCESS EXCLUSIVE MODE; }
step create { CREATE TABLE created_read_view (id integer) USING iceberg; INSERT INTO created_read_view VALUES (1); }
step publish { COMMIT; }

permutation am begin snapshot create read own_write read rollback read
permutation custom begin snapshot create read own_write read rollback read
permutation offload begin snapshot create read own_write read rollback read
permutation offload lock stable_read create publish fresh_count
