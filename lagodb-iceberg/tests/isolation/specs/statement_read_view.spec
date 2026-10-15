# The reader already has a cached plan and an execution snapshot when it waits
# for the metadata catalog lock. Publication during that wait must not change
# the statement's rows; its next READ COMMITTED execution must see the commit.
# Each permutation exercises a distinct metadata-binding entry point.

setup
{
  CREATE EXTENSION IF NOT EXISTS lagodb_iceberg;
  CREATE TABLE statement_read_view (id integer) USING iceberg;
  INSERT INTO statement_read_view VALUES (1);
}

teardown
{
  DROP TABLE statement_read_view;
}

session reader
setup { SET max_parallel_workers_per_gather = 0; }
step am { SET lagodb.customscan_mode = 'off'; SET lagodb.query_offload_mode = 'off'; }
step custom { SET lagodb.customscan_mode = 'force'; SET lagodb.query_offload_mode = 'off'; }
step offload { SET lagodb.customscan_mode = 'off'; SET lagodb.query_offload_mode = 'force'; }
step prepare { PREPARE read_view AS SELECT a.id FROM statement_read_view a JOIN statement_read_view b USING (id) ORDER BY a.id; }
# Parameter-free PREPARE uses a generic plan. Warm it outside the transaction
# so planning's catalog locks have been released before the writer locks it.
step read { EXECUTE read_view; }
step begin { BEGIN ISOLATION LEVEL READ COMMITTED; }
step commit { COMMIT; }
step deallocate { DEALLOCATE read_view; }

session writer
step lock { BEGIN; LOCK TABLE iceberg.iceberg_metadata IN ACCESS EXCLUSIVE MODE; }
step insert { INSERT INTO statement_read_view VALUES (2); }
step publish { COMMIT; }

permutation am prepare read lock begin read insert publish read commit deallocate
permutation custom prepare read lock begin read insert publish read commit deallocate
permutation offload prepare read lock begin read insert publish read commit deallocate
