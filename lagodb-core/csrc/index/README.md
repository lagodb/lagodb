# Index access method bridge

`lagodb_index_am.c/.h` holds the parts of the index access method framework
that PostgreSQL exposes only to C. It is an adapter, not a source fork.

- `lagodb_index_scan_record_search` expands `pgstat_count_index_scan`, a macro
  pgrx does not bind (`src/include/pgstat.h`). On PostgreSQL 18 it also
  increments `IndexScanInstrumentation.nsearches` (`src/include/access/genam.h`),
  as `nbtsearch.c` `_bt_first` and `contrib/bloom/blscan.c` do.
- `lagodb_index_unique_violation` raises the unique violation that
  `nbtinsert.c` `_bt_check_unique` reports. `errtableconstraint` sets the
  schema, table and constraint fields, which a Rust error value cannot carry.

Baselines: PostgreSQL 16.15, 17.11 and 18.6. The macro body is identical in
all three; `scan->instrument` exists only on PostgreSQL 18.
