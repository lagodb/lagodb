# Table maintenance ownership

`table_maintenance` contains the format-neutral provider requests, reports,
and errors. Its `postgres` module owns the one PostgreSQL mechanism a provider
cannot get through its table AM, VACUUM FULL:

- `MaintenanceCommand` contains validated PostgreSQL options.
- `MaintenancePlan` owns the expanded native and provider relation lists and a
  Portal child context.
- `MaintenanceExecutor` owns guarded C calls and the VACUUM FULL transaction,
  snapshot, and parent-delegation sequence.
- The VACUUM C bridge lives in core alongside COPY and ModifyTable.

`lagodb-base` owns the process-global hooks and captured parent chain, the
provider registry, admission, provider budget / clock selection, and runtime
SQL / configuration. Provider DSOs do not install another global scope or keep
another runtime registry.

## What PostgreSQL runs natively

A provider table is an ordinary `relkind = 'r'` relation, so PostgreSQL owns:

- ANALYZE, including autovacuum's scheduling, through the TableAM sampling
  callbacks;
- TRUNCATE, through `relation_nontransactional_truncate` and
  `relation_set_new_filelocator`;
- routine VACUUM, through `relation_vacuum`. The callback reports its pass with
  `RelationHandle::with_vacuum_report`, because autovacuum schedules from the
  counters that only that report resets.

## VACUUM FULL execution contract

Only a VACUUM statement with FULL enabled is inspected. Base prepares options
through core before admission. A negative probe delegates the original statement
to the captured parent. A positive probe expands one ordered command, classifying
live relations by access method.

The command then runs three steps, in order:

1. native relations go to the parent as a rewritten VACUUM statement;
2. provider relations run the provider's rewrite;
3. when requested, provider relations go to the parent as a native ANALYZE.

ANALYZE runs through the parent rather than `analyze_rel()` because PostgreSQL's
`vacuum()` holds the recursion guard that `analyze_rel()` relies on. Provider
clock and budget preparation returns its errors before any step commits, and is
not called for empty plans.

Core guards errorful C entries and allows Rust-owned plans to unwind after C
cleanup. Provider Result errors and diagnostics still reach the existing runtime
FFI report boundary; executor helpers do not report them early. See the
[VACUUM contract](../../csrc/vacuum/README.md).
