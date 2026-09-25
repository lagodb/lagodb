# Table maintenance ownership

`table_maintenance` contains the format-neutral provider requests, reports,
and errors. Its `postgres` module owns the PostgreSQL execution mechanisms:

- `MaintenanceCommand` contains validated PG17 options.
- `MaintenancePlan` owns expanded relation order and a Portal child context.
- `MaintenanceExecutor` owns guarded C calls and VACUUM/ANALYZE transaction,
  snapshot, command-counter, cost-accounting, and database-statistics sequencing.
- The VACUUM and TRUNCATE C bridges live in core alongside ANALYZE, COPY, and
  ModifyTable. No base module redeclares core's ANALYZE C entry point.

`lagodb-base` owns the process-global hooks and captured parent chain, the
provider registry, admission, maintenance route policy, provider budget / clock
selection, and runtime SQL / configuration. Its command-scope C bridge remains
in base: it owns recursion protection and calls the captured parent directly
inside `PG_FINALLY`. Provider DSOs do not install another global scope or keep
another runtime registry.

This boundary follows the history of the framework. `e81e6c9` introduced the
maintenance mechanisms in core; `9b99bfa` established the cross-DSO provider ABI;
`af46172` unified RuntimeApi and moved the global ProcessUtility router and its
VACUUM bridge to the runtime. The runtime ownership of hooks and registration
is retained. PostgreSQL execution mechanisms are reusable core implementation,
and do not require a new registry or another cross-DSO API.

## Execution contract

Base prepares options through core before admission. Negative probes still
delegate to the captured parent; positive probes expand one ordered command.
The plan never caches AM ownership across maintenance transactions. Runtime
callbacks classify live execution-locked relations and select provider actions.

VACUUM preserves the existing sequence: expansion; buffer strategy; provider
clock and budget preparation for nonempty plans; initial snapshot pop / commit;
ordered per-relation VACUUM and optional ANALYZE; final transaction start and
database statistics. The provider-context initializer returns errors before
that initial commit and is not called for empty plans.

ANALYZE keeps the outer transaction when in a transaction block or processing
one relation. Standalone multi-relation ANALYZE retains per-relation snapshots,
transactions and command counters. Expanded nodes and buffer strategies live
in the plan's Portal child, independent of those transactions.

TRUNCATE executes one atomic command with the original PG17 permissions,
locks, FK expansion, triggers, sequences, native storage, FDW and WAL ordering.
Only the provider-owned partitioned table storage step is dispatched to the runtime callback.

Core guards errorful C entries and allows Rust-owned plans to unwind after C
cleanup. Provider Result errors and diagnostics still reach the existing runtime
FFI report boundary; executor helpers do not report them early. The runtime's
admission, parent-chain and recursion contracts are unchanged, including their
documented negative-probe and hook-observation limits. See the
[VACUUM contract](../../csrc/vacuum/README.md) and
[TRUNCATE contract](../../csrc/truncate/README.md).
