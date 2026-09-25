# PostgreSQL VACUUM C bridge

## PostgreSQL source provenance

- Baseline release: PostgreSQL 17.10
- Upstream tag: `REL_17_10`
- Primary source: `src/backend/commands/vacuum.c`
- Primary source SHA-256:
  `8dab2237d1d25b5870520ca4694a93bbd96a76882e179a436c35bcaacaea09bd`
- Public contract: `src/include/commands/vacuum.h`
- Public contract SHA-256:
  `273a972adfd62e0579ec22ff35c5f38cbf69ac5c2140a8e682883c6a14675449`

`lagodb_vacuum.c` is a version-pinned reconstruction of the private option
parsing, relation expansion, and `vacuum_rel()` state machine needed to route a
table-maintenance provider in place of PostgreSQL's storage action.
With registered maintenance providers, `MaintenanceExecutor::prepare_command`
performs command state checks, full option parsing and validation, then VACUUM's
transaction-block check before name resolution. Both runtime routers reuse its
validated command. Core owns the expanded plan and execution sequencing. Plain ANALYZE remains valid inside a
transaction block. ONLY_DATABASE_STATS is passed to the parent after preparation,
without relation classification.

`lagodb_vacuum_probe.c` implements the routing probe for both VACUUM and ANALYZE,
which PostgreSQL represents with `VacuumStmt`.
An explicit-relation command uses a target-lock-free probe: relation
names and partition descendants are read with `NoLock`. The classifier receives
the prepared option bits instead of interpreting options a second time. It does
not perform maintenance permission checks, emit maintenance warnings, or execute
provider actions. PostgreSQL name resolution still checks schema USAGE and invokes
namespace search hooks, so the probe is not side-effect-free. Command validation
precedes these lookups to preserve PostgreSQL's option/transaction/name error order.
Database-wide commands use the same classification policy in a normal `pg_class`
catalog scan; an empty relation list is never interpreted as no relations.

A negative probe passes the original statement unchanged to the captured
parent ProcessUtility hook. This does not preserve the parent's validation timing:
invalid native commands are rejected by LagoDB before reaching the parent, and
parent hooks cannot first rewrite them or interpret additional options. For valid
native commands, the native path repeats option parsing after LagoDB preparation.
Positive commands reuse the preparation result. This work is per command, not
per row. Name resolution can also invoke namespace hooks again during execution
expansion or parent processing; the classifier makes no hook-side-effect guarantee.

The probe is intentionally only a routing hint. PostgreSQL documents that a
`NoLock` name lookup may become stale under concurrent DDL. The authoritative
expansion and every provider ownership decision are therefore repeated from
the live `Relation` after the execution lock is acquired. The plan stores only
expanded relation order and OIDs; it never stores AM, relkind, or ownership
across transaction boundaries.

Core's private `MaintenanceBridge` owns the hand-written `lagodb_*` extern
declarations. The public `MaintenanceExecutor` owns PostgreSQL transaction, snapshot,
and cost-accounting sequencing; base supplies admission and provider policy. Unlike pgrx bindgen's `pg_sys` functions, these bridge calls do not
receive generated ERROR protection. The adapter guards the C entries that can
raise ERROR or invoke Rust callbacks, so PostgreSQL errors unwind Rust-owned
plans after C cleanup. It does not wrap already-protected `pg_sys` calls or the
two C functions that only initialize/reset maintenance cost globals.

Provider execution uses one typed `execute_maintenance` operation for both
local TableAM calls and runtime dispatch. The runtime checks the descriptor's
ANALYZE capability before a compound storage action. Cross-DSO execute/inspect
callbacks return a status and `CallbackErrorReport`; the router reconstructs
`Result` errors synchronously so its PostgreSQL execution boundary can add
command context before reporting.

## Accepted tradeoff: negative-probe TOCTOU

The locked reclassification protects only a positive probe, because only a
positive result enters the LagoDB executor. A negative result immediately
returns the original statement to the parent ProcessUtility path and therefore
has a definite time-of-check/time-of-use window:

1. The probe resolves a target name with `NoLock` and observes a native
   relation.
2. A concurrent transaction drops that relation and creates a provider-owned
   provider-owned partitioned table with the same name.
3. The parent ProcessUtility path resolves and locks the replacement relation.
4. PostgreSQL treats the partitioned table as storage-less, so provider VACUUM
   or non-inherited ANALYZE is omitted. For a provider-owned partitioned table without PostgreSQL
   leaves, the native maintenance path completes without touching provider
   storage.

This produces a silent maintenance omission. It does not require `ALTER TABLE
SET ACCESS METHOD`; drop-and-recreate is sufficient. A database-wide probe
has the same boundary because its `pg_class` lock does not lock every target
relation.

The relevant PostgreSQL contracts are in `src/backend/catalog/namespace.c`:
`RangeVarGetRelidExtended(..., NoLock, ...)` returns its first lookup result
without processing invalidations, and in `src/backend/commands/analyze.c`:
non-inherited analysis skips `RELKIND_PARTITIONED_TABLE`. PostgreSQL VACUUM
likewise has no provider storage action for that provider-owned partitioned table.

The design is retained as the best available solution under the current
product constraints: LagoDB does not patch PostgreSQL and does not require a
particular ProcessUtility hook load order. The extension-only alternatives do
not preserve the required behavior:

- Always consuming provider-enabled VACUUM/ANALYZE removes native-only
  statements from the captured parent hook and unnecessarily subjects them to
  LagoDB's forked executor.
- Acquiring target locks before deciding to fall back makes captured parent
  hooks run after LagoDB has locked relations. This changes PostgreSQL's
  hook/lock ordering and introduces lock-order dependencies absent from the
  native path.
- Releasing those locks before invoking the parent makes the classification
  stale again and recreates the same TOCTOU.
- Requiring LagoDB to be installed first transfers correctness to extension
  load order and is not an acceptable deployment contract.

Consequently the probe remains target-lock-free and performs no provider
maintenance actions. Negative results reach the parent with the original
statement after command validation, and positive results are reclassified from
locked `Relation` objects. Moving validation ahead of the probe does not close
the negative-result DDL window. This is an explicit correctness limitation rather
than a complete solution. It must be revisited if PostgreSQL provides a suitable
maintenance execution seam or the current integration constraints change.

### Why retaining probe locks changes the native lifecycle

PostgreSQL's `vacuum.c::expand_vacuum_rel()` takes a transient `AccessShareLock` on
each named target and releases it before execution. Its comments explicitly
reject locking descendants during expansion and retaining locks on multiple
relations because of deadlock risk. `vacuum()` commits the initial transaction
before VACUUM and before standalone multi-relation ANALYZE, while ANALYZE in a
user transaction keeps that transaction. `proc.c::ProcReleaseLocks()` releases
transaction relation locks at commit; session relation locks survive it.

Retaining an `AccessShareLock` through parent expansion can protect that name
lookup, but retaining it into ANALYZE adds a concrete upgrade deadlock: backend A
holds the probe's `AccessShareLock(t)`; backend B takes `ShareLock(t)` (for example
with `LOCK TABLE t IN SHARE MODE`) and requests `AccessExclusiveLock(t)`; A then
requests ANALYZE's `ShareUpdateExclusiveLock(t)`. B waits for A's read lock, and A
waits for B's share lock. The transient expansion lock in native PostgreSQL is
released before requesting the execution lock.

Keeping targets locked across VACUUM's commits requires session locks. With
session `AccessShareLock(t)` retained by two VACUUM FULL backends, both requests
for `AccessExclusiveLock(t)` wait for the other backend's read lock. Exclusive
probe locks avoid that read-lock upgrade but block native readers and writers
early, and accumulating them over multiple targets adds lock-order dependencies.
These cycles follow PostgreSQL's `lock.c::LockConflicts` and `proc.c::ProcSleep`.
`LockCheckConflicts()` excludes a backend's own locks: taking a second lock on
the same relation does not, by itself, deadlock that backend. In all cases,
locking before fallback also makes parent hooks run with target locks already
held; releasing the locks before fallback restores the name-replacement race.

Lock-free VACUUM routing that uses `RangeVarGetRelid(..., NoLock, ...)` for
named targets and `find_all_inheritors(..., NoLock, ...)` for descendants has
the same negative-result boundary. When a negative result passes the original
statement to the captured parent, concurrent name replacement can omit provider
maintenance, including compaction. PostgreSQL's native VACUUM emits a warning
and skips foreign tables, whereas a storage-less provider-owned partitioned table can complete
without such a warning. Delegating plain ANALYZE entirely to PostgreSQL does
not address the provider-root routing limitation described here.

Routine root maintenance uses `ShareUpdateExclusiveLock`, sets
`PROC_IN_VACUUM` before taking its snapshot, and commits once per relation.
FULL uses `AccessExclusiveLock`. The runtime's
`../../../lagodb-base/csrc/maintenance/lagodb_maintenance.c` owns one command-scoped recursion
guard for both VACUUM and ANALYZE. It stays active through provider execution
and native parent delegation, including commands that take the hook's fast
path without registered providers. The Rust hook checks it before pre-hooks
and again after statement rewrites. The C entry rejects recursion before
acquiring the scope, so an inner rejection cannot clear the outer command's
state. `PG_FINALLY` clears the state on normal and ERROR exits; transaction
commits and relation/phase transitions do not end the scope.

Unclaimed statements reach the captured parent hook exactly once, directly
from C, or `standard_ProcessUtility` when no parent was captured. Provider
errors retain their existing runtime report boundary; the C scope only cleans
up and propagates ERROR. LagoDB pre/post hooks run outside the scope. The
captured parent's entire invocation is inside it, so maintenance submitted
by that hook's own before/after logic is also rejected until it returns.
Ordinary nested utility commands remain allowed. PostgreSQL's private `in_vacuum`
has a narrower interval inside `vacuum()`; the runtime scope intentionally
covers the delegated command rather than that private interval. This scope
does not intercept native maintenance entered directly by the kernel.

After a positive probe, `MaintenancePlan` owns a command-level
child of PortalContext. Expanded nodes, lists (including concatenation results)
and buffer strategies are allocated in that child, so they survive per-relation
transaction boundaries and are deleted when the router returns or guarded ERROR
unwinds it. Relation names and column lists retain the input statement's lifetime.
The context uses a static name; creating it does not leave a separately allocated
name in the outer Portal. Buffer strategies remain available for native relations
in mixed commands; Iceberg data-file maintenance does not use their shared-buffer
policy.

Hook snapshots have a separate lifetime. Ordinary commands and nested ANALYZE
allocate the original node copy in the caller's CurrentMemoryContext. Top-level
VACUUM/ANALYZE snapshots use a separate Portal child that PostgreSQL reclaims
after the complete ProcessUtility invocation, including post-hooks. Releasing
the maintenance plan therefore cannot invalidate the original node for post-hooks.

PostgreSQL 17.10 is the provenance baseline, not the only accepted PG17 minor.
Before adopting a new baseline or extending another major:

1. Compare option parsing and validation in `ExecVacuum()`.
2. Compare `expand_vacuum_rel()` and `get_all_vacuum_rels()` against the
   execution expansion, including permissions, partitions, `SKIP_LOCKED`, and
   database-wide selection.
3. Compare `vacuum_rel()` transaction, snapshot, security-context and search
   path boundaries.
4. Reconcile every initialized `VacuumParams` field.
5. Refresh both hashes and run the mixed provider/native routine/FULL/ANALYZE
   regression matrix, including a provider-owned partitioned table with no `pg_inherits` leaves.
