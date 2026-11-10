# PostgreSQL VACUUM FULL bridge

## PostgreSQL source provenance

- Baseline releases: PostgreSQL 15.19, 16.15, 17.11 and 18.6
- Upstream tags: `REL_15_19`, `REL_16_15`, `REL_17_11` and `REL_18_6`
- Primary source: `src/backend/commands/vacuum.c`
- Primary source SHA-256:
  - PG15.19:
    `ed7c0b1b0962118d17b3d38821ef1226857f7b15bb2c0407ebaf13b8bd51e89c`
  - PG16.15:
    `a266c29d7695fc84093f722a6b6fb61acc19c5ffe688a89618497e1dd28a3f17`
  - PG17.11:
    `988e3967c1c4700dd80b00dcc5a4dae3df049a6bf46cafc1d88030fddc0f8ae3`
  - PG18.6:
    `bb9071c3813d8dc92b67364a5886aa6139c9ac27e3dc5952e1cb3032b16a0106`
- Public contract: `src/include/commands/vacuum.h`
- Public contract SHA-256:
  - PG15.19:
    `d4e0b1ed8ef9c1f2ede5f888ac80ef268b60aaf75bcb25543b523844b9dd07d7`
  - PG16.15:
    `30221f19f758dc4d07b28cd3f9d83cebbbf9f09a15e4a1b67cd7435951376cf1`
  - PG17.11:
    `273a972adfd62e0579ec22ff35c5f38cbf69ac5c2140a8e682883c6a14675449`
  - PG18.6:
    `3596f803b8fed1b099376de503a26a3b988d728dbc0670f0713e96002782b334`

VACUUM FULL rewrites storage through `relation_copy_for_cluster()`, which a table
provider cannot satisfy because its data lives outside PostgreSQL relfiles.
`lagodb_vacuum.c` is a version-pinned reconstruction of the private option
parsing and relation expansion needed to split one VACUUM FULL statement into a
native part and a provider part. It also holds the per-relation entry that runs
a provider rewrite under PostgreSQL's locks and ownership checks.

Nothing else is routed. Routine VACUUM reaches a provider through
`relation_vacuum`, and ANALYZE runs natively. A statement without FULL enabled
is not parsed.

`MaintenanceExecutor::prepare_command` performs command state checks, full option
parsing and validation, then VACUUM's transaction-block check before name
resolution. A preparation failure therefore precedes PostgreSQL's own, and hooks
below LagoDB in the chain do not see invalid FULL commands. FULL without
`PROCESS_MAIN` is passed to the parent after preparation.

`lagodb_vacuum_probe.c` implements the routing probe. It reads named targets with
`NoLock`, and classifies database-wide commands in a normal `pg_class` catalog
scan. An unresolved name makes the probe negative, so the parent reports it.
PostgreSQL name resolution still checks schema USAGE and invokes namespace search
hooks, so the probe is not side-effect-free. Providers and native tables never
share an inheritance tree, which core's provider hierarchy guard enforces, so a
named target decides for everything it expands to.

A negative probe passes the original statement unchanged to the captured parent.
The probe is only a routing hint: PostgreSQL documents that a `NoLock` name lookup
may become stale under concurrent DDL. Consumed commands repeat expansion and
classify every expanded relation.

Core's private `MaintenanceBridge` owns the hand-written `lagodb_*` extern
declarations. Unlike pgrx bindgen's `pg_sys` functions, these bridge calls do not
receive generated ERROR protection, so the adapter guards the C entries that can
raise ERROR or invoke Rust callbacks. PostgreSQL errors unwind Rust-owned plans
after C cleanup.

After a positive probe, `MaintenancePlan` owns a command-level child of
PortalContext. Expanded nodes and the rewritten statements live in that child,
so they survive `vacuum()`'s per-relation transactions and are deleted when the
router returns or guarded ERROR unwinds it.

Provider execution uses one typed `execute_maintenance` operation for both local
TableAM calls and runtime dispatch. Cross-DSO callbacks return a status and
`CallbackErrorReport`; the router reconstructs `Result` errors synchronously so
its PostgreSQL execution boundary can add command context before reporting.

## Accepted tradeoffs

Native relations run before provider relations, so VACUUM FULL does not follow
PostgreSQL's catalog order across the two groups. The native part and the provider
ANALYZE phase each reach the parent as their own call, so a parent hook sees up to
two calls. Provider rewrites run through the provider callback and never reach it.

## Negative-probe DDL window

The expanded reclassification protects only a positive probe. A negative result
returns the statement to the parent with a definite time-of-check/time-of-use
window: a concurrent transaction can replace a native relation with a provider
table of the same name. PostgreSQL then reaches `relation_copy_for_cluster()`. A
provider built on core's `TableAccessMethod` rejects it by default, as Iceberg
does, so the window fails closed with an error instead of omitting maintenance.
The Delta skeleton installs heap's routine, which can rewrite its heap storage.

### Why the probe holds no locks

PostgreSQL's `vacuum.c::expand_vacuum_rel()` takes a transient `AccessShareLock` on
each named target and releases it before execution. Its comments explicitly
reject locking descendants during expansion and retaining locks on multiple
relations because of deadlock risk.

Keeping targets locked across VACUUM's commits requires session locks. With
session `AccessShareLock(t)` retained by two VACUUM FULL backends, both requests
for `AccessExclusiveLock(t)` wait for the other backend's read lock. Exclusive
probe locks avoid that read-lock upgrade but block native readers and writers
early, and accumulating them over multiple targets adds lock-order dependencies.
These cycles follow PostgreSQL's `lock.c::LockConflicts` and `proc.c::ProcSleep`.
Locking before fallback also makes parent hooks run with target locks already
held; releasing the locks before fallback restores the same window.

PostgreSQL 15 and 16 predate the `MAINTAIN` privilege and
`RestrictSearchPath()`. There the adapter checks permissions with
`vacuum_is_relation_owner()` and leaves `search_path` unchanged, as their own
`vacuum_rel()` does.

PostgreSQL 15 has no `PROCESS_MAIN`, `SKIP_DATABASE_STATS`,
`ONLY_DATABASE_STATS` or `BUFFER_USAGE_LIMIT` option, so the parser rejects them
as unrecognized there; FULL always processes the main relation and the command
always updates `datfrozenxid`. A database-wide expansion checks permissions
before the relkind filter, as PostgreSQL 15's `get_all_vacuum_rels()` does.
PostgreSQL 15 checks `DISABLE_PAGE_SKIPPING` and `PROCESS_TOAST` against FULL in
`vacuum()`, after the transaction-block check; preparation checks them first on
every major. A statement failing both checks reports the option error instead of
the transaction-block error. Either way it fails before any relation is
touched. The `vacuum.c` changes across 15.0–15.19 do not touch option parsing,
expansion or `vacuum_rel()`'s FULL path.

PostgreSQL 15.19, 16.15, 17.11 and 18.6 are the provenance baselines, not the only
accepted minor releases. Before adopting a new baseline or extending another major:

1. Compare option parsing and validation in `ExecVacuum()`.
2. Compare `expand_vacuum_rel()` and `get_all_vacuum_rels()` against the
   execution expansion, including permissions, partitions, `SKIP_LOCKED`, and
   database-wide selection.
3. Compare `vacuum_rel()` transaction, snapshot, security-context and search
   path boundaries.
4. Reconcile every initialized `VacuumParams` field.
5. Refresh both hashes and run the mixed provider/native VACUUM FULL and
   FULL ANALYZE regression matrix.
