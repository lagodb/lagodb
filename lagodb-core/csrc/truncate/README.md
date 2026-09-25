# TRUNCATE bridge

`lagodb_truncate.c` is core's PostgreSQL-major-version boundary derived from the
PostgreSQL `ExecuteTruncate` / `ExecuteTruncateGuts` flow in
`src/backend/commands/tablecmds.c`.

## PostgreSQL source provenance

- Baseline release: PostgreSQL 17.10
- Upstream tag: `REL_17_10`
- Source: `src/backend/commands/tablecmds.c`
- Adapted scope: `ExecuteTruncate`, `ExecuteTruncateGuts`, and their validation helpers

## Statement admission

Provider registration only enables inspection; it does not claim a command.
`lagodb-base/src/process_utility/truncate_router.rs` resolves the explicitly named targets
with `NoLock`, then reads each target's `relkind` and `relam` from one pinned
`pg_class` syscache tuple. A target is a managed partitioned table only when it is a
partitioned table and its own access method matches a provider that declares
partitioned table ownership. The tuple is released before provider matching, and
the probe keeps no relation handles, target locks, or OIDs for execution.

When no explicit target matches, the original complete statement is passed to
the captured parent `ProcessUtility` hook. This includes native-only commands
and commands targeting ordinary managed Iceberg tables, whose TableAM handles
native TRUNCATE. A missing name or catalog row ends the probe immediately and
delegates the complete statement to parent. The executor owns missing-target
errors; the probe does not resolve later targets whose errors would take
precedence over an earlier missing target.

If a provider-owned partitioned table matches before the probe encounters an unresolved target,
the bridge consumes the complete statement.
It resolves names again and classifies the actual relations under execution
locks. Native and provider targets are not split into separate utility commands.
Consumed commands, including mixed commands, still bypass the captured parent
hook. This routing change does not promise parent-hook observation of those
commands or change VACUUM/ANALYZE routing.

## Target topology

The probe does not inspect `RangeVar.inh`, traverse descendants, or construct a
foreign-key CASCADE closure. Those remain executor responsibilities. Explicit
target classification relies on managed partitioned tables being standalone: they must not
be children or parents in PostgreSQL's partition/inheritance topology.

Iceberg enforces this at the `pg_inherits` object-access boundary in
`lagodb-iceberg/src/managed_table/hooks/table_ddl/topology.rs`. PostgreSQL's
`StoreCatalogInheritance1()` already holds the required relation locks and
emits `OAT_POST_ALTER` after recording the relationship. The guard rejects new
relationships involving a managed partitioned table before ATTACH clones indexes, triggers,
or foreign keys; it also covers direct `CREATE TABLE PARTITION OF ... USING
iceberg`. It uses `SnapshotSelf` to see the current command's changes and
distinguish creation from removal. No new target locks are taken by the guard.
Providers declaring root ownership must enforce this standalone topology for
the explicit-target routing contract to hold.

Current Iceberg DDL also rejects foreign keys on its managed relations: both
`ALTER TABLE ADD CONSTRAINT` and CREATE's generated constraint subcommands go
through that guard. Root-side foreign keys are therefore not another supported
way for a native-only CASCADE command to discover a managed partitioned table. Attachment
cannot bypass the restriction by cloning a native parent's constraints because
the topology guard rejects that relationship before constraint cloning.

## Accepted limits of NoLock admission

A negative probe is not protected against concurrent DDL:

1. The probe resolves `t` and observes a native relation.
2. Another backend drops it and creates a managed partitioned table named `t`.
3. The parent utility path resolves and locks the replacement.
4. PostgreSQL skips the partitioned table's storage action, so provider TRUNCATE is omitted.

PostgreSQL's `namespace.c` documents that `RangeVarGetRelidExtended(..., NoLock, ...)`
returns its initial answer without the lock/invalidation retry. Reclassification
under execution locks protects only positive probes; a negative probe never
enters this executor. This correctness limit is accepted to retain parent
fallback without introducing target locks ahead of the parent hook. A parent
hook that rewrites a negative-probe command to target a root is likewise outside
this admission guarantee.

NoLock means no target relation lock, not side-effect-free classification.
Name resolution still checks schema permissions and invokes namespace search
hooks before parent fallback, and the parent resolves the names again. The
probe does not perform TRUNCATE ACL, activity, FK, sequence, or object-truncate
checks, and does not run provider storage actions.
Stopping at unresolved targets preserves the missing-target error order, but
does not establish complete native error ordering: a later schema lookup error
can still precede an earlier target's TRUNCATE ACL or activity error.

Retaining an AccessShareLock through fallback introduces a new upgrade path:
two backends can both hold the probe read lock and then each wait for the
other's lock when TRUNCATE requests AccessExclusiveLock. Taking the final
exclusive lock instead avoids that upgrade but moves target locking before
captured parent hooks; for example, pg_stat_statements then excludes that
earlier lock wait from its utility timing. Releasing probe locks before
fallback restores the DDL window. The chosen probe adds no target-lock upgrade
or retained target locks; it does not claim that native TRUNCATE itself is
deadlock-free.

## Execution and PostgreSQL version boundary

The consumed path preserves PostgreSQL's per-target sequence: command-state
checks; pre-lock relation/type/system-catalog/object-hook/ACL validation with
DDL retry; relation locking and activity checks; inheritance expansion; then
iterative FK CASCADE expansion and validation. Native heap, partitioned-root,
and foreign-table targets retain PostgreSQL's storage behavior. Only a
provider-owned partitioned table replaces the native storage-less partitioned table
step with the provider action. Mixed statements remain one atomic trigger,
storage, sequence, WAL, and failure lifecycle.

When porting to another PostgreSQL major, compare relation expansion, ACL and
FK ordering, trigger setup/teardown, foreign-table batching, relfilenumber and
index handling, sequence reset, logical-decoding WAL, and utility read-only /
parallel / recovery checks. Core's execution plan and Rust declarations, runtime admission policy, and
regression cases in `lagodb-iceberg/tests/pg_regress/sql/truncate.sql` are part
of that audit.

Logical-replication apply calls PostgreSQL's `ExecuteTruncateGuts` directly and
does not pass through `ProcessUtility`; this bridge intentionally does not
claim to add provider-root TRUNCATE support to that separate execution path.
