# ModifyTable C fork

## PostgreSQL source provenance

- PostgreSQL 15 baseline: `REL_15_19`
  - Source: `src/backend/executor/nodeModifyTable.c`
  - SHA-256:
    `2b801f382f287e6b4dac19e45adf25b0f022a208e1b56a5c9b8b6be771e84277`
- PostgreSQL 16 baseline: `REL_16_15`
  - Source: `src/backend/executor/nodeModifyTable.c`
  - SHA-256:
    `f59813a20cd984f3607ced2ad39ce98263dede986222dadbd89be41ce0bb21c5`
- PostgreSQL 17 baseline: `REL_17_11`
  - Source: `src/backend/executor/nodeModifyTable.c`
  - SHA-256:
    `4757e3ce690f5370f1cda6fdcdd10106ca31e31b5e5aa89b233d1297cadac38b`
- PostgreSQL 18 baseline: `REL_18_6`
  - Source: `src/backend/executor/nodeModifyTable.c`
  - SHA-256:
    `0fc3cb180b3443f216142954e43d8ed5f84713d75094ce8cd2785ad8f604bd68`
- Fork: `lagodb_modify_table.c`
- Lifecycle adapter: `lagodb_modify.c`
- Lifecycle contracts: `src/backend/executor/execProcnode.c` at the corresponding
  baseline release
  - PG15.19 SHA-256:
    `19836c50a272741a4eac653541e655437c2e00710a541e5348d6a277d0669d7c`
  - PG16.15 SHA-256:
    `e48c08e555f8cb4e4bb43df516c4b8906ce9bc374b2a745d98a1fc8c22cc5099`
  - PG17.11 SHA-256:
    `a77576e158b94cb01fa8c5174ba133004eabdd727660323f8afc66c8d2e757b8`
  - PG18.6 SHA-256:
    `f8a06a3f539077249b20664b2812433db6d7bd12b2c0ca633525db43d06f112a`
- Copied scope: the complete file, preserving helper order and control flow
- LagoDB edits: symbol prefixing, standard `ctid`/`wholerow` extraction,
  slot-first insert/update/delete bridge calls that replace the native storage
  calls, and the exported executor entry

The fork is not a global replacement for PostgreSQL's `ExecModifyTable`.
Planner-selected LagoDB targets are wrapped in an outer CustomScan and call
the exported entry; every other ModifyTable node continues to use PostgreSQL's
registered executor methods. The planner selects from the nominal relation and
core's provider hierarchy guard keeps other access methods out of the hierarchy,
so every result relation is provider-owned and the fork has no native storage
path. An inherited target's root `ResultRelInfo` is outside the result array,
which is why `resolve_relation` can still meet a relation for the first time.

Upstream control flow is otherwise retained because row triggers, transition
tables, WCO/RLS, MERGE, generated columns, and RETURNING are executor
responsibilities that cannot be reconstructed safely as independent table-AM
callbacks. Partition routing stays as unmodified upstream code: a provider
relation is never a PostgreSQL partition, so it does not run here.

The lifecycle adapter owns initialization, execution, end, and the unsupported
rescan entry. It allocates a stable object with a `ModifyTableState` prefix and
a node-local borrowed mutation bridge, then lets the fork initialize that
prefix in place. It follows `ExecInitNode`'s common tail: installing the PG
execution wrapper, initializing this node's initPlans, and allocating requested
instrumentation. Execution goes through PostgreSQL's `ExecProcNode`, so stack
checking and instrumentation use the backend's normal wrappers; Rust does not
add a second timer. No global bridge pointer or copied executor state is used.

End follows `ExecEndNode`'s common prefix and calls the fork's end function.
The fork ends native child nodes through `ExecEndNode` as before. Rust releases
mutation bindings only after that child teardown, and retains its existing
ResourceOwner abort cleanup for ERROR paths. ModifyTable rescans remain
unsupported, matching PostgreSQL; the adapter invokes the fork's rejection.

The C bridge resolves a `ResultRelInfo` only when the active result relation
changes. Per-row trigger and mutation branches reuse the cached opaque
`ResultRelationState`; they must not call back into Rust for provider discovery
or hash-map lookup.

For immediate AFTER ROW triggers, the fork gives PostgreSQL a statement-local
synthetic `ctid`; `tuple_fetch_row_version` resolves that carrier from a
query-level, relation-keyed PostgreSQL tuplestore populated from the
already-carried wholerow. A core-owned temporary-ID namespace routes sibling
ModifyTable nodes and nested SPI queries without relation-OID guessing. This
follows the FDW storage model without changing shared relcache `relkind` or
retaining complete rows in a Rust `HashMap`. Two bounded read slots cover
repeated OLD/NEW fetches for multiple triggers. Deferrable AFTER ROW triggers
are rejected because their event lifetime exceeds the query-local tuplestore.

Audit a PostgreSQL minor update by comparing `lagodb_modify_table.c` with
`src/backend/executor/nodeModifyTable.c` from the recorded release tag and the
new release. Reconcile the `LAGODB BEGIN/END` sections and every local
`PG_VERSION_NUM` compatibility branch, then update the release tag and source
hash before running the full PostgreSQL regression suite for the target version. Concurrent
identity-aware EPQ and PostgreSQL indexes remain deliberately unsupported for
Iceberg relations.

Also compare the adapter's initialization and end contracts with the new
`execProcnode.c`, and confirm `ExecSetExecProcNode`/`ExecProcNode` still own the
execution wrappers. Audit EXPLAIN instrumentation, initPlans, auxiliary
modifying CTE completion, and ERROR cleanup together when updating this
lifecycle.

PostgreSQL minor compatibility epochs are kept at their semantic sites:

- PG15.2 added the `EState` lists of pending FDW batch inserts
  (`LAGODB_HAS_INSERT_PENDING_LISTS`); PG15.0-15.1 flush the remaining batches
  from the opened or routed result relations when the node finishes;
- PG15.2 added `ResultRelInfoExtra` (`LAGODB_HAS_RESULT_REL_INFO_EXTRA`), which
  holds the generated columns that PostgreSQL's `ExecGetExtraUpdatedCols()`
  reads; PG15.0-15.1 read the planner's `extraUpdatedCols` instead;
- PG15.4 added `EvalPlanQualInitExt()` with the result relations that EPQ
  blocks (`LAGODB_HAS_EPQ_RESULT_RELATIONS`); PostgreSQL 16 takes them in
  `EvalPlanQualInit()`;
- PG15.9, PG16.5 and PG17.1 added `ResultRelInfo::ri_needLockTagTuple` and its
  tuple-lock flow (`LAGODB_HAS_RI_NEED_LOCK_TAG_TUPLE`);
- PG17.6 added the merge-aware `ExecBR*TriggersNew()` entry points;
- PG15.14, PG16.10 and PG17.6 added inherited MERGE root-relation projection
  initialization (`LAGODB_HAS_MERGE_ROOT_WITH_CHECK`);
- PG15.15, PG16.11 and PG17.7 added `CheckValidResultRelNew()` while retaining
  the older entry point (`LAGODB_HAS_CHECK_VALID_RESULT_REL_NEW`).

PG15.3 added `ExecBR*TriggersNew()` with TM result outputs. Those outputs are
written only when the trigger locks a row by TID; the fork always passes the
carried wholerow, so it calls the original `ExecBR*Triggers()` entry points
present in every PG15 release.

The fork selects those paths with local `PG_VERSION_NUM` branches and the named
capabilities in `lagodb_pg_compat.h`. Other PostgreSQL
minor differences remain part of the baseline diff review; they must not be
promoted into global minor-version rejection.

PostgreSQL 15 differs from PostgreSQL 16 only in executor APIs: one
`ri_GeneratedExprs` array per result relation, which `ExecInitModifyTable`
fills for every generated column because a MERGE can both update and insert;
a boolean index-update result; the 7-argument `ExecInsertIndexTuples()`; the
2-argument `build_attrmap_by_name()`; `ri_RootToPartitionMap` instead of
`ExecGetRootToChildMap()`; and the 2-argument `pgstat_count_heap_update()`.
Its MERGE control flow matches PostgreSQL 16.

PostgreSQL 16 keeps MERGE actions in separate `ri_matchedMergeAction` and
`ri_notMatchedMergeAction` lists selected by `MergeAction::matched`, and records
the running action in `ModifyTableContext::relaction`. It has no `WHEN NOT
MATCHED BY SOURCE`, MERGE `RETURNING` or `merge_action()`, so the fork compiles
those PG17 paths out. `LAGODB_MERGE_ACTION()` names the running action on every
major.

PostgreSQL 18 differences are likewise kept at the owning executor methods. In
particular, the fork preserves PG18's OLD/NEW RETURNING semantics, runtime
pruning of result relations, generated-column dependency tracking, trigger
entry points, and cross-partition UPDATE tuple lifetime. LagoDB's slot-first
mutation bridge remains the only change to those upstream flows.
