# ModifyTable C fork

## PostgreSQL source provenance

- Baseline release: PostgreSQL 17.10
- Upstream tag: `REL_17_10`
- Source: `src/backend/executor/nodeModifyTable.c`
- Source SHA-256:
  `4757e3ce690f5370f1cda6fdcdd10106ca31e31b5e5aa89b233d1297cadac38b`
- Fork: `lagodb_modify_table.c`
- Lifecycle adapter: `lagodb_modify.c`
- Lifecycle contracts: `src/backend/executor/execProcnode.c` at the same baseline release
- Copied scope: the complete file, preserving helper order and control flow
- LagoDB edits: symbol prefixing, standard `ctid`/`wholerow` extraction,
  slot-first insert/update/delete bridge calls, and the exported executor entry

The fork is not a global replacement for PostgreSQL's `ExecModifyTable`.
Planner-selected LagoDB targets are wrapped in an outer CustomScan and call
the exported entry; every other ModifyTable node continues to use PostgreSQL's
registered executor methods. The complete upstream control flow is retained
because row triggers, transition tables, WCO/RLS, MERGE, partition routing,
generated columns, and RETURNING are executor responsibilities that cannot be
reconstructed safely as independent table-AM callbacks.

The lifecycle adapter owns initialization, execution, end, and the unsupported
rescan entry. It allocates a stable object with a `ModifyTableState` prefix and
a node-local borrowed mutation bridge, then lets the fork initialize that
prefix in place. It follows `ExecInitNode`'s common tail: installing the PG
execution wrapper, initializing this node's initPlans, and allocating requested
instrumentation. Execution goes through PostgreSQL's `ExecProcNode`, so stack
checking and instrumentation use the backend's normal wrappers; Rust does not
add a second timer. No global bridge pointer or copied executor state is used.

End follows `ExecEndNode`'s common prefix and calls the fork's end function,
which releases a standalone MERGE root slot even without partition routing.
The fork ends native child nodes through `ExecEndNode` as before. Rust releases
mutation bindings only after that child teardown, and retains its existing
ResourceOwner abort cleanup for ERROR paths. ModifyTable rescans remain
unsupported, matching PostgreSQL; the adapter invokes the fork's rejection.

The C bridge resolves a `ResultRelInfo` only when the active result relation
changes. Per-row trigger and mutation branches reuse the cached opaque
`ResultRelationState`; they must not call back into Rust for provider discovery
or hash-map lookup.

A provider may declare that it owns a PostgreSQL partitioned table as one
logical storage object. Ownership is still resolved per plan: the Rust
initializer compares the actual root RTE's relkind and access method with the
selected provider before entering the C fork. For a provider-owned partitioned
table the fork does
not build `PartitionTupleRouting`, including the MERGE-not-matched
initialization path, and `ExecInsert` reaches the cached root relation state
directly. The root projection slot is retained because MERGE still projects
PostgreSQL rows through the root tuple descriptor. Physical or mixed partition
trees that are not owned by the root's provider keep PostgreSQL's original
routing behavior.

Core's `RelationTriggerPolicy` rejects provider-owned partitioned tables with
AFTER ROW triggers for the statement's operations before entering the mutation
loop. MERGE uses its concrete action set. PostgreSQL's native AFTER ROW queue
requires physical leaf relations for partitioned tables, so the temporary-row
mechanism below supports ordinary provider tables, not provider-owned partitioned tables. The
same policy checks COPY FROM during its locked relation preparation.

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
modifying CTE completion, standalone MERGE root slots, and ERROR cleanup
together when updating this lifecycle.

Confirmed minor compatibility epochs are kept at their semantic sites:

- PG17.1 added `ResultRelInfo::ri_needLockTagTuple` and its tuple-lock flow;
- PG17.6 added the merge-aware `ExecBR*TriggersNew()` entry points and inherited
  MERGE root-relation projection initialization;
- PG17.7 added `CheckValidResultRelNew()` while retaining the older entry point.

The fork selects those paths with local `PG_VERSION_NUM` branches. Other PostgreSQL
minor differences remain part of the baseline diff review; they must not be
promoted into global minor-version rejection.
