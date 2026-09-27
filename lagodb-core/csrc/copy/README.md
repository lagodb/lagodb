# COPY C bridge

## PostgreSQL source provenance

- Baseline release: PostgreSQL 17.10
- Upstream tag: `REL_17_10`
- Internal contract: `src/include/commands/copy.h`
- COPY preparation owner: `src/backend/commands/copy.c`
- COPY FROM owner: `src/backend/commands/copyfrom.c`
- COPY TO owner: `src/backend/commands/copyto.c`

Audited baseline hashes:

- `commands/copy.h`:
  `cb4402e503464fd2988f0506fc1dd6e24dec3d8e2f0d2e5b9ecccc81f775d55b`
- `copyfrom.c`:
  `c78adfdcc4acfc678a798837b7b7f17663f27d485f6f3eb03b7b8c0ce618c10a`
- `copyto.c`:
  `21d2a34fd16fcea4c41f55f650729333fe300dc4242739522f82ab32ab0dfaa1`
- `commands/copy.c`:
  `6336c0d7049f4e883a22c4be6ccc430539584857b79367f7ae8ba07245495d64`

`lagodb_copy_from.c` and `lagodb_copy_to.c` are executors derived from
`src/backend/commands/copyfrom.c` and `src/backend/commands/copyto.c` at the
recorded PostgreSQL release tag. Their externally visible PostgreSQL symbols are renamed, every
LagoDB control-flow change is bounded by `LAGODB BEGIN/END`, and the build
enables them only for the audited PG17 feature.

`lagodb_copy_prepare.c` owns the command preparation derived from `DoCopy`:
utility restrictions, endpoint privileges, relation and column permissions,
RLS, and `COPY FROM WHERE`. `lagodb_copy.c` owns parser and row-encoder adapters.
Neither adapter is a complete copy of PostgreSQL's COPY implementation.
The derived executors add two independent, precomputed routes:

- byte parsing versus a typed Datum source;
- PostgreSQL leaf routing versus a provider-owned partitioned table.

Core classifies `CopyEndpoint` once for each statement view. PROGRAM takes
precedence over URI recognition. Both FROM and TO preparations check PostgreSQL's
server program/file roles before preparing their relation or query; STDIN/STDOUT
and external URI callbacks do not use those server-I/O privileges. Consumers
share this classification, including the runtime's fail-closed URI fallback.
The executors rely on this preparation contract rather than repeating role
checks at file open or per-row dispatch.

The managed partitioned table consumer's admission probe does not open or lock the target.
Core uses PostgreSQL's `LookupNamespaceNoError` / `RelnameGetRelid` name lookup
and copies identity from a pinned `RELOID` syscache tuple. Missing names,
cross-database references, schema ACLs and relation-kind validation are left to
command preparation. Native relations and query COPY delegate to the captured
parent without first taking a target relation lock; URI routing is unchanged.

Admission identity is only a candidate. After PostgreSQL preparation acquires
the execution lock and performs command validation, core binds the actual
target to the claiming AM's provider-owned partitioned table. Routed drivers accept only that bound
preparation. COPY FROM reuses preparation's target classification; COPY TO uses
the prepared named target OID, including RLS query rewrites that close the
relation reference while retaining `AccessShareLock`. A target that no longer
matches is rejected with SQLSTATE `55000` before driver/source/destination I/O;
it is not delegated after preparation. The probe OID need not equal the locked
OID when the name now resolves to another root of the same AM.

This unlocked admission deliberately accepts a negative-probe DDL window:
a native or missing target replaced by a managed partitioned table is still delegated to
the parent. PostgreSQL namespace hooks can also run during the probe, so their
observations and errors need not have native COPY timing. A consumed managed
root continues to bypass the parent. No admission check runs per row.

In PostgreSQL 17, `Relation.rd_tableam` remains null for a
partitioned table even when `pg_class.relam` names a table AM. For a root that
has already been classified as provider-owned, the COPY fork resolves that
AM's handler once from `pg_am` and retains the returned server-lifetime
`TableAmRoutine`; it never mutates the shared relcache relation. COPY FROM
uses the retained routine only at the tuple/multi-insert boundary, where the
normal table-AM callbacks enter the existing COPY-frame `AmCopySession`.
Relation COPY TO uses the same retained routine for one begin/next/end scan.
Ordinary relations continue to use `rd_tableam`.

Query COPY, including RLS rewrites, constructs this fork's COPY destination
receiver directly. PostgreSQL's global `CreateDestReceiver(DestCopyOut)` selects
the native receiver, whose private COPY state layout differs from this fork's
extended state. Keeping the receiver and state in the same executor preserves
their memory-context and row-encoding contract.

Before starting any byte or typed COPY FROM driver, core's relation preparation
classifies the locked target once and applies `RelationTriggerPolicy`.
Provider-owned partitioned tables with AFTER INSERT ROW triggers are rejected
before source I/O or row insertion; PostgreSQL's native trigger queue expects
physical leaves for partitioned-root row events. Disabled and WHEN-false
triggers are also rejected because that native contract precedes
`TriggerEnabled`. BEFORE ROW and statement-level triggers are unchanged. The
preparation retains the selected route for byte and typed drivers, so they do
not repeat provider discovery.

Typed input reports format conversion failures as structured row rejections.
The fork alone applies PostgreSQL `ON_ERROR` policy and owns skipped/processed
counts. A rejection carries its original SQLSTATE, so `ON_ERROR STOP` does not
collapse numeric, datetime, encoding, and other class-22 failures into one
generic text-input error. Sources also expose a monotonic physical-byte
counter; the fork updates `PROGRESS_COPY_BYTES_PROCESSED` after every callback
outcome, including rejected and final reads. Each accepted typed row reports
the materialized pass-by-reference Datum footprint through the same callback,
so PostgreSQL's existing multi-insert byte limit remains effective without a
second datum traversal. Schema, object I/O, provider-state, and internal
conversion failures remain fatal and cross the single typed callback error
boundary.

COPY TO similarly selects PostgreSQL byte encoding or a typed slot
destination once per execution. No format or storage-provider identity appears
in the C fork. Both routes execute in PostgreSQL's reset-per-row allocation
context. Typed destinations report the encoder's monotonic physical byte count
after each row and once more after final footer/flush work, before COPY progress
state is closed.

Query COPY TO finishes the executor and releases its query snapshot before a
typed destination commits its object. COPY state remains alive through writer
finalization so its final byte count still reaches the progress counters.
Shutdown takes the `QueryDesc` out of COPY state before invoking executor
callbacks, following PostgreSQL's `PortalCleanup` in `commands/portalcmds.c`. A failed
execution or shutdown does not run those callbacks again: COPY releases its
snapshot and output workspace, while transaction abort releases remaining
executor resources. The original error propagates to the utility boundary;
the typed destination is aborted without committing its object.
Typed row views reuse `TupleSlotRow`: slot deformation happens once per row,
and selected attributes are read through descriptor-bound array indexes.

`lagodb_copy_datum.c` binds external COPY values to target typmods through
PostgreSQL's `coerce_to_target_type` in assignment context and `ExecInitExpr`.
Scalar coercion functions and `ArrayCoerceExpr` element rules therefore come
from `src/backend/parser/parse_coerce.c`, rather than format-specific casts.
The binding follows PL/pgSQL's `CaseTestExpr` cached-cast pattern in
`src/pl/plpgsql/src/pl_exec.c`. Unconstrained, already-matching, and planner
simplified no-op typmods need no expression. A bound plan owns its expression
context; result datums are allocated in COPY's current per-tuple
context. Conversion errors return through the format source's existing row
rejection boundary. Semantic text construction rejects NUL before creating a
PostgreSQL varlena, including elements of text arrays.
Because `expression_planner` copies expression nodes, no-op detection compares
the planned placeholder structurally with PostgreSQL's `equal`, rather than
using its pre-planning address. This is resolved once at binding, so simplified
scalar typmods such as `time(6)` need no per-datum FFI or expression evaluation.

The relation-bound Text/CSV Foreign Table row encoder uses the source-derived
text/CSV part of `CopyOneRowTo` and its escaping routines from `copyto.c`.
Its opaque `LagodbCopyRowEncoder` owns only the local encoding state; it neither
creates a COPY executor nor reads PostgreSQL's private `CopyToStateData` layout.
It binds COPY options and PostgreSQL output functions once, reuses a
`StringInfo` output buffer, and resets its per-row allocation context before
encoding each slot. Rust receives the completed row buffer without a callback
per row or a line terminator. The encoder allocates its state in an explicitly
owned context under `TopMemoryContext`; Rust's `CopyRowEncoder::finish`/`Drop`
releases it through the local encoder cleanup function. The source-derived
serializer contract was compared across `REL_17_0` through `REL_17_10` and is
unchanged throughout that epoch, so the row encoder has no minor-version branch.

The shared `lagodb_pg_compat.h` gate rejects every major except PostgreSQL
17. The public COPY preparation has local semantic epochs: PG17.0-17.6 has no
generated-column validation, PG17.7-17.9 adds that validation, and PG17.10
adds the system-attribute guard that the bridge also applies to the earlier
validation epoch. The row encoder shares the PG17 build gate, but its local
state layout is independent of PostgreSQL's private COPY executor layout.
A future PG17 minor that changes the encoder's options, output-function, or
serializer contract must add a local `PG_VERSION_NUM` branch here; a new major
remains rejected by the shared compatibility gate.

Before adopting a new PostgreSQL baseline or supporting another release:

1. Compare the internal declarations in `commands/copy.h`.
2. Compare `DoCopy` preparation ordering and relation/permission handling in
   `commands/copy.c`, including each supported version's semantic epochs.
3. Compare the state lifecycle and callback contracts in `copyfrom.c` and
   `copyto.c`.
4. Add a local `PG_VERSION_NUM` branch only when a COPY bridge contract differs.
5. Reconcile the Rust FFI declarations and run the complete COPY regression
   matrix before enabling that PostgreSQL version.
