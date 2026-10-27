# COPY C bridge

## PostgreSQL source provenance

- Baseline releases: PostgreSQL 16.15, 17.11 and 18.6
- Upstream tags: `REL_16_15`, `REL_17_11` and `REL_18_6`
- Internal contract: `src/include/commands/copy.h`
- COPY preparation owner: `src/backend/commands/copy.c`
- COPY FROM owner: `src/backend/commands/copyfrom.c`
- COPY TO owner: `src/backend/commands/copyto.c`

Audited baseline hashes:

- `commands/copy.h`: PG16.15
  `3c316924ad3ae5cca7f727752234bd26d35d58082f04cd6f0977b7815dad73d4`,
  PG17.11
  `cb4402e503464fd2988f0506fc1dd6e24dec3d8e2f0d2e5b9ecccc81f775d55b`,
  PG18.6
  `e03cbf5e07eef661f95b1a5d37111c729d30e7659970b26bb163b73d00e8bb88`
- `copyfrom.c`: PG16.15
  `9cab577858992d631bef19a6d7280cfed3c3606de864d4f4ceebb8cc12c1f52a`,
  PG17.11
  `c78adfdcc4acfc678a798837b7b7f17663f27d485f6f3eb03b7b8c0ce618c10a`,
  PG18.6
  `2eacfb8e896414428a8d124921fdc38629afe55c391c0fc16bce99b2db8bd86f`
- `copyto.c`: PG16.15
  `eea3d037e88f70b1906389b6da2784de8e6ea57495524e432480ab9a2765d553`,
  PG17.11
  `21d2a34fd16fcea4c41f55f650729333fe300dc4242739522f82ab32ab0dfaa1`,
  PG18.6
  `f160930f5821d7320f3ac686f4edc4a7b341f84697ee5200eeff352f331c0bd5`
- `commands/copy.c`: PG16.15
  `71ab43c934612c473dde6332317aafa5f058c21d5390b3b40ce8ad853a392947`,
  PG17.11
  `6336c0d7049f4e883a22c4be6ccc430539584857b79367f7ae8ba07245495d64`,
  PG18.6
  `f50ecc1531109fc172d841d461e209cef611f3d017ef4184034c39e128deabbe`

`lagodb_copy_from.c` and `lagodb_copy_to.c` are executors derived from
`src/backend/commands/copyfrom.c` and `src/backend/commands/copyto.c` at the
recorded PostgreSQL release tag. Their externally visible PostgreSQL symbols are renamed, every
LagoDB control-flow change is bounded by `LAGODB BEGIN/END`, and the build
enables them only for the audited PG16, PG17 and PG18 features.

`lagodb_copy_prepare.c` owns the command preparation derived from `DoCopy`:
utility restrictions, endpoint privileges, relation and column permissions,
RLS, and `COPY FROM WHERE`. `lagodb_copy.c` owns parser and row-encoder adapters.
Neither adapter is a complete copy of PostgreSQL's COPY implementation.
The derived executors add one precomputed route: byte parsing versus a typed
Datum source.

Core classifies `CopyEndpoint` once for each statement view. PROGRAM takes
precedence over URI recognition. Both FROM and TO preparations check PostgreSQL's
server program/file roles before preparing their relation or query; STDIN/STDOUT
and external URI callbacks do not use those server-I/O privileges. Consumers
share this classification, including the runtime's fail-closed URI fallback.
The executors rely on this preparation contract rather than repeating role
checks at file open or per-row dispatch.

A table AM that needs the write bridge is identified before preparation. The
admission probe does not open or lock the target. Core uses PostgreSQL's
`LookupNamespaceNoError` / `RelnameGetRelid` name lookup and copies identity from
a pinned `RELOID` syscache tuple. Missing names, cross-database references, schema
ACLs and relation-kind validation are left to command preparation.

Query COPY, including RLS rewrites, constructs this fork's COPY destination
receiver directly. PostgreSQL's global `CreateDestReceiver(DestCopyOut)` selects
the native receiver, whose private COPY state layout differs from this fork's
extended state. Keeping the receiver and state in the same executor preserves
their memory-context and row-encoding contract.

Typed input reports format conversion failures as structured row rejections.
The fork alone applies PostgreSQL `ON_ERROR` policy and owns skipped/processed
counts. A rejection carries its original SQLSTATE, so `ON_ERROR STOP` does not
collapse numeric, datetime, encoding, and other class-22 failures into one
generic text-input error. PostgreSQL 16 has no `ON_ERROR` option, so every
rejection there ends the statement with its SQLSTATE. Sources also expose a monotonic physical-byte
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
serializer contract was compared across `REL_17_0` through `REL_17_11` and
against `REL_16_15` and `REL_18_6`; the owned contract is unchanged, so the row encoder needs
no version branch.

The shared `lagodb_pg_compat.h` gate accepts PostgreSQL 16, 17 and 18. The
public COPY preparation has local semantic epochs: PG16.0-16.10 and PG17.0-17.6
have no generated-column validation, PG16.11-16.13 and PG17.7-17.9 add that
validation (`LAGODB_HAS_COPY_WHERE_GENERATED_CHECK`), and PG16.14 and PG17.10
add the system-attribute guard that the bridge also applies to the earlier
validation epoch. PG16 also lacks `ON_ERROR`, `LOG_VERBOSITY` and the
`PqMsg_*` protocol constants; the fork defines the COPY TO bytes locally. PG18
COPY FROM delegates byte-format parsing through a
`CopyFromRoutine`; the fork installs the matching text, CSV, or binary routine
before any call to `NextCopyFrom`. Typed input deliberately bypasses that byte
parser. COPY TO retains LagoDB's complete row-encoding lifecycle and adapts
only the PG18 executor signature and populated-materialized-view semantics;
duplicating PG18's native format-routine layer there would not own any work.
The row encoder shares the supported-major build gate, but its local state
layout is independent of PostgreSQL's private COPY executor layout. A future
supported minor that changes the encoder's options, output-function, or
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
