# DML Lifecycle Principles

This module builds a managed DML lifecycle on top of PostgreSQL's TableAM
tuple callbacks.

PostgreSQL tells a table access method when an individual tuple is inserted,
updated, or deleted. It does not provide a single high-level callback that says
"this logical write operation has started" or "all writes for this operation
have completed successfully". That distinction matters for lakehouse-style
access methods: file writers, metadata updates, and pending cleanup usually
need a broader lifecycle than a single tuple callback.

The purpose of this module is to provide that broader lifecycle without making
AM implementations infer it themselves.

## Core Model

The framework treats a PostgreSQL write operation as a DML frame.

A frame represents one logical write boundary, such as a ModifyTable execution
or a COPY FROM command. A frame may touch one relation or many relations. The
multi-relation case is normal for partition routing, where one SQL statement
can write to several leaf relations.

Within a frame, the framework creates relation-local sessions lazily. The first
write to a relation creates that relation's session; later writes to the same
relation and frame reuse it. This keeps session state scoped to the real
PostgreSQL write boundary while avoiding work for plans that never produce
tuple writes.

Auxiliary state established before the first tuple callback can register a
frame cleanup. Registration materializes the frame eagerly, so zero-row writes
and failures during scan setup still release that state. Frame cleanup runs
after relation sessions finish or abort, preserving session access to the
auxiliary state for their full lifetime.

Conceptually, the lifecycle is:

```text
frame starts
  relation session starts on first write
  tuple callbacks are dispatched to the relation session
frame succeeds
  touched relation sessions are finalized
frame fails, aborts, or rolls back
  unfinalized relation sessions discard their work
```

The important boundary is the frame, not the individual tuple callback.

## Success And Failure

Successful DML completion is recognized only at PostgreSQL boundaries that
mean the write operation really finished. For ModifyTable execution, that is
the end of the ModifyTable node. For COPY FROM, that is the successful end of
the COPY command.

Executor teardown is not treated as success. Teardown can happen after success,
after errors, or during cleanup, so using it as the commit boundary would mix
normal completion with failure recovery.

Failure handling is tied to PostgreSQL ResourceOwner cleanup. PostgreSQL ERROR
paths use non-local control flow, so Rust code cannot rely on normal stack
returns to observe every failure. ResourceOwner cleanup is the PostgreSQL
mechanism that runs during abort, rollback-to-savepoint, and error cleanup.

This gives the framework two simple rules:

- success explicitly finalizes the frame;
- anything left unfinalized is aborted by cleanup.

AM implementations should therefore keep externally visible publication out of
per-tuple callbacks. They should stage work during the frame and publish only
from a point that remains correct if later relation sessions in the same frame
fail.

## Relation Sessions

A relation session is the AM-owned state for one target relation inside one
DML frame. It is the place for state such as open writers, buffered rows,
temporary files, or metadata staged for later publication.

Sessions are relation-local because PostgreSQL can route rows from a single
logical DML operation into multiple relations. A single global session for the
whole statement would either conflate relation state or force AMs to implement
their own relation dispatch layer.

Session construction also receives the frame's logical target-read
requirement. This is resolved from the initialized ModifyTable plan, not
inferred from whether a scan callback happened. Most UPDATE, DELETE, and MERGE
plans require a target read. A MERGE whose target side PostgreSQL proved
unreachable, such as `MERGE ... ON FALSE`, has no target read and its remaining
insert action is an independent append. An AM combines this requirement with
its own storage-version observation; a required but unobserved read is an
invariant failure, while an independent append needs no read validation.

The framework does not attempt to turn dynamic source rows into key-level
conflict predicates. That would make memory and commit cost proportional to the
source cardinality and would incorrectly imply primary-key enforcement. A
table AM may narrow validation with a static target predicate; otherwise it
must explicitly choose a conservative whole-table scope, weaker snapshot
isolation, or reject the operation. Cross-table serializability remains the
responsibility of PostgreSQL SSI integration rather than this frame lifecycle.

Sessions are finalized in first-touch order. If finalization of a later session
fails, earlier sessions may already have been finalized while later sessions
are aborted. AM designs should account for that. For storage formats with
external metadata commits, the safer pattern is to stage per-relation work and
defer irreversible publication to transaction-scoped state with matching
cleanup.

## Tuple Ownership And Batching

The DML callback boundary is intentionally slot-first.

PostgreSQL hands the table AM a tuple slot for insert and update callbacks. The
framework dispatches a short-lived, callback-scoped view of that slot to the AM
session rather than eagerly copying it into an owned row. Core must not decide
the tuple's storage representation before the AM has chosen its write strategy.

This preserves two different hot paths:

- A row-oriented AM buffers rows that must outlive the callback, so it
  materializes owned values. PostgreSQL can reuse tuple slots and reset memory
  contexts once a callback returns, so buffered rows cannot keep data borrowed
  from the slot.
- A columnar AM appends slot values directly into its own column builders and
  avoids the intermediate owned-row allocation entirely.

The default slot methods fall back to the owned-row path. That keeps
row-oriented AMs simple while letting columnar AMs override the slot methods and
stay on the direct path.

## Nested And Reentrant Execution

PostgreSQL execution is not always a single flat statement. Triggers, SPI,
data-modifying CTEs, partition routing, MERGE, and COPY can create nested or
interleaved write paths.

The framework tracks the current frame as backend-local execution state rather
than as a process-global singleton. This lets nested writes resolve to the
frame that is actually active at the TableAM callback boundary.

Per-row session access relies on a reentrancy *contract* (see `AmDmlSession`)
rather than a per-row runtime guard: a tuple callback must not synchronously
re-enter the table-AM write path for the same frame. PostgreSQL's executor
upholds this (it completes `table_tuple_*` before index maintenance and AFTER
triggers, and nested trigger/SPI DML runs in a new ModifyTable frame), so the
hot path can hand out a `&mut` to the per-relation session without paying to
defend a case the supported execution model never produces.

## COPY FROM

COPY FROM is not a ModifyTable execution path, but it still invokes TableAM
insert callbacks. It therefore needs its own frame boundary.

The principle is the same as ModifyTable:

- create a frame for the COPY operation;
- dispatch insert callbacks through relation-local sessions;
- finalize only if COPY completes successfully;
- rely on ResourceOwner cleanup if COPY exits through ERROR.

The implementation must keep COPY framing separate from ModifyTable framing
because PostgreSQL reaches the TableAM through different executor paths.

## Unsupported Write Paths

The framework should fail unsupported write paths explicitly rather than
silently running them outside a managed DML frame.

Examples include rewrite-style or receiver-based paths that do not naturally
pass through the managed ModifyTable or COPY FROM boundaries. Future support
for those paths should add a dedicated frame boundary for that PostgreSQL
execution path. It should not overload an unrelated frame type just to make the
callback compile.

## Design Invariants

The DML lifecycle is built around these invariants:

- every tuple write handled by this framework belongs to a current frame;
- each frame owns zero or more relation-local sessions;
- frame-scoped auxiliary cleanup runs after relation sessions are released;
- a relation has at most one session per frame;
- each relation session receives its logical target-read requirement from the
  owning frame rather than inferring it from physical scan callbacks;
- DML callbacks dispatch callback-scoped slot views first; owned-row
  materialization is a row-mode fallback, not a core callback default;
- slot and datum views are callback-scoped and must not be stored across
  callbacks;
- row batches own their values; columnar fast paths append slot data directly
  into AM-owned builders;
- successful frame completion finalizes touched sessions exactly once;
- unfinalized sessions abort when their frame is cleaned up;
- rollback-to-savepoint aborts only work owned by the rolled-back scope;
- RELEASE SAVEPOINT preserves work by moving ownership to the parent scope;
- unsupported paths return a clear error instead of bypassing lifecycle
  management.

These invariants are more important than the particular helper functions used
to implement them. Code comments should explain local mechanics; this document
should stay focused on the lifecycle model and the PostgreSQL boundaries that
make it necessary.
