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

Sessions are finalized in first-touch order. If finalization of a later session
fails, earlier sessions may already have been finalized while later sessions
are aborted. AM designs should account for that. For storage formats with
external metadata commits, the safer pattern is to stage per-relation work and
defer irreversible publication to transaction-scoped state with matching
cleanup.

## Nested And Reentrant Execution

PostgreSQL execution is not always a single flat statement. Triggers, SPI,
data-modifying CTEs, partition routing, MERGE, and COPY can create nested or
interleaved write paths.

The framework tracks the current frame as backend-local execution state rather
than as a process-global singleton. This lets nested writes resolve to the
frame that is actually active at the TableAM callback boundary.

The implementation also treats same-frame reentrancy during finalization or
mutable session access as an internal lifecycle error. Re-entering the same
frame at those points would risk creating duplicate session state or publishing
work through an ambiguous lifecycle.

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
- a relation has at most one session per frame;
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
