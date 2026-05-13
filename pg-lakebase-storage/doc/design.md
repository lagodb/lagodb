Design
======

This document describes the design rationale behind pg-lakebase-storage. It
focuses on *why* the system works the way it does rather than *what* each
code path does — the latter belongs in per-module READMEs and code
documentation. Subsystem-specific design documents live alongside the code:

- `src/cache/README.md` — cache model, invariants, lifecycle, and eviction.
- `src/protocol/README.md` — wire protocol and codec design.
- `src/connection/README.md` — connection pipeline and concurrency model.
- `src/staging/README.md` — staging write lifecycle for database transactions.


1  Positioning
==============

pg-lakebase-storage is a user-space caching service that sits between a
database process and one or more remote object stores (S3, GCS, Azure Blob).
It is not a filesystem, not a FUSE mount, not a general HTTP proxy. The
design optimizes for a narrow workload:

- Objects are immutable once uploaded. The object store guarantees that a
  given `(bucket, key)` pair always returns the same bytes and the same
  `(size, etag)`.
- Reads are sequential or range-sequential within a single object.
- Writes go through an explicit stage→commit flow driven by database
  transaction boundaries.
- The caller (database engine) knows when cached content is stale and
  explicitly invalidates.

These assumptions let the cache skip expensive freshness probes and version
reconciliation that a general-purpose cache would need.


2  Design Principles
====================

2.1  Single-version, externally invalidated cache
--------------------------------------------------

The cache enforces three invariants:

1. **Immutable identity.** Once `size` and `etag` enter a cache row or
   large-fill session, they are immutable facts for the current cache
   lifecycle of that key. The server does not reconcile, overwrite, or
   repair those values from later backend observations.

2. **No generations.** One `(store_id, bucket, key)` maps to at most one
   cached residency at a time. The system does not support multiple
   concurrent versions for the same object key.

3. **External invalidation.** Cache freshness is the caller's
   responsibility. The only boundary that retires a cached residency is an
   explicit `invalidate_object_cache(store_id, bucket, key)` call (or
   capacity-driven eviction, which is not a freshness event). A cache hit
   never issues a backend HEAD to check whether a newer version exists.

These invariants eliminate reconciliation logic, version conflict resolution,
and background polling — complexity that is unnecessary when the upstream
object store guarantees immutable objects and the database engine already
tracks which objects are current.

2.2  The cache is derived data
------------------------------

Every cached byte can be re-fetched from the backend. This means:

- A crash between writing a cache file and updating metadata leaves an
  orphan on disk, not a corruption. Startup recovery and runtime orphan
  passes clean those up.
- Eviction is always safe as long as active handles are respected.
- The metadata index (redb) is a performance optimization, not the source
  of truth for object content.

2.3  Thin network shell
-----------------------

The server architecture is deliberately shallow. A single accept loop feeds
per-connection pipelines. Each pipeline decodes frames, spawns per-request
tasks, and funnels responses through a single-writer queue. There is no
internal routing table, no message broker, no cross-connection coordination
beyond the shared service and cache.

This keeps the data path short: a cache-hit read is decode → handle lookup →
`pread` or KV get → encode → write. A miss adds one backend round-trip and a
cache admission step.

2.4  Synchronous client boundary
--------------------------------

The client is intentionally synchronous and blocking. It is the boundary
designed to be wrapped by other languages (C, Python, Java via JNI). The
server side remains fully async (Tokio). Separating the concurrency boundary
at the Unix socket keeps FFI wrappers simple: they call blocking functions
and never need to manage an event loop.

2.5  Unix domain sockets, not TCP
---------------------------------

The service is co-located with its caller on the same host. UDS avoids TCP
overhead (handshake, Nagle, congestion control) and enables FD passing for
direct-I/O cache reads. The wire format still uses fixed big-endian encoding
so non-Rust clients and cross-endian hosts can share the protocol.

2.6  Staging is path-handoff, not streamed write
-------------------------------------------------

The staging subsystem is designed for database transaction workflows where a
write may happen hours before a commit. Instead of streaming bytes through
the server, the server creates an empty local file and returns its path. The
client writes bytes with standard filesystem calls. At commit time, the
server reads the file and uploads it to the backend.

This design means:

- The server holds no per-staging-file state between `StageCreate` and
  `Commit`/`Abort`.
- A database transaction can write from one connection, drop it, and
  commit from a different connection hours later.
- Orphan staging files from crashed clients are cleaned up by a single
  mechanism: startup wipe of the staging directory.

See `src/staging/README.md` for the full staging lifecycle.


3  Layered Architecture
=======================

```
  +-----------+    +-----------+
  |  client   |    |  server   |   process boundary (Unix socket)
  +-----------+    +-----------+
                        |
  +---------------------v-----------------------+
  |               transport                     |   frame I/O, FD channel
  +---------------------------------------------+
  |               protocol                      |   codec, opcodes, limits
  +---------------------------------------------+
  |               connection                    |   reader, writer, dispatch
  +---------------------------------------------+
  |               service                       |   command routing, open/read
  +-----+-----------------------------------+---+
        |                                   |
  +-----v-----+                       +-----v-----+
  |  backend  |                       |   cache   |
  +-----------+                       +-----------+
  | registry  |                       | index     |
  | obj store |                       | chunks    |
  | memory    |                       | store     |
  +-----------+                       | eviction  |
                                      | staging   |
                                      +-----------+
```

Each layer has a single responsibility:

- **Transport** moves framed bytes and ancillary FDs on a Unix stream.
- **Protocol** defines the binary wire format (opcodes, field layout, limits).
- **Connection** manages per-socket lifetime: concurrent request tasks,
  backpressure via a bounded response queue, and graceful shutdown on reader
  EOF.
- **Service** translates typed commands into cache and backend operations.
- **Backend** wraps `object_store` crate clients behind a trait so tests can
  use in-memory stores and production can use S3/GCS/Azure.
- **Cache** owns the on-disk layout, metadata index, small-object KV, large
  file chunk sessions, and LRU eviction.
- **Staging** manages the separate staging tree for write workflows.


4  Read Path
============

```
  OPEN
   |
   +--- cache hit (SmallKv)?  -----> bind payload from index
   |
   +--- cache hit (CompleteFile)? --> bind file metadata
   |
   +--- live fill session? --------> join existing session
   |
   +--- establishment election ----+
        |                          |
        +-- leader: HEAD backend --+
        |   size <= small limit?   |
        |   yes: GET, admit small  |
        |   no:  create fill session
        |                          |
        +-- follower: wait --------+
            then re-enter lookup

  READ (on open handle)
   |
   +--- SmallKv residency --------> slice from in-memory payload
   |
   +--- CompleteFile residency ----> pread from cache file
   |    (or direct-I/O FD)
   |
   +--- LargeFill residency ------> claim chunk slot
        |                           leader: fetch from backend, write partial
        |                           follower: wait for leader
        +-- all chunks present? --> promote partial → complete
```

The key design choice: OPEN freezes the residency for the handle's lifetime.
Subsequent READs on the same handle never re-probe the index. This makes
reads zero-KV-operation after the initial open.


5  Write Path (Staging)
=======================

```
  StageCreate(store_id, bucket, key)
   |
   +--- server creates empty file under staging/
   |    returns absolute path to client
   |
   v
  Client writes bytes with local filesystem calls (append-only)
   |
   +--- hours may pass (database transaction in progress)
   |
   v
  Commit(store_id, bucket, key)           or    Abort(store_id, bucket, key)
   |                                              |
   +--- server reads staging file                 +--- server unlinks staging file
   |    uploads to backend via put_from_file              (idempotent)
   |    unlinks staging file on success
   |    returns {size, etag}
   |
   +--- NOTE: does NOT invalidate cache
        caller must call invalidate_object_cache
        if they want new opens to see the upload
```

Staging semantic contract (client-side):

- **Append-only.** The client opens the staging file with `O_APPEND`.
- **Single writer.** Only one staging file per `(store_id, bucket, key)` at
  a time (enforced server-side by `O_EXCL` on create).
- **No readers before commit.** Staged bytes are invisible to the database
  until the transaction commits and metadata is published. It is the caller's
  responsibility not to open the staged key before commit.

Cleanup: the entire staging directory is wiped on server startup. There is no
online orphan reaper for staging. A database process that crashes leaves
staging files that will be removed on the next restart.


6  Concurrency Model
====================

- One Tokio accept loop → per-connection tasks.
- Each connection: concurrent request decoding, but a single writer task
  serializes response frames and FD sends.
- Per-connection semaphore bounds in-flight requests.
- Response backlog is bounded by both item count and byte budget, so a slow
  consumer cannot cause unbounded memory growth.
- READ reserves its maximum response bytes before any allocation, so a
  slow peer cannot accumulate `queued_responses * max_read_size` of payload.
- A response write timeout disconnects peers that stop consuming.
- Connection shutdown is an explicit state machine: reader EOF stops
  accepting new requests, gives in-flight work a bounded drain window, then
  closes the writer.

See `src/connection/README.md` for the detailed concurrency design.


7  Cache Crash-Recovery Boundary
================================

The cache intentionally splits durable (KV) and non-durable (file) state:

- Chunk writes land in partial files and process-local memory only.
- Only after all chunks are present does promotion rename the partial file
  to the complete path and write metadata to the index.
- A crash between these steps leaves an orphan file but never metadata
  pointing at a partial file.

This is acceptable because the cache is derived data. Startup recovery
scans the cache directory and deletes unclaimed files. If metadata points at
a missing complete file, the I/O error is returned to the caller — the
server does not attempt repair.


8  Future Directions
====================

- **Generation-addressed residency.** Supporting old and new versions of the
  same key concurrently (for zero-downtime schema migrations) would require a
  generation dimension in the cache key and residency leases that outlive the
  current single-version model.
- **Filesystem block accounting.** Eviction currently uses logical payload
  bytes. Accounting for filesystem block overhead would give more accurate
  capacity decisions.
- **Minimum-free-disk pressure.** A policy that triggers eviction when host
  free disk drops below a threshold, independent of cache byte budgets.
- **Online staging orphan reaper.** Periodic cleanup of staging files that
  have been abandoned beyond a configurable timeout.
