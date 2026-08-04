Service Layer
=============

The service layer is the central request dispatcher. It maps inbound wire
commands to typed outputs, wiring together attached backend contexts, the
shared cache, staging uploader, and per-connection handle/list tables.


1  StorageService
=================

`StorageService<I: CacheIndex>` owns:

```
  managed_stores   ManagedStoreRegistry runtime-owned volume attach source
  backend_pool     Arc<BackendPool>     weak configured-backend interning
  cache            Arc<CacheManager<I>> residency, reads, eviction
  staging_uploader Arc<StagingUploader> caller-owned staging upload path
  config           StorageServiceConfig limits (max_read_size, etc.)
```

A single `StorageService` is shared across all connections via `Arc`.
Per-connection `StorageContext` owns the attached backend context, open-file
`HandleTable`, and list-session table. Attach happens once before concurrent
dispatch starts, so ordinary handlers do not resolve a registry identifier.


2  Command Dispatch
===================

Every inbound wire verb is parsed into a `StorageCommand` variant.
`execute` matches the variant and delegates to the appropriate handler:

```
  StorageCommand     Handler           Module
  ─────────────────  ────────────────  ───────────────
  AttachManaged      resolve_attach     connection/attach.rs
  AttachConfigured   resolve_attach     connection/attach.rs
  Open               handle_open       open.rs
  Read               handle_read       range_reader.rs
  Close              handle_close      mod.rs
  Upload             handle_upload     object_ops.rs
  ProbeStore         handle_probe_store object_ops.rs
  Head               handle_head       object_ops.rs
  InvalidateObjectCache  handle_invalidate_object_cache  object_ops.rs
  Delete             handle_delete     object_ops.rs
  DeletePrefix       handle_delete_prefix   object_ops.rs
  DeleteObjects      handle_delete_objects  object_ops.rs
  List               handle_list       list_ops.rs
  CloseList          handle_close_list list_ops.rs
```

READ has an additional entry point — `handle_admitted_read` — for reads
that were pre-admitted on the connection's inbound path (see the
connection README for admission ordering).


3  Open Flow
============

OPEN establishes a cached residency for an object and mints a handle:

```
  1. Read the immutable attached context; build physical ObjectLocation and
     validate cache paths
  2. Reserve an open handle slot (semaphore)
  3. Establish residency:
     a. Cache hit (SmallKv or CompleteFile) → bind immediately
     b. Live fill session exists → join it
     c. Miss → single-flight establishment:
        Leader: HEAD backend → decide small vs large → admit
        Followers: wait for leader outcome, re-enter lookup
  4. Bind residency to handle:
     SmallKv      → in-memory payload, no attachment
     CompleteFile  → direct-I/O file descriptor attachment
     LargeFill    → join fill session, no attachment
```

The single-flight establishment ensures concurrent OPENs on the same
cold key issue exactly one backend HEAD. The leader calls `succeed()`
only after the residency is observable in the cache, so followers can
find it on re-entry.


4  Read Flow
============

READ routes by the residency kind frozen at OPEN time:

```
  SmallKv       slice in-memory payload → ReadBody::Bytes
  CompleteFile  pread from cache file   → ReadBody::FileRange + CacheActivityGuard
  LargeFill     per-chunk leader/follower fetch → ReadBody::FileRange
```

Reads on complete files and large fills return a `ReadFileRange` that
holds a `CacheActivityGuard`. This guard keeps the cached payload active
so eviction cannot retire it while the response is in flight on the wire.

Length clamping: every read is capped at `max_read_size()`.


5  Staging Commands
===================

Staging is physical-identity-keyed (`BackendDataIdentity`, `bucket`, `key`), not
handle-based:

- **Upload** — uses the connection's attached backend, uploads the
  caller-created staging file via `put_from_file`, and
  returns size and etag. It does not touch the cache — the caller must
  invalidate if needed.

The server has no stage-create or abort wire verb. The database creates
and removes staging files directly through the filesystem using
`StagingFile` / `StagingPathResolver`.


6  List Sessions
================

List uses server-side cursor-based pagination:

```
  List(bucket, prefix, cursor=None)
    → start backend list stream, insert into ListSessionTable
    → drain up to page_size entries
    → return entries + next_cursor (None if exhausted)

  List(bucket, prefix, cursor=Some(c))
    → drain existing session by cursor
    → return next page
```

Sessions are connection-scoped. A cursor cannot be resumed from another
socket and closing the socket releases all retained backend streams. The
session is removed from the connection-local map during drain to serialize
same-cursor access.

Stream errors consume the session (no partial progress on the wire).


7  Delete Operations
====================

- **Delete** — backend delete, then best-effort cache invalidation.
- **DeletePrefix** — streams backend list into delete_stream; per
  deleted key, attempts local cache invalidation (errors swallowed).
  Empty prefix is rejected.
