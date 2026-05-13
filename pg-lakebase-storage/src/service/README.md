Service Layer
=============

The service layer is the central request dispatcher. It maps inbound wire
commands to typed outputs, wiring together the backend registry, cache,
staging area, and per-connection handle tables.


1  StorageService
=================

`StorageService<I: CacheIndex>` owns:

```
  registry        StoreRegistry         named backend resolution
  cache           Arc<CacheManager<I>>  residency, reads, eviction
  staging         Arc<StagingArea>      client-writable staging files
  list_sessions   Arc<ListSessionTable> server-side list continuation
  config          StorageServiceConfig  limits (max_read_size, etc.)
```

A single `StorageService` is shared across all connections via `Arc`.
Per-connection state (open handles) lives in the `HandleTable` passed to
each `execute` call.


2  Command Dispatch
===================

Every inbound wire verb is parsed into a `StorageCommand` variant.
`execute` matches the variant and delegates to the appropriate handler:

```
  StorageCommand     Handler           Module
  ─────────────────  ────────────────  ───────────────
  Open               handle_open       open.rs
  Read               handle_read       range_reader.rs
  Close              handle_close      mod.rs
  StageCreate        handle_stage_create   mod.rs
  Commit             handle_commit     mod.rs
  Abort              handle_abort      mod.rs
  RegisterStore      handle_register_store     mod.rs
  UnregisterStore    handle_unregister_store   mod.rs
  PurgeStoreCache    handle_purge_store_cache  mod.rs
  InvalidateObjectCache  handle_invalidate_object_cache  mod.rs
  Delete             handle_delete     mod.rs
  DeletePrefix       handle_delete_prefix   mod.rs
  List               handle_list       mod.rs
```

READ has an additional entry point — `handle_admitted_read` — for reads
that were pre-admitted on the connection's inbound path (see the
connection README for admission ordering).


3  Open Flow
============

OPEN establishes a cached residency for an object and mints a handle:

```
  1. Build ObjectLocation, resolve store, validate cache paths
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

Staging is identity-keyed (`store_id`, `bucket`, `key`), not
handle-based:

- **StageCreate** — resolves the store (fail-fast), creates an empty
  staging file, returns the absolute path for client-side writes.
- **Commit** — re-resolves the store (registry may have changed),
  uploads via `put_from_file`, returns size and etag. Does not touch
  the cache — the caller must invalidate if needed.
- **Abort** — unlinks the staging file. Idempotent.


6  List Sessions
================

List uses server-side cursor-based pagination:

```
  List(store_id, bucket, prefix, cursor=None)
    → start backend list stream, insert into ListSessionTable
    → drain up to page_size entries
    → return entries + next_cursor (None if exhausted)

  List(store_id, bucket, prefix, cursor=Some(c))
    → drain existing session by cursor
    → return next page
```

Sessions are service-scoped, not connection-scoped — a client can resume
listing from a different connection within the idle TTL. The session is
removed from the map during drain to serialize same-cursor access.

Stream errors consume the session (no partial progress on the wire).


7  Delete Operations
====================

- **Delete** — backend delete, then best-effort cache invalidation.
- **DeletePrefix** — streams backend list into delete_stream; per
  deleted key, attempts local cache invalidation (errors swallowed).
  Empty prefix is rejected.
