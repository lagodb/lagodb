Cache Subsystem
===============

The cache subsystem owns on-disk storage of object data, a persistent
metadata index, and the policies that decide what stays resident and what
gets evicted.


1  Design Invariants
====================

Every cache decision rests on three non-negotiable invariants:

1. **Immutable identity.** Once `size` and `etag` enter a cache row or
   large-fill session, they are immutable facts for the current cache
   lifecycle of that key. The server never reconciles, overwrites, or
   repairs those values from later backend observations.

2. **No object-version generations.** One credential-free physical
   `(backend identity, bucket, key)` maps to at most one
   cached residency at a time. The system does not introduce a generation
   field and does not support multiple cached versions for the same object
   key.

3. **External invalidation.** Freshness is the caller's responsibility.
   `invalidate_object_cache(bucket, key)` on an attached connection is the only explicit
   freshness boundary. Backend object changes are never detected
   automatically — until invalidated, the server assumes the current key's
   identity remains valid. A cache hit does not HEAD the backend.

These invariants exist because the upstream object store guarantees immutable
objects and the database engine already knows which objects are current. They
eliminate reconciliation logic, version conflicts, and background polling.

`BackendDataIdentity` contains only physical addressing fields and excludes
credentials. The cache is trusted cluster-local derived data: credentials A
and B share a residency when they address the same physical service, bucket,
and key. A hit does not execute backend HEAD to repeat B's remote permission
check. Endpoint validation prevents userinfo/query/fragment secrets from
entering this persistent identity.


2  On-Disk Layout
=================

```
cache_dir/
  db/
    index.redb                            persistent metadata index
  objects/
    <encoded-backend-identity>/<bucket>/<parent>/
      pgl-cache.<name>.complete           fully cached large object
      pgl-cache.<name>.part               in-progress large object fill
  staging/
    <encoded-backend-identity>/<bucket>/<parent>/
      pgl-staging.<name>                  write staging (see staging README)
```

Small objects (at or below the small-object limit, default 4 KiB) are stored
directly in the redb index as embedded KV payloads. They do not have on-disk
files.

Large objects are fetched in chunks from the backend and written to a partial
file next to its complete counterpart. Once all chunks are present, the
partial file is atomically renamed to
the complete path, and durable metadata is inserted into the index.


3  Residency States
===================

A cached object is in exactly one state:

```
  +----------+     promote     +---------------+
  |  (miss)  | ------------->  | SmallKv       |  payload in redb KV
  +----------+                 +---------------+
       |
       |  create fill session
       v
  +------------------+   all chunks   +---------------+
  | LargeFillSession | ------------->  | CompleteFile   |  .complete on disk
  | (process-local)  |    rename       +---------------+
  +------------------+
```

- **SmallKv**: metadata and payload bytes live together in the persistent
  index. A single KV read serves the whole object.
- **CompleteFile**: metadata in the index, payload as a `.complete` file.
  Reads use `pread` (or a direct-I/O FD passed to the client).
- **LargeFillSession**: process-local, in-memory only. Chunks are fetched
  from the backend and written to a `.part` file. Partial progress is
  tracked in memory; no durable metadata exists until promotion.

Transitions are one-directional. There is no path from CompleteFile back to
LargeFillSession or from SmallKv to CompleteFile. Invalidation or eviction
removes the entire residency; re-opening the key starts a fresh miss flow.


4  OPEN and Admission
=====================

OPEN classifies each key through a single lookup path:

```
  1. Index hit?
     SmallKv  -> bind in-memory payload, mint open lease
     Complete -> bind file metadata, mint open lease

  2. Live fill session for this key?
     -> join existing session, mint open lease

  3. Establishment election (single-flight)
     Leader   -> HEAD backend, decide small vs large, admit
     Follower -> wait for leader outcome, re-enter step 1
```

The establishment single-flight ensures that concurrent OPENs on the same
missing key issue exactly one backend HEAD. Followers wait for the leader's
outcome and re-enter the lookup. The KV-level `admit_small_if_absent` is
retained as defense-in-depth but is not the primary deduplication layer.

The per-object lock covers the entire lookup→admit window. Releasing the lock
between lookup and admission would let eviction or invalidation retire the
observed snapshot before the lease is minted.


5  Read Routing
===============

OPEN freezes the residency for the handle's lifetime. Subsequent READs on
the same handle execute zero KV operations:

- **SmallKv**: reads slice directly from the in-memory payload bound at OPEN.
- **CompleteFile**: reads `pread` from the cache file (or the direct-I/O FD
  negotiated at OPEN).
- **LargeFill**: reads coordinate chunk fetches through the session. Each
  chunk has a leader/follower flight — the leader fetches from the backend
  once, followers wait for the outcome.

Direct-I/O eligibility: when the server opens a complete-file cache entry
successfully, it duplicates the FD and sends it to the client via ancillary
data. The client then reads via `pread` locally, bypassing wire READ frames.
Tiny objects (SmallKv) always use in-band payloads.


6  Large-Object Fill Sessions
=============================

```
  OPEN (miss, size > small limit)
   |
   v
  Create or join LargeFillSession
   |
   +--- per-chunk flight: leader/follower
   |    leader fetches chunk from backend
   |    writes to .part file
   |    followers wait on outcome
   |
   +--- all chunks present?
        |
        v
  Promote: rename .part -> .complete
           insert CompleteFile metadata
           clear fill slot
```

Partial progress lives only in process memory. If the process crashes, the
partial file becomes an orphan — cleaned up by startup recovery or runtime
orphan passes.

Session ownership is reference-counted. When the last consumer drops its
reference to a session that never completed, the reaper task acquires the
per-object lock, aborts the session, unlinks the partial file, and clears the
fill slot. The reaper exists because async cleanup cannot run in synchronous
`Drop`.


7  Persistent Index
===================

The persistent index (redb) has one primary table and two derived structures:

```
  object_meta          primary: stable cached residency (SmallKv or CompleteFile)
       |
       +--- small_object     child: embedded payloads for SmallKv entries
       |
       +--- lru_by_access    secondary index: oldest-first for capacity eviction
```

All metadata writes update derived state in the same write transaction.
Writing or replacing a metadata row removes the old LRU key and inserts a
new one when the row is cache-resident. After commit, the runtime
resident-byte counter is adjusted by the transaction's delta.

The resident-byte counter is eventually consistent during concurrent writes.
Cleanup treats it as a fast capacity signal and re-checks while evicting.
Startup reconciliation installs the authoritative value before the cache
manager is exposed to traffic.


8  Eviction and Cleanup
=======================

Eviction targets are high/low watermarks of the configured `max_cache_bytes`:

```
                      target                   start
  |----[evict to here]---|---[healthy zone]---|---[trigger]---|
  0%                   70%                  80%            100%
```

Small KV and complete-file entries share one budget and one LRU index.
Partial files are unowned physical artifacts and do not contribute to logical
cache usage.

Eviction ordering: metadata is removed before unlinking the payload so
resident-byte accounting drops immediately. A failed unlink leaves an orphan
that later cleanup passes will remove.

Eviction skips objects with active service reads, chunk downloads,
promotions, or handle-scoped cache leases.

Cleanup splits into two responsibilities with different drivers:

- **Capacity eviction** — bring `resident_bytes` below the policy's target.
  Threshold-driven; runs only when usage crosses `start_bytes`.
- **Orphan reclamation** — delete partial files left by aborted fills and
  complete payloads whose unlink failed during eviction. Correctness-driven,
  not threshold-driven; runs on every periodic / reload / manual pass.

The two are dispatched by a single background actor (`CleanupScheduler`)
through four trigger paths:

- **Startup**: `recover()` deletes startup-time orphans and then
  `cleanup_capacity_only()` runs an LRU pass. The cleanup scheduler is
  started **after** these run, so startup work never overlaps with
  background work.
- **Write-path nudge** (automatic when `max_cache_bytes` is set): after a
  successful admit/promote, the write path calls `nudge_cleanup_after_write`
  synchronously — zero awaits, zero allocations. The actor wakes and runs a
  **capacity-only** pass when usage is over `start_bytes`. Nudges that fire
  below the watermark or when no capacity cap is configured are no-ops.
  Write nudges do **not** run the orphan walker — successful writes do not
  create orphans, and routing every admit through the orphan-candidate
  snapshot would cost a janitor pass per write with nothing to do.
- **Periodic trigger** (opt-in via `cleanup_interval`): runs the orphan +
  capacity pass on the configured interval. Without `cleanup_interval`,
  runtime orphan reclamation runs only on hot-reload, manual cleanup, or
  next startup. Embedders that disable both `cleanup_interval` and
  `max_cache_bytes` accept that runtime orphan reclamation is dormant
  between restarts; the trade-off keeps the write path free of unnecessary
  scans when the cache is healthy.
- **Hot-reload of `CacheRuntimeConfig`**: equivalent to a periodic tick.
  A reload that opens or tightens caps takes effect immediately, without
  waiting for a future nudge or interval.
- **Manual**: `CacheManager::cleanup_orphans()` runs orphan reclamation
  alone; `CacheManager::cleanup_with_capacity(policy)` runs orphan
  reclamation followed by LRU eviction toward the policy's target. Manual
  cleanup is not threshold-gated.

The scheduler holds a single async gate that serialises janitor traversals.
There is exactly one background actor; the only contender for the gate is
`run_manual`. The actor uses `lock().await` with shutdown cancellation so a
trigger that arrives while a manual cleanup is in flight is queued behind
the gate rather than dropped.


9  Crash Recovery
=================

The cache splits durable (KV) and non-durable (file) state intentionally:

- Chunk writes update only the partial file and process memory.
- Promotion renames the partial file before inserting metadata.
- A crash between rename and metadata insert leaves an unclaimed complete
  file (orphan), never metadata pointing at a partial.

Startup recovery performs:

1. One paged metadata scan to install the runtime resident-byte counter.
2. One streaming physical scan of the cache directory and small-object store.
   For each payload, derive the owning key, do a keyed metadata lookup, and
   delete unclaimed payloads.

The LRU index is maintained transactionally and is not rebuilt during normal
startup.
