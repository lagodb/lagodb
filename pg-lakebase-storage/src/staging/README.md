Staging Subsystem
=================

The staging subsystem lets database transactions write new objects through
the local filesystem and upload them to the backend on commit. It is
intentionally thin: the server's role is limited to path allocation, upload,
and cleanup. All file I/O between create and finalize happens on the client
side.


1  Design Motivation
====================

This service is built for database workloads. A database transaction may:

1. Write a new data file (e.g. a Parquet file for a new table partition).
2. Continue processing for minutes or hours.
3. Eventually commit (upload the file) or abort (discard it).

A traditional approach — streaming writes through the server — would force
the server to maintain per-file state for the entire transaction lifetime
and tie the staging file to a single connection. If the connection drops, the
write is lost.

The path-handoff design avoids both problems:

- The server creates an empty file, returns its path, and forgets about it.
- The client writes bytes with standard filesystem calls.
- Any connection can later commit or abort the same key.
- Orphan files from crashed clients are cleaned up on the next restart.


2  Lifecycle
============

```
  StageCreate(store_id, bucket, key)
   |
   +--- server: create empty file with O_CREAT | O_EXCL
   |    path: <cache_dir>/staging/<store>/<bucket>/<parent>/pgl-staging.<name>
   |    return absolute path
   |
   v
  Client: open path with O_APPEND, write bytes, close file
   |
   +--- hours may pass (database transaction in progress)
   |
   +--- connection may drop and reconnect
   |
   v
  Commit(store_id, bucket, key)                Abort(store_id, bucket, key)
   |                                            |
   +--- server: read staging file               +--- server: unlink file
   |    upload via backend put_from_file                (missing = success)
   |    success: unlink staging file
   |    failure: leave file on disk (client retries)
   |    return {size, etag}
   |
   +--- NOTE: does NOT touch the cache
        caller must invalidate_object_cache
        to see the uploaded bytes
```


3  Client-Side Semantic Contract
================================

The server does not observe writes to the staging file. The following rules
are enforced or documented on the client side:

- **Append-only.** The client opens the staging file with `O_APPEND`. Bytes
  always land at EOF. A misbehaving caller cannot rewind over bytes already
  written.

- **Single writer.** Only one staging file exists per `(store_id, bucket,
  key)` at any time. The server enforces this by using `O_EXCL` on create —
  a second `StageCreate` for the same key returns `Busy`.

- **No readers before commit.** The staged bytes are invisible outside the
  staging tree until the database transaction commits and publishes metadata
  that references the object. It is the caller's responsibility not to read
  the staged key before commit.

- **No concurrent readers or writers.** At any moment there is at most one
  writer for a staging file, and no reader. Concurrent access is not a
  supported scenario.

These are semantic guidelines, not all server-enforced. They are documented
so that client implementations in other languages follow the same contract.


4  Relationship to Cache Invariants
====================================

Commit does **not** invalidate or update the cache. The three cache
invariants (immutable size/etag, no generations, external invalidation)
apply to staging as follows:

- If a cached copy of `(store_id, bucket, key)` exists when Commit
  succeeds, the cached copy is left untouched.
- If the caller wants to read the just-uploaded bytes, they must call
  `invalidate_object_cache` before the next `Open`.
- This is exactly the same contract used for any externally modified object.

The staging tree is a separate directory from the cache tree. Cache eviction,
cleanup, and orphan scanning never touch the staging directory.


5  Cleanup
==========

Staging files are cleaned up by exactly one mechanism: **startup wipe**.

On server startup, the entire `<cache_dir>/staging/` directory is removed
and recreated empty. This is safe because:

- The server keeps no in-memory state about staging files, so nothing needs
  to be reconciled across a restart.
- Client semantics assume "write, then either commit or abort." A crashed
  client that loses both must treat the file as gone, which matches the
  startup wipe.
- The staging tree is never walked during normal cache cleanup or eviction.

There is no online orphan reaper for staging. The staging directory is
intentionally kept separate from the cache directory so that online cleanup
(which interacts with leases, activity guards, and the LRU index) never
needs to reason about staging files.

The rationale: the startup-wipe model is simpler and sufficient for the
database use case. A database process crash triggers a full restart, which
wipes staging. Long-lived staging files from healthy transactions survive
across connection drops because nothing touches them until an explicit Commit
or Abort.


6  Error Handling
=================

- **Commit failure:** the staging file is preserved on disk. Upload failures
  are frequently transient (network, throttling), and staged data may be
  GB-scale. Forcing a rewrite on every retry is unacceptable. The client
  decides: retry commit, or abort and start over.

- **Abort is idempotent.** Aborting a key whose staging file is already gone
  (from a prior abort, a successful commit, or startup wipe) returns
  success. This simplifies client retry logic.

- **StageCreate is exclusive.** Creating a staging file for a key that
  already has one returns `Busy`. The client must Abort or Commit the
  existing file first.


7  Directory Layout
===================

Staging paths mirror the cache `objects/` layout (same path encoding, same
store/bucket/key partitioning) but live under a separate root:

```
  <cache_dir>/staging/<store>/<bucket>/<parent>/pgl-staging.<name>
```

This makes operator debugging straightforward — the staging path for a given
key is derivable from the same identity tuple used everywhere else. Path
components are encoded with the same segment-encoding and length-validation
rules as cache paths, preventing traversal attacks and rejecting paths that
would exceed portable filesystem limits.
