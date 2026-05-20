Staging Subsystem
=================

The staging subsystem lets database transactions write new objects through
the local filesystem and upload them to the backend with an explicit
`Upload` request. Upload is not a database transaction commit; it only copies
a closed staging file into the object backend and returns the resulting object
facts. All file I/O before upload happens on the client side, and the
**database owns the staging directory's lifecycle**.


1  Design Motivation
====================

This service is built for database workloads. A database transaction may:

1. Write a new data file (e.g. a Parquet file for a new table partition).
2. Close the local staging file.
3. Upload that file immediately so network/backend errors are returned to the
   SQL path that produced the file.
4. Continue the database transaction and later publish or discard metadata.

A traditional approach — streaming writes through the server — would force
the server to maintain per-file state for the entire transaction lifetime
and tie the staging file to a single connection. If the connection drops, the
write is lost.

The path-handoff design avoids both problems:

- The database creates a staging file directly under the staging tree.
- The database writes bytes with standard filesystem calls.
- Any connection can later upload the same key.
- Cleanup (transaction abort, crash recovery, post-upload removal) is
  performed by the database directly through the filesystem.

Uploads are intentionally not deferred until the final database transaction
commit. Long transactions may generate many files across many SQL statements;
uploading each closed file promptly avoids a large end-of-transaction upload
burst and keeps network errors attached to the statement that produced the
file.


2  Lifecycle
============

```
  StagingFile::create(store_id, bucket, key)
   |
   +--- caller: create empty file with O_CREAT | O_EXCL
   |    path: <cache_dir>/staging/<store>/<bucket>/<parent>/pgl-staging.<name>
   |
   v
  Client: open path with O_APPEND, write bytes, close file
   |
   v
  Upload(store_id, bucket, key)
   |
   +--- server reads the staging file
   |    uploads to backend
   |    returns {size, etag}
   |    *** does NOT unlink ***
   |
   +--- caller may unlink local staging file after successful upload
   |
   +--- NOTE: Upload does not invalidate the cache.
        Caller must call invalidate_object_cache to see
        the uploaded bytes through Open.
```


3  Client-Side Semantic Contract
================================

The server does not observe writes to the staging file. The following rules
are enforced or documented on the client side:

- **Append-only.** The client opens the staging file with `O_APPEND`. Bytes
  always land at EOF. A misbehaving caller cannot rewind over bytes already
  written.

- **Single writer.** Only one staging file exists per `(store_id, bucket,
  key)` at any time. `StagingFile::create` uses `O_EXCL`, so a second create
  for the same key returns `Busy`. The caller is expected to remove a stale
  staging file before re-staging the same key.

- **No readers before upload/publication.** The staged bytes are invisible
  outside the staging tree until `Upload` succeeds. Even after upload, the
  database controls when metadata starts referencing the object. It is the
  caller's responsibility not to read the staged key too early.

- **No concurrent readers or writers.** At any moment there is at most one
  writer for a staging file, and no reader. Concurrent access is not a
  supported scenario.

These are semantic guidelines, not all server-enforced. They are documented
so that client implementations in other languages follow the same contract.


4  Relationship to Cache Invariants
====================================

Upload does **not** invalidate or update the cache. The three cache
invariants (immutable size/etag, no generations, external invalidation)
apply to staging as follows:

- If a cached copy of `(store_id, bucket, key)` exists when Upload
  succeeds, the cached copy is left untouched.
- If the caller wants to read the just-uploaded bytes, they must call
  `invalidate_object_cache` before the next `Open`.
- This is exactly the same contract used for any externally modified object.

The staging tree is a separate directory from the cache tree. Cache eviction,
cleanup, and orphan scanning never touch the staging directory.


5  Cleanup — owned by the database
==================================

The database (the caller) is the sole owner of staging-directory cleanup.
The server never wipes, never reapers, and never decides on its own to
remove a staging file.

In practice this means the database:

- Tracks the local staging files it has created until each one is either
  uploaded and unlinked or discarded.
- After a successful upload, may unlink the local staging file immediately;
  the object bytes already live in the backend.
- On a transaction abort before upload, unlinks the local staging file.
- On database startup / crash recovery, removes any leftover staging files
  before resuming normal writes.
- Handles uploaded-but-unpublished objects at the database/catalog layer
  (for Iceberg, this belongs with metadata commit failure handling and orphan
  file cleanup), not in the storage worker.

The storage server's only contribution to this lifecycle is:

- Uploading a caller-created staging file via `Upload`.
- Providing `StagingPathResolver` / `StagingFile` library APIs so Rust
  callers can create and locate staging files consistently.

There is no online orphan reaper, no startup wipe, no stage-create wire op,
and no abort wire op. The server-side staging uploader is crate-internal and
only uploads caller-created files.


6  Error Handling
=================

- **Upload failure:** the staging file is preserved on disk. Upload failures
  are frequently transient (network, throttling), and staged data may be
  GB-scale. Forcing a rewrite on every retry is unacceptable. The caller
  decides: retry upload, leave the file in place, or unlink it through the
  filesystem.

- **Caller-side unlink is idempotent.** Removing a staging file that is
  already gone (e.g. after a crash + recovery sweep) is a normal `ENOENT`
  the database can ignore.

- **Create is exclusive.** Creating a staging file for a key that
  already has one returns `Busy`. The caller must remove the existing file
  first.


7  Directory Layout
===================

Staging paths mirror the cache `objects/` layout (same path encoding, same
store/bucket/key partitioning) but live under a separate root:

```
  <cache_dir>/staging/<store>/<bucket>/<parent>/pgl-staging.<name>
```

The full path is derived by `StagingPathResolver` and is the caller's handle
for filesystem-level cleanup. The encoding helpers
(`StagingPathResolver::path_for`) are exposed publicly so callers can compute
the path from `(store_id, bucket, key)` during ordinary writes or crash
recovery. Path components are
encoded with the same segment-encoding and length-validation rules as
cache paths, preventing traversal attacks and rejecting paths that would
exceed portable filesystem limits.
