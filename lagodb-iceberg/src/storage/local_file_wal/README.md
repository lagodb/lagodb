# Iceberg Storage WAL Contract

This module logs local Iceberg file bytes to PostgreSQL WAL. During WAL replay
on a standby, including a standby that accepts hot-standby read-only queries, or
during archive recovery, these records can reconstruct local Iceberg files when
they are missing from that instance's local disk. Object/distributed storage
does not use this WAL path; it relies on object-store durability and separate
orphan cleanup. This is an availability-first, lossy reconstruction mechanism
for local Iceberg files, not a heap/smgr-equivalent physical storage contract.
For local crash-only recovery, PostgreSQL may call this custom resource
manager's redo routine, but the rmgr intentionally skips `WRITE_FILE` replay
because the primary writer calls `FileSync` on successful close.

Because these are custom WAL resource manager records, `lagodb_iceberg` must be
loaded via `shared_preload_libraries` while any such records may need to be
replayed or decoded.

## Transaction Boundary

PostgreSQL relation storage can put relfilenode cleanup directly in transaction
commit/abort records through `pendingDeletes` and `smgr`. A table access method
extension cannot append arbitrary Iceberg paths to those core commit/abort
records, and PostgreSQL 17's `smgr` switch is not a third-party registration
API. Because both extension points are closed to this AM, Iceberg cannot make
directory cleanup perfectly transaction-bound in the same way as heap storage.

Because of that limitation:

- `WRITE_FILE` records may be emitted while the transaction is in progress.
- A transaction abort can leave replayed files as Iceberg orphans on standby.
- Delete WAL must not be emitted before the PostgreSQL transaction outcome is
  known.
- Missing local files during replay are treated as lossy reconstruction gaps,
  not PostgreSQL recovery-fatal corruption, when the missing file is the base
  for a later `WRITE_FILE` chunk.
- There is no abort-time `DELETE_FILE` WAL operation. Abort and staging cleanup
  are not committed table-state facts and still rely on primary-local cleanup
  plus orphan maintenance. A bounded post-commit `DELETE_FILES` operation is
  used only for transaction-created files canceled by a successful final
  metadata commit.
- Local table-directory deletion is modeled as post-commit cleanup: write and
  flush `DELETE_DIRECTORY`, then remove the primary directory.

This is intentionally not the same as native heap/smgr semantics. It favors
never deleting committed data over perfectly mirroring best-effort cleanup.
The downside is a cleanup gap: if the server crashes after the PostgreSQL commit
but before post-commit `DELETE_DIRECTORY` WAL is written, standby WAL replay or
archive recovery may keep a dropped table directory as an orphan until external
cleanup reclaims it.

## Orphans

Iceberg writers create data and metadata files before the catalog pointer is
advanced. Files that are not referenced by committed metadata are orphans and
must be handled by cleanup tooling. WAL replay may also create orphans when a
transaction wrote local files and later aborted. Those files are not visible to
queries because scans follow committed Iceberg metadata.

Use Iceberg orphan cleanup, such as `remove_orphan_files`, to reclaim files that
were created but never referenced by committed table metadata.

### Canceled transaction-created files

A canceled file is a data/delete file created by the current PostgreSQL
transaction that the final effective Iceberg action no longer references. For
example, an INSERT may create `D1`, and a later TRUNCATE in the same transaction
may discard that INSERT. The committed catalog pointer never references `D1`.
This is different from a file inherited from an older committed snapshot, which
must remain available for snapshot history, refs, and time travel.

Deleting a canceled file is not required for WAL, transaction, or recovery
correctness. If deletion fails, the file is an unreachable orphan; it does not
become visible because Iceberg readers follow committed metadata. The bounded
post-commit `DELETE_FILES` record mirrors this cleanup to standby and
archive-recovery targets that previously replayed `WRITE_FILE`. It remains a
physical storage-hygiene enhancement, not a truncate correctness requirement,
and cannot close the crash window between PostgreSQL commit and emission of the
cleanup record.

That operation belongs to this Iceberg resource manager, not to
`lagodb_core::wal`: core owns generic custom-rmgr mechanics, while local
Iceberg path policy, record layout, validation, and redo belong in
`lagodb-iceberg/src/storage/local_file_wal`. It should only be emitted for local storage using the
Iceberg `WRITE_FILE` WAL path. Object/distributed storage must continue to use
its storage API and orphan maintenance; WAL replay must not require remote
credentials or network availability and must not assume ownership of shared
objects.

Cleanup redo is idempotent, treats a missing file as success, and remains best
effort for other unlink failures. The primary writes and flushes cleanup WAL
before unlinking. Each record is bounded to 256 paths and 64 KiB of path payload;
larger cleanup sets are split into multiple records, and flushing the last LSN
flushes all preceding batches. This does not replace periodic orphan-file
maintenance.

## Lossy Replay

`WRITE_FILE` redo is best effort for local Iceberg files. An `offset == 0`
record creates or truncates the file. If replay later sees an `offset > 0`
record but the base file is missing, it logs a warning, marks that path as
lossy-skipped, and skips subsequent chunks for the same path. This favors keeping
PostgreSQL recovery available over proving every local Iceberg file was
reconstructed. If committed Iceberg metadata references the missing file, the
problem should surface when the table is read.

Only missing base files get this lossy treatment. Environment problems such as
permission errors, invalid path strings, or write failures still fail redo
because they indicate the recovery target cannot safely write local files at all.

`DELETE_DIRECTORY` redo is also best effort. Missing directories are success;
other stat/delete failures are reported as warnings and recovery continues. This
may leave dropped table directories behind for later cleanup. Until the
`TODO(storage-layout)` below is fixed, those leftovers can also interact badly
with relfilenumber reuse, so cleanup warnings should be treated as operational
signals rather than harmless noise.

Directory cleanup currently uses `std::fs`, matching the local storage cleanup
path. If local filesystem behavior grows more complex, introduce a small
`LocalFileOps`/`WalReplayFileOps` abstraction to keep create/write/fsync/delete
error policy in one place instead of adding ad hoc helpers.

## Operational Cost

`WRITE_FILE` records contain the actual file bytes. This is physical file
replication through PostgreSQL WAL and can substantially increase WAL volume,
replication bandwidth, archive size, and `max_wal_size` pressure. Large local
tables should prefer object storage or a future file-shipping design.

## Known Design Debt

Local table directories are currently based on PostgreSQL relfilenumber with an
`_iceberg` suffix. PostgreSQL protects native relation files from relfilenumber
reuse hazards inside `mdunlink`, but extension-owned directories are not covered
by that mechanism.

TODO(storage-layout): include a table UUID or storage id in the local directory
name. This is not changed here because it is a storage-layout and catalog-design
decision with a larger blast radius than the WAL cleanup fixes.
