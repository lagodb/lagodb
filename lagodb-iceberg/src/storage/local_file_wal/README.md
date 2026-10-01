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
because the primary writer calls `FileSync` on successful close. Directories
are intentionally not fsynced; see the local durability limitation under
Known Design Debt.

Because these are custom WAL resource manager records, `lagodb_iceberg` must be
loaded via `shared_preload_libraries` while any such records may need to be
replayed or decoded.

## UNLOGGED tables

`CREATE UNLOGGED TABLE ... USING iceberg` is supported for both ordinary and
managed partitioned tables, including creation through the default table AM.
It disables local Iceberg file WAL through the existing PostgreSQL
`RelationNeedsWAL` policy. File writes, partitioned-table reservation creation,
in-place TRUNCATE and post-commit file/directory deletion all follow that
policy. File synchronization, transaction visibility, savepoint
rollback and commit/abort cleanup retain their ordinary behavior.

This is an intentional departure from PostgreSQL heap UNLOGGED semantics:

- Iceberg does not create a native init fork or an auxiliary reset table.
  PG17 `ResetUnloggedRelations()` only processes native relation forks with an
  init fork; it does not invoke the table AM or access the Iceberg catalog.
  Neither the Iceberg directory nor its metadata pointer is reset to an empty
  table after a crash.
- The shared `iceberg.iceberg_metadata` catalog remains a logged heap table.
  Catalog and metadata-pointer updates still produce PostgreSQL WAL; only
  Iceberg file WAL is disabled.
- Successful writers still sync files, without syncing directories. Existing
  local files can survive a crash, but post-recovery table availability is not guaranteed,
  and the implementation does not deliberately mark the table unusable.
- No Iceberg file WAL is available to reconstruct missing files during
  standby replay or archive recovery. This mode does not provide those file
  recovery guarantees.
- PostgreSQL's own persistence rules still apply, including recovery-time
  access restrictions and persistence of implicit serial/identity sequences.
  Object-backed tables also accept UNLOGGED; their existing storage policy
  already emits no Iceberg file WAL and is unchanged.

`ALTER TABLE SET LOGGED/UNLOGGED` remains rejected. PostgreSQL handles it as a
storage rewrite, which requires a separate Iceberg storage-migration design.

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
- Retirement WAL (`DELETE_DIRECTORY`, `DELETE_FILES`) must not be emitted before
  the PostgreSQL transaction outcome is known.
- Missing local files during replay are treated as lossy reconstruction gaps,
  not PostgreSQL recovery-fatal corruption, when the missing file is the base
  for a later `WRITE_FILE` chunk.
- There is no abort-time `DELETE_FILE` WAL operation. Abort and staging cleanup
  are not committed table-state facts and still rely on primary-local cleanup
  plus orphan maintenance. A bounded post-commit `DELETE_FILES` operation is
  used for transaction-created files canceled by a successful final metadata
  commit, committed VACUUM cleanup, and retired partitioned-table reservations.
- Local table-directory deletion is modeled as post-commit cleanup. WAL-enabled
  storage writes and flushes `DELETE_DIRECTORY` before removing the primary
  directory; WAL-free storage removes it without logging.

Commit cleanup follows PostgreSQL's release ordering: first determine the
transaction outcome, then release transaction locks, then remove retired
storage. Owned cleanup transfers to core's `CommittedCleanup` at
`XACT_EVENT_COMMIT`; `lagodb_core::resource` executes it in the actual top
transaction owner's `RESOURCE_RELEASE_AFTER_LOCKS` callback. PG17 uses this
ordering for native storage too, calling `smgrDoPendingDeletes(true)` after
ResourceOwner's lock-release phases. The extension uses the public
ResourceOwner callback because it cannot register its paths with `smgr`.

The storage retirement collection uses the transaction framework's savepoint
callbacks to promote or cancel its entries. Each `LocalTableRetirement` owns a
generation's directory, optional partitioned-table reservation and storage
capability; none requires a live relation or snapshot. At `XACT_EVENT_COMMIT`,
the collection transfers one owned batch to core's committed cleanup. This
includes retirements registered by `ON COMMIT DROP`, which PostgreSQL executes
after `PRE_COMMIT` callbacks. After lock release, the batch records every
WAL-enabled retirement and flushes the last LSN once before removing directories
in registration order and releasing their reservations. WAL-enabled non-temporary
reservations remain as empty main-fork files: core forwards an MD
`SYNC_UNLINK_REQUEST` to PostgreSQL, which unlinks them only after a safe checkpoint
completes. The request
is registered after both deletion records are flushed, so the checkpoint's REDO
point excludes them before the number can be reused. WAL-free entries emit no
deletion WAL and unlink their reservations directly; temporary reservations use
their backend-specific paths. This separates normal committed cleanup from
ResourceOwner's forgotten-resource fallback and its
resource-leak warnings. The batch remains backend-local and does not close the
crash gap described below.

Pending retirements reject `PREPARE TRANSACTION`. If savepoint rollback has
canceled every entry, PREPARE is allowed; core's successful-PREPARE callback
removes the registered collection, and storage releases its backend-local
reference without executing any retirement.

### In-place local TRUNCATE

PG17 `ExecuteTruncateGuts()` selects nontransactional truncation only when the
table or its current locator was created in the current subtransaction. Abort
discards that generation, so its contents can be removed immediately before
bootstrapping the empty local Iceberg table. Clearing the disposable generation
in place avoids accumulating old path sets across repeated truncations and
requires no per-file retirement WAL.

Local partitioned tables use the metadata tracker's rebuild level for the same
ownership decision because PostgreSQL does not manage their storage locator.
CREATE and a generation-changing TRUNCATE record that owner; RELEASE reparents
it and rollback restores it. Only a generation owned by the current nesting
level is cleared in place. A generation owned by a parent or an earlier
transaction is replaced, preserving it for rollback. In-place clearing retains
the reservation file and still bootstraps new Iceberg metadata.

WAL-enabled storage emits `TRUNCATE_DIRECTORY`, using the same path layout as
`DELETE_DIRECTORY` but a different recovery contract. It precedes the
replacement's file writes on standby/archive replay. Primary crash recovery
skips it, just as it skips
`WRITE_FILE`; replaying an older truncation there would erase the new synced
table. This exception does not change post-commit retirement of an earlier
generation containing committed data.

Local tables are private to PostgreSQL. Their TRUNCATE intentionally creates a
new Iceberg UUID, snapshot history and row lineage while retaining the current
definition and properties. Object storage retains the existing Iceberg truncate
action and commit protocol.

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
may leave dropped table directories behind for later cleanup. Bootstrap rejects
a nonempty reused directory rather than mixing it with a new table, so cleanup
warnings should be treated as operational signals rather than harmless noise.

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

Local tables are private to PostgreSQL and retain file `FileSync`, but do not
implement directory fsync. File synchronization alone does not guarantee that
new file or directory entries survive an OS crash or power loss. Consequently,
committed catalog metadata can reference missing local paths, and primary crash
recovery cannot repair them because it skips `WRITE_FILE` redo. This limitation
applies to logged and UNLOGGED local tables. Private ownership, catalog CAS and
the local TRUNCATE rebuild protocol do not provide directory durability.

Local directories use PostgreSQL relation paths with an `_iceberg` suffix.
Ordinary tables use their native locator; partitioned tables allocate a PG file
number and reserve it with an AM-owned empty file. A nonempty directory cannot
be reused during bootstrap. Retired WAL-enabled partitioned-table reservations protect
against number reuse until PostgreSQL's checkpoint unlink queue can release them;
primary recovery still replays their existing deletion WAL. Retired directories
and reservation handoff still depend on backend transaction state and post-commit
cleanup. The crash window
between PostgreSQL commit and cleanup WAL emission remains unchanged; closing
it requires recoverable retirement records.
