# Catalog metadata tracker notes

This document records the design context for `metadata_tracker.rs`, especially
the transaction-local Iceberg overlay used to avoid statement-time metadata
materialization. It is intended as background for future refactors, not as API
documentation.

## Required semantics

`pg-lakebase` exposes Iceberg tables through PostgreSQL access-method behavior,
so PostgreSQL-like Read Committed semantics must be preserved:

- A later statement in the same transaction must see data files appended by
  earlier statements in that transaction.
- A later statement in the same transaction must also see rows committed by
  concurrent transactions before that later statement starts.
- A single statement must scan a consistent base.

That means an implementation cannot pin the first metadata file seen by the
transaction. Doing so would preserve read-your-own-writes, but it would miss
concurrent committed rows that became visible between statements.

### PostgreSQL isolation scope

The Iceberg AM supports PostgreSQL `READ COMMITTED` metadata visibility.
`READ UNCOMMITTED` is normalized to `READ COMMITTED`, matching PostgreSQL.
`REPEATABLE READ` is rejected because the tracker intentionally does not pin a
transaction-wide Iceberg snapshot.

PostgreSQL `SERIALIZABLE` currently strengthens the command-specific Iceberg
row-level isolation policy to Iceberg `Serializable`, but metadata visibility
remains statement-scoped and the AM does not yet participate in PostgreSQL's
SSI predicate-lock/read-write dependency tracking. This is an explicitly
incomplete PostgreSQL `SERIALIZABLE` implementation; see
`access::isolation::PgTransactionIsolation`.

`write.delete.isolation-level`, `write.update.isolation-level`, and
`write.merge.isolation-level` are accepted as Iceberg table options with
`snapshot` and `serializable` values. They are persisted in Iceberg metadata,
which remains the runtime source of truth; `rd_amcache` does not duplicate
them. The default is `serializable`. At `READ COMMITTED`, the table property is
preserved; PostgreSQL `SERIALIZABLE` always raises the effective Iceberg policy
to `Serializable`.

## Current `metadata_tracker.rs` behavior

`metadata_tracker.rs` keeps a transaction-local action log per modified Iceberg
table. The log preserves the order of prepared schema updates and data epochs;
data epochs contain `SnapshotDelta` file operations such as added data files,
position delete files, and removed data files. These actions are not written to
Iceberg metadata during statement execution.

The read path for scans and planner statistics is:

```text
current_table_metadata
  -> IcebergMetadata::get(relid)                    // latest committed catalog pointer
  -> TableMetadata::read_from(file_io, &location)   // committed Iceberg metadata
  -> replay staged schema actions
  -> attach combined Arc<SnapshotDelta> if this transaction has staged file changes
```

The scan path then plans files from the committed metadata plus the attached
overlay. Because the committed metadata is read fresh for each statement, a
later Read Committed statement sees concurrent commits that finished before the
statement starts. Because the overlay is attached to the statement view, the same
statement also sees this transaction's earlier writes.

The write setup path adds idempotent registration in front:

```text
begin_table_modify
  -> register_table(relid)                          // tracker state only
  -> current_table_metadata(relid, file_io)          // latest committed metadata + action overlay
```

Statement write staging records logical file operations in the current data
epoch:

```text
stage_data_files
  -> SnapshotDelta::add_data_file(...)

stage_position_delete_file
  -> SnapshotDelta::add_position_delete_file(...)

stage_remove_data_file
  -> SnapshotDelta::remove_data_file(...)
```

Each mutation inside a savepoint stores a lightweight action-log marker: the
action length plus the current data epoch's `SnapshotDeltaMarker` and validation
length, when the last action is a data epoch. `ROLLBACK TO SAVEPOINT` truncates
the action log back to that marker, then truncates the current data epoch if it
existed. Top-level mutations do not need history frames because top-level abort
drops the whole tracker.

The top-level pre-commit path is the only place that writes Iceberg metadata:

```text
on_pre_commit
  -> commit_all
       -> IcebergMetadata::get(relid)                         // latest committed base
       -> TableMetadata::read_from(file_io, &latest_location)
       -> apply staged schema/data actions in log order
       -> Transaction::commit(&StagedCatalog)                 // writes manifests/metadata
       -> IcebergMetadata::cas_update(expected = latest_location)
       -> on conflict: read latest base and retry
```

`StagedCatalog` is a storage-only catalog wrapper. It writes standard Iceberg
manifest, manifest-list, snapshot, and table metadata files, but it does not
update the PostgreSQL catalog row. The catalog-visible update happens only when
`IcebergMetadata::cas_update` swaps `lakebase.iceberg_metadata.metadata_location`
from the base location to the newly written metadata location.

This design deliberately uses one materialization path for append-only and mixed
append/delete/remove transactions. `AddData` is part of the overlay model, so
INSERT and future DELETE/UPDATE staging share the same read and commit
semantics.

## Why this is the current shape

The tracker now satisfies the required PostgreSQL-style Read Committed behavior
without statement-time Iceberg metadata files:

- Each statement reads the latest committed Iceberg metadata pointer from the
  PostgreSQL catalog.
- Each statement overlays this transaction's staged `SnapshotDelta`.
- Scans hold an `Arc` to the statement-local delta view, so later DML in the
  same transaction cannot mutate an already planned statement view.
- Top-level commit materializes exactly the staged delta on top of the latest
  committed base and retries on CAS conflict.

The overlay and `SnapshotDeltaAction` live in iceberg-lite as additive code, not
as a destructive rewrite of upstream iceberg-rust APIs. That matters because
iceberg-lite is periodically synchronized from upstream.

## pg_lake comparison

Snowflake-Labs `pg_lake` provides a useful contrast. The code investigated was
`Snowflake-Labs/pg_lake` commit
`d72c1da88221396791a947df0d44d8c26503d18d`.

For internal writable Iceberg tables, `pg_lake` does not make `SELECT` generate a
temporary Iceberg metadata file. Instead, it maintains a PostgreSQL heap catalog
that is effectively a relational file index:

- `lake_table.files` stores `table_name`, file `path`, `row_count`, `file_size`,
  `content`, `deleted_row_count`, `first_row_id`, and related file metadata.
- Writes generate data files or position delete files, convert them to
  `TableMetadataOperation`s, and apply them to `lake_table.files` plus related
  partition/stat catalogs.
- For Iceberg tables, the transaction also records that Iceberg metadata must be
  synchronized at commit time.

The read path constructs the scan base directly from PostgreSQL MVCC state:

```text
FDW scan startup
  -> CreatePgLakeScanSnapshot
  -> GetTransactionSnapshot()
  -> GetTableDataFilesFromCatalog(..., snapshot)
  -> SPI_execute_snapshot(...)
  -> PgLakeScanSnapshot
  -> external engine read over the selected file paths
```

Important implementation details:

- `CreatePgLakeScanSnapshot` takes one PostgreSQL transaction snapshot and uses
  it for all tables in the statement, giving a consistent multi-table scan base.
- `GetTableDataFilesFromCatalog` uses `SPI_execute_snapshot` against
  `lake_table.files`.
- It deliberately sets `readOnly = false` even for a read-only metadata query so
  SPI uses the current transaction snapshot and can see changes made by the same
  transaction.
- In Read Committed, each statement gets a fresh `GetTransactionSnapshot()`, so
  a later statement sees both the transaction's own previous file-catalog writes
  and other transactions' newly committed file-catalog writes.

This is why `pg_lake` can support PostgreSQL-like Read Committed without
temporary Iceberg metadata files: its scan engine does not require Iceberg
metadata JSON as the statement scan base. It can scan from a file list generated
from PostgreSQL heap rows.

This mechanism applies to `pg_lake` internal/writable tables. `pg_lake` external
Iceberg tables still read `metadata_location`, load Iceberg table metadata, and
derive scans from real Iceberg metadata files.

The isolation tests in `pg_lake` are consistent with this design:

- `isolation_concurrent_iceberg_dml.spec` includes cases where selects do not
  block each other and are not blocked by insert/update/delete.
- The expected output for the concurrent Iceberg insert case shows a later
  select seeing both inserted rows after the blocking writer completes.
- `isolation_iceberg_repeatable_read.spec` shows snapshot stability under
  Repeatable Read, which follows from using PostgreSQL snapshots for the file
  catalog.
- The writable table isolation tests distinguish plain writable tables from
  Iceberg tables: plain writable table inserts can avoid blocking each other,
  while Iceberg writes still need catalog-level serialization for Iceberg
  metadata synchronization.

Relevant upstream references:

- FDW and `lake_table.files`:
  <https://github.com/Snowflake-Labs/pg_lake/blob/d72c1da88221396791a947df0d44d8c26503d18d/pg_lake_table/pg_lake_table--3.0.sql#L46-L51>
  <https://github.com/Snowflake-Labs/pg_lake/blob/d72c1da88221396791a947df0d44d8c26503d18d/pg_lake_table/pg_lake_table--3.0.sql#L91-L126>
- Statement scan snapshot:
  <https://github.com/Snowflake-Labs/pg_lake/blob/d72c1da88221396791a947df0d44d8c26503d18d/pg_lake_table/src/fdw/snapshot.c#L72-L91>
- Internal table file catalog scan:
  <https://github.com/Snowflake-Labs/pg_lake/blob/d72c1da88221396791a947df0d44d8c26503d18d/pg_lake_table/src/fdw/snapshot.c#L193-L211>
- `SPI_execute_snapshot` and `readOnly = false`:
  <https://github.com/Snowflake-Labs/pg_lake/blob/d72c1da88221396791a947df0d44d8c26503d18d/pg_lake_table/src/fdw/data_files_catalog.c#L224-L258>
- Commit-time Iceberg metadata synchronization:
  <https://github.com/Snowflake-Labs/pg_lake/blob/d72c1da88221396791a947df0d44d8c26503d18d/pg_lake_table/src/transaction/transaction_hooks.c#L42-L58>
  <https://github.com/Snowflake-Labs/pg_lake/blob/d72c1da88221396791a947df0d44d8c26503d18d/pg_lake_iceberg/src/iceberg/metadata_operations.c#L383-L390>

## Iceberg spec and engine behavior

The Iceberg specification and most open-source engines that use the standard
Iceberg Java library usually expose a narrower SQL transaction surface than
PostgreSQL.

Iceberg itself focuses on table metadata atomicity:

- A write produces new metadata files and atomically swaps the table's current
  metadata pointer in the catalog.
- Readers use the table metadata/snapshot loaded for their operation until they
  refresh.
- Optimistic concurrency is handled by detecting catalog conflicts and retrying
  or failing.

The Iceberg Java `Transaction` API is also table-level: a caller may create a
transaction for one table, stage one or more table updates, and call
`commitTransaction()`. That API is not a SQL session protocol for
`BEGIN; statement; statement; COMMIT`.

The Apache Iceberg project has an open multi-table transaction API proposal,
which confirms that current Iceberg transaction support is table-level rather
than a general multi-table SQL transaction mechanism:
<https://github.com/apache/iceberg/issues/10617>

Typical engine behavior:

- Spark Iceberg supports SQL writes such as `INSERT`, `MERGE`, `DELETE`,
  `UPDATE`, and `INSERT OVERWRITE`, but Spark SQL does not expose PostgreSQL-like
  interactive `BEGIN`/`COMMIT` transactions for multiple statements. The normal
  unit is one SQL statement or one job producing one Iceberg commit.
- Trino's Iceberg connector supports DML, but Trino documents that most
  connectors do not support full SQL transactions. The connector SPI also has
  `isSingleStatementWritesOnly()`, representing connectors whose writes must run
  in single-statement transactions.
- Athena Iceberg documents ACID support for SQL DML on tables and transactional
  single statements such as `MERGE INTO`, not a PostgreSQL-style multi-statement
  transaction block.
- Flink Iceberg focuses on batch/streaming write jobs, checkpointing, and sink
  commits, not interactive multi-statement SQL transactions.
- Hive transaction documentation has historically stated that
  `BEGIN`/`COMMIT`/`ROLLBACK` are not supported and language operations are
  auto-commit; Hive Iceberg integration likewise does not expose the PostgreSQL
  multi-statement transaction problem.

Because these engines generally use a single SQL statement, query, or job as the
transactional unit, they often avoid exposing this specific visibility problem:

```text
BEGIN;
INSERT INTO iceberg_table ...;
SELECT * FROM iceberg_table;  -- must see own insert and concurrent commits
COMMIT;
```

Snowflake is an important exception. Its Iceberg table documentation describes
multi-statement transactions for Iceberg tables, and its transaction
documentation describes Read Committed behavior where a later statement in the
same transaction can see commits made by other transactions between statements
while also seeing its own previous writes. That is the same semantic class as
PostgreSQL-like Read Committed.

Relevant references:

- Iceberg spec goals and optimistic concurrency:
  <https://iceberg.apache.org/spec/?h=snapshot#goals>
  <https://iceberg.apache.org/spec/?h=snapshot#optimistic-concurrency>
- Iceberg Java API transactions:
  <https://apache.github.io/iceberg/docs/latest/api/#transactions>
- Spark Iceberg SQL writes:
  <https://apache.github.io/iceberg/docs/latest/spark-writes/#writing-with-sql>
- Spark SQL syntax:
  <https://spark.apache.org/docs/latest/sql-ref-syntax.html>
- Trino Iceberg data management:
  <https://trino.io/docs/current/connector/iceberg.html#data-management>
- Trino SQL transaction support:
  <https://trino.io/docs/current/language/sql-support.html#transactions>
- Trino connector SPI:
  <https://javadoc.io/static/io.trino/trino-spi/461/io/trino/spi/connector/Connector.html#isSingleStatementWritesOnly-->
- Athena Iceberg ACID transactions:
  <https://docs.aws.amazon.com/athena/latest/ug/acid-transactions.html>
- Athena `MERGE INTO`:
  <https://docs.aws.amazon.com/athena/latest/ug/merge-into-statement.html>
- Flink Iceberg writes:
  <https://iceberg.apache.org/docs/nightly/flink-writes/>
- Hive transactions:
  <https://hive.apache.org/docs/latest/user/hive-transactions/>
- Hive Iceberg:
  <https://iceberg.apache.org/docs/nightly/hive/>
- Snowflake Iceberg multi-statement transactions:
  <https://docs.snowflake.com/en/user-guide/tables-iceberg-transactions#multi-statement-transactions>
- Snowflake Read Committed:
  <https://docs.snowflake.com/en/sql-reference/transactions#read-committed-isolation-level>

## Design implications for `pg-lakebase`

`pg-lakebase` exposes a stronger transaction surface than most open-source
Iceberg query engines because PostgreSQL users expect multi-statement
transactions and Read Committed visibility. Iceberg's metadata commit protocol is
necessary but not sufficient for that SQL contract.

The current solution is an iceberg-lite overlay:

- PostgreSQL transaction state stores a logical `SnapshotDelta`.
- Read Committed statements scan the latest committed Iceberg metadata plus that
  transaction-local delta.
- Top-level commit materializes the delta with `SnapshotDeltaAction` and
  publishes the resulting metadata location through catalog CAS.

This is the chosen direction for `pg-lakebase` now. It keeps the architecture
centered on Iceberg metadata and iceberg-lite while avoiding statement-time
manifest and metadata writes. The pg_lake-style PostgreSQL heap file catalog is
useful as a comparison point, but it is not part of the current roadmap: the
overlay already provides the required Read Committed behavior without adding a
second authoritative in-transaction file index or rewriting scans to consume
MVCC-visible file rows directly.

Guardrails for future refactors:

- Preserve PostgreSQL-like Read Committed. A scan after a write must read the
  latest committed metadata pointer and overlay this transaction's staged delta.
- Keep logical writes separate from statement views. Reading must not create a
  logical delta operation or savepoint history frame.
- Keep iceberg-lite changes additive where possible; this repository regularly
  merges iceberg-lite from upstream iceberg-rust.
- Keep append-only and mixed append/delete/remove materialization on the same
  `SnapshotDeltaAction` path for the tracker. Splitting INSERT back to a separate
  fast append path would reintroduce read/commit semantic drift.
- Do not introduce a pg_lake-style heap file catalog as an incremental
  optimization. Revisit it only if a concrete requirement cannot be satisfied by
  the overlay model, because it would be a separate architecture for scan
  planning, transaction visibility, commit-time Iceberg metadata synchronization,
  savepoint handling, and cleanup of orphaned physical files.
