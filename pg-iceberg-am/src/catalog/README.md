# Catalog metadata tracker notes

This document records the design context for `metadata_tracker.rs`, especially
the temporary metadata materialization done during statement-level rebases. It is
intended as background for a future refactor, not as API documentation.

## Required semantics

`pg-lakebase` exposes Iceberg tables through PostgreSQL access-method behavior,
so PostgreSQL-like Read Committed semantics must be preserved:

- A later statement in the same transaction must see data files appended by
  earlier statements in that transaction.
- A later statement in the same transaction must also see rows committed by
  concurrent transactions before that later statement starts.
- A single statement must scan a consistent base.

That means an implementation cannot simply return the transaction's currently
tracked metadata location after the first write. Doing so would preserve
read-your-own-writes, but it would miss concurrent committed rows that became
visible between statements.

## Current `metadata_tracker.rs` behavior

The read path is:

```text
current_table_metadata
  -> current_metadata_location (private)
       -> rebase_for_statement(relid, Vec::new(), file_io)  // read-side rebase
            -> rebase_inner
            -> Transaction::commit(&StagedCatalog)
  -> TableMetadata::read_from(file_io, &location)
```

The write path adds idempotent registration in front:

```text
begin_table_modify
  -> register_table (private, idempotent)
  -> current_table_metadata (as above)
```

For writes that are flushing accumulated rows, the staging path is:

```text
end_modify
  -> stage_data_files
  -> rebase_for_statement(relid, new_data_files, file_io)  // write-side staging
  -> rebase_inner
  -> Transaction::commit(&StagedCatalog)
```

`rebase_inner` reads the latest globally committed Iceberg metadata location from
the PostgreSQL catalog, builds a base `Table`, replays the transaction's
accumulated data files plus the new statement files through iceberg-lite
`fast_append`, and commits through `StagedCatalog`. `StagedCatalog` is a
storage-only catalog wrapper; the PostgreSQL catalog row is not updated there.
The real catalog-visible update happens later in `commit_all` through
`IcebergMetadata::cas_update`.

This preserves Read Committed, but it also means non-final statement processing
can write Iceberg artifacts:

- `SELECT` after a write can write a new intermediate metadata file when a
  concurrent commit invalidates the fast path.
- `end_modify` writes an intermediate metadata file at the end of a write
  statement so subsequent statements can scan a real metadata location.
- `commit_all` may write another metadata file during final rebase/CAS retry.

This is the storage write marked by `TODO(metadata-preview)` in
`metadata_tracker.rs`: iceberg-lite currently exposes the scan-ready
`base snapshot + pending appends` view only by committing through a catalog,
which writes manifest, manifest-list, and metadata files.

## Why this is temporarily accepted

The current query path through iceberg-lite needs an actual metadata location
that exists on storage. Without a real metadata file, the scan code does not have
a scan-ready table snapshot to hand to iceberg-lite.

The desired long-term shape is an iceberg-lite API that can produce a read-only
append preview:

```text
base metadata + pending appended DataFiles -> scan-ready in-memory Table/snapshot
```

Such an API should not write manifest, manifest-list, or table metadata files.
`metadata_tracker.rs` could then keep a logical append log during statement
execution and defer physical metadata materialization until top-level
pre-commit, except when attempting the real catalog-visible commit.

We intentionally keep this as a crate-local TODO for now because iceberg-lite is
periodically merged from upstream `iceberg-rust`; adding a local API there would
create recurring merge cost.

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
  -> DuckDB read_parquet/read_* call over the selected file paths
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
transactional unit, they often avoid exposing this specific problem:

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

There are two plausible long-term approaches to remove statement-time metadata
writes:

1. Extend iceberg-lite with a read-only/in-memory append preview API that returns
   a scan-ready table/snapshot without writing metadata artifacts.
2. Add a pg_lake-style PostgreSQL heap file catalog and make scans consume
   MVCC-visible file rows directly, then synchronize Iceberg metadata at commit.

Approach 1 is smaller and keeps the current architecture centered on Iceberg
metadata and iceberg-lite. Approach 2 avoids the temporary metadata-file problem
more completely, but it is a larger architectural change: it introduces a second
authoritative in-transaction file index and requires the scan path to consume
file lists directly.

Until one of those exists, the current intermediate metadata writes are a
deliberate tradeoff, not accidental behavior. They are what makes
PostgreSQL-like Read Committed work with the current iceberg-lite scan contract.

Guardrails for a future refactor:

- Preserve PostgreSQL-like Read Committed. A scan after a write must not be
  changed to return only the transaction's previously tracked metadata location.
- Keep logical writes separate from derived scan previews. In the target design,
  a read-side preview should not become a logical data-file change or savepoint
  history frame merely because it had to construct a fresher scan base.
- Avoid adding long-lived local APIs to iceberg-lite unless there is a plan to
  carry or upstream them; this repository regularly merges iceberg-lite from
  upstream iceberg-rust.
- If the pg_lake-style heap file catalog approach is chosen, treat it as an
  architectural change, not a helper-function patch. The scan path, transaction
  visibility, commit-time Iceberg metadata synchronization, savepoint handling,
  and cleanup of orphaned physical files all need to be designed together.
