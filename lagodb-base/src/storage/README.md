# Runtime Storage Worker

This module is the runtime-owned host for the LagoDB storage singleton.
`lagodb_base` calls it from `_PG_init`; AM crates do not call this path.

- `storage::define_gucs()` defines the storage GUCs on every load.
- `storage::init()` runs only during a normal shared preload. It cleans
  the postmaster-wide staging directory once at startup and registers one
  static bgworker named `lagodb-storage`.

The worker connects to SPI with `Some("postgres")`. PostgreSQL cannot open
`pg_tablespace` through relcache without a selected database, even though
`pg_tablespace` is a shared catalog. We intentionally avoid `template1` because
a persistent background-worker session there would block `CREATE DATABASE`.

The storage worker is not stored in `lagodb.workers`; that table is reserved
for database-local extension workers. Storage observability is exposed through
`lagodb.storage_service_status()`, backed by this module's own shared-memory
state rather than the worker subsystem's `Store`.

The worker publishes `running` only after startup reconciliation, cache recovery,
and Unix socket binding succeed. Regression setup waits for this state before
using object storage; PostgreSQL accepting connections does not imply that its
asynchronously started storage worker is ready.
