# Runtime Storage Worker

This module is the runtime-owned host for the Lakebase storage singleton.
`pg_lakebase_runtime` calls `storage::init()` from `_PG_init`; AM crates do not call
this path.

`storage::init()` performs three operations while PostgreSQL is processing
`shared_preload_libraries`:

1. Register storage GUC backing statics.
2. Clean the postmaster-wide staging directory once at startup.
3. Register one static bgworker named `pg-lakebase-storage` when
   `pg_lakebase.storage_server_enabled = on`.

The worker connects to SPI with `Some("postgres")`. PostgreSQL 17 cannot open
`pg_tablespace` through relcache without a selected database, even though
`pg_tablespace` is a shared catalog. We intentionally avoid `template1` because
a persistent background-worker session there would block `CREATE DATABASE`.

The storage worker is not stored in `lakebase.workers`; that table is reserved
for database-local extension workers. Storage observability is exposed through
`lakebase.storage_service_status()`, backed by this module's own shared-memory
state rather than the worker subsystem's `Store`.
