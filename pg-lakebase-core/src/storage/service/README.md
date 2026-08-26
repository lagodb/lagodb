# Storage Consumer API

`pg-lakebase-core::storage::service` is a consumer-facing module. It does not
own storage-service GUC backing statics, does not register a background worker,
and does not export a bgworker entry point.

The `pg_lakebase` runtime extension owns the storage singleton. Access-method
crates resolve the endpoint with `StorageEndpoint::from_pg_gucs()`, which reads
PostgreSQL's global GUC registry by name:

- `lagodb.storage_server_enabled`
- `lagodb.storage_server_socket_path`
- `lagodb.storage_server_cache_dir`
- `lagodb.storage_backend_max_idle_connections`

This avoids rlib static duplication: AM crates never read a `GucSetting` static
compiled into their own shared object.

`lagodb.workers` stores database-local extension workers only. The storage
server is a runtime-owned static bgworker and is never inserted into that table.
