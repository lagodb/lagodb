//! Composes the synchronous SQLite catalog with PG-owned local file storage.
//!
//! SQLite plus local Iceberg files is a single-machine convenience mode, with
//! no requirement to preserve committed table data across power loss or OS
//! crashes. This is an accepted product tradeoff: LocalStorage syncs files but
//! not parent directories, and SQLite's own durable commit does not extend to
//! the separate table files. Committed catalog metadata can therefore reference
//! missing paths after such a crash.
//!
//! Keep explicit file sync and error propagation, but do not add directory-sync
//! machinery solely to provide a power-loss guarantee this mode does not offer.
//! Revisit this choice if that durability requirement changes. This acceptance
//! is specific to the external SQLite mode, not a new managed-table guarantee.
//!
//! Connection ownership and FD budgeting were reviewed on 2026-10-14. Keep
//! iceberg-lite's existing RAII chain: SqliteCatalog owns an Arc to its inner
//! state, whose Mutex<Connection> closes through rusqlite when the last owner
//! drops. PreparedSqliteCommit retains the same catalog until publication or
//! drop. SQLite owns its database, journal and WAL resources; an additional PG
//! owner is not needed to release them.
//!
//! PG external-FD accounting is a separate, deliberately deferred budget
//! improvement. Its absence alone does not establish a resource leak or catalog
//! correctness defect. A single REST-style FD lease does not cover SQLite's
//! auxiliary files, and reserving three slots per connection is a conservative
//! policy that consumes PG quota, not exact accounting. Adding a SQLite VFS or
//! open/close hooks solely for this would expand the current integration scope.
//! Retain RAII and the existing connection reuse; reuse does not itself account
//! for FDs. Revisit budgeting with concrete FD-pressure evidence or changed
//! resource requirements, rather than reopening it as a missing cleanup fix.

use std::collections::HashMap;
use std::sync::Arc;

use iceberg_lite::catalog::sqlite::{SqliteCatalog, SqliteCatalogBuilder};
use iceberg_lite::io::{Storage, StorageConfig, StorageFactory};
use iceberg_lite::{CatalogBuilder, Result};

use crate::storage::LocalStorage;

pub(crate) struct PgSqliteCatalogBuilder;

impl PgSqliteCatalogBuilder {
    pub(crate) fn load(
        name: String,
        properties: HashMap<String, String>,
    ) -> Result<SqliteCatalog> {
        SqliteCatalogBuilder::default()
            .with_storage_factory(Arc::new(PgLocalStorageFactory))
            .load(name, properties)
    }
}

#[derive(Debug)]
struct PgLocalStorageFactory;

impl StorageFactory for PgLocalStorageFactory {
    fn build(&self, config: StorageConfig) -> Result<Arc<dyn Storage>> {
        // The synchronous FDW constructs, calls and drops this storage on the
        // owning PG backend thread, as required by LocalStorage's adapter.
        let storage = LocalStorage::with_wal(false).with_absolute_paths();
        storage.resolve_uri(config.location())?;
        Ok(Arc::new(storage))
    }
}
