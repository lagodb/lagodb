//! Remote table versions shared by scans in one PostgreSQL executor.
//!
//! External catalogs have no mapping from a PostgreSQL visibility snapshot to a remote
//! commit. Choose each source on first bind, then keep that immutable Table
//! until executor teardown. The next execution loads it again, including when
//! it reuses a cached plan. This does not pin files against Iceberg expiration.
//! REST catalog bindings are shared across tables only within this executor;
//! keeping a client does not select another table's metadata or storage routes.

use std::cell::RefCell;
use std::rc::{Rc, Weak};

use iceberg_lite::table::Table;
use lagodb_core::handles::ExecutorHandle;
use pgrx::pg_sys;

use super::catalog::ForeignCatalog;
use super::error::IcebergFdwError;
use super::options::CatalogRuntimeConfig;
use super::relation::{RemoteTableKey, ResolvedCatalogBinding};

struct CachedTable {
    key: RemoteTableKey,
    catalog: Rc<ResolvedCatalogBinding>,
    table: Table,
}

struct ActiveReadViews {
    estate: *mut pg_sys::EState,
    views: Weak<ForeignReadViews>,
}

thread_local! {
    static ACTIVE: RefCell<Vec<ActiveReadViews>> = const { RefCell::new(Vec::new()) };
}

pub(super) struct ForeignReadViews {
    rest_catalogs: RefCell<Vec<Rc<ResolvedCatalogBinding>>>,
    tables: RefCell<Vec<CachedTable>>,
}

impl ForeignReadViews {
    pub(super) fn acquire(executor: &ExecutorHandle<'_>) -> Rc<Self> {
        ACTIVE.with_borrow_mut(|active| {
            // Only the executor context owns a persistent strong reference.
            // Dead entries cannot alias an EState address reused by PG.
            active.retain(|entry| entry.views.strong_count() != 0);
            if let Some(views) = active
                .iter()
                .find(|entry| entry.estate == executor.as_raw())
                .and_then(|entry| entry.views.upgrade())
            {
                return views;
            }
            let views = Rc::new(Self {
                rest_catalogs: RefCell::new(Vec::new()),
                tables: RefCell::new(Vec::new()),
            });
            executor.retain(Rc::clone(&views));
            active.push(ActiveReadViews {
                estate: executor.as_raw(),
                views: Rc::downgrade(&views),
            });
            views
        })
    }

    /// Select each table once, sharing REST clients for matching server/user
    /// identities and configurations without enrolling a write transaction.
    pub(super) fn bind_table(
        &self,
        key: &RemoteTableKey,
        config: CatalogRuntimeConfig,
        connect: impl FnOnce() -> Result<ForeignCatalog, IcebergFdwError>,
    ) -> Result<(Rc<ResolvedCatalogBinding>, Table), IcebergFdwError> {
        if let Some(cached) =
            self.tables.borrow().iter().find(|entry| &entry.key == key)
        {
            if cached.catalog.runtime_config != config {
                return Err(IcebergFdwError::CatalogBindingChanged);
            }
            return Ok((Rc::clone(&cached.catalog), cached.table.clone()));
        }
        let cached_catalog = self
            .rest_catalogs
            .borrow()
            .iter()
            .find(|binding| {
                &binding.key == key.catalog_binding()
                    && binding.runtime_config == config
            })
            .map(Rc::clone);
        let is_new = cached_catalog.is_none();
        // No cache borrow survives catalog or table-file I/O.
        let catalog = match cached_catalog {
            Some(catalog) => catalog,
            None => Rc::new(ResolvedCatalogBinding {
                key: key.catalog_binding().clone(),
                runtime_config: config,
                catalog: connect()?,
            }),
        };
        let table = catalog.catalog.as_catalog().load_table(key.table())?;
        // SQLite retains its existing transaction-connection reuse policy.
        // Only successful REST table loads populate the shared client bindings.
        if is_new && matches!(&catalog.catalog, ForeignCatalog::Rest(_)) {
            self.rest_catalogs.borrow_mut().push(Rc::clone(&catalog));
        }
        self.tables.borrow_mut().push(CachedTable {
            key: key.clone(),
            catalog: Rc::clone(&catalog),
            table: table.clone(),
        });
        Ok((catalog, table))
    }
}
