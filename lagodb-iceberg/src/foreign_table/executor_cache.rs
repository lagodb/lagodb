//! Remote table versions shared by scans in one PostgreSQL executor.
//!
//! External catalogs have no mapping from a PostgreSQL visibility snapshot to a remote
//! commit. Choose each source on first bind, then keep that immutable Table
//! until executor teardown. The next execution loads it again, including when
//! it reuses a cached plan. This does not pin files against Iceberg expiration.
//! REST catalog sessions are shared across tables only within this executor;
//! keeping a client does not select another table's metadata or storage routes.

use std::cell::RefCell;
use std::rc::{Rc, Weak};

use iceberg_lite::table::Table;
use lagodb_core::handles::ExecutorHandle;
use pgrx::pg_sys;

use super::catalog::{
    CatalogSession, CatalogSessionRequest, ForeignCatalog, RemoteTableKey,
};
use super::error::IcebergFdwError;

struct CachedTable {
    key: RemoteTableKey,
    catalog: Rc<CatalogSession>,
    base: Table,
}

struct ActiveExecutionCaches {
    estate: *mut pg_sys::EState,
    cache: Weak<ExecutorForeignTableCache>,
}

thread_local! {
    static ACTIVE: RefCell<Vec<ActiveExecutionCaches>> = const { RefCell::new(Vec::new()) };
}

pub(super) struct ExecutorForeignTableCache {
    rest_catalogs: RefCell<Vec<Rc<CatalogSession>>>,
    tables: RefCell<Vec<CachedTable>>,
}

impl ExecutorForeignTableCache {
    pub(super) fn acquire(executor: &ExecutorHandle<'_>) -> Rc<Self> {
        ACTIVE.with_borrow_mut(|active| {
            // Only the executor context owns a persistent strong reference.
            // Dead entries cannot alias an EState address reused by PG.
            active.retain(|entry| entry.cache.strong_count() != 0);
            if let Some(cache) = active
                .iter()
                .find(|entry| entry.estate == executor.as_raw())
                .and_then(|entry| entry.cache.upgrade())
            {
                return cache;
            }
            let cache = Rc::new(Self {
                rest_catalogs: RefCell::new(Vec::new()),
                tables: RefCell::new(Vec::new()),
            });
            executor.retain(Rc::clone(&cache));
            active.push(ActiveExecutionCaches {
                estate: executor.as_raw(),
                cache: Rc::downgrade(&cache),
            });
            cache
        })
    }

    /// Select each table once, sharing REST clients for matching server/user
    /// identities and configurations without enrolling a write transaction.
    pub(super) fn bind_table(
        &self,
        key: &RemoteTableKey,
        request: CatalogSessionRequest,
        transaction_catalog: Option<ForeignCatalog>,
    ) -> Result<(Rc<CatalogSession>, Table), IcebergFdwError> {
        if let Some(cached) =
            self.tables.borrow().iter().find(|entry| &entry.key == key)
        {
            if &cached.catalog.runtime_config != request.runtime_config() {
                return Err(IcebergFdwError::CatalogConfigurationChanged);
            }
            return Ok((Rc::clone(&cached.catalog), cached.base.clone()));
        }
        let cached_catalog = self
            .rest_catalogs
            .borrow()
            .iter()
            .find(|session| {
                &session.identity == request.identity()
                    && &session.runtime_config == request.runtime_config()
            })
            .map(Rc::clone);
        let is_new = cached_catalog.is_none();
        // No cache borrow survives catalog or table-file I/O.
        let catalog = match cached_catalog {
            Some(catalog) => catalog,
            None => Rc::new(request.connect(transaction_catalog)?),
        };
        let base = catalog.as_catalog().load_table(key.table())?;
        // SQLite retains its existing transaction-connection reuse policy.
        // Only successful REST table loads populate the shared client bindings.
        if is_new && matches!(&catalog.catalog, ForeignCatalog::Rest(_)) {
            self.rest_catalogs.borrow_mut().push(Rc::clone(&catalog));
        }
        self.tables.borrow_mut().push(CachedTable {
            key: key.clone(),
            catalog: Rc::clone(&catalog),
            base: base.clone(),
        });
        Ok((catalog, base))
    }
}
