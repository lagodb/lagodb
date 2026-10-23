//! Resolution of a PostgreSQL foreign table into one Iceberg catalog table.

use std::rc::Rc;

use iceberg_lite::table::Table;
use lagodb_core::handles::ExecutorHandle;
use pgrx::pg_sys;

use super::catalog::{BoundCatalogTable, CatalogSessionRequest, RemoteTableKey};
use super::error::IcebergFdwError;
use super::executor_cache::ExecutorForeignTableCache;
use super::options::ForeignTableIdentity;
use super::transaction::ForeignTransaction;

mod target;

pub(super) use target::RemoteTableTarget;

/// A resolved catalog session or staged generation. Metadata consumers use its
/// table directly; scans bind command-visible data through [`ForeignTransaction`].
pub(crate) struct ResolvedForeignTableBinding {
    identity: ForeignTableIdentity,
    catalog_table: BoundCatalogTable,
}

impl ResolvedForeignTableBinding {
    pub(crate) fn resolve(
        relation_oid: pg_sys::Oid,
        effective_user: pg_sys::Oid,
    ) -> Result<Self, IcebergFdwError> {
        Self::resolve_binding(relation_oid, effective_user, None)
    }

    /// Resolve local options on every bind, but select the remote Table only
    /// once for this executor and catalog/user/table identity.
    pub(crate) fn resolve_for_execution(
        relation_oid: pg_sys::Oid,
        effective_user: pg_sys::Oid,
        executor: &ExecutorHandle<'_>,
    ) -> Result<Self, IcebergFdwError> {
        Self::resolve_binding(relation_oid, effective_user, Some(executor))
    }

    fn resolve_binding(
        relation_oid: pg_sys::Oid,
        effective_user: pg_sys::Oid,
        executor: Option<&ExecutorHandle<'_>>,
    ) -> Result<Self, IcebergFdwError> {
        // SAFETY: planner/executor contexts supply a live foreign-table OID;
        // the returned catalog object is consumed before this method returns.
        let foreign_table = unsafe { &*pg_sys::GetForeignTable(relation_oid) };
        let identity = ForeignTableIdentity::from_foreign_table(foreign_table)?;
        Self::bind_catalog_table(
            relation_oid,
            foreign_table.serverid,
            effective_user,
            identity,
            executor,
        )
    }

    pub(crate) fn load(
        relation_oid: pg_sys::Oid,
        server_oid: pg_sys::Oid,
        effective_user: pg_sys::Oid,
        identity: ForeignTableIdentity,
    ) -> Result<Self, IcebergFdwError> {
        Self::bind_catalog_table(
            relation_oid,
            server_oid,
            effective_user,
            identity,
            None,
        )
    }

    fn bind_catalog_table(
        relation_oid: pg_sys::Oid,
        server_oid: pg_sys::Oid,
        effective_user: pg_sys::Oid,
        identity: ForeignTableIdentity,
        executor: Option<&ExecutorHandle<'_>>,
    ) -> Result<Self, IcebergFdwError> {
        let request = CatalogSessionRequest::resolve(server_oid, effective_user)?;
        let key =
            RemoteTableKey::new(request.identity().clone(), identity.table().clone());
        if let Some((catalog, table)) = ForeignTransaction::staged_table(
            relation_oid,
            &key,
            request.runtime_config(),
        )? {
            return Ok(Self {
                identity,
                catalog_table: BoundCatalogTable::new(
                    key,
                    Rc::new(request.connect(Some(catalog))?),
                    table,
                ),
            });
        }
        let reusable_catalog = ForeignTransaction::catalog_from_transaction(
            request.identity(),
            request.runtime_config(),
        )?;
        let (catalog, table) = match executor {
            Some(executor) => ExecutorForeignTableCache::acquire(executor)
                .bind_table(&key, request, reusable_catalog)?,
            None => {
                let catalog = Rc::new(request.connect(reusable_catalog)?);
                let table = catalog.as_catalog().load_table(key.table())?;
                (catalog, table)
            }
        };
        Ok(Self {
            identity,
            catalog_table: BoundCatalogTable::new(key, catalog, table),
        })
    }

    pub(crate) fn key(&self) -> &RemoteTableKey {
        self.catalog_table.key()
    }

    pub(crate) fn identity(&self) -> &ForeignTableIdentity {
        &self.identity
    }

    pub(crate) fn table(&self) -> &Table {
        self.catalog_table.table()
    }

    pub(crate) fn into_catalog_table(self) -> BoundCatalogTable {
        self.catalog_table
    }
}
