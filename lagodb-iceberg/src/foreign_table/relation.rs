//! Resolution of a PostgreSQL foreign table into one Iceberg catalog table.

use iceberg_lite::catalog::TableIdent;
use iceberg_lite::spec::SchemaRef;
use iceberg_lite::table::Table;
use lagodb_core::handles::ExecutorHandle;
use pgrx::pg_sys;

use super::catalog::ForeignCatalog;
use super::error::IcebergFdwError;
use super::options::{
    CatalogConnection, CatalogRuntimeConfig, ForeignIdentifier, ForeignTableIdentity,
    ServerBindingKey,
};
use super::read_view::ForeignReadViews;
use super::transaction::ForeignTransaction;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct RemoteTableKey {
    catalog: ServerBindingKey,
    table: TableIdent,
}

impl PartialOrd for RemoteTableKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RemoteTableKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (
            u32::from(self.catalog.server_oid),
            u32::from(self.catalog.effective_user),
            &self.table,
        )
            .cmp(&(
                u32::from(other.catalog.server_oid),
                u32::from(other.catalog.effective_user),
                &other.table,
            ))
    }
}

impl RemoteTableKey {
    pub(super) fn new(
        catalog: ServerBindingKey,
        identity: &ForeignTableIdentity,
    ) -> Self {
        Self {
            catalog,
            table: identity.table().clone(),
        }
    }

    pub(crate) fn catalog_binding(&self) -> &ServerBindingKey {
        &self.catalog
    }

    pub(crate) fn publication_name(&self) -> String {
        ForeignIdentifier::table_path(&self.table)
    }
}

pub(crate) struct ResolvedCatalogBinding {
    pub(crate) key: ServerBindingKey,
    pub(crate) runtime_config: CatalogRuntimeConfig,
    pub(crate) catalog: ForeignCatalog,
}

pub(crate) struct IcebergForeignTable {
    key: RemoteTableKey,
    identity: ForeignTableIdentity,
    catalog: ResolvedCatalogBinding,
    table: Table,
}

impl IcebergForeignTable {
    pub(crate) fn resolve(
        relation_oid: pg_sys::Oid,
        effective_user: pg_sys::Oid,
    ) -> Result<Self, IcebergFdwError> {
        Self::resolve_visible(relation_oid, effective_user, None)
    }

    /// Resolve local options on every bind, but select the remote Table only
    /// once for this executor and catalog/user/table identity.
    pub(crate) fn resolve_for_execution(
        relation_oid: pg_sys::Oid,
        effective_user: pg_sys::Oid,
        executor: &ExecutorHandle<'_>,
    ) -> Result<Self, IcebergFdwError> {
        Self::resolve_visible(relation_oid, effective_user, Some(executor))
    }

    fn resolve_visible(
        relation_oid: pg_sys::Oid,
        effective_user: pg_sys::Oid,
        executor: Option<&ExecutorHandle<'_>>,
    ) -> Result<Self, IcebergFdwError> {
        // SAFETY: planner/executor contexts supply a live foreign-table OID;
        // the returned catalog object is consumed before this method returns.
        let foreign_table = unsafe { &*pg_sys::GetForeignTable(relation_oid) };
        let identity = ForeignTableIdentity::from_foreign_table(foreign_table)?;
        Self::bind_catalog_table(
            foreign_table.serverid,
            effective_user,
            identity,
            executor,
        )
    }

    pub(crate) fn load(
        server_oid: pg_sys::Oid,
        effective_user: pg_sys::Oid,
        identity: ForeignTableIdentity,
    ) -> Result<Self, IcebergFdwError> {
        Self::bind_catalog_table(server_oid, effective_user, identity, None)
    }

    fn bind_catalog_table(
        server_oid: pg_sys::Oid,
        effective_user: pg_sys::Oid,
        identity: ForeignTableIdentity,
        executor: Option<&ExecutorHandle<'_>>,
    ) -> Result<Self, IcebergFdwError> {
        let connection = CatalogConnection::resolve(server_oid, effective_user)?;
        let catalog_key = connection.server_binding_key();
        let runtime_config = connection.runtime_config();
        let reusable_catalog =
            ForeignTransaction::catalog_for_binding(&catalog_key, &runtime_config)?;
        let key = RemoteTableKey::new(catalog_key.clone(), &identity);
        let load = || -> Result<_, IcebergFdwError> {
            let catalog = match reusable_catalog {
                Some(catalog) => catalog,
                None => connection.connect()?,
            };
            // Reusing a connection does not reuse another statement's metadata.
            let table = catalog.as_catalog().load_table(identity.table())?;
            Ok((catalog, table))
        };
        let (catalog, table) = match executor {
            Some(executor) => ForeignReadViews::acquire(executor).bind_table(
                &key,
                &runtime_config,
                load,
            )?,
            None => load()?,
        };
        Ok(Self {
            key,
            identity,
            catalog: ResolvedCatalogBinding {
                key: catalog_key,
                runtime_config,
                catalog,
            },
            table,
        })
    }

    pub(crate) fn key(&self) -> &RemoteTableKey {
        &self.key
    }

    pub(crate) fn identity(&self) -> &ForeignTableIdentity {
        &self.identity
    }

    pub(crate) fn table(&self) -> &Table {
        &self.table
    }

    pub(crate) fn definition_schema(&self) -> Result<&SchemaRef, IcebergFdwError> {
        self.identity.definition_schema(self.table.metadata())
    }

    pub(crate) fn into_parts(
        self,
    ) -> (RemoteTableKey, ResolvedCatalogBinding, Table) {
        (self.key, self.catalog, self.table)
    }
}
