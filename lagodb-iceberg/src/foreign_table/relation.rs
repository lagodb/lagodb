//! Resolution of a PostgreSQL foreign table into one REST catalog table.

use iceberg_lite::catalog::rest::RestCatalog;
use iceberg_lite::catalog::{Catalog, NamespaceIdent, TableIdent};
use iceberg_lite::table::Table;
use pg_lakebase_core::storage::foreign::ForeignOptionView;
use pgrx::pg_sys;

use super::error::IcebergFdwError;
use super::options::{
    CatalogBindingKey, CatalogRuntimeConfig, ForeignTableIdentity,
    RestCatalogConnection, ServerBindingKey,
};
use super::transaction::ForeignTransaction;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct RemoteTableKey {
    catalog: CatalogBindingKey,
    namespace: String,
    table_name: String,
}

impl PartialOrd for RemoteTableKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RemoteTableKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (
            u32::from(self.catalog.server.server_oid),
            u32::from(self.catalog.server.effective_user),
            &self.catalog.catalog_name,
            &self.namespace,
            &self.table_name,
        )
            .cmp(&(
                u32::from(other.catalog.server.server_oid),
                u32::from(other.catalog.server.effective_user),
                &other.catalog.catalog_name,
                &other.namespace,
                &other.table_name,
            ))
    }
}

impl RemoteTableKey {
    fn new(catalog: CatalogBindingKey, identity: &ForeignTableIdentity) -> Self {
        Self {
            catalog,
            namespace: identity.namespace().to_owned(),
            table_name: identity.table_name().to_owned(),
        }
    }

    pub(crate) fn catalog_binding(&self) -> &CatalogBindingKey {
        &self.catalog
    }

    pub(crate) fn publication_name(&self) -> String {
        format!(
            "{}:{}.{}",
            self.catalog.catalog_name, self.namespace, self.table_name
        )
    }
}

pub(crate) struct ResolvedCatalogBinding {
    pub(crate) key: CatalogBindingKey,
    pub(crate) server_key: ServerBindingKey,
    pub(crate) runtime_config: CatalogRuntimeConfig,
    pub(crate) catalog: RestCatalog,
}

pub(crate) struct RestForeignTable {
    key: RemoteTableKey,
    identity: ForeignTableIdentity,
    catalog: ResolvedCatalogBinding,
    table: Table,
}

impl RestForeignTable {
    pub(crate) fn resolve(
        relation_oid: pg_sys::Oid,
        effective_user: pg_sys::Oid,
    ) -> Result<Self, IcebergFdwError> {
        // SAFETY: planner/executor contexts supply a live foreign-table OID;
        // the returned catalog object is consumed before this method returns.
        let foreign_table = unsafe { &*pg_sys::GetForeignTable(relation_oid) };
        let options = unsafe { ForeignOptionView::from_raw(foreign_table.options) };
        let identity = ForeignTableIdentity::from_view(options)?;
        Self::load(foreign_table.serverid, effective_user, identity)
    }

    pub(crate) fn load(
        server_oid: pg_sys::Oid,
        effective_user: pg_sys::Oid,
        identity: ForeignTableIdentity,
    ) -> Result<Self, IcebergFdwError> {
        let connection = RestCatalogConnection::resolve(
            server_oid,
            effective_user,
            identity.catalog_name().to_owned(),
        )?;
        let catalog_key = connection.catalog_binding_key();
        let server_key = connection.server_binding_key();
        let runtime_config = connection.runtime_config();
        ForeignTransaction::validate_runtime_binding(&server_key, &runtime_config)?;
        let key = RemoteTableKey::new(catalog_key.clone(), &identity);
        let catalog = connection.connect()?;
        let identifier = TableIdent::new(
            NamespaceIdent::new(identity.namespace().to_owned()),
            identity.table_name().to_owned(),
        );
        let table = catalog.load_table(&identifier)?;
        if identity.mode().is_writable() {
            catalog.ensure_transaction_commit_supported()?;
        }
        Ok(Self {
            key,
            identity,
            catalog: ResolvedCatalogBinding {
                key: catalog_key,
                server_key,
                runtime_config,
                catalog,
            },
            table,
        })
    }

    pub(crate) fn identity(&self) -> &ForeignTableIdentity {
        &self.identity
    }

    pub(crate) fn table(&self) -> &Table {
        &self.table
    }

    pub(crate) fn into_parts(
        self,
    ) -> (RemoteTableKey, ResolvedCatalogBinding, Table) {
        (self.key, self.catalog, self.table)
    }
}
