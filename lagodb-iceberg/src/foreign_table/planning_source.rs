//! Foreign-table metadata shared by planner facets for one base relation.

use iceberg_lite::table::Table;
use once_cell::unsync::OnceCell;
use pgrx::pg_sys;

use super::catalog::{CatalogAccessIdentity, RemoteTableKey};
use super::error::IcebergFdwError;
use super::options::ForeignTableIdentity;
use super::relation::ResolvedForeignTableBinding;
use super::transaction::ForeignTransaction;

/// Relation-owned foreign metadata resolved at most once during planning.
/// Execution deliberately resolves the table again against its own catalog
/// state instead of retaining this object beyond the planner lifetime.
pub(crate) struct ForeignPlanningSource {
    relation_oid: pg_sys::Oid,
    server_oid: pg_sys::Oid,
    effective_user_oid: pg_sys::Oid,
    identity: ForeignTableIdentity,
    table: OnceCell<Table>,
}

impl ForeignPlanningSource {
    pub(crate) fn new(
        relation_oid: pg_sys::Oid,
        effective_user_oid: pg_sys::Oid,
    ) -> Result<Self, IcebergFdwError> {
        // SAFETY: the caller supplies the live foreign relation being planned.
        let table = unsafe { &*pg_sys::GetForeignTable(relation_oid) };
        Ok(Self {
            relation_oid,
            server_oid: table.serverid,
            effective_user_oid,
            identity: ForeignTableIdentity::from_foreign_table(table)?,
            table: OnceCell::new(),
        })
    }

    pub(crate) fn identity(&self) -> &ForeignTableIdentity {
        &self.identity
    }

    pub(crate) fn has_local_actions(&self) -> bool {
        let key = RemoteTableKey::new(
            CatalogAccessIdentity {
                server_oid: self.server_oid,
                effective_user: self.effective_user_oid,
            },
            self.identity.table().clone(),
        );
        ForeignTransaction::has_local_actions(&key)
    }

    pub(crate) fn table(&self) -> Result<&Table, IcebergFdwError> {
        self.table.get_or_try_init(|| {
            let resolved = ResolvedForeignTableBinding::load(
                self.relation_oid,
                self.server_oid,
                self.effective_user_oid,
                self.identity.clone(),
            )?;
            let (_key, _session, table) = resolved.into_catalog_table().into_parts();
            Ok(table)
        })
    }
}
