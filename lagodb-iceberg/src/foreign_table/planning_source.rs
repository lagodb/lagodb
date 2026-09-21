//! Foreign-table metadata shared by planner facets for one base relation.

use once_cell::unsync::OnceCell;
use pgrx::pg_sys;

use super::error::IcebergFdwError;
use super::options::ForeignTableIdentity;
use super::relation::RestForeignTable;

/// Relation-owned foreign metadata resolved at most once during planning.
/// Execution deliberately resolves the table again against its own catalog
/// state instead of retaining this object beyond the planner lifetime.
pub(crate) struct ForeignPlanningSource {
    server_oid: pg_sys::Oid,
    effective_user_oid: pg_sys::Oid,
    identity: ForeignTableIdentity,
    resolved: OnceCell<RestForeignTable>,
}

impl ForeignPlanningSource {
    pub(crate) fn new(
        relation_oid: pg_sys::Oid,
        effective_user_oid: pg_sys::Oid,
    ) -> Result<Self, IcebergFdwError> {
        // SAFETY: the caller supplies the live foreign relation being planned.
        let table = unsafe { &*pg_sys::GetForeignTable(relation_oid) };
        Ok(Self {
            server_oid: table.serverid,
            effective_user_oid,
            identity: ForeignTableIdentity::from_foreign_table(table)?,
            resolved: OnceCell::new(),
        })
    }

    pub(crate) fn identity(&self) -> &ForeignTableIdentity {
        &self.identity
    }

    pub(crate) fn resolved(&self) -> Result<&RestForeignTable, IcebergFdwError> {
        self.resolved.get_or_try_init(|| {
            RestForeignTable::load(
                self.server_oid,
                self.effective_user_oid,
                self.identity.clone(),
            )
        })
    }
}
