//! One catalog's session and table mutations in a top-level transaction.

use std::collections::HashMap;

use lagodb_core::catalog::{
    ForeignConfigInvalidation, ForeignConfigInvalidationKeys,
};

use super::state::ForeignTableState;
use crate::foreign_table::catalog::{
    CatalogRuntimeConfig, CatalogSession, ForeignCatalog, RemoteTableKey,
};
use crate::foreign_table::error::IcebergFdwError;

/// Transaction-owned state rather than a refreshable backend cache entry.
///
/// Accepted policy: matching syscache invalidations are conservative and
/// monotonic for this transaction. They include ACL-only updates such as a
/// GRANT to another role, even when the captured runtime options are unchanged.
/// Later binds and publication admission reject that state; callbacks neither
/// reread catalogs nor compare individual options. PG17 postgres_fdw uses the
/// same coarse matching in contrib/postgres_fdw/connection.c's
/// pgfdw_inval_callback, explicitly avoiding option-by-option comparison.
/// Its response differs: it lets an in-use connection finish its transaction
/// before disconnecting. LagoDB's rejection at admission is our own policy.
#[derive(Debug)]
pub(super) struct CatalogTransactionState {
    pub(super) catalog: ForeignCatalog,
    pub(super) runtime_config: CatalogRuntimeConfig,
    invalidation_keys: ForeignConfigInvalidationKeys,
    pub(super) invalidated: bool,
    pub(super) tables: HashMap<RemoteTableKey, ForeignTableState>,
}

impl CatalogTransactionState {
    pub(super) fn new(session: &CatalogSession) -> Self {
        Self {
            catalog: session.catalog.clone(),
            runtime_config: session.runtime_config.clone(),
            invalidation_keys: ForeignConfigInvalidationKeys::new(
                session.identity.server_oid,
                session.identity.effective_user,
                session.runtime_config.mapping_user(),
            ),
            invalidated: false,
            tables: HashMap::new(),
        }
    }

    pub(super) fn validate(
        &self,
        session: &CatalogSession,
    ) -> Result<(), IcebergFdwError> {
        if self.invalidated || self.runtime_config != session.runtime_config {
            return Err(IcebergFdwError::CatalogConfigurationChanged);
        }
        Ok(())
    }

    pub(super) fn invalidate_if_matches(
        &mut self,
        invalidation: ForeignConfigInvalidation,
    ) {
        self.invalidated |= self.invalidation_keys.matches(invalidation);
    }

    pub(super) fn has_catalog_changes(&self) -> bool {
        self.tables
            .values()
            .any(ForeignTableState::has_catalog_changes)
    }
}
