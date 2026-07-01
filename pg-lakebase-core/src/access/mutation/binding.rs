//! Opaque executor binding shared by an outer ModifyTable owner and one
//! provider scan.

use pgrx::pg_sys;

use crate::api::{AmModifyQueryState, AmResult, ModifyQueryState};

/// Query-lifetime handle used by a Modify-purpose scan to register physical
/// identity sources in the AM's shared state for one target relation.
pub struct ModifyScanBinding<Q: AmModifyQueryState> {
    query_state: ModifyQueryState<Q>,
    relation_oid: pg_sys::Oid,
}

impl<Q: AmModifyQueryState> Clone for ModifyScanBinding<Q> {
    fn clone(&self) -> Self {
        Self {
            query_state: self.query_state.clone(),
            relation_oid: self.relation_oid,
        }
    }
}

impl<Q: AmModifyQueryState> PartialEq for ModifyScanBinding<Q> {
    fn eq(&self, other: &Self) -> bool {
        self.relation_oid == other.relation_oid
            && self.query_state.same_owner(&other.query_state)
    }
}

impl<Q: AmModifyQueryState> Eq for ModifyScanBinding<Q> {}

impl<Q: AmModifyQueryState> ModifyScanBinding<Q> {
    pub(crate) fn new(
        query_state: ModifyQueryState<Q>,
        relation_oid: pg_sys::Oid,
    ) -> Self {
        Self {
            query_state,
            relation_oid,
        }
    }

    /// Register a provider-owned physical identity source in this relation's
    /// executor-query namespace.
    pub fn register_identity_source(
        &self,
        source: &Q::ScanIdentitySource<'_>,
    ) -> AmResult<Q::RegisteredScanIdentity> {
        self.query_state.update(|state| {
            state.register_scan_identity_source(self.relation_oid, source)
        })
    }
}
