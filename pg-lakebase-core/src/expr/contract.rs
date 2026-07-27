//! Stage-neutral expression contracts shared by planning and execution.

use pgrx::pg_sys;

/// Persisted metadata for one scan-relation column in one pushed expression.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnRef {
    pub expr_index: usize,
    pub rel_oid: pg_sys::Oid,
    pub attno: pg_sys::AttrNumber,
    pub atttypid: pg_sys::Oid,
    pub attcollation: pg_sys::Oid,
    pub name: Option<String>,
}

/// Provider filtering obligation attached to a pushed clause.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PushdownContract {
    ExactRowFilter,
    ConservativePruning,
}

impl PushdownContract {
    #[inline]
    pub fn requires_residual(self) -> bool {
        matches!(self, Self::ConservativePruning)
    }

    #[inline]
    pub fn requires_recheck(self) -> bool {
        matches!(self, Self::ExactRowFilter)
    }
}

/// Whether a pushed expression contributes to scan-volume costing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PushdownCosting {
    CostedPruning,
    UncostedBestEffort,
}

impl PushdownCosting {
    #[inline]
    pub fn is_costed(self) -> bool {
        matches!(self, Self::CostedPruning)
    }
}

/// Provider decision for one structurally supported predicate leaf.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QualPushdownDecision {
    Pushable {
        contract: PushdownContract,
        costing: PushdownCosting,
    },
    Unsupported,
}

/// Stable operator identity used by provider capability policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PgComparisonIdentity {
    pub opno: pg_sys::Oid,
    pub opcollid: pg_sys::Oid,
    pub inputcollid: pg_sys::Oid,
}

/// Operator metadata supplied to execution translators.
///
/// Equality and hashing deliberately live on [`PgComparisonIdentity`]; function
/// and result OIDs are diagnostic/execution metadata, not capability identity.
#[derive(Clone, Copy, Debug)]
pub struct PgComparisonOp {
    pub opno: pg_sys::Oid,
    pub opfuncid: pg_sys::Oid,
    pub opresulttype: pg_sys::Oid,
    pub opcollid: pg_sys::Oid,
    pub inputcollid: pg_sys::Oid,
}

impl PgComparisonOp {
    #[inline]
    pub fn identity(self) -> PgComparisonIdentity {
        PgComparisonIdentity {
            opno: self.opno,
            opcollid: self.opcollid,
            inputcollid: self.inputcollid,
        }
    }
}

/// Full identity of a PostgreSQL plan parameter.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ParamKey {
    pub paramkind: pg_sys::ParamKind::Type,
    pub param_id: core::ffi::c_int,
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn comparison_identity_excludes_diagnostic_fields() {
        let base = PgComparisonOp {
            opno: pg_sys::Oid::from(96u32),
            opfuncid: pg_sys::Oid::from(65u32),
            opresulttype: pg_sys::BOOLOID,
            opcollid: pg_sys::Oid::INVALID,
            inputcollid: pg_sys::Oid::INVALID,
        };
        let different_diagnostics = PgComparisonOp {
            opfuncid: pg_sys::Oid::from(999u32),
            opresulttype: pg_sys::INT4OID,
            ..base
        };

        let identities =
            HashSet::from([base.identity(), different_diagnostics.identity()]);
        assert_eq!(identities.len(), 1);
    }
}
