//! Central query-structure policy for query-offload candidates.

use pgrx::pg_sys;

/// A PostgreSQL query block viewed through the stage-specific offload policy.
///
/// The common structural exclusions live here, while each candidate keeps a
/// distinct entry point for the features it owns. In particular, aggregate,
/// DISTINCT, join, and lifted-subquery shapes are not interchangeable.
pub(super) struct QueryShape<'query> {
    query: &'query pg_sys::Query,
}

impl<'query> QueryShape<'query> {
    #[inline]
    pub(super) const fn new(query: &'query pg_sys::Query) -> Self {
        Self { query }
    }

    pub(super) fn supports_aggregate(&self) -> bool {
        self.supports_relational_base()
            && (self.query.hasAggs || !self.query.groupClause.is_null())
            && !self.query.hasDistinctOn
            && !self.query.groupDistinct
            && self.query.groupingSets.is_null()
            && self.query.windowClause.is_null()
    }

    pub(super) fn supports_distinct(&self) -> bool {
        self.supports_relational_base()
            && !self.query.hasAggs
            && !self.query.hasSubLinks
            && !self.query.hasDistinctOn
            && !self.query.groupDistinct
            && self.query.groupingSets.is_null()
            && self.query.groupClause.is_null()
            && self.query.havingQual.is_null()
            && self.query.windowClause.is_null()
            && !self.query.distinctClause.is_null()
    }

    pub(super) fn supports_join(&self) -> bool {
        self.supports_relational_base()
            && !self.query.hasAggs
            && self.query.groupClause.is_null()
            // DISTINCT owns the complete relation tree at its upper hook. A
            // lower Force-cost Join path could otherwise hide standard child
            // JoinPaths needed by DistinctPlanBuilder.
            && self.query.distinctClause.is_null()
    }

    pub(super) fn supports_subplan_root(&self) -> bool {
        self.supports_relational_base()
            && self.query.hasSubLinks
            && !self.query.hasAggs
            && self.query.groupClause.is_null()
    }

    pub(super) fn supports_lifted_subquery(&self) -> bool {
        self.supports_relational_base()
            && !self.query.hasAggs
            && self.query.groupClause.is_null()
            && self.query.havingQual.is_null()
            && !self.query.hasDistinctOn
            && self.query.groupingSets.is_null()
            && self.query.windowClause.is_null()
            && self.query.limitOffset.is_null()
            && self.query.limitCount.is_null()
    }

    /// Structural exclusions shared by every current relational candidate.
    /// Operator-owned clauses deliberately do not belong in this predicate.
    fn supports_relational_base(&self) -> bool {
        self.query.commandType == pg_sys::CmdType::CMD_SELECT
            && !self.query.hasWindowFuncs
            && !self.query.hasTargetSRFs
            && !self.query.hasRecursive
            && !self.query.hasModifyingCTE
            && !self.query.hasForUpdate
            && !self.query.hasRowSecurity
            && self.query.cteList.is_null()
            && self.query.rowMarks.is_null()
            && self.query.setOperations.is_null()
    }
}
