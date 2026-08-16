//! Read-only finalized filter-plan view for FDW providers.

use crate::expr::pushdown::{
    FilterPlanSummary, FilterPushdown, FilterQualLocation, NegotiatedFilter,
    NegotiatedFilterSet, FilterValueSlotId,
};
use crate::expr::{PushdownContract, PushdownCosting};

/// Location of an accepted predicate's original clause in the final scan plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForeignPlanQualLocation {
    /// PostgreSQL evaluates the original clause as a local scan qual.
    Local { index: usize },
    /// PostgreSQL evaluates the original clause as an FDW recheck qual.
    Recheck { index: usize },
}

/// Planning-time text for the PostgreSQL values referenced by one provider
/// predicate. Indices are the predicate-local [`FilterValueSlotId`] values
/// supplied to the provider during filter negotiation.
#[derive(Clone, Copy)]
pub struct ForeignFilterExplainValues<'a> {
    values: &'a [String],
}

impl<'a> ForeignFilterExplainValues<'a> {
    pub(crate) const fn new(values: &'a [String]) -> Self {
        Self { values }
    }

    #[inline]
    pub fn value(self, id: FilterValueSlotId) -> &'a str {
        &self.values[id.index()]
    }

    #[inline]
    pub const fn len(self) -> usize {
        self.values.len()
    }

    #[inline]
    pub const fn is_empty(self) -> bool {
        self.values.is_empty()
    }
}

/// One finalized provider predicate visible while building an FDW plan.
pub struct ForeignPlanFilter<'a, P: FilterPushdown> {
    filter: &'a NegotiatedFilter<P::PlannedPredicate>,
}

impl<P: FilterPushdown> ForeignPlanFilter<'_, P> {
    /// Provider-native predicate accepted by core's final negotiation.
    #[inline]
    pub fn predicate(&self) -> &P::PlannedPredicate {
        &self.filter.planned
    }

    /// Semantic obligation attached to this predicate.
    #[inline]
    pub const fn contract(&self) -> PushdownContract {
        self.filter.effective.contract
    }

    /// Costing role attached to this predicate.
    #[inline]
    pub const fn costing(&self) -> PushdownCosting {
        self.filter.effective.costing
    }

    /// Half-open range in the framework-owned filter binding-expression prefix.
    #[inline]
    pub fn binding_range(&self) -> core::ops::Range<usize> {
        self.filter.binding_start
            ..self.filter.binding_start + self.filter.binding_count
    }

    /// Location of this predicate's original clause in the final scan plan.
    #[inline]
    pub const fn qual_location(&self) -> ForeignPlanQualLocation {
        match self.filter.qual_location {
            FilterQualLocation::Local(index) => {
                ForeignPlanQualLocation::Local { index }
            }
            FilterQualLocation::Recheck(index) => {
                ForeignPlanQualLocation::Recheck { index }
            }
        }
    }
}

/// Read-only authoritative filter result supplied to `FdwScan::build_plan`.
///
/// The provider can compose its final private plan from these accepted
/// predicates, but cannot re-negotiate PostgreSQL expressions at this stage.
pub struct ForeignPlanFilters<'a, P: FilterPushdown> {
    filters: &'a NegotiatedFilterSet<P::PlannedPredicate>,
}

impl<'a, P: FilterPushdown> ForeignPlanFilters<'a, P> {
    pub(crate) fn new(filters: &'a NegotiatedFilterSet<P::PlannedPredicate>) -> Self {
        Self { filters }
    }

    /// Aggregate summary of this final plan.
    #[inline]
    pub fn summary(&self) -> FilterPlanSummary {
        FilterPlanSummary::from_negotiated_set(self.filters)
    }

    /// Iterate in the same order used by the framework plan envelope.
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = ForeignPlanFilter<'_, P>> + '_ {
        self.filters
            .planned
            .iter()
            .map(|filter| ForeignPlanFilter { filter })
    }

    /// Number of framework-owned filter binding expressions.
    #[inline]
    pub fn binding_count(&self) -> usize {
        self.filters.bindings.len()
    }

    /// Number of clauses that remain as PostgreSQL local quals.
    #[inline]
    pub fn residual_count(&self) -> usize {
        self.filters.residual.len()
    }

    /// Number of exact predicates retained as PostgreSQL recheck quals.
    #[inline]
    pub fn recheck_count(&self) -> usize {
        self.filters.recheck.len()
    }
}
