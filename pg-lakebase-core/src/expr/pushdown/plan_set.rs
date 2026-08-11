//! Finalized filter-plan entries and path-stage summaries.

use pgrx::pg_sys;
use std::ops::Range;

use crate::expr::contract::{PushdownContract, PushdownCosting};

use super::{EffectiveFilterContract, FilterBindingExpr, NormalizedFilter};

/// One provider-owned planned predicate and its local binding range.
pub(crate) struct NegotiatedFilter<P> {
    pub original_expr: *mut pg_sys::Expr,
    pub pushed_expr: *mut pg_sys::Expr,
    pub planned: P,
    pub effective: EffectiveFilterContract,
    pub qual_location: FilterQualLocation,
    pub binding_start: usize,
    pub binding_count: usize,
}

#[derive(Clone, Copy)]
pub(crate) enum FilterQualLocation {
    Local(usize),
    Recheck(usize),
}

/// Decoded plan record used by executor adapters.
pub(crate) struct PlannedFilterRecord<P> {
    pub planned: P,
    pub contract: PushdownContract,
    pub binding_range: Range<usize>,
}

/// Authoritative final result produced by [`super::FilterNegotiator`].
pub(crate) struct NegotiatedFilterSet<P> {
    pub residual: Vec<*mut pg_sys::Expr>,
    pub planned: Vec<NegotiatedFilter<P>>,
    pub recheck: Vec<*mut pg_sys::Expr>,
    pub bindings: Vec<FilterBindingExpr>,
}

impl<P> NegotiatedFilterSet<P> {
    pub(crate) fn new() -> Self {
        Self {
            residual: Vec::new(),
            planned: Vec::new(),
            recheck: Vec::new(),
            bindings: Vec::new(),
        }
    }

    pub(crate) fn push_residual(&mut self, expr: *mut pg_sys::Expr) {
        self.residual.push(expr);
    }

    pub(crate) fn accept(
        &mut self,
        original_expr: *mut pg_sys::Expr,
        normalized: NormalizedFilter,
        planned: P,
        effective: EffectiveFilterContract,
    ) {
        let pushed_expr = normalized.pushed_expr;
        let binding_start = self.bindings.len();
        let binding_count = normalized.bindings.len();
        self.bindings.extend(normalized.bindings);

        let qual_location = if effective.contract.requires_recheck() {
            let index = self.recheck.len();
            self.recheck.push(original_expr);
            FilterQualLocation::Recheck(index)
        } else {
            let index = self.residual.len();
            self.residual.push(original_expr);
            FilterQualLocation::Local(index)
        };
        self.planned.push(NegotiatedFilter {
            original_expr,
            pushed_expr,
            planned,
            effective,
            qual_location,
            binding_start,
            binding_count,
        });
    }

    pub(crate) fn into_path_set(self) -> PathFilterSet {
        let planned = self
            .planned
            .into_iter()
            .map(|entry| PathPlannedFilter {
                original_expr: entry.original_expr,
                contract: entry.effective.contract,
                costing: entry.effective.costing,
            })
            .collect();
        PathFilterSet {
            residual: self.residual,
            planned,
            recheck: self.recheck,
        }
    }

    pub(crate) fn path_set(&self) -> PathFilterSet {
        let planned = self
            .planned
            .iter()
            .map(|entry| PathPlannedFilter {
                original_expr: entry.original_expr,
                contract: entry.effective.contract,
                costing: entry.effective.costing,
            })
            .collect();
        PathFilterSet {
            residual: self.residual.clone(),
            planned,
            recheck: self.recheck.clone(),
        }
    }
}

/// Provider-facing, artifact-free summary of one negotiated filter set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FilterPlanSummary {
    planned_count: usize,
    exact_count: usize,
    conservative_count: usize,
    costed_count: usize,
}

impl FilterPlanSummary {
    pub(crate) fn from_path_set(filters: &PathFilterSet) -> Self {
        let exact_count = filters
            .planned
            .iter()
            .filter(|filter| filter.contract.requires_recheck())
            .count();
        let costed_count = filters
            .planned
            .iter()
            .filter(|filter| filter.costing.is_costed())
            .count();
        Self {
            planned_count: filters.planned.len(),
            exact_count,
            conservative_count: filters.planned.len() - exact_count,
            costed_count,
        }
    }

    pub(crate) fn from_negotiated_set<P>(filters: &NegotiatedFilterSet<P>) -> Self {
        let exact_count = filters
            .planned
            .iter()
            .filter(|filter| filter.effective.contract.requires_recheck())
            .count();
        let costed_count = filters
            .planned
            .iter()
            .filter(|filter| filter.effective.costing.is_costed())
            .count();
        Self {
            planned_count: filters.planned.len(),
            exact_count,
            conservative_count: filters.planned.len() - exact_count,
            costed_count,
        }
    }

    #[inline]
    pub const fn planned_count(self) -> usize {
        self.planned_count
    }

    #[inline]
    pub const fn exact_count(self) -> usize {
        self.exact_count
    }

    #[inline]
    pub const fn conservative_count(self) -> usize {
        self.conservative_count
    }

    #[inline]
    pub const fn costed_count(self) -> usize {
        self.costed_count
    }

    #[inline]
    pub const fn is_empty(self) -> bool {
        self.planned_count == 0
    }
}

/// Provider-free path-stage record. Planned artifacts are deliberately dropped.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PathPlannedFilter {
    pub original_expr: *mut pg_sys::Expr,
    pub contract: PushdownContract,
    pub costing: PushdownCosting,
}

/// Cloneable result used only for path eligibility and costing.
#[derive(Debug, Clone)]
pub(crate) struct PathFilterSet {
    pub residual: Vec<*mut pg_sys::Expr>,
    pub planned: Vec<PathPlannedFilter>,
    pub recheck: Vec<*mut pg_sys::Expr>,
}

impl PathFilterSet {
    #[inline]
    pub(crate) fn has_planned_filters(&self) -> bool {
        !self.planned.is_empty()
    }

    pub(crate) fn costed_pruning_exprs(
        &self,
    ) -> impl Iterator<Item = *mut pg_sys::Expr> + '_ {
        self.planned
            .iter()
            .filter(|entry| entry.costing.is_costed())
            .map(|entry| entry.original_expr)
    }

    pub(crate) fn merged(&self, right: &Self) -> Self {
        let mut residual =
            Vec::with_capacity(self.residual.len() + right.residual.len());
        residual.extend_from_slice(&self.residual);
        residual.extend_from_slice(&right.residual);

        let mut planned =
            Vec::with_capacity(self.planned.len() + right.planned.len());
        planned.extend_from_slice(&self.planned);
        planned.extend_from_slice(&right.planned);

        let mut recheck =
            Vec::with_capacity(self.recheck.len() + right.recheck.len());
        recheck.extend_from_slice(&self.recheck);
        recheck.extend_from_slice(&right.recheck);

        Self {
            residual,
            planned,
            recheck,
        }
    }
}

#[cfg(test)]
mod tests {
    use core::mem::MaybeUninit;

    use super::*;
    use crate::expr::pushdown::{
        FilterColumn, FilterFragment, FilterNode, FilterScalar, FilterTypeMetadata,
    };

    fn normalized_filter(pushed_expr: *mut pg_sys::Expr) -> NormalizedFilter {
        let column = FilterColumn {
            rel_oid: pg_sys::Oid::from(16_384_u32),
            attno: 1,
            declared_type: FilterTypeMetadata {
                type_oid: pg_sys::INT4OID,
                typmod: -1,
                collation: pg_sys::Oid::INVALID,
            },
            value_type: FilterTypeMetadata {
                type_oid: pg_sys::INT4OID,
                typmod: -1,
                collation: pg_sys::Oid::INVALID,
            },
        };
        NormalizedFilter {
            fragment: FilterFragment::new(
                FilterNode::IsNull(FilterScalar::Column(column)),
                Vec::new(),
            ),
            bindings: Vec::new(),
            pushed_expr,
        }
    }

    #[test]
    fn exact_and_conservative_contracts_own_recheck_and_residual() {
        let mut exact_expr = MaybeUninit::<pg_sys::Expr>::uninit();
        let mut conservative_expr = MaybeUninit::<pg_sys::Expr>::uninit();
        let exact_expr = exact_expr.as_mut_ptr();
        let conservative_expr = conservative_expr.as_mut_ptr();
        let mut filters = NegotiatedFilterSet::new();

        filters.accept(
            exact_expr,
            normalized_filter(exact_expr),
            "exact",
            EffectiveFilterContract {
                contract: PushdownContract::ExactRowFilter,
                costing: PushdownCosting::CostedPruning,
            },
        );
        filters.accept(
            conservative_expr,
            normalized_filter(conservative_expr),
            "conservative",
            EffectiveFilterContract {
                contract: PushdownContract::ConservativePruning,
                costing: PushdownCosting::UncostedBestEffort,
            },
        );

        assert_eq!(filters.recheck, vec![exact_expr]);
        assert_eq!(filters.residual, vec![conservative_expr]);
        assert_eq!(filters.planned.len(), 2);
        assert_eq!(filters.planned[0].pushed_expr, exact_expr);
        assert_eq!(filters.planned[1].pushed_expr, conservative_expr);
        assert!(matches!(
            filters.planned[0].qual_location,
            FilterQualLocation::Recheck(0)
        ));
        assert!(matches!(
            filters.planned[1].qual_location,
            FilterQualLocation::Local(0)
        ));

        let path_filters = filters.into_path_set();
        let summary = FilterPlanSummary::from_path_set(&path_filters);
        assert_eq!(summary.planned_count(), 2);
        assert_eq!(summary.exact_count(), 1);
        assert_eq!(summary.conservative_count(), 1);
        assert_eq!(summary.costed_count(), 1);
    }
}
