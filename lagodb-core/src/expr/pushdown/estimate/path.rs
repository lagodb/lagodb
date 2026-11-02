//! Path-stage estimates of the filters a provider executes for one path.

use pgrx::pg_sys;

use crate::expr::pushdown::plan_set::PathPlannedFilter;

use super::{FilterEstimate, PruningEstimate, RelationFilterEstimator};

/// What one path's planned filters cost and leave, for CustomScan and FDW
/// paths alike, computed on demand.
///
/// Every estimate reads the expression the provider executes
/// (`pushed_expr`), or the part of it the provider evaluates on each row
/// (`row_filter`), as `genericcostestimate` prices an index scan by the
/// index quals derived from a clause rather than the clause itself
/// (selfuncs.c). A widened filter is wider than its source clause, which
/// PostgreSQL keeps and evaluates as a residual.
#[derive(Clone, Copy)]
pub struct PathFilterEstimates<'a> {
    filters: &'a [PathPlannedFilter],
    estimator: RelationFilterEstimator,
    units: Option<f64>,
}

impl<'a> PathFilterEstimates<'a> {
    pub(crate) fn new(
        filters: &'a [PathPlannedFilter],
        estimator: RelationFilterEstimator,
        units: Option<f64>,
    ) -> Self {
        Self {
            filters,
            estimator,
            units,
        }
    }

    /// Scan share the costed filters leave, layout and statistics pruning
    /// together.
    pub fn pruning(&self) -> PruningEstimate {
        self.estimator.pruning(
            self.filters
                .iter()
                .map(|filter| (filter.pushed_expr, filter.costing)),
            self.units,
        )
    }

    /// Scan share the layout-pruned filters alone leave, for a relation whose
    /// statistics do not follow its physical order.
    pub fn layout_fraction(&self) -> f64 {
        self.estimator.layout_selectivity(
            self.filters
                .iter()
                .map(|filter| (filter.pushed_expr, filter.costing)),
        )
    }

    /// Evaluation cost of the filters, for a provider that evaluates them
    /// itself: each row filter on each row it reads, the rest of a filter
    /// once, at startup, when the scan binds it.
    pub fn provider_filter_cost(&self) -> pg_sys::QualCost {
        self.estimator.provider_filter_cost(
            self.filters
                .iter()
                .map(|filter| (filter.pushed_expr, filter.row_filter)),
        )
    }

    /// Share of the rows it reads such a provider returns: those its row
    /// filters keep, whatever their contract, as `genericcostestimate`
    /// estimates the tuples an index returns by the lossy quals it executes
    /// (selfuncs.c).
    pub fn returned_selectivity(&self) -> f64 {
        self.estimator.selectivity(
            self.filters
                .iter()
                .filter_map(|filter| filter.row_filter.expr(filter.pushed_expr)),
        )
    }

    /// Selectivity and evaluation cost of the costed filters as one implicit
    /// AND, for a provider priced by selectivity alone.
    pub fn costed_filters(&self) -> FilterEstimate {
        self.estimator.filters(
            self.filters
                .iter()
                .filter(|filter| filter.costing.is_costed())
                .map(|filter| filter.pushed_expr),
        )
    }
}
