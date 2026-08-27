//! Reader-owned filter pushdown contracts.

use lagodb_core::expr::pushdown::{
    FilterBindResult, FilterFragment, FilterPlan, FilterValueBindings,
};
use lagodb_core::fdw::ForeignFilterExplainValues;
use lagodb_core::plan_data::PlanDataWriter;

use crate::error::ConnectorError;

use super::FormatKind;
use super::parquet::ParquetBoundPredicate;

/// A format predicate accepted during scan planning.
///
/// The selected reader creates the concrete implementation. Keeping the
/// codec and runtime binder on the planned predicate means the FDW adapter
/// does not need a format match when a plan is serialized or bound.
pub(crate) trait FormatFilterPlan: 'static {
    fn kind(&self) -> FormatKind;

    fn encode(&self, writer: &mut PlanDataWriter) -> Result<(), ConnectorError>;

    fn explain(&self, values: ForeignFilterExplainValues<'_>) -> String;

    fn bind(
        &self,
        values: FilterValueBindings<'_>,
    ) -> Result<FilterBindResult<FormatBoundFilter>, ConnectorError>;
}

pub(crate) type FormatPlannedFilter = Box<dyn FormatFilterPlan>;

/// Closed set of runtime predicates produced by the configured format. This
/// mirrors `FormatKind` and avoids untyped downcasts in scan hot paths.
pub(crate) enum FormatBoundFilter {
    Parquet(ParquetBoundPredicate),
}

impl FormatBoundFilter {
    pub(crate) fn parquet(&self) -> &ParquetBoundPredicate {
        match self {
            Self::Parquet(predicate) => predicate,
        }
    }
}

/// Relation-scoped filter planner owned by the selected reader.
pub(crate) trait FormatFilterPlanner: 'static {
    fn try_plan_filter(
        &mut self,
        fragment: &FilterFragment,
    ) -> Result<FilterPlan<FormatPlannedFilter>, ConnectorError>;
}

/// Default reader planner for formats that do not push predicates down.
pub(super) struct NoPushdownFilterPlanner;

impl FormatFilterPlanner for NoPushdownFilterPlanner {
    fn try_plan_filter(
        &mut self,
        _fragment: &FilterFragment,
    ) -> Result<FilterPlan<FormatPlannedFilter>, ConnectorError> {
        Ok(FilterPlan::Unsupported)
    }
}
