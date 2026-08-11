//! Reader-owned filter pushdown contracts.

use pg_lakebase_core::expr::pushdown::{
    FilterBindResult, FilterFragment, FilterPlan, FilterValueBindings,
};
use pg_lakebase_core::plan_data::PlanDataWriter;

use crate::error::ConnectorError;

use super::FormatKind;

/// A format predicate accepted during scan planning.
///
/// The selected reader creates the concrete implementation. Keeping the
/// codec and runtime binder on the planned predicate means the FDW adapter
/// does not need a format match when a plan is serialized or bound.
pub(crate) trait FormatFilterPlan: 'static {
    fn kind(&self) -> FormatKind;

    fn encode(&self, writer: &mut PlanDataWriter) -> Result<(), ConnectorError>;

    fn bind(
        &self,
        values: FilterValueBindings<'_>,
    ) -> Result<FilterBindResult<FormatBoundFilter>, ConnectorError>;
}

/// Format-owned bound predicate consumed by a scan implementation.
pub(crate) trait FormatBoundPredicate: 'static {}

pub(crate) type FormatPlannedFilter = Box<dyn FormatFilterPlan>;
pub(crate) type FormatBoundFilter = Box<dyn FormatBoundPredicate>;

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
