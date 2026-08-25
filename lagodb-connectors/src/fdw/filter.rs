//! Core filter-pushdown adapter.

use pg_lakebase_core::expr::pushdown::{
    FilterBindResult, FilterFragment, FilterPlan, FilterPlanningContext,
    FilterPushdown, FilterPushdownPlanner, FilterValueBindings,
};
use pg_lakebase_core::plan_data::{PlanDataReader, PlanDataWriter};

use crate::format::{
    FormatBoundFilter, FormatFilterPlanner, FormatKind, FormatPlannedFilter,
};

use super::{LagodbConnectors, ResolvedForeignRelation};
use crate::error::ConnectorError;

/// Planner state created once for one relation planning invocation.
pub(crate) struct ConnectorFilterPlanner {
    inner: Box<dyn FormatFilterPlanner>,
}

impl FilterPushdownPlanner for ConnectorFilterPlanner {
    type PlannedPredicate = FormatPlannedFilter;
    type Error = ConnectorError;

    fn try_plan_filter(
        &mut self,
        fragment: &FilterFragment,
    ) -> Result<FilterPlan<Self::PlannedPredicate>, Self::Error> {
        self.inner.try_plan_filter(fragment)
    }
}

/// Format-owned planned predicate exposed through the core adapter.
pub(crate) type ConnectorPlannedFilter = FormatPlannedFilter;

/// Format-owned bound predicate exposed through the core adapter.
pub(crate) type ConnectorBoundFilter = FormatBoundFilter;

impl FilterPushdown for LagodbConnectors {
    type Planner = ConnectorFilterPlanner;
    type PlannedPredicate = ConnectorPlannedFilter;
    type BoundPredicate = ConnectorBoundFilter;
    type Error = ConnectorError;

    fn begin_filter_planning(
        context: &FilterPlanningContext,
    ) -> Result<Self::Planner, Self::Error> {
        let selected = ResolvedForeignRelation::resolve(context.relation_oid())?;
        let reader = selected.into_reader();
        Ok(ConnectorFilterPlanner {
            inner: reader.begin_filter_planning(context)?,
        })
    }

    fn encode_planned(
        predicate: &Self::PlannedPredicate,
        writer: &mut PlanDataWriter,
    ) -> Result<(), Self::Error> {
        writer.append_i32(predicate.kind().wire());
        predicate.encode(writer)
    }

    fn decode_planned(
        reader: &mut PlanDataReader<'_>,
        binding_count: usize,
    ) -> Result<Self::PlannedPredicate, Self::Error> {
        let wire = reader.read_i32()?;
        let kind = FormatKind::from_wire(wire)
            .ok_or_else(|| ConnectorError::invalid_plan_format(wire))?;
        kind.decode_filter(reader, binding_count)
    }

    fn bind_filter(
        predicate: &Self::PlannedPredicate,
        values: FilterValueBindings<'_>,
    ) -> Result<FilterBindResult<Self::BoundPredicate>, Self::Error> {
        predicate.bind(values)
    }
}
