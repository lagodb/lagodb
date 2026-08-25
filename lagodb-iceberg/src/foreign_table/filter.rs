//! Lazy FDW adapter over the shared Iceberg predicate implementation.

use pg_lakebase_core::expr::pushdown::{
    FilterBindResult, FilterFragment, FilterPlan, FilterPlanningContext,
    FilterPushdown, FilterPushdownPlanner, FilterValueBindings,
};
use pg_lakebase_core::fdw::ForeignFilterExplainValues;
use pg_lakebase_core::plan_data::{PlanDataReader, PlanDataWriter};

use super::error::IcebergFdwError;
use super::provider::IcebergFdw;
use super::relation::RestForeignTable;
use super::source_identity::PlanSourceIdentity;
use crate::engine::predicate::{
    BoundIcebergPredicate, IcebergFilterPlanner, PlannedIcebergNode,
    PlannedIcebergPredicate,
};

pub(crate) struct FdwPlannedPredicate {
    source: PlanSourceIdentity,
    predicate: PlannedIcebergPredicate,
}

impl FdwPlannedPredicate {
    pub(crate) fn source(&self) -> &PlanSourceIdentity {
        &self.source
    }

    pub(crate) fn explain(&self, values: ForeignFilterExplainValues<'_>) -> String {
        Self::explain_node(self.predicate.root(), values)
    }

    fn explain_node(
        node: &PlannedIcebergNode,
        values: ForeignFilterExplainValues<'_>,
    ) -> String {
        match node {
            PlannedIcebergNode::Comparison {
                operator,
                column,
                value,
                ..
            } => format!(
                "{} {} {}",
                column.debug_name,
                operator.explain_symbol(),
                values.value(*value)
            ),
            PlannedIcebergNode::IsNull(column) => {
                format!("{} IS NULL", column.debug_name)
            }
            PlannedIcebergNode::IsNotNull(column) => {
                format!("{} IS NOT NULL", column.debug_name)
            }
            PlannedIcebergNode::And(children) => {
                Self::explain_logical(children, values, " AND ")
            }
            PlannedIcebergNode::Or(children) => {
                Self::explain_logical(children, values, " OR ")
            }
            PlannedIcebergNode::Not(child) => {
                format!("NOT ({})", Self::explain_node(child, values))
            }
        }
    }

    fn explain_logical(
        children: &[PlannedIcebergNode],
        values: ForeignFilterExplainValues<'_>,
        operator: &str,
    ) -> String {
        let predicates = children
            .iter()
            .map(|child| Self::explain_node(child, values))
            .collect::<Vec<_>>();
        format!("({})", predicates.join(operator))
    }
}

enum IcebergFdwFilterPlannerState {
    Pending(FilterPlanningContext),
    Ready {
        source: PlanSourceIdentity,
        planner: IcebergFilterPlanner,
    },
}

pub(crate) struct IcebergFdwFilterPlanner {
    state: IcebergFdwFilterPlannerState,
}

impl IcebergFdwFilterPlanner {
    fn ready(
        &mut self,
    ) -> Result<(&PlanSourceIdentity, &mut IcebergFilterPlanner), IcebergFdwError>
    {
        let context = match &self.state {
            IcebergFdwFilterPlannerState::Pending(context) => Some(*context),
            IcebergFdwFilterPlannerState::Ready { .. } => None,
        };
        if let Some(context) = context {
            let table = RestForeignTable::resolve(
                context.relation_oid(),
                context.effective_user_id(),
            )?;
            let source = PlanSourceIdentity::from_table(table.table());
            let planner = IcebergFilterPlanner::from_schema(
                &context,
                table.table().metadata().current_schema(),
            )?;
            self.state = IcebergFdwFilterPlannerState::Ready { source, planner };
        }
        match &mut self.state {
            IcebergFdwFilterPlannerState::Ready { source, planner } => {
                Ok((source, planner))
            }
            IcebergFdwFilterPlannerState::Pending(_) => {
                unreachable!("pending filter planner was initialized above")
            }
        }
    }
}

impl FilterPushdownPlanner for IcebergFdwFilterPlanner {
    type PlannedPredicate = FdwPlannedPredicate;
    type Error = IcebergFdwError;

    fn try_plan_filter(
        &mut self,
        fragment: &FilterFragment,
    ) -> Result<FilterPlan<Self::PlannedPredicate>, Self::Error> {
        let (source, planner) = self.ready()?;
        let plan = planner.try_plan_filter(fragment)?;
        let wrap = |predicate| FdwPlannedPredicate {
            source: source.clone(),
            predicate,
        };
        Ok(match plan {
            FilterPlan::Unsupported => FilterPlan::Unsupported,
            FilterPlan::Exact(planned) => {
                FilterPlan::exact(wrap(planned.predicate), planned.costing)
            }
            FilterPlan::Conservative(planned) => {
                FilterPlan::conservative(wrap(planned.predicate), planned.costing)
            }
        })
    }
}

impl FilterPushdown for IcebergFdw {
    type Planner = IcebergFdwFilterPlanner;
    type PlannedPredicate = FdwPlannedPredicate;
    type BoundPredicate = BoundIcebergPredicate;
    type Error = IcebergFdwError;

    fn begin_filter_planning(
        context: &FilterPlanningContext,
    ) -> Result<Self::Planner, Self::Error> {
        Ok(IcebergFdwFilterPlanner {
            state: IcebergFdwFilterPlannerState::Pending(*context),
        })
    }

    fn encode_planned(
        predicate: &Self::PlannedPredicate,
        writer: &mut PlanDataWriter,
    ) -> Result<(), Self::Error> {
        predicate.source.encode(writer);
        predicate.predicate.encode(writer);
        Ok(())
    }

    fn decode_planned(
        reader: &mut PlanDataReader<'_>,
        binding_count: usize,
    ) -> Result<Self::PlannedPredicate, Self::Error> {
        Ok(FdwPlannedPredicate {
            source: PlanSourceIdentity::decode(reader)?,
            predicate: PlannedIcebergPredicate::decode(reader, binding_count)?,
        })
    }

    fn bind_filter(
        predicate: &Self::PlannedPredicate,
        values: FilterValueBindings<'_>,
    ) -> Result<FilterBindResult<Self::BoundPredicate>, Self::Error> {
        predicate.predicate.bind(values).map_err(Into::into)
    }
}
