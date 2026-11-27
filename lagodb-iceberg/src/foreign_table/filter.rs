//! Lazy FDW adapter over the shared Iceberg predicate implementation.

use std::rc::Rc;

use lagodb_core::expr::RuntimeValueBindings;
use lagodb_core::expr::pushdown::{
    FilterBindResult, FilterPlan, FilterPlanningContext, FilterPushdown,
    FilterPushdownPlanner, PredicateFragment,
};
use lagodb_core::fdw::ForeignFilterExplainValues;
use lagodb_core::plan_data::{PlanDataReader, PlanDataWriter};

use super::error::IcebergFdwError;
use super::planning_source::ForeignPlanningSource;
use super::provider::LagodbIceberg;
use super::source_identity::PlanSourceIdentity;
use crate::predicate::{
    IcebergBoundPredicate, IcebergFilterPlanner, IcebergPlannedPredicate,
    PredicateStorageContract,
};

pub(crate) struct FdwPlannedPredicate {
    source: PlanSourceIdentity,
    predicate: IcebergPlannedPredicate,
}

impl FdwPlannedPredicate {
    pub(crate) fn source(&self) -> &PlanSourceIdentity {
        &self.source
    }

    pub(crate) fn explain(&self, values: ForeignFilterExplainValues<'_>) -> String {
        self.predicate.explain_with(|id| values.value(id))
    }
}

enum IcebergFdwFilterPlannerState {
    Pending {
        context: FilterPlanningContext,
        source: Rc<ForeignPlanningSource>,
    },
    Ready {
        source: PlanSourceIdentity,
        planner: IcebergFilterPlanner,
    },
}

pub(crate) struct IcebergFdwFilterPlanner {
    state: IcebergFdwFilterPlannerState,
}

impl IcebergFdwFilterPlanner {
    pub(super) fn new(
        context: &FilterPlanningContext,
        source: Rc<ForeignPlanningSource>,
    ) -> Self {
        Self {
            state: IcebergFdwFilterPlannerState::Pending {
                context: *context,
                source,
            },
        }
    }

    fn ready(
        &mut self,
    ) -> Result<(&PlanSourceIdentity, &mut IcebergFilterPlanner), IcebergFdwError>
    {
        let pending = match &self.state {
            IcebergFdwFilterPlannerState::Pending { context, source } => {
                Some((*context, Rc::clone(source)))
            }
            IcebergFdwFilterPlannerState::Ready { .. } => None,
        };
        if let Some((context, planning_source)) = pending {
            let table = planning_source.table()?;
            let source = PlanSourceIdentity::from_metadata(table.metadata());
            let planner = IcebergFilterPlanner::new(
                &context,
                table.metadata().current_schema(),
                table.metadata(),
                PredicateStorageContract::Foreign,
            )?;
            self.state = IcebergFdwFilterPlannerState::Ready { source, planner };
        }
        match &mut self.state {
            IcebergFdwFilterPlannerState::Ready { source, planner } => {
                Ok((source, planner))
            }
            IcebergFdwFilterPlannerState::Pending { .. } => {
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
        fragment: &PredicateFragment,
    ) -> Result<FilterPlan<Self::PlannedPredicate>, Self::Error> {
        let (source, planner) = self.ready()?;
        Ok(planner
            .try_plan_filter(fragment)?
            .map(|predicate| FdwPlannedPredicate {
                source: source.clone(),
                predicate,
            }))
    }
}

impl FilterPushdown for LagodbIceberg {
    type Planner = IcebergFdwFilterPlanner;
    type PlannedPredicate = FdwPlannedPredicate;
    type BoundPredicate = IcebergBoundPredicate;
    type Error = IcebergFdwError;

    fn begin_filter_planning(
        context: &FilterPlanningContext,
    ) -> Result<Self::Planner, Self::Error> {
        let source = Rc::new(ForeignPlanningSource::new(
            context.relation_oid(),
            context.effective_user_id(),
        )?);
        Ok(IcebergFdwFilterPlanner::new(context, source))
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
            predicate: IcebergPlannedPredicate::decode(reader, binding_count)?,
        })
    }

    fn bind_filter(
        predicate: &Self::PlannedPredicate,
        values: RuntimeValueBindings<'_>,
    ) -> Result<FilterBindResult<Self::BoundPredicate>, Self::Error> {
        IcebergBoundPredicate::bind(&predicate.predicate, values).map_err(Into::into)
    }
}
