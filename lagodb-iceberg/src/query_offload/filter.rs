//! Query-offload predicate planning for both Iceberg relation adapters.

use lagodb_core::expr::RuntimeValueBindings;
use lagodb_core::expr::pushdown::{
    FilterBindResult, FilterPlan, FilterPlanningContext, FilterPushdown,
    FilterPushdownPlanner, PredicateFragment,
};
use lagodb_core::plan_data::{PlanDataReader, PlanDataWriter};
use pgrx::pg_sys;

use crate::error::IcebergError;
use crate::foreign_table::ForeignPlanningSource;
use crate::managed_table::ManagedTableReadView;
use crate::predicate::{
    BoundIcebergPredicate, IcebergFilterPlanner, PlannedIcebergPredicate,
};

use super::error::Error;

pub(super) struct Filter;

pub(super) struct FilterPlanner(IcebergFilterPlanner);

impl FilterPlanner {
    pub(super) fn managed(context: &FilterPlanningContext) -> Result<Self, Error> {
        let view = ManagedTableReadView::load(context.relation_oid())?;
        Ok(Self(IcebergFilterPlanner::new(
            context,
            view.schema(),
            view.metadata(),
            Some(view.pruning_units()?),
        )?))
    }

    pub(super) fn foreign(
        context: &FilterPlanningContext,
        source: &ForeignPlanningSource,
    ) -> Result<Self, Error> {
        let metadata = source.table()?.metadata();
        // Foreign scans price layout pruning alone (`ScanVolume::foreign`),
        // which needs no unit count.
        Ok(Self(IcebergFilterPlanner::new(
            context,
            metadata.current_schema(),
            metadata,
            None,
        )?))
    }
}

impl FilterPushdownPlanner for FilterPlanner {
    type PlannedPredicate = PlannedIcebergPredicate;
    type Error = Error;

    fn try_plan_filter(
        &mut self,
        fragment: &PredicateFragment,
    ) -> Result<FilterPlan<Self::PlannedPredicate>, Self::Error> {
        Ok(self.0.try_plan_filter(fragment)?)
    }

    fn pruning_units(&self) -> Option<f64> {
        self.0.pruning_units()
    }
}

impl FilterPushdown for Filter {
    type Planner = FilterPlanner;
    type PlannedPredicate = PlannedIcebergPredicate;
    type BoundPredicate = BoundIcebergPredicate;
    type Error = Error;

    fn begin_filter_planning(
        context: &FilterPlanningContext,
    ) -> Result<Self::Planner, Self::Error> {
        match unsafe { pg_sys::get_rel_relkind(context.relation_oid()) } as u8 {
            pg_sys::RELKIND_RELATION => FilterPlanner::managed(context),
            pg_sys::RELKIND_FOREIGN_TABLE => {
                let source = ForeignPlanningSource::new(
                    context.relation_oid(),
                    context.effective_user_id(),
                )?;
                FilterPlanner::foreign(context, &source)
            }
            _ => Err(IcebergError::InvariantViolated(
                "query filter planning received a non-table relation",
            )
            .into()),
        }
    }

    fn encode_planned(
        predicate: &Self::PlannedPredicate,
        writer: &mut PlanDataWriter,
    ) -> Result<(), Self::Error> {
        predicate.encode(writer);
        Ok(())
    }

    fn decode_planned(
        reader: &mut PlanDataReader<'_>,
        binding_count: usize,
    ) -> Result<Self::PlannedPredicate, Self::Error> {
        Ok(PlannedIcebergPredicate::decode(reader, binding_count)?)
    }

    fn bind_filter(
        predicate: &Self::PlannedPredicate,
        values: RuntimeValueBindings<'_>,
    ) -> Result<FilterBindResult<Self::BoundPredicate>, Self::Error> {
        Ok(predicate.bind(values)?)
    }
}
