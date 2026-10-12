//! Adapt shared Parquet predicates to the FDW planning contract.

use lagodb_core::expr::pushdown::{FilterBindResult, FilterPlan, PredicateFragment};
use lagodb_core::expr::{PushdownCosting, RuntimeValueBindings};
use lagodb_core::fdw::ForeignFilterExplainValues;
use lagodb_core::plan_data::PlanDataWriter;

use crate::error::ConnectorError;
use crate::fdw::format_adapters::{
    FormatBoundFilter, FormatFilterPlan, FormatFilterPlanner, FormatPlannedFilter,
};
use crate::format::FormatKind;
use crate::format::parquet::{ParquetFilterPlanner, ParquetPlannedPredicate};

impl FormatFilterPlanner for ParquetFilterPlanner {
    fn try_plan_filter(
        &mut self,
        fragment: &PredicateFragment,
    ) -> Result<FilterPlan<FormatPlannedFilter>, ConnectorError> {
        let Some(predicate) = self.plan_filter(fragment) else {
            return Ok(FilterPlan::Unsupported);
        };
        Ok(FilterPlan::exact(
            Box::new(predicate),
            PushdownCosting::CostedPruning,
        ))
    }
}

impl FormatFilterPlan for ParquetPlannedPredicate {
    fn kind(&self) -> FormatKind {
        FormatKind::Parquet
    }

    fn encode(&self, writer: &mut PlanDataWriter) -> Result<(), ConnectorError> {
        self.encode(writer);
        Ok(())
    }

    fn explain(&self, values: ForeignFilterExplainValues<'_>) -> String {
        self.explain(|id| values.value(id))
    }

    fn bind(
        &self,
        values: RuntimeValueBindings<'_>,
    ) -> Result<FilterBindResult<FormatBoundFilter>, ConnectorError> {
        Ok(FilterBindResult::Bound(FormatBoundFilter::Parquet(
            self.bind(values)?,
        )))
    }
}
