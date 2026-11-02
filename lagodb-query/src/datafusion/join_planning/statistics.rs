//! Statistics source that feeds PostgreSQL estimates to JoinSelection.

use std::sync::Arc;

use datafusion::common::Result;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{HashJoinExec, NestedLoopJoinExec};
use datafusion::physical_plan::operator_statistics::{
    ExtendedStatistics, StatisticsProvider, StatisticsResult,
};

use super::estimates::PlanEstimates;
use crate::datafusion::parallel::ParallelTableScanExec;
use crate::datafusion::table_scan::ExternalTableScanExec;

/// Answers for the operators that correspond to a relation-tree node, and
/// leaves pass-through operators to DataFusion's built-in providers.
#[derive(Debug)]
pub(super) struct PlanStatisticsProvider {
    estimates: Arc<PlanEstimates>,
}

impl PlanStatisticsProvider {
    pub(super) const fn new(estimates: Arc<PlanEstimates>) -> Self {
        Self { estimates }
    }

    fn is_relation_operator(plan: &dyn ExecutionPlan) -> bool {
        plan.is::<HashJoinExec>()
            || plan.is::<NestedLoopJoinExec>()
            || plan.is::<FilterExec>()
            || plan.is::<ExternalTableScanExec>()
            || plan.is::<ParallelTableScanExec>()
    }
}

impl StatisticsProvider for PlanStatisticsProvider {
    fn compute_statistics(
        &self,
        plan: &dyn ExecutionPlan,
        _child_stats: &[ExtendedStatistics],
    ) -> Result<StatisticsResult> {
        if !Self::is_relation_operator(plan) {
            return Ok(StatisticsResult::Delegate);
        }
        Ok(match self.estimates.relation(plan) {
            Some(relation) => StatisticsResult::Computed(ExtendedStatistics::new(
                relation.statistics(&plan.schema()),
            )),
            None => StatisticsResult::Delegate,
        })
    }
}
