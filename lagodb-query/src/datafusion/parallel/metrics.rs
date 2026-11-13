//! Merge worker task reports into statement metrics.
//!
//! Upstream collects one report per task and hands them to the leader once every
//! task is accounted for.

use std::sync::Arc;

use datafusion::common::Result;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_distributed::{
    DistributedExec, DistributedMetricsFormat, TaskMetrics,
    rewrite_distributed_plan_with_metrics,
};

use super::scan_metrics::ParallelScanMetrics;
use crate::datafusion::memory::TaskMemoryPool;
use crate::datafusion::metrics::ExecutionMetrics;

/// What upstream reports once every task finished.
pub(super) type TaskReports = Vec<TaskMetrics>;

pub(super) struct ParallelMetrics {
    plan: Arc<dyn ExecutionPlan>,
    execution: Option<Arc<ExecutionMetrics>>,
}

impl ParallelMetrics {
    pub(super) fn new(
        plan: &Arc<dyn ExecutionPlan>,
        execution: Option<&Arc<ExecutionMetrics>>,
    ) -> Option<Self> {
        // The planner inserts exactly one `DistributedExec` at the root, and it
        // is also the only node that holds the report store.
        plan.downcast_ref::<DistributedExec>()?;
        Some(Self {
            plan: Arc::clone(plan),
            execution: execution.map(Arc::clone),
        })
    }

    /// Waits for every task's report; `None` means the plan collects none.
    ///
    /// The caller must still race the wait against the run watch, because a
    /// failed worker never reports.
    pub(super) async fn wait(&self) -> Option<TaskReports> {
        self.plan
            .downcast_ref::<DistributedExec>()?
            .wait_for_metrics()
            .await
            // Every report is folded identically here, so only the values are
            // retained instead of coupling LagoDB to DSD's task map.
            .map(|reports| reports.into_values().collect())
    }

    pub(super) fn record(&self, reports: TaskReports) -> Result<()> {
        let Some(execution) = &self.execution else {
            return Ok(());
        };
        for report in &reports {
            TaskMemoryPool::record_worker_peaks(&report.task_metrics, execution);
            for node in &report.pre_order_plan_metrics {
                ParallelScanMetrics::merge_worker_report(node, execution)?;
            }
        }
        Ok(())
    }

    pub(super) async fn plan_with_metrics(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        rewrite_distributed_plan_with_metrics(
            plan,
            DistributedMetricsFormat::Aggregated,
        )
        .await
    }
}
