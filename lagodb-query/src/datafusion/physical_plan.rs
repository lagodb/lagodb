//! Statement-owned DataFusion physical plan for serial query execution.

mod metrics_accumulator;

use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion::common::DataFusionError;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::DynamicFilterTracking;
use datafusion::physical_plan::{ExecutionPlan, execute_stream};

pub(super) use metrics_accumulator::PhysicalPlanMetricsAccumulator;

/// Statement-scoped executable plan and current-instance metrics source.
pub(super) struct CompiledPhysicalPlan {
    plan: Arc<dyn ExecutionPlan>,
    contains_dynamic_filters: bool,
}

impl CompiledPhysicalPlan {
    pub(super) fn try_new(
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Self, DataFusionError> {
        let contains_dynamic_filters = Self::contains_dynamic_filters(plan.as_ref())?;
        Ok(Self {
            plan,
            contains_dynamic_filters,
        })
    }

    pub(super) fn execute(
        &self,
        task_context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        execute_stream(Arc::clone(&self.plan), task_context)
    }

    pub(super) fn schema(&self) -> SchemaRef {
        self.plan.schema()
    }

    pub(super) const fn has_dynamic_filters(&self) -> bool {
        self.contains_dynamic_filters
    }

    fn contains_dynamic_filters(
        plan: &dyn ExecutionPlan,
    ) -> Result<bool, DataFusionError> {
        let mut found = false;
        plan.apply_expressions(&mut |expression| {
            found |=
                DynamicFilterTracking::classify(expression).contains_dynamic_filter();
            Ok(TreeNodeRecursion::Continue)
        })?;
        if found {
            return Ok(true);
        }
        for child in plan.children() {
            if Self::contains_dynamic_filters(child.as_ref())? {
                return Ok(true);
            }
        }
        Ok(false)
    }
}
