//! DataFusion physical plan and execution-semantic metadata.

mod metrics_accumulator;

use std::sync::Arc;

use crate::plan::PlanExplainNode;
use arrow_schema::SchemaRef;
use datafusion::common::DataFusionError;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::DynamicFilterTracking;
use datafusion::physical_plan::execution_plan::reset_plan_states;
use datafusion::physical_plan::{ExecutionPlan, execute_stream};

pub(super) use metrics_accumulator::PhysicalPlanMetricsAccumulator;

/// Statement-scoped executable plan and current-instance metrics source.
#[derive(Clone)]
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

    pub(super) fn reset_for_rescan(&mut self) -> Result<(), DataFusionError> {
        // The execution owner recompiles plans with dynamic filters. All other
        // plans still need DataFusion's reset contract: joins retain build-side
        // futures, visited-row bitmaps, and probe completion state after a run.
        self.plan = reset_plan_states(Arc::clone(&self.plan))?;
        Ok(())
    }

    pub(super) fn plan(&self) -> &Arc<dyn ExecutionPlan> {
        &self.plan
    }

    pub(super) const fn has_dynamic_filters(&self) -> bool {
        self.contains_dynamic_filters
    }

    pub(super) fn explain_tree(&self) -> PlanExplainNode {
        PhysicalPlanMetricsAccumulator::plan_tree(self)
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
