//! Statement-owned DataFusion physical plan for serial query execution.

use std::ffi::{CStr, CString};
use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion::common::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::{ExecutionPlan, execute_stream};

/// Compiled plan plus its stable, statement-scoped EXPLAIN description.
pub(super) struct CompiledPhysicalPlan {
    plan: Arc<dyn ExecutionPlan>,
    description: CString,
}

impl CompiledPhysicalPlan {
    pub(super) fn new(plan: Arc<dyn ExecutionPlan>) -> Self {
        let mut description = String::new();
        Self::write_operator_names(&plan, &mut description);
        let description = CString::new(description)
            .expect("DataFusion operator names contain no NUL bytes");
        Self { plan, description }
    }

    fn write_operator_names(plan: &Arc<dyn ExecutionPlan>, output: &mut String) {
        if !output.is_empty() {
            output.push_str(" -> ");
        }
        output.push_str(plan.name());
        for child in plan.children() {
            Self::write_operator_names(child, output);
        }
    }

    pub(super) fn execute(
        &self,
        task_context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        execute_stream(Arc::clone(&self.plan), task_context)
    }

    pub(super) fn description(&self) -> &CStr {
        &self.description
    }

    pub(super) fn schema(&self) -> SchemaRef {
        self.plan.schema()
    }
}
