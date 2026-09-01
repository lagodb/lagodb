//! Closed operator-set audit for the S1M engine memory contract.

use std::ffi::{CStr, CString};
use std::sync::Arc;

use datafusion::common::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::{ExecutionPlan, execute_stream};

const AGGREGATE: &str = "AggregateExec";
const PROJECTION: &str = "ProjectionExec";
const EXTERNAL_SOURCE: &str = "ExternalSourceExec";

/// A physical-plan shape that falls outside the currently closed S1M audit.
#[derive(Debug, thiserror::Error)]
pub enum PhysicalPlanAuditError {
    #[error("operator {name} has no S1M memory audit")]
    UnsupportedOperator { name: String },
    #[error("operator named AggregateExec has an unexpected concrete type")]
    UnexpectedAggregateType,
    #[error("AggregateExec is not the audited single-stage scalar COUNT")]
    InvalidScalarCountAggregate,
    #[error(
        "expected one AggregateExec, one ExternalSourceExec, and at most one ProjectionExec; got {operators}"
    )]
    InvalidTopology { operators: String },
}

struct PhysicalPlanAudit {
    description: CString,
}

impl PhysicalPlanAudit {
    fn inspect(
        root: &Arc<dyn ExecutionPlan>,
    ) -> Result<Self, PhysicalPlanAuditError> {
        let mut operators = Vec::with_capacity(3);
        Self::visit(root, &mut operators)?;
        let aggregate_count = operators
            .iter()
            .filter(|operator| **operator == AGGREGATE)
            .count();
        let source_count = operators
            .iter()
            .filter(|operator| **operator == EXTERNAL_SOURCE)
            .count();
        let projection_count = operators
            .iter()
            .filter(|operator| **operator == PROJECTION)
            .count();
        if aggregate_count != 1
            || source_count != 1
            || projection_count > 1
            || operators.len() != aggregate_count + source_count + projection_count
        {
            return Err(PhysicalPlanAuditError::InvalidTopology {
                operators: operators.join(" -> "),
            });
        }
        let description = CString::new(operators.join(" -> "))
            .expect("DataFusion operator names contain no NUL bytes");
        Ok(Self { description })
    }

    fn visit(
        plan: &Arc<dyn ExecutionPlan>,
        operators: &mut Vec<&'static str>,
    ) -> Result<(), PhysicalPlanAuditError> {
        let name = plan.name();
        let audited_name = match name {
            AGGREGATE => AGGREGATE,
            PROJECTION => PROJECTION,
            EXTERNAL_SOURCE => EXTERNAL_SOURCE,
            _ => {
                return Err(PhysicalPlanAuditError::UnsupportedOperator {
                    name: name.to_owned(),
                });
            }
        };
        if audited_name == AGGREGATE {
            let aggregate = plan
                .downcast_ref::<AggregateExec>()
                .ok_or(PhysicalPlanAuditError::UnexpectedAggregateType)?;
            if aggregate.mode() != &AggregateMode::Single
                || !aggregate.group_expr().is_true_no_grouping()
                || aggregate.aggr_expr().len() != 1
                || aggregate.aggr_expr()[0].fun().name() != "count"
            {
                return Err(PhysicalPlanAuditError::InvalidScalarCountAggregate);
            }
        }
        operators.push(audited_name);
        for child in plan.children() {
            Self::visit(child, operators)?;
        }
        Ok(())
    }
}

/// A physical plan that cannot be constructed without completing its audit.
pub(super) struct AuditedPhysicalPlan {
    plan: Arc<dyn ExecutionPlan>,
    audit: PhysicalPlanAudit,
}

impl AuditedPhysicalPlan {
    pub(super) fn try_new(
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Self, PhysicalPlanAuditError> {
        let audit = PhysicalPlanAudit::inspect(&plan)?;
        Ok(Self { plan, audit })
    }

    pub(super) fn execute(
        &self,
        task_context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        execute_stream(Arc::clone(&self.plan), task_context)
    }

    pub(super) fn description(&self) -> &CStr {
        &self.audit.description
    }
}
