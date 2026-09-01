//! Compilation of validated query semantics into audited DataFusion plans.

use std::sync::Arc;

use datafusion::common::DataFusionError;
use datafusion::execution::context::SessionContext;
use datafusion::functions_aggregate::count::count_all;

use crate::plan::{AggregateExpression, QueryFragment};

use super::audit::{AuditedPhysicalPlan, PhysicalPlanAuditError};
use super::source::ExternalSourceProvider;

#[derive(Debug, thiserror::Error)]
pub(super) enum DataFusionPlanError {
    #[error("DataFusion plan compilation failed: {0}")]
    DataFusion(#[from] DataFusionError),
    #[error("physical plan audit failed: {0}")]
    Audit(#[from] PhysicalPlanAuditError),
}

/// Statement-local compiler from LagoDB query semantics to a DataFusion plan.
///
/// The compiler borrows the statement session and owns no runtime, source,
/// stream, or output state. S1M compiles only the validated scalar COUNT shape.
pub(super) struct DataFusionPlanCompiler<'session> {
    session: &'session SessionContext,
}

impl<'session> DataFusionPlanCompiler<'session> {
    pub(super) const fn new(session: &'session SessionContext) -> Self {
        Self { session }
    }

    pub(super) async fn compile(
        &self,
        fragment: &QueryFragment,
        source: Arc<ExternalSourceProvider>,
    ) -> Result<AuditedPhysicalPlan, DataFusionPlanError> {
        // The validated CountStar type selects the only S1M aggregate lowering
        // rule. Project is an identity mapping for the single aggregate output.
        let (_source, aggregate, _project) = fragment.scalar_count_parts();
        let [expression] = aggregate.aggregates() else {
            unreachable!("validated scalar COUNT contains one aggregate expression")
        };
        let aggregate_expression = match expression {
            AggregateExpression::CountStar(_) => count_all(),
        };
        let data_frame = self
            .session
            .read_table(source)?
            .aggregate(Vec::new(), vec![aggregate_expression])?;
        let physical_plan = data_frame.create_physical_plan().await?;
        AuditedPhysicalPlan::try_new(physical_plan).map_err(Into::into)
    }
}
