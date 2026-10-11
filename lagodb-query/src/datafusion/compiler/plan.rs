//! Relational-plan lowering into a DataFusion physical plan.

use std::sync::Arc;

use datafusion::dataframe::DataFrame;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::limit::GlobalLimitExec;
use lagodb_core::expr::RuntimeValue;

use crate::plan::{QueryFragment, QueryNode};

use super::DataFusionPlanError;
use super::logical::DataFusionLogicalPlanCompiler;
use super::runtime_value::LimitWindow;
use crate::datafusion::parallel::ParallelTableProvider;
use crate::datafusion::physical_plan::CompiledPhysicalPlan;
use crate::datafusion::postgres_eval::PgExprRuntime;
use crate::datafusion::scan_binding::ScanBindings;
use crate::datafusion::table_scan::ExternalTableProvider;

pub(in crate::datafusion) struct DataFusionPlanCompiler<'session> {
    session: &'session SessionContext,
    postgres: PgExprRuntime,
}

impl<'session> DataFusionPlanCompiler<'session> {
    pub(in crate::datafusion) const fn new(
        session: &'session SessionContext,
        postgres: PgExprRuntime,
    ) -> Self {
        Self { session, postgres }
    }

    pub(in crate::datafusion) async fn compile(
        &self,
        fragment: &QueryFragment,
        scans: &[Arc<ExternalTableProvider>],
        values: &[RuntimeValue],
    ) -> Result<CompiledPhysicalPlan, DataFusionPlanError> {
        self.compile_with_bindings(fragment, ScanBindings::new(scans), values)
            .await
    }

    pub(in crate::datafusion) async fn compile_parallel(
        &self,
        fragment: &QueryFragment,
        scans: &[Arc<ParallelTableProvider>],
        values: &[RuntimeValue],
    ) -> Result<CompiledPhysicalPlan, DataFusionPlanError> {
        self.compile_with_bindings(fragment, ScanBindings::parallel(scans), values)
            .await
    }

    async fn compile_with_bindings(
        &self,
        fragment: &QueryFragment,
        scans: ScanBindings,
        values: &[RuntimeValue],
    ) -> Result<CompiledPhysicalPlan, DataFusionPlanError> {
        let logical =
            DataFusionLogicalPlanCompiler::new(&scans, values, self.postgres);
        if let QueryNode::Limit(limit) = fragment.root() {
            let window = LimitWindow::bind(limit, values)?;
            if !window.supports_bounded_top_k() {
                let frame = DataFrame::new(
                    self.session.state(),
                    logical.compile(limit.input())?,
                );
                let input = frame.create_physical_plan().await?;
                let exact_limit = Arc::new(GlobalLimitExec::new(
                    input,
                    window.offset,
                    window.count,
                ));
                return Ok(CompiledPhysicalPlan::try_new(exact_limit)?);
            }
        }
        let frame =
            DataFrame::new(self.session.state(), logical.compile(fragment.root())?);
        let physical_plan = frame.create_physical_plan().await?;
        Ok(CompiledPhysicalPlan::try_new(physical_plan)?)
    }
}
