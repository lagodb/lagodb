//! Begin-owned read sources and expressions awaiting physical-plan preparation.

use std::sync::Arc;

use lagodb_core::customscan::custom_exprs::PgExpressionSections;
use lagodb_core::expr::RuntimeValueState;
use pgrx::pg_sys;

use super::prepared_execution::{PreparedQueryExecution, QueryPreparation};
use super::table_scans::BoundTableScans;
use super::{QueryExecutionState, RunState};
use crate::datafusion::error::QueryExecutionError;
use crate::datafusion::execution::{
    ExecutionMetricsMode, QueryExecution, QueryExecutionRequest,
};
use crate::datafusion::memory::QueryExecutionLimits;
use crate::datafusion::metrics::ExecutionMetrics;
use crate::datafusion::parallel::ParallelQueryOptions;
use crate::datafusion::postgres_eval::{PgExprRuntime, without_pg_cleanup};
use crate::plan::{QueryFragment, QueryTupleLayout};

/// Begin-owned bindings, consumed once to create the row-producing execution.
/// The host must close or abort an unstarted value when its portal is released.
#[must_use]
pub struct PendingQueryExecution {
    metrics: Option<Arc<ExecutionMetrics>>,
    fragment: QueryFragment,
    layout: QueryTupleLayout,
    limits: QueryExecutionLimits,
    parallel: Option<ParallelQueryOptions>,
    bound_scans: BoundTableScans,
    runtime_values: RuntimeValueState,
    postgres: PgExprRuntime,
    parent: *mut pg_sys::PlanState,
}

impl PendingQueryExecution {
    pub fn try_new(
        request: QueryExecutionRequest<'_>,
    ) -> Result<Self, QueryExecutionError> {
        let metrics = (request.metrics_mode == ExecutionMetricsMode::Enabled)
            .then(|| Arc::new(ExecutionMetrics::new(request.scans.len())));
        let QueryExecutionRequest {
            query,
            scans,
            callbacks,
            parallel,
            limits,
            runtime_exprs,
            parent,
            metrics_mode: _,
        } = request;
        let (fragment, layout, runtime_layout) = query.into_parts();
        let postgres = unsafe { PgExprRuntime::from_plan_state(parent) };
        let expression_sections = unsafe {
            PgExpressionSections::from_custom_exprs(
                runtime_exprs,
                runtime_layout.len(),
                0,
            )
        }
        .map_err(QueryExecutionError::ExpressionSections)?;
        let runtime_exprs = unsafe { expression_sections.runtime_binding_list() };
        let mut runtime_values = unsafe {
            RuntimeValueState::initialize(runtime_layout, runtime_exprs, parent)
        }
        .map_err(QueryExecutionError::RuntimeValues)?;
        let econtext = unsafe { (*parent).ps_ExprContext };
        unsafe { runtime_values.bind_initial_template(econtext) };
        // SAFETY: parent is the initialized host PlanState for this execution.
        let estate = unsafe { (*parent).state };
        let bound_scans = BoundTableScans::bind(scans, callbacks, estate)?;
        Ok(Self {
            metrics,
            fragment,
            layout,
            limits,
            parallel,
            bound_scans,
            runtime_values,
            postgres,
            parent,
        })
    }

    /// Prepare the physical plan once from the Begin-bound sources and current
    /// runtime parameters, before row output.
    pub fn start(self) -> Result<QueryExecution, QueryExecutionError> {
        let Self {
            metrics,
            fragment,
            layout,
            limits,
            parallel,
            bound_scans,
            runtime_values,
            postgres,
            parent,
        } = self;
        // prepare owns cleanup if physical-plan or output preparation fails.
        let (prepared, output) = PreparedQueryExecution::prepare(
            QueryPreparation {
                fragment: &fragment,
                layout: &layout,
                limits,
                parallel,
            },
            bound_scans,
            runtime_values,
            postgres,
            metrics.as_ref(),
            parent,
        )?;
        Ok(QueryExecution {
            metrics,
            state: QueryExecutionState {
                run_state: RunState::Ready,
                prepared,
                output,
            },
        })
    }

    pub fn fragment(&self) -> &QueryFragment {
        &self.fragment
    }

    pub fn close(self) -> Result<(), QueryExecutionError> {
        self.bound_scans.close()
    }

    pub fn abort(self) {
        let _ = without_pg_cleanup(|| self.close());
    }
}
