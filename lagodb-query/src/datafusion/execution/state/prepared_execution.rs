//! Statement-stable prepared plan and resource ownership.

use std::sync::Arc;

use arrow_array::RecordBatch;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::runtime_env::RuntimeEnv;
use futures::StreamExt;
use lagodb_core::expr::RuntimeValueState;
use pgrx::pg_sys;
use tokio::runtime::{Builder, Runtime};

use super::super::output::QueryOutputDecoder;
use super::QueryRun;
use super::prepared_plan::{PhysicalPlanPreparation, PreparedPhysicalPlan};
use super::table_scans::BoundTableScans;
use crate::datafusion::error::QueryExecutionError;
use crate::datafusion::memory::QueryExecutionLimits;
use crate::datafusion::metrics::ExecutionMetrics;
use crate::datafusion::parallel::{ParallelQueryOptions, ParallelRun};
use crate::datafusion::physical_plan::PhysicalPlanMetricsAccumulator;
use crate::datafusion::postgres_eval::{PgExprRuntime, without_pg_cleanup};
use crate::plan::{PlanExplainNode, QueryFragment, QueryTupleLayout};

pub(super) struct QueryPreparation<'a> {
    pub(super) fragment: &'a QueryFragment,
    pub(super) layout: &'a QueryTupleLayout,
    pub(super) limits: QueryExecutionLimits,
    pub(super) parallel: Option<ParallelQueryOptions>,
}

/// Resources whose values remain stable for one PostgreSQL statement.
///
/// Field order preserves the unwind fallback order: the physical plan and
/// session release their bound-scan shares before the runtime stops and
/// before the provider-owned binding is released.
pub(super) struct PreparedQueryExecution {
    parallel_run: Option<ParallelRun>,
    prepared_plan: PreparedPhysicalPlan,
    physical_metrics: Option<PhysicalPlanMetricsAccumulator>,
    metrics: Option<Arc<ExecutionMetrics>>,
    fragment: QueryFragment,
    postgres: PgExprRuntime,
    runtime: Runtime,
    environment: Arc<RuntimeEnv>,
    bound_scans: BoundTableScans,
    runtime_values: RuntimeValueState,
    parent: *mut pg_sys::PlanState,
    dynamic_rebind_required: bool,
    physical_plan_rebuild_required: bool,
    completed_peak_memory_bytes: usize,
    limits: QueryExecutionLimits,
    parallel: Option<ParallelQueryOptions>,
}

impl PreparedQueryExecution {
    pub(super) fn prepare(
        preparation: QueryPreparation<'_>,
        bound_scans: BoundTableScans,
        runtime_values: RuntimeValueState,
        postgres: PgExprRuntime,
        metrics: Option<&Arc<ExecutionMetrics>>,
        parent: *mut pg_sys::PlanState,
    ) -> Result<(Self, QueryOutputDecoder), QueryExecutionError> {
        let QueryPreparation {
            fragment,
            layout,
            limits,
            parallel,
        } = preparation;
        let parallel = parallel.filter(|_| fragment.postgres_fallback_count() == 0);
        let result: Result<_, QueryExecutionError> = (|| {
            let runtime = Builder::new_current_thread()
                .enable_time()
                .build()
                .map_err(QueryExecutionError::Runtime)?;
            let runtime_resources = limits.planning_runtime_env()?;
            let environment = runtime_resources.environment;
            let prepared_plan =
                PreparedPhysicalPlan::prepare(PhysicalPlanPreparation {
                    runtime: &runtime,
                    environment: Arc::clone(&environment),
                    fragment,
                    limits,
                    metrics,
                    bound_scans: &bound_scans,
                    postgres,
                    runtime_values: &runtime_values,
                    parallel: parallel.as_ref(),
                })?;
            let output =
                QueryOutputDecoder::try_new(layout, &prepared_plan.plan().schema())?;
            Ok((prepared_plan, runtime, environment, output))
        })();
        match result {
            Ok((prepared_plan, runtime, environment, output)) => Ok((
                Self {
                    parallel_run: None,
                    prepared_plan,
                    physical_metrics: metrics
                        .is_some()
                        .then(PhysicalPlanMetricsAccumulator::default),
                    metrics: metrics.map(Arc::clone),
                    fragment: fragment.clone(),
                    postgres,
                    runtime,
                    environment,
                    bound_scans,
                    dynamic_rebind_required: runtime_values.has_dynamic_values(),
                    physical_plan_rebuild_required: false,
                    completed_peak_memory_bytes: 0,
                    runtime_values,
                    parent,
                    limits,
                    parallel,
                },
                output,
            )),
            Err(primary) => {
                let cleanup = bound_scans.close().err().map(Box::new);
                Err(QueryExecutionError::Initialization {
                    primary: Box::new(primary),
                    cleanup,
                })
            }
        }
    }

    fn rebuild_plan_if_required(&mut self) -> Result<(), QueryExecutionError> {
        if !self.dynamic_rebind_required && !self.physical_plan_rebuild_required {
            if let Some(physical_metrics) = &mut self.physical_metrics {
                self.prepared_plan.record_serial_metrics(physical_metrics);
            }
            self.prepared_plan.reset_serial_for_rescan()?;
            return Ok(());
        }
        if self.dynamic_rebind_required {
            let econtext = unsafe { (*self.parent).ps_ExprContext };
            unsafe { self.runtime_values.rebind_complete(econtext) };
        }
        if let Some(previous) = self.parallel_run.take() {
            if let Some(physical_metrics) = &mut self.physical_metrics {
                physical_metrics.record(previous.plan());
            }
            drop(previous);
        }
        if let Some(physical_metrics) = &mut self.physical_metrics {
            self.prepared_plan.record_serial_metrics(physical_metrics);
        }
        self.completed_peak_memory_bytes = self
            .completed_peak_memory_bytes
            .max(self.prepared_plan.peak_reserved());
        let prepared_plan =
            PreparedPhysicalPlan::prepare(self.physical_plan_preparation())?;
        self.prepared_plan = prepared_plan;
        self.dynamic_rebind_required = false;
        self.physical_plan_rebuild_required = false;
        Ok(())
    }

    pub(super) fn start_run(&mut self) -> Result<QueryRun, QueryExecutionError> {
        self.rebuild_plan_if_required()?;
        let parallel_run =
            match (self.prepared_plan.parallel_mut(), self.parallel.as_ref()) {
                (Some(prepared), Some(options)) => ParallelRun::launch(
                    prepared,
                    options,
                    self.limits,
                    self.metrics.as_ref(),
                )?,
                (None, _) => None,
                (Some(_), None) => {
                    unreachable!(
                        "a prepared parallel plan retains its launch options"
                    )
                }
            };
        let stream = match parallel_run {
            Some(run) => {
                let stream = run.execute(&self.runtime)?;
                if let Some(metrics) = &self.metrics {
                    metrics.record_parallel_run(run.worker_count());
                }
                self.parallel_run = Some(run);
                stream
            }
            None => {
                if self.prepared_plan.parallel().is_some() {
                    self.prepared_plan = PreparedPhysicalPlan::prepare_serial(
                        self.physical_plan_preparation(),
                    )?;
                }
                let stream = self.prepared_plan.execute_serial()?;
                if let Some(metrics) = &self.metrics {
                    metrics.record_serial_run();
                }
                stream
            }
        };
        Ok(QueryRun {
            stream,
            batch: None,
            next_row: 0,
            batch_rows: 0,
        })
    }

    pub(super) fn finish_run(&mut self) -> Result<(), QueryExecutionError> {
        let parallel = self
            .parallel_run
            .as_mut()
            .map_or(Ok(()), |run| run.finish(&self.runtime));
        let scans = self.bound_scans.finish_run();
        parallel.and(scans)
    }

    pub(super) fn next_batch(
        &self,
        stream: &mut SendableRecordBatchStream,
    ) -> Result<Option<RecordBatch>, QueryExecutionError> {
        match &self.parallel_run {
            Some(parallel) => parallel.next_batch(&self.runtime, stream),
            None => Ok(self.runtime.block_on(stream.next()).transpose()?),
        }
    }

    pub(super) unsafe fn mark_changed_runtime_values(
        &mut self,
        changed_parameters: *mut pg_sys::Bitmapset,
    ) {
        if self.runtime_values.has_dynamic_values()
            && unsafe { self.runtime_values.values_changed(changed_parameters) }
        {
            self.dynamic_rebind_required = true;
        }
    }

    pub(super) fn mark_dynamic_filter_plan_consumed(&mut self) {
        // A distributed plan owns execute-once network/operator state. Preserve
        // the rescan contract by rebuilding that plan after the run;
        // serial plans with dynamic filters also need recompilation. Other
        // serial plans reset operator state without recompiling the query.
        if self.prepared_plan.parallel().is_some()
            || self.prepared_plan.plan().has_dynamic_filters()
        {
            self.physical_plan_rebuild_required = true;
        }
    }

    fn physical_plan_preparation(&self) -> PhysicalPlanPreparation<'_> {
        PhysicalPlanPreparation {
            runtime: &self.runtime,
            environment: Arc::clone(&self.environment),
            fragment: &self.fragment,
            limits: self.limits,
            metrics: self.metrics.as_ref(),
            bound_scans: &self.bound_scans,
            postgres: self.postgres,
            runtime_values: &self.runtime_values,
            parallel: self.parallel.as_ref(),
        }
    }

    pub(super) fn close(mut self) -> Result<(), QueryExecutionError> {
        let finish = self
            .parallel_run
            .as_mut()
            .map_or(Ok(()), |run| run.finish(&self.runtime));
        let Self {
            parallel_run,
            prepared_plan,
            physical_metrics,
            metrics: _,
            fragment,
            postgres: _,
            runtime,
            environment,
            bound_scans,
            runtime_values,
            parent: _,
            dynamic_rebind_required: _,
            physical_plan_rebuild_required: _,
            completed_peak_memory_bytes: _,
            limits: _,
            parallel: _,
        } = self;
        drop(parallel_run);
        drop(prepared_plan);
        drop(physical_metrics);
        drop(fragment);
        drop(runtime);
        drop(environment);
        drop(runtime_values);
        let close = bound_scans.close();
        finish.and(close)
    }

    pub(super) fn abort(mut self) {
        self.parallel_run = None;
        let _ = without_pg_cleanup(|| self.close());
    }

    pub(super) fn physical_plan_analyze(
        &self,
        include_timing: bool,
    ) -> Option<PlanExplainNode> {
        self.physical_metrics.as_ref().map(|metrics| {
            metrics.analyze_tree(
                self.parallel_run
                    .as_ref()
                    .map_or(self.prepared_plan.plan(), ParallelRun::plan),
                include_timing,
            )
        })
    }

    pub(super) fn physical_plan_explain(&self) -> PlanExplainNode {
        self.parallel_run
            .as_ref()
            .map_or(self.prepared_plan.plan(), ParallelRun::plan)
            .explain_tree()
    }

    pub(super) fn peak_reserved(&self) -> usize {
        self.completed_peak_memory_bytes
            .max(self.prepared_plan.peak_reserved())
    }

    pub(super) fn planned_parallel(&self) -> bool {
        self.prepared_plan.parallel().is_some()
    }

    pub(super) fn fragment(&self) -> &QueryFragment {
        &self.fragment
    }
}
