//! Statement-owned query preparation and one-run-at-a-time execution state.

mod prepared_plan;
mod table_scans;

use std::mem;
use std::sync::Arc;

use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::runtime_env::RuntimeEnv;
use futures::StreamExt;
use lagodb_arrow::BoundBatch;
use lagodb_core::customscan::custom_exprs::PgExpressionSections;
use lagodb_core::expr::RuntimeValueState;
use pgrx::pg_sys;
use tokio::runtime::{Builder, Runtime};

use super::{QueryExecutionError, QueryExecutionRequest, QueryOutputDecoder};
use crate::datafusion::ParallelQueryOptions;
use crate::datafusion::QueryExecutionLimits;
use crate::datafusion::metrics::ExecutionMetrics;
use crate::datafusion::parallel::ParallelRun;
use crate::datafusion::physical_plan::PhysicalPlanMetricsAccumulator;
use crate::datafusion::postgres_eval::{PgExprRuntime, without_pg_cleanup};
use crate::plan::{PlanExplainNode, QueryFragment, QueryTupleLayout};
use prepared_plan::{PhysicalPlanPreparation, PreparedPhysicalPlan};
use table_scans::BoundTableScans;

enum QueryExecutionState {
    Ready,
    Running(QueryRun),
    Exhausted,
}

struct QueryRun {
    stream: SendableRecordBatchStream,
    batch: Option<BoundBatch>,
    next_row: usize,
    batch_rows: usize,
}

struct QueryPreparation<'a> {
    fragment: &'a QueryFragment,
    layout: &'a QueryTupleLayout,
    limits: QueryExecutionLimits,
    parallel: Option<ParallelQueryOptions>,
}

impl QueryRun {
    /// Drop the fully consumed output batch before asking the stream to
    /// materialize its successor. `BoundBatch` owns cloned Arrow array
    /// references, so retaining it across `poll_next` would keep two output
    /// batches alive at the boundary.
    fn release_consumed_batch(&mut self) {
        debug_assert!(self.next_row >= self.batch_rows);
        self.batch = None;
        self.next_row = 0;
        self.batch_rows = 0;
    }

    fn install_batch(&mut self, batch: BoundBatch, rows: usize) {
        debug_assert_ne!(rows, 0);
        debug_assert!(self.batch.is_none());
        self.batch = Some(batch);
        self.batch_rows = rows;
    }
}

/// Resources whose values remain stable for one PostgreSQL statement.
///
/// Field order preserves the unwind fallback order: the physical plan and
/// session release their bound-scan shares before the runtime stops and
/// before the provider-owned binding is released.
struct PreparedQueryExecution {
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
    fn prepare(
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

    fn start_run(&mut self) -> Result<QueryRun, QueryExecutionError> {
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

    fn finish_run(&mut self) -> Result<(), QueryExecutionError> {
        let parallel = self
            .parallel_run
            .as_mut()
            .map_or(Ok(()), |run| run.finish(&self.runtime));
        let scans = self.bound_scans.finish_run();
        parallel.and(scans)
    }

    unsafe fn mark_changed_runtime_values(
        &mut self,
        changed_parameters: *mut pg_sys::Bitmapset,
    ) {
        if self.runtime_values.has_dynamic_values()
            && unsafe { self.runtime_values.values_changed(changed_parameters) }
        {
            self.dynamic_rebind_required = true;
        }
    }

    fn mark_dynamic_filter_plan_consumed(&mut self) {
        // A distributed plan owns execute-once network/operator state. Preserve
        // the rescan contract by rebuilding that plan after the run;
        // serial plans rebuild only when dynamic filters require fresh state.
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

    fn close(mut self) -> Result<(), QueryExecutionError> {
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

    fn physical_plan_analyze(&self, include_timing: bool) -> Option<PlanExplainNode> {
        self.physical_metrics.as_ref().map(|metrics| {
            metrics.analyze_tree(
                self.parallel_run
                    .as_ref()
                    .map_or(self.prepared_plan.plan(), ParallelRun::plan),
                include_timing,
            )
        })
    }

    fn physical_plan_explain(&self) -> PlanExplainNode {
        self.parallel_run
            .as_ref()
            .map_or(self.prepared_plan.plan(), ParallelRun::plan)
            .explain_tree()
    }

    fn peak_reserved(&self) -> usize {
        self.completed_peak_memory_bytes
            .max(self.prepared_plan.peak_reserved())
    }

    fn planned_parallel(&self) -> bool {
        self.prepared_plan.parallel().is_some()
    }

    fn fragment(&self) -> &QueryFragment {
        &self.fragment
    }
}

/// The single resource owner consumed by explicit close or the unwind fallback.
pub(super) struct QueryExecutionResources {
    // Rust drops fields in declaration order. The run must release its stream
    // before the physical plan/session/runtime and bound provider handles.
    state: QueryExecutionState,
    prepared: PreparedQueryExecution,
}

impl QueryExecutionResources {
    pub(super) fn prepare(
        request: QueryExecutionRequest<'_>,
        metrics: Option<&Arc<ExecutionMetrics>>,
    ) -> Result<(Self, QueryOutputDecoder), QueryExecutionError> {
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
        let bound_scans = BoundTableScans::bind(scans, callbacks)?;
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
            metrics,
            parent,
        )?;
        Ok((
            Self {
                state: QueryExecutionState::Ready,
                prepared,
            },
            output,
        ))
    }

    fn start_run_if_ready(&mut self) -> Result<(), QueryExecutionError> {
        if matches!(&self.state, QueryExecutionState::Ready) {
            let run = self.prepared.start_run()?;
            self.state = QueryExecutionState::Running(run);
        }
        Ok(())
    }

    /// Consume one already-bound Arrow row. Batch validation and downcasting
    /// happen only when a new batch is installed.
    ///
    /// # Safety
    ///
    /// `slot` must be the live scan slot built from the query target list, and
    /// `datum_context` must be its live datum context.
    pub(super) unsafe fn next_into_slot(
        &mut self,
        output_decoder: &QueryOutputDecoder,
        slot: *mut pg_sys::TupleTableSlot,
        datum_context: pg_sys::MemoryContext,
    ) -> Result<bool, QueryExecutionError> {
        if matches!(&self.state, QueryExecutionState::Exhausted) {
            return Ok(false);
        }
        self.start_run_if_ready()?;

        let QueryExecutionState::Running(run) = &mut self.state else {
            unreachable!("ready query execution was started immediately above")
        };
        loop {
            if run.next_row < run.batch_rows {
                let row = run.next_row;
                run.next_row += 1;
                unsafe {
                    output_decoder.write_row(
                        run.batch.as_ref().expect("active row has a bound batch"),
                        row,
                        slot,
                        datum_context,
                    )
                }
                .map_err(QueryExecutionError::OutputConversion)?;
                unsafe { pg_sys::ExecStoreVirtualTuple(slot) };
                return Ok(true);
            }
            run.release_consumed_batch();
            let next = match &self.prepared.parallel_run {
                Some(parallel) => {
                    parallel.next_batch(&self.prepared.runtime, &mut run.stream)?
                }
                None => self
                    .prepared
                    .runtime
                    .block_on(run.stream.next())
                    .transpose()?,
            };
            match next {
                Some(batch) => {
                    if batch.num_columns() != output_decoder.width()
                        || !output_decoder.accepts_nulls(&batch)
                    {
                        return Err(QueryExecutionError::InvalidQueryOutput {
                            columns: batch.num_columns(),
                            rows: batch.num_rows(),
                        });
                    }
                    let rows = batch.num_rows();
                    if rows == 0 {
                        continue;
                    }
                    let batch = output_decoder
                        .bind(batch)
                        .map_err(QueryExecutionError::OutputConversion)?;
                    run.install_batch(batch, rows);
                }
                None => {
                    let finished =
                        mem::replace(&mut self.state, QueryExecutionState::Exhausted);
                    drop(finished);
                    self.prepared.finish_run()?;
                    return Ok(false);
                }
            }
        }
    }

    pub(super) unsafe fn rescan(
        &mut self,
        changed_parameters: *mut pg_sys::Bitmapset,
    ) -> Result<(), QueryExecutionError> {
        let prior = mem::replace(&mut self.state, QueryExecutionState::Ready);
        let (result, plan_was_executed) = match prior {
            QueryExecutionState::Running(run) => {
                drop(run);
                (self.prepared.finish_run(), true)
            }
            QueryExecutionState::Exhausted => (Ok(()), true),
            QueryExecutionState::Ready => (Ok(()), false),
        };
        unsafe {
            self.prepared
                .mark_changed_runtime_values(changed_parameters)
        };
        if plan_was_executed {
            self.prepared.mark_dynamic_filter_plan_consumed();
        }
        result
    }

    pub(super) fn close(self) -> Result<(), QueryExecutionError> {
        let Self { state, prepared } = self;
        drop(state);
        prepared.close()
    }

    pub(super) fn abort(self) {
        // PG transaction cleanup owns termination after ERROR. Drop the mesh
        // owner without the normal completion wait, then close serial bindings.
        let Self {
            state,
            mut prepared,
        } = self;
        drop(state);
        prepared.parallel_run = None;
        let _ = without_pg_cleanup(|| prepared.close());
    }

    pub(super) fn physical_plan_analyze(
        &self,
        include_timing: bool,
    ) -> Option<PlanExplainNode> {
        self.prepared.physical_plan_analyze(include_timing)
    }

    pub(super) fn physical_plan_explain(&self) -> PlanExplainNode {
        self.prepared.physical_plan_explain()
    }

    pub(super) fn peak_reserved(&self) -> usize {
        self.prepared.peak_reserved()
    }

    pub(super) fn planned_parallel(&self) -> bool {
        self.prepared.planned_parallel()
    }

    pub(super) fn fragment(&self) -> &QueryFragment {
        self.prepared.fragment()
    }
}
