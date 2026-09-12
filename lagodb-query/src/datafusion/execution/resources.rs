//! Statement-owned query preparation and one-run-at-a-time execution state.

mod table_scans;

use std::mem;
use std::sync::Arc;

use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::execution::memory_pool::PeakRecordingPool;
use datafusion::execution::session_state::SessionStateBuilder;
use futures::StreamExt;
use lagodb_arrow::BoundBatch;
use lagodb_core::customscan::custom_exprs::PgExpressionSections;
use lagodb_core::expr::RuntimeValueState;
use pgrx::pg_sys;
use tokio::runtime::{Builder, Runtime};

use super::{QueryExecutionError, QueryOutputDecoder};
use crate::datafusion::SerialExecutionLimits;
use crate::datafusion::metrics::ExecutionMetrics;
use crate::datafusion::physical_plan::{
    CompiledPhysicalPlan, PhysicalPlanMetricsAccumulator,
};
use crate::datafusion::plan_compiler::DataFusionPlanCompiler;
use crate::datafusion::postgres_eval::{PgExprRuntime, without_pg_cleanup};
use crate::datafusion::scan_callbacks::SerialTableScanCallbacks;
use crate::datafusion::table_scan::ExternalTableProvider;
use crate::plan::{
    PlanExplainNode, PlannedTableScan, QueryFragment, QueryPlanData, QueryTupleLayout,
};
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
    limits: SerialExecutionLimits,
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
    physical_plan: CompiledPhysicalPlan,
    physical_metrics: Option<PhysicalPlanMetricsAccumulator>,
    providers: Box<[Arc<ExternalTableProvider>]>,
    fragment: QueryFragment,
    postgres: PgExprRuntime,
    session: SessionContext,
    runtime: Runtime,
    bound_scans: BoundTableScans,
    runtime_values: RuntimeValueState,
    parent: *mut pg_sys::PlanState,
    dynamic_rebind_required: bool,
    physical_plan_rebuild_required: bool,
    physical_plan_executed: bool,
}

impl PreparedQueryExecution {
    fn prepare(
        preparation: QueryPreparation<'_>,
        bound_scans: BoundTableScans,
        runtime_values: RuntimeValueState,
        postgres: PgExprRuntime,
        metrics: Option<&Arc<ExecutionMetrics>>,
        parent: *mut pg_sys::PlanState,
    ) -> Result<(Self, Arc<PeakRecordingPool>, QueryOutputDecoder), QueryExecutionError>
    {
        let QueryPreparation {
            fragment,
            layout,
            limits,
        } = preparation;
        let result: Result<_, QueryExecutionError> = (|| {
            let runtime = Builder::new_current_thread()
                .build()
                .map_err(QueryExecutionError::Runtime)?;
            let runtime_resources = limits.runtime_env()?;
            let session_config = SessionConfig::new()
                .with_target_partitions(1)
                .with_batch_size(limits.maximum_batch_rows());
            let state = SessionStateBuilder::new()
                .with_config(session_config)
                .with_runtime_env(runtime_resources.environment)
                .with_default_features()
                .build();
            // `push_down_filter` can move HAVING below Aggregate. Keep that
            // semantic boundary in LagoDB's validated IR and leave all other
            // DataFusion rewrites enabled.
            let has_having = fragment.has_having_filter();
            let state = if !has_having {
                state
            } else {
                let rules = state
                    .optimizers()
                    .iter()
                    .filter(|rule| rule.name() != "push_down_filter")
                    .cloned()
                    .collect();
                SessionStateBuilder::new_from_existing(state)
                    .with_optimizer_rules(rules)
                    .build()
            };
            let session = SessionContext::new_with_state(state);
            let providers = bound_scans.providers(fragment, limits, metrics)?;
            let compiler = DataFusionPlanCompiler::new(&session, postgres);
            let physical_plan = runtime.block_on(compiler.compile(
                fragment,
                &providers,
                runtime_values.values(),
            ))?;
            let output =
                QueryOutputDecoder::try_new(layout, &physical_plan.schema())?;
            Ok((
                physical_plan,
                providers,
                session,
                runtime,
                runtime_resources.memory,
                output,
            ))
        })();
        match result {
            Ok((physical_plan, providers, session, runtime, memory, output)) => Ok((
                Self {
                    physical_plan,
                    physical_metrics: metrics
                        .is_some()
                        .then(PhysicalPlanMetricsAccumulator::default),
                    providers,
                    fragment: fragment.clone(),
                    postgres,
                    session,
                    runtime,
                    bound_scans,
                    dynamic_rebind_required: runtime_values.has_dynamic_values(),
                    physical_plan_rebuild_required: false,
                    physical_plan_executed: false,
                    runtime_values,
                    parent,
                },
                memory,
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
        let compiler = DataFusionPlanCompiler::new(&self.session, self.postgres);
        let physical_plan = self.runtime.block_on(compiler.compile(
            &self.fragment,
            &self.providers,
            self.runtime_values.values(),
        ))?;
        if self.physical_plan_executed
            && let Some(physical_metrics) = &mut self.physical_metrics
        {
            physical_metrics.record(&self.physical_plan);
        }
        self.physical_plan = physical_plan;
        self.physical_plan_executed = false;
        self.dynamic_rebind_required = false;
        self.physical_plan_rebuild_required = false;
        Ok(())
    }

    fn start_run(&mut self) -> Result<QueryRun, QueryExecutionError> {
        self.rebuild_plan_if_required()?;
        let stream = self.physical_plan.execute(self.session.task_ctx())?;
        self.physical_plan_executed = true;
        Ok(QueryRun {
            stream,
            batch: None,
            next_row: 0,
            batch_rows: 0,
        })
    }

    fn finish_run(&self) -> Result<(), QueryExecutionError> {
        self.bound_scans.finish_run()
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
        if self.physical_plan.has_dynamic_filters() {
            self.physical_plan_rebuild_required = true;
        }
    }

    fn close(self) -> Result<(), QueryExecutionError> {
        let Self {
            physical_plan,
            physical_metrics,
            providers,
            fragment,
            postgres: _,
            session,
            runtime,
            bound_scans,
            runtime_values,
            parent: _,
            dynamic_rebind_required: _,
            physical_plan_rebuild_required: _,
            physical_plan_executed: _,
        } = self;
        drop(physical_plan);
        drop(physical_metrics);
        drop(providers);
        drop(fragment);
        drop(session);
        drop(runtime);
        drop(runtime_values);
        bound_scans.close()
    }

    fn physical_plan_analyze(&self, include_timing: bool) -> Option<PlanExplainNode> {
        self.physical_metrics
            .as_ref()
            .map(|metrics| metrics.analyze_tree(&self.physical_plan, include_timing))
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
        query: QueryPlanData,
        scans: &[PlannedTableScan<'_>],
        callbacks: &[SerialTableScanCallbacks],
        limits: SerialExecutionLimits,
        metrics: Option<&Arc<ExecutionMetrics>>,
        runtime_exprs: *mut pg_sys::List,
        parent: *mut pg_sys::PlanState,
    ) -> Result<(Self, Arc<PeakRecordingPool>, QueryOutputDecoder), QueryExecutionError>
    {
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
        let (prepared, memory, output) = PreparedQueryExecution::prepare(
            QueryPreparation {
                fragment: &fragment,
                layout: &layout,
                limits,
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
            memory,
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
            match self.prepared.runtime.block_on(run.stream.next()) {
                Some(batch) => {
                    let batch = batch?;
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
        let _ = without_pg_cleanup(|| self.close());
    }

    pub(super) fn physical_plan_analyze(
        &self,
        include_timing: bool,
    ) -> Option<PlanExplainNode> {
        self.prepared.physical_plan_analyze(include_timing)
    }

    pub(super) fn fragment(&self) -> &QueryFragment {
        self.prepared.fragment()
    }
}
