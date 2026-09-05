//! Statement-owned query preparation and one-run-at-a-time execution state.

use std::ffi::CStr;
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
use lagodb_core::query_contract::ScanId;
use lagodb_core::runtime_api::TableScanRuntimeValue;
use pgrx::pg_sys;
use tokio::runtime::{Builder, Runtime};

use super::{QueryExecutionError, QueryOutputDecoder};
use crate::datafusion::SerialExecutionLimits;
use crate::datafusion::metrics::ExecutionMetrics;
use crate::datafusion::physical_plan::CompiledPhysicalPlan;
use crate::datafusion::plan_compiler::DataFusionPlanCompiler;
use crate::datafusion::postgres_eval::{PgExprRuntime, without_pg_cleanup};
use crate::datafusion::scan_callbacks::{
    PreparedTableScanHandle, SerialTableScanCallbacks,
};
use crate::datafusion::table_scan::{
    ExternalTableProvider, ExternalTableScanLimits, ExternalTableStatistics,
};
use crate::plan::{
    PlannedTableScan, QueryFragment, QueryNode, QueryPlanData, QueryTupleLayout,
    ScanNode,
};

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

struct PreparedScan {
    scan: ScanId,
    statistics: ExternalTableStatistics,
    handle: Arc<PreparedTableScanHandle>,
}

/// Dense, statement-owned provider handles corresponding to the plan scan table.
struct PreparedTableScans {
    entries: Box<[PreparedScan]>,
}

impl PreparedTableScans {
    fn prepare(
        scans: &[PlannedTableScan<'_>],
        callbacks: &[SerialTableScanCallbacks],
        runtime_values: &[TableScanRuntimeValue],
    ) -> Result<Self, QueryExecutionError> {
        if scans.len() != callbacks.len() {
            return Err(QueryExecutionError::ScanCallbackCount {
                scans: scans.len(),
                callbacks: callbacks.len(),
            });
        }

        let mut entries = Vec::with_capacity(scans.len());
        for (scan, callbacks) in scans.iter().zip(callbacks) {
            let runtime_values = scan.runtime_bindings().select(runtime_values);
            // SAFETY: the selected plan ties every provider payload to the
            // live PostgreSQL plan-data input, and registry resolution supplies
            // the callbacks for this entry's provider identity. Selected-plan
            // validation bounds the scan-local runtime binding view.
            let handle = match unsafe {
                callbacks.prepare(scan.scan(), scan.provider_plan(), runtime_values)
            } {
                Ok(handle) => handle,
                Err(error) => {
                    let cleanup = Self {
                        entries: entries.into_boxed_slice(),
                    }
                    .close()
                    .err()
                    .map(Box::new);
                    return Err(QueryExecutionError::Initialization {
                        primary: Box::new(QueryExecutionError::ScanPrepare(error)),
                        cleanup,
                    });
                }
            };
            entries.push(PreparedScan {
                scan: scan.scan(),
                statistics: ExternalTableStatistics::from_estimate(scan.estimate()),
                handle: Arc::new(handle),
            });
        }
        Ok(Self {
            entries: entries.into_boxed_slice(),
        })
    }

    fn providers(
        &self,
        fragment: &QueryFragment,
        limits: SerialExecutionLimits,
        metrics: &Arc<ExecutionMetrics>,
    ) -> Result<Box<[Arc<ExternalTableProvider>]>, QueryExecutionError> {
        self.entries
            .iter()
            .map(|entry| {
                let schema = entry
                    .handle
                    .schema()
                    .map_err(QueryExecutionError::ScanPrepare)?;
                let projected_attnos: Box<[pg_sys::AttrNumber]> =
                    Self::scan_node(fragment.root(), entry.scan)
                        .map(|scan| {
                            scan.columns().iter().map(|column| column.attno).collect()
                        })
                        .ok_or(QueryExecutionError::MissingScanMetadata {
                            scan: entry.scan.index(),
                        })?;
                Ok(Arc::new(ExternalTableProvider::new(
                    entry.scan,
                    schema,
                    projected_attnos,
                    entry.statistics,
                    ExternalTableScanLimits {
                        maximum_batch_rows: limits.maximum_batch_rows() as u64,
                    },
                    Arc::clone(&entry.handle),
                    Arc::clone(metrics),
                )?))
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Vec::into_boxed_slice)
    }

    fn scan_node(node: &QueryNode, scan: ScanId) -> Option<&ScanNode> {
        match node {
            QueryNode::Scan(node) => (node.scan() == scan).then_some(node),
            QueryNode::Aggregate(node) => Self::scan_node(node.input(), scan),
            QueryNode::Distinct(node) => Self::scan_node(node.input(), scan),
            QueryNode::Filter(node) => Self::scan_node(node.input(), scan),
            QueryNode::Project(node) => Self::scan_node(node.input(), scan),
        }
    }

    fn finish_run(&self) -> Result<(), QueryExecutionError> {
        let mut first_error = None;
        // Streams are opened from the prepared-handle acquisition stack.
        // Finish every participant in reverse order even when one release
        // reports an error; otherwise a future multi-source run can leave a
        // later participant's stream live.
        for entry in self.entries.iter().rev() {
            let result = entry
                .handle
                .finish_serial_stream()
                .map_err(QueryExecutionError::ScanRelease);
            if first_error.is_none() {
                first_error = result.err();
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(())
    }

    fn close(self) -> Result<(), QueryExecutionError> {
        let mut first_error = None;
        // Prepared handles form an acquisition stack. Release in reverse so a
        // later provider can never observe an earlier dependency torn down
        // while it is still closing.
        for entry in self.entries.into_vec().into_iter().rev() {
            let result = Arc::try_unwrap(entry.handle)
                .map_err(|_| QueryExecutionError::PreparedScanStillShared {
                    scan: entry.scan.index(),
                })
                .and_then(|handle| {
                    handle.close().map_err(QueryExecutionError::ScanRelease)
                });
            if first_error.is_none() {
                first_error = result.err();
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

/// Resources whose values remain stable for one PostgreSQL statement.
///
/// Field order preserves the unwind fallback order: the physical plan and
/// session release their prepared-scan shares before the runtime stops and
/// before the provider-owned prepared handle is released.
struct PreparedQueryExecution {
    physical_plan: CompiledPhysicalPlan,
    session: SessionContext,
    runtime: Runtime,
    prepared_scans: PreparedTableScans,
    runtime_values: RuntimeValueState,
}

impl PreparedQueryExecution {
    fn prepare(
        fragment: &QueryFragment,
        layout: &QueryTupleLayout,
        limits: SerialExecutionLimits,
        prepared_scans: PreparedTableScans,
        runtime_values: RuntimeValueState,
        postgres: PgExprRuntime,
        metrics: &Arc<ExecutionMetrics>,
    ) -> Result<(Self, Arc<PeakRecordingPool>, QueryOutputDecoder), QueryExecutionError>
    {
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
            let providers = prepared_scans.providers(fragment, limits, metrics)?;
            let compiler = DataFusionPlanCompiler::new(&session, postgres);
            let physical_plan = runtime.block_on(compiler.compile(
                fragment,
                &providers,
                runtime_values.values(),
            ))?;
            let output =
                QueryOutputDecoder::try_new(layout, &physical_plan.schema())?;
            drop(providers);
            Ok((
                physical_plan,
                session,
                runtime,
                runtime_resources.memory,
                output,
            ))
        })();
        match result {
            Ok((physical_plan, session, runtime, memory, output)) => Ok((
                Self {
                    physical_plan,
                    session,
                    runtime,
                    prepared_scans,
                    runtime_values,
                },
                memory,
                output,
            )),
            Err(primary) => {
                let cleanup = prepared_scans.close().err().map(Box::new);
                Err(QueryExecutionError::Initialization {
                    primary: Box::new(primary),
                    cleanup,
                })
            }
        }
    }

    fn start_run(&self) -> Result<QueryRun, QueryExecutionError> {
        let stream = self.physical_plan.execute(self.session.task_ctx())?;
        Ok(QueryRun {
            stream,
            batch: None,
            next_row: 0,
            batch_rows: 0,
        })
    }

    fn finish_run(&self) -> Result<(), QueryExecutionError> {
        self.prepared_scans.finish_run()
    }

    fn close(self) -> Result<(), QueryExecutionError> {
        let Self {
            physical_plan,
            session,
            runtime,
            prepared_scans,
            runtime_values,
        } = self;
        drop(physical_plan);
        drop(session);
        drop(runtime);
        drop(runtime_values);
        prepared_scans.close()
    }

    fn physical_operators(&self) -> &CStr {
        self.physical_plan.description()
    }
}

/// The single resource owner consumed by explicit close or the unwind fallback.
pub(super) struct QueryExecutionResources {
    // Rust drops fields in declaration order. The run must release its stream
    // before the physical plan/session/runtime and prepared provider handles.
    state: QueryExecutionState,
    prepared: PreparedQueryExecution,
}

impl QueryExecutionResources {
    pub(super) fn prepare(
        query: QueryPlanData,
        scans: &[PlannedTableScan<'_>],
        callbacks: &[SerialTableScanCallbacks],
        limits: SerialExecutionLimits,
        metrics: &Arc<ExecutionMetrics>,
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
        unsafe { runtime_values.bind_initial(econtext) };
        let raw_values = runtime_values
            .values()
            .iter()
            .map(|value| TableScanRuntimeValue {
                datum: unsafe { value.datum() },
                is_null: value.is_null(),
            })
            .collect::<Vec<_>>();
        let prepared_scans =
            PreparedTableScans::prepare(scans, callbacks, &raw_values)?;
        let (prepared, memory, output) = PreparedQueryExecution::prepare(
            &fragment,
            &layout,
            limits,
            prepared_scans,
            runtime_values,
            postgres,
            metrics,
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

    pub(super) fn rescan(&mut self) -> Result<(), QueryExecutionError> {
        let prior = mem::replace(&mut self.state, QueryExecutionState::Ready);
        match prior {
            QueryExecutionState::Running(run) => {
                drop(run);
                self.prepared.finish_run()
            }
            QueryExecutionState::Ready | QueryExecutionState::Exhausted => Ok(()),
        }
    }

    pub(super) fn close(self) -> Result<(), QueryExecutionError> {
        let Self { state, prepared } = self;
        drop(state);
        prepared.close()
    }

    pub(super) fn abort(self) {
        let _ = without_pg_cleanup(|| self.close());
    }

    pub(super) fn physical_operators(&self) -> &CStr {
        self.prepared.physical_operators()
    }
}
