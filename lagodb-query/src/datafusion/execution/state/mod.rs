//! Statement-owned query preparation and one-run-at-a-time execution state.

mod prepared_execution;
mod prepared_plan;
mod table_scans;

use std::mem;
use std::sync::Arc;

use datafusion::execution::SendableRecordBatchStream;
use lagodb_arrow::BoundBatch;
use lagodb_core::customscan::custom_exprs::PgExpressionSections;
use lagodb_core::expr::RuntimeValueState;
use pgrx::pg_sys;

use super::QueryExecutionRequest;
use super::output::QueryOutputDecoder;
use crate::datafusion::error::QueryExecutionError;
use crate::datafusion::metrics::ExecutionMetrics;
use crate::datafusion::postgres_eval::PgExprRuntime;
use crate::plan::{PlanExplainNode, QueryFragment};
use prepared_execution::{PreparedQueryExecution, QueryPreparation};
use table_scans::BoundTableScans;

enum RunState {
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

/// The execution state consumed by explicit close or the unwind fallback.
pub(super) struct QueryExecutionState {
    // Rust drops fields in declaration order. The run must release its stream
    // before the physical plan/session/runtime and bound provider handles.
    run_state: RunState,
    prepared: PreparedQueryExecution,
}

impl QueryExecutionState {
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
                run_state: RunState::Ready,
                prepared,
            },
            output,
        ))
    }

    fn start_run_if_ready(&mut self) -> Result<(), QueryExecutionError> {
        if matches!(&self.run_state, RunState::Ready) {
            let run = self.prepared.start_run()?;
            self.run_state = RunState::Running(run);
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
        if matches!(&self.run_state, RunState::Exhausted) {
            return Ok(false);
        }
        self.start_run_if_ready()?;

        let RunState::Running(run) = &mut self.run_state else {
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
            let next = self.prepared.next_batch(&mut run.stream)?;
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
                        mem::replace(&mut self.run_state, RunState::Exhausted);
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
        let prior = mem::replace(&mut self.run_state, RunState::Ready);
        let (result, plan_was_executed) = match prior {
            RunState::Running(run) => {
                drop(run);
                (self.prepared.finish_run(), true)
            }
            RunState::Exhausted => (Ok(()), true),
            RunState::Ready => (Ok(()), false),
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
        let Self {
            run_state,
            prepared,
        } = self;
        drop(run_state);
        prepared.close()
    }

    pub(super) fn abort(self) {
        // PG transaction cleanup owns termination after ERROR. Drop the mesh
        // owner without the normal completion wait, then close serial bindings.
        let Self {
            run_state,
            prepared,
        } = self;
        drop(run_state);
        prepared.abort();
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
