//! Statement-owned COUNT preparation and one-run-at-a-time execution state.

use std::ffi::CStr;
use std::mem;
use std::sync::Arc;

use arrow_array::Array;
use arrow_schema::Schema;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::execution::memory_pool::PeakRecordingPool;
use futures::StreamExt;
use lagodb_core::batch::BatchRowDecoder;
use lagodb_core::query_contract::SourceId;
use lagodb_core::tuple::SlotColumns;
use pg_arrow_conv::ArrowColumnDecoder;
use pgrx::{PgMemoryContexts, pg_sys};
use tokio::runtime::{Builder, Runtime};

use super::QueryExecutionError;
use crate::datafusion::SerialExecutionLimits;
use crate::datafusion::audit::AuditedPhysicalPlan;
use crate::datafusion::metrics::ExecutionMetrics;
use crate::datafusion::plan_compiler::DataFusionPlanCompiler;
use crate::datafusion::source::{
    ExternalSourceLimits, ExternalSourceProvider, ExternalSourceStatistics,
};
use crate::datafusion::source_ffi::PreparedSourceHandle;
use crate::plan::QueryFragment;

enum CountExecutionState {
    Ready,
    Running(CountRun),
    Exhausted,
}

struct CountRun {
    stream: SendableRecordBatchStream,
}

/// Resources whose values remain stable for one PostgreSQL statement.
///
/// Field order preserves the unwind fallback order: the physical plan and
/// session release their prepared-source shares before the runtime stops and
/// before the provider-owned prepared handle is released.
struct PreparedCountExecution {
    physical_plan: AuditedPhysicalPlan,
    session: SessionContext,
    runtime: Runtime,
    prepared_source: Arc<PreparedSourceHandle>,
}

impl PreparedCountExecution {
    fn prepare(
        fragment: &QueryFragment,
        source: SourceId,
        statistics: ExternalSourceStatistics,
        limits: SerialExecutionLimits,
        prepared_source: &Arc<PreparedSourceHandle>,
        metrics: &Arc<ExecutionMetrics>,
    ) -> Result<(Self, Arc<PeakRecordingPool>), QueryExecutionError> {
        let runtime = Builder::new_current_thread()
            .build()
            .map_err(QueryExecutionError::Runtime)?;
        let runtime_resources = limits.runtime_env()?;
        let session_config = SessionConfig::new()
            .with_target_partitions(1)
            .with_batch_size(limits.maximum_batch_rows());
        let session = SessionContext::new_with_config_rt(
            session_config,
            runtime_resources.environment,
        );
        let provider = Arc::new(ExternalSourceProvider::new(
            source,
            Arc::new(Schema::empty()),
            statistics,
            ExternalSourceLimits {
                maximum_batch_rows: limits.maximum_batch_rows() as u64,
            },
            Arc::clone(prepared_source),
            Arc::clone(metrics),
        ));
        let compiler = DataFusionPlanCompiler::new(&session);
        let physical_plan = runtime.block_on(compiler.compile(fragment, provider))?;
        Ok((
            Self {
                physical_plan,
                session,
                runtime,
                prepared_source: Arc::clone(prepared_source),
            },
            runtime_resources.memory,
        ))
    }

    fn start_run(&self) -> Result<CountRun, QueryExecutionError> {
        let stream = self.physical_plan.execute(self.session.task_ctx())?;
        Ok(CountRun { stream })
    }

    fn finish_run(&self) -> Result<(), QueryExecutionError> {
        self.prepared_source
            .finish_serial_stream()
            .map_err(QueryExecutionError::SourceRelease)
    }

    fn close(self) -> Result<(), QueryExecutionError> {
        let Self {
            physical_plan,
            session,
            runtime,
            prepared_source,
        } = self;
        drop(physical_plan);
        drop(session);
        drop(runtime);
        let prepared_source = Arc::try_unwrap(prepared_source)
            .map_err(|_| QueryExecutionError::PreparedSourceStillShared)?;
        prepared_source
            .close()
            .map_err(QueryExecutionError::SourceRelease)
    }

    fn physical_operators(&self) -> &CStr {
        self.physical_plan.description()
    }
}

/// The single resource owner consumed by explicit close or the unwind fallback.
pub(super) struct CountExecutionResources {
    prepared: PreparedCountExecution,
    state: CountExecutionState,
}

impl CountExecutionResources {
    pub(super) fn prepare(
        fragment: &QueryFragment,
        source: SourceId,
        statistics: ExternalSourceStatistics,
        limits: SerialExecutionLimits,
        prepared_source: PreparedSourceHandle,
        metrics: &Arc<ExecutionMetrics>,
    ) -> Result<(Self, Arc<PeakRecordingPool>), QueryExecutionError> {
        let prepared_source = Arc::new(prepared_source);
        match PreparedCountExecution::prepare(
            fragment,
            source,
            statistics,
            limits,
            &prepared_source,
            metrics,
        ) {
            Ok((prepared, memory)) => {
                drop(prepared_source);
                Ok((
                    Self {
                        prepared,
                        state: CountExecutionState::Ready,
                    },
                    memory,
                ))
            }
            Err(primary) => {
                let cleanup = match Arc::try_unwrap(prepared_source) {
                    Ok(prepared_source) => prepared_source
                        .close()
                        .err()
                        .map(QueryExecutionError::SourceRelease)
                        .map(Box::new),
                    Err(_) => {
                        Some(Box::new(QueryExecutionError::PreparedSourceStillShared))
                    }
                };
                Err(QueryExecutionError::Initialization {
                    primary: Box::new(primary),
                    cleanup,
                })
            }
        }
    }

    fn start_run_if_ready(&mut self) -> Result<(), QueryExecutionError> {
        if matches!(&self.state, CountExecutionState::Ready) {
            let run = self.prepared.start_run()?;
            self.state = CountExecutionState::Running(run);
        }
        Ok(())
    }

    /// Drain the single scalar result and write it into the PostgreSQL slot.
    ///
    /// # Safety
    ///
    /// `slot` must be the live scan slot whose descriptor was validated against
    /// `expected_columns`, and `datum_context` must be its live datum context.
    pub(super) unsafe fn next_into_slot(
        &mut self,
        expected_columns: usize,
        output_decoder: &mut ArrowColumnDecoder,
        slot: *mut pg_sys::TupleTableSlot,
        datum_context: pg_sys::MemoryContext,
    ) -> Result<bool, QueryExecutionError> {
        if matches!(&self.state, CountExecutionState::Exhausted) {
            return Ok(false);
        }
        self.start_run_if_ready()?;

        let CountExecutionState::Running(run) = &mut self.state else {
            unreachable!("ready COUNT execution was started immediately above")
        };
        let mut output = None;
        while let Some(batch) = self.prepared.runtime.block_on(run.stream.next()) {
            let batch = batch?;
            if batch.num_columns() != expected_columns {
                return Err(QueryExecutionError::InvalidCountOutput {
                    columns: batch.num_columns(),
                    rows: batch.num_rows(),
                });
            }
            let rows = batch.num_rows();
            if rows == 0 {
                continue;
            }
            if rows != 1 || batch.column(0).is_null(0) || output.is_some() {
                return Err(QueryExecutionError::InvalidCountOutput {
                    columns: expected_columns,
                    rows,
                });
            }
            output = Some(
                output_decoder
                    .bind(batch)
                    .map_err(QueryExecutionError::OutputConversion)?,
            );
        }
        let output = output.ok_or(QueryExecutionError::MissingCountOutput)?;

        let finished = mem::replace(&mut self.state, CountExecutionState::Exhausted);
        drop(finished);
        self.prepared.finish_run()?;

        let mut columns = unsafe { SlotColumns::new(slot, datum_context) };
        unsafe {
            PgMemoryContexts::For(datum_context)
                .switch_to(|_| output_decoder.write_row(&output, 0, &mut columns))
        }
        .map_err(QueryExecutionError::OutputConversion)?;
        unsafe { pg_sys::ExecStoreVirtualTuple(slot) };
        Ok(true)
    }

    pub(super) fn rescan(&mut self) -> Result<(), QueryExecutionError> {
        let prior = mem::replace(&mut self.state, CountExecutionState::Ready);
        drop(prior);
        self.prepared.finish_run()
    }

    pub(super) fn close(self) -> Result<(), QueryExecutionError> {
        let Self { prepared, state } = self;
        drop(state);
        prepared.close()
    }

    pub(super) fn physical_operators(&self) -> &CStr {
        self.prepared.physical_operators()
    }
}
