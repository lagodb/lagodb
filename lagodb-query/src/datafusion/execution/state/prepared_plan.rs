//! Statement-owned serial or distributed physical-plan preparation.

use std::sync::Arc;

use datafusion::common::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::execution::memory_pool::PeakRecordingPool;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::session_state::SessionStateBuilder;
use lagodb_core::expr::RuntimeValueState;
use tokio::runtime::Runtime;

use crate::datafusion::compiler::{DataFusionPlanCompiler, DataFusionPlanError};
use crate::datafusion::memory::QueryExecutionLimits;
use crate::datafusion::metrics::ExecutionMetrics;
use crate::datafusion::parallel::{
    ParallelInterruptGuard, ParallelQueryOptions, ParallelSession,
    ParallelSourceCatalog, PreparedParallelPlan,
};
use crate::datafusion::physical_plan::{
    CompiledPhysicalPlan, PhysicalPlanMetricsAccumulator,
};
use crate::datafusion::postgres_eval::PgExprRuntime;
use crate::datafusion::table_scan::ExternalTableProvider;
use crate::plan::QueryFragment;

use super::table_scans::BoundTableScans;
use crate::datafusion::error::QueryExecutionError;

impl From<DataFusionPlanError> for QueryExecutionError {
    fn from(error: DataFusionPlanError) -> Self {
        match error {
            DataFusionPlanError::DataFusion(error) => Self::DataFusion(error),
            DataFusionPlanError::MissingScan { index } => {
                Self::DataFusion(DataFusionError::Internal(format!(
                    "query fragment references missing table scan {index}"
                )))
            }
            error => Self::DataFusion(DataFusionError::Internal(error.to_string())),
        }
    }
}

pub(super) struct PhysicalPlanPreparation<'a> {
    pub(super) runtime: &'a Runtime,
    pub(super) environment: Arc<RuntimeEnv>,
    pub(super) fragment: &'a QueryFragment,
    pub(super) limits: QueryExecutionLimits,
    pub(super) metrics: Option<&'a Arc<ExecutionMetrics>>,
    pub(super) bound_scans: &'a BoundTableScans,
    pub(super) postgres: PgExprRuntime,
    pub(super) runtime_values: &'a RuntimeValueState,
    pub(super) parallel: Option<&'a ParallelQueryOptions>,
}

pub(super) struct PreparedSerialPlan {
    plan: CompiledPhysicalPlan,
    _providers: Box<[Arc<ExternalTableProvider>]>,
    session: SessionContext,
    memory: Arc<PeakRecordingPool>,
    executed: bool,
}

pub(super) enum PreparedPhysicalPlan {
    Serial(PreparedSerialPlan),
    Parallel(PreparedParallelPlan),
}

impl PreparedPhysicalPlan {
    pub(super) fn prepare(
        preparation: PhysicalPlanPreparation<'_>,
    ) -> Result<Self, QueryExecutionError> {
        if let Some(parallel) = preparation.prepare_parallel()? {
            return Ok(Self::Parallel(parallel));
        }
        preparation.prepare_serial().map(Self::Serial)
    }

    pub(super) fn prepare_serial(
        preparation: PhysicalPlanPreparation<'_>,
    ) -> Result<Self, QueryExecutionError> {
        preparation.prepare_serial().map(Self::Serial)
    }

    pub(super) fn plan(&self) -> &CompiledPhysicalPlan {
        match self {
            Self::Serial(serial) => &serial.plan,
            Self::Parallel(parallel) => parallel.plan(),
        }
    }

    pub(super) fn parallel(&self) -> Option<&PreparedParallelPlan> {
        match self {
            Self::Serial(_) => None,
            Self::Parallel(parallel) => Some(parallel),
        }
    }

    pub(super) fn parallel_mut(&mut self) -> Option<&mut PreparedParallelPlan> {
        match self {
            Self::Serial(_) => None,
            Self::Parallel(parallel) => Some(parallel),
        }
    }

    pub(super) fn peak_reserved(&self) -> usize {
        match self {
            Self::Serial(serial) => serial.memory.peak_reserved(),
            Self::Parallel(parallel) => parallel.peak_reserved(),
        }
    }

    pub(super) fn execute_serial(
        &mut self,
    ) -> Result<SendableRecordBatchStream, QueryExecutionError> {
        let Self::Serial(serial) = self else {
            unreachable!("serial execution requires a prepared serial plan")
        };
        serial.executed = true;
        serial
            .plan
            .execute(serial.session.task_ctx())
            .map_err(QueryExecutionError::from)
    }

    pub(super) fn record_serial_metrics(
        &self,
        metrics: &mut PhysicalPlanMetricsAccumulator,
    ) {
        if let Self::Serial(serial) = self
            && serial.executed
        {
            metrics.record(&serial.plan);
        }
    }

    pub(super) fn reset_serial_for_rescan(
        &mut self,
    ) -> Result<(), QueryExecutionError> {
        if let Self::Serial(serial) = self
            && serial.executed
        {
            serial.plan.reset_for_rescan()?;
            serial.executed = false;
        }
        Ok(())
    }
}

impl PhysicalPlanPreparation<'_> {
    fn prepare_serial(&self) -> Result<PreparedSerialPlan, QueryExecutionError> {
        let config = SessionConfig::new()
            .with_target_partitions(1)
            .with_batch_size(self.limits.maximum_batch_rows());
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(Arc::clone(&self.environment))
            .with_default_features()
            .build();
        // `push_down_filter` can move HAVING below Aggregate. Keep that
        // semantic boundary in LagoDB's validated IR and leave all other
        // DataFusion rewrites enabled.
        let state = if self.fragment.has_having_filter() {
            let rules = state
                .optimizers()
                .iter()
                .filter(|rule| rule.name() != "push_down_filter")
                .cloned()
                .collect();
            SessionStateBuilder::new_from_existing(state)
                .with_optimizer_rules(rules)
                .build()
        } else {
            state
        };
        let session = SessionContext::new_with_state(state);
        let providers =
            self.bound_scans
                .providers(self.fragment, self.limits, self.metrics)?;
        let compiler = DataFusionPlanCompiler::new(&session, self.postgres);
        let plan = self.runtime.block_on(compiler.compile(
            self.fragment,
            &providers,
            self.runtime_values.values(),
        ))?;
        let resources = self.limits.runtime_env_for_plan(plan.plan())?;
        let state = SessionStateBuilder::new_from_existing(session.state())
            .with_runtime_env(resources.environment)
            .build();
        Ok(PreparedSerialPlan {
            plan,
            _providers: providers,
            session: SessionContext::new_with_state(state),
            memory: resources.memory,
            executed: false,
        })
    }

    fn prepare_parallel(
        &self,
    ) -> Result<Option<PreparedParallelPlan>, QueryExecutionError> {
        let Some(options) = self.parallel else {
            return Ok(None);
        };
        let worker_cap = options.host.worker_cap();
        if worker_cap < 2 {
            return Ok(None);
        }
        let sources = Arc::new(ParallelSourceCatalog::new(options.callbacks.len()));
        let session = ParallelSession::build(
            Arc::clone(&self.environment),
            self.limits.maximum_batch_rows(),
            worker_cap as usize,
            self.metrics.is_some(),
            self.fragment.has_having_filter(),
        )?;
        let providers = self.bound_scans.parallel_providers(
            self.fragment,
            self.limits,
            &options.callbacks,
            Arc::clone(&sources),
        )?;
        let compiler = DataFusionPlanCompiler::new(&session, self.postgres);
        let plan = {
            let _held = ParallelInterruptGuard::new(options.host.as_ref());
            self.runtime.block_on(compiler.compile_parallel(
                self.fragment,
                &providers,
                self.runtime_values.values(),
            ))
        };
        options
            .host
            .process_interrupts()
            .map_err(QueryExecutionError::ParallelHost)?;
        PreparedParallelPlan::try_new(
            plan?,
            session,
            &sources,
            worker_cap,
            self.limits,
        )
    }
}
