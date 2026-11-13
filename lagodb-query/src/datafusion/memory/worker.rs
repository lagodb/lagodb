//! Worker task memory admission and peak reporting.

use std::fmt::{self, Display, Formatter};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use datafusion::common::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation, PeakRecordingPool,
};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::metrics::{
    ExecutionPlanMetricsSet, MetricBuilder, MetricValue, MetricsSet,
};
use datafusion_distributed::{
    TaskFinishedEvent, TaskFinishedEventResponse, TaskFinishedHandler,
    WorkerPlanRewriteEvent, WorkerPlanRewriteEventResponse, WorkerPlanRewriteHandler,
};

use crate::datafusion::metrics::ExecutionMetrics;

use super::QueryExecutionLimits;

/// Peak of one task's independently bounded pool.
const FRAGMENT_PEAK_MEMORY: &str = "lagodb_fragment_peak_memory_bytes";
/// Peak of everything this worker participant reserved.
const PARTICIPANT_PEAK_MEMORY: &str = "lagodb_participant_peak_memory_bytes";
/// Diagnostic identity of the pool whose real limit arrives with the plan.
const DEFERRED_POOL_NAME: &str = "lagodb_deferred_task_memory";

/// Worker-local high-water mark across independently bounded fragment pools.
///
/// One recorder belongs to a worker participant and is shared by every task it
/// runs, so the reported peak covers tasks that hold memory at the same time. It
/// never participates in admission.
#[derive(Debug, Default)]
pub(in crate::datafusion) struct ParticipantMemoryRecorder {
    reserved: AtomicUsize,
    peak: AtomicUsize,
}

impl ParticipantMemoryRecorder {
    fn peak_reserved(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }

    fn grow(&self, additional: usize) {
        let reserved =
            self.reserved.fetch_add(additional, Ordering::Relaxed) + additional;
        self.peak.fetch_max(reserved, Ordering::Relaxed);
    }

    fn shrink(&self, returned: usize) {
        self.reserved.fetch_sub(returned, Ordering::Relaxed);
    }
}

/// One worker task's memory pool, whose limit arrives with the decoded plan.
///
/// Upstream builds the task session before it decodes the plan, and its rewrite
/// hook then publishes the limit before any operator registers a consumer. Calls
/// into the unpublished pool are outside the supported worker lifecycle.
#[derive(Clone)]
pub(in crate::datafusion) struct TaskMemoryPool {
    deferred: Arc<DeferredLimitPool>,
    peak: Arc<PeakRecordingPool>,
    environment: Arc<RuntimeEnv>,
}

impl TaskMemoryPool {
    /// `participant` is the worker-wide peak recorder, present only while metrics
    /// are collected.
    pub(in crate::datafusion) fn new(
        limits: QueryExecutionLimits,
        participant: Option<Arc<ParticipantMemoryRecorder>>,
    ) -> Self {
        let deferred = Arc::new(DeferredLimitPool {
            limits,
            participant,
            pool: OnceLock::new(),
        });
        let peak = Arc::new(PeakRecordingPool::new(
            Arc::clone(&deferred) as Arc<dyn MemoryPool>
        ));
        // The disk manager is part of the session policy and cannot be deferred;
        // upstream publishes the pool before execution can spill.
        let environment = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(&peak) as Arc<dyn MemoryPool>)
            .with_disk_manager_builder(limits.disk_manager())
            .build()
            .expect("a runtime environment with an explicit pool and disk policy always builds");
        Self {
            deferred,
            peak,
            environment: Arc::new(environment),
        }
    }

    /// The environment every partition of this task runs in.
    pub(in crate::datafusion) fn environment(&self) -> Arc<RuntimeEnv> {
        Arc::clone(&self.environment)
    }

    /// Publishes the limit the decoded plan earns.
    fn publish(&self, plan: &Arc<dyn ExecutionPlan>) -> DataFusionResult<()> {
        let bytes = self.deferred.limits.pool_bytes(plan)?;
        let _ = self
            .deferred
            .pool
            .set(self.deferred.limits.memory_pool(bytes));
        Ok(())
    }

    /// The two peaks this task observed, as task-scoped metrics; the participant
    /// peak is the worker-wide high-water mark.
    fn metrics(&self) -> MetricsSet {
        let set = ExecutionPlanMetricsSet::new();
        if let Some(participant) = &self.deferred.participant {
            MetricBuilder::new(&set)
                .peak_memory_usage(FRAGMENT_PEAK_MEMORY, 0)
                .set(self.peak.peak_reserved());
            MetricBuilder::new(&set)
                .peak_memory_usage(PARTICIPANT_PEAK_MEMORY, 0)
                .set(participant.peak_reserved());
        }
        set.clone_inner()
    }

    /// Folds the peaks of one worker report into the statement counters.
    pub(in crate::datafusion) fn record_worker_peaks(
        report: &MetricsSet,
        execution: &ExecutionMetrics,
    ) {
        let (mut fragment, mut participant) = (0, 0);
        for metric in report.iter() {
            let MetricValue::PeakMemoryUsage { name, gauge } = metric.value() else {
                continue;
            };
            match name.as_ref() {
                FRAGMENT_PEAK_MEMORY => fragment = gauge.value(),
                PARTICIPANT_PEAK_MEMORY => participant = gauge.value(),
                _ => {}
            }
        }
        execution.record_worker_memory_peaks(fragment, participant);
    }
}

#[async_trait::async_trait]
impl WorkerPlanRewriteHandler for TaskMemoryPool {
    async fn handle(
        &self,
        ev: WorkerPlanRewriteEvent<'_>,
    ) -> DataFusionResult<WorkerPlanRewriteEventResponse> {
        self.publish(&ev.plan)?;
        Ok(WorkerPlanRewriteEventResponse::new(ev.plan))
    }
}

impl TaskFinishedHandler for TaskMemoryPool {
    fn handle(
        &self,
        _ev: TaskFinishedEvent<'_>,
    ) -> DataFusionResult<TaskFinishedEventResponse> {
        Ok(TaskFinishedEventResponse::new(self.metrics()))
    }
}

/// Pool that delegates to the task's real pool once its limit is known.
#[derive(Debug)]
struct DeferredLimitPool {
    limits: QueryExecutionLimits,
    /// The participant-wide recorder, absent while metrics are not collected.
    participant: Option<Arc<ParticipantMemoryRecorder>>,
    pool: OnceLock<Arc<dyn MemoryPool>>,
}

impl DeferredLimitPool {
    fn published(&self) -> DataFusionResult<&Arc<dyn MemoryPool>> {
        self.pool.get().ok_or_else(|| {
            DataFusionError::ResourcesExhausted(
                "this task reserved memory before its plan published the limit"
                    .to_owned(),
            )
        })
    }
}

impl Display for DeferredLimitPool {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(DEFERRED_POOL_NAME)
    }
}

impl MemoryPool for DeferredLimitPool {
    fn name(&self) -> &str {
        DEFERRED_POOL_NAME
    }

    fn register(&self, consumer: &MemoryConsumer) {
        if let Some(pool) = self.pool.get() {
            pool.register(consumer);
        }
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        if let Some(pool) = self.pool.get() {
            pool.unregister(consumer);
        }
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        if let Some(pool) = self.pool.get() {
            pool.grow(reservation, additional);
            if let Some(participant) = &self.participant {
                participant.grow(additional);
            }
        }
    }

    fn shrink(&self, reservation: &MemoryReservation, returned: usize) {
        if let Some(pool) = self.pool.get() {
            pool.shrink(reservation, returned);
            if let Some(participant) = &self.participant {
                participant.shrink(returned);
            }
        }
    }

    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> DataFusionResult<()> {
        let pool = self.published()?;
        pool.try_grow(reservation, additional)?;
        if let Some(participant) = &self.participant {
            participant.grow(additional);
        }
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.pool.get().map_or(0, |pool| pool.reserved())
    }

    fn memory_limit(&self) -> MemoryLimit {
        self.pool
            .get()
            .map_or(MemoryLimit::Finite(0), |pool| pool.memory_limit())
    }
}
