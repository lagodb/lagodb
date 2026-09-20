//! Merge worker task reports into physical-plan and statement metrics.

use std::sync::Arc;

use super::scan_metrics::ParallelScanMetrics;
use crate::datafusion::metrics::ExecutionMetrics;
use datafusion::common::Result;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::execution::memory_pool::PeakRecordingPool;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_distributed::shm::MppMesh as ParallelMesh;
use datafusion_distributed::{
    DistributedExec, DistributedMetricsFormat, MetricsStore, NetworkBoundaryExt,
    TaskKey, decode_task_metrics, proto, rewrite_distributed_plan_with_metrics,
};
use tokio::sync::mpsc::UnboundedReceiver;

use crate::datafusion::memory::ParticipantMemoryRecorder;

const FRAGMENT_PEAK_MEMORY: &str = "lagodb_fragment_peak_memory_bytes";
const PARTICIPANT_PEAK_MEMORY: &str = "lagodb_participant_peak_memory_bytes";

/// Memory telemetry attached to one worker fragment's normal task report.
///
/// The participant recorder is shared by every fragment on the worker, while
/// the fragment recorder belongs to this task's independent bounded pool.
pub(super) struct WorkerMemoryMetrics {
    fragment: Arc<PeakRecordingPool>,
    participant: Arc<ParticipantMemoryRecorder>,
}

impl WorkerMemoryMetrics {
    pub(super) fn new(
        fragment: Arc<PeakRecordingPool>,
        participant: Arc<ParticipantMemoryRecorder>,
    ) -> Self {
        Self {
            fragment,
            participant,
        }
    }

    pub(super) fn attach_to(&self, report: &mut proto::TaskMetrics) {
        let task_metrics = report.task_metrics.get_or_insert_default();
        task_metrics.metrics.extend([
            proto::Metric {
                labels: Vec::new(),
                partition: None,
                value: Some(proto::metric::Value::PeakMemoryUsage(
                    proto::PeakMemoryUsage {
                        name: FRAGMENT_PEAK_MEMORY.to_owned(),
                        value: u64::try_from(self.fragment.peak_reserved())
                            .expect("DataFusion memory usage fits in u64"),
                    },
                )),
            },
            proto::Metric {
                labels: Vec::new(),
                partition: None,
                value: Some(proto::metric::Value::PeakMemoryUsage(
                    proto::PeakMemoryUsage {
                        name: PARTICIPANT_PEAK_MEMORY.to_owned(),
                        value: u64::try_from(self.participant.peak_reserved())
                            .expect("DataFusion memory usage fits in u64"),
                    },
                )),
            },
        ]);
    }

    fn record_report(report: &proto::TaskMetrics, execution: &ExecutionMetrics) {
        let Some(task_metrics) = &report.task_metrics else {
            return;
        };
        let mut fragment_peak = 0;
        let mut participant_peak = 0;
        for metric in &task_metrics.metrics {
            let Some(proto::metric::Value::PeakMemoryUsage(memory)) = &metric.value
            else {
                continue;
            };
            match memory.name.as_str() {
                FRAGMENT_PEAK_MEMORY => fragment_peak = memory.value,
                PARTICIPANT_PEAK_MEMORY => participant_peak = memory.value,
                _ => {}
            }
        }
        execution.record_worker_memory_peaks(fragment_peak, participant_peak);
    }
}

pub(super) struct ParallelMetrics {
    receiver: UnboundedReceiver<(u32, u32, proto::TaskMetrics)>,
    store: Arc<MetricsStore>,
    key: TaskKey,
    expected: usize,
    received: usize,
    execution: Option<Arc<ExecutionMetrics>>,
}

impl ParallelMetrics {
    pub(super) fn new(
        plan: &Arc<dyn ExecutionPlan>,
        mesh: &ParallelMesh,
        execution: Option<&Arc<ExecutionMetrics>>,
    ) -> Result<Option<Self>> {
        let mut store = None;
        let mut key = None;
        let mut expected = 0;
        plan.apply(|node| {
            if let Some(distributed) = node.downcast_ref::<DistributedExec>() {
                store = distributed.metrics_store();
            }
            if let Some(boundary) = node.as_network_boundary() {
                let stage = boundary.input_stage();
                key.get_or_insert(TaskKey {
                    query_id: stage.query_id(),
                    stage_id: 0,
                    task_number: 0,
                });
                expected += stage.task_count();
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(match (store, key) {
            (Some(store), Some(key)) => {
                mesh.take_task_metrics_receiver().map(|receiver| Self {
                    receiver,
                    store,
                    key,
                    expected,
                    received: 0,
                    execution: execution.map(Arc::clone),
                })
            }
            _ => None,
        })
    }

    pub(super) fn drain(&mut self) -> Result<()> {
        while let Ok((stage_id, task_number, metrics)) = self.receiver.try_recv() {
            if let Some(execution) = &self.execution {
                WorkerMemoryMetrics::record_report(&metrics, execution);
            }
            let metrics = decode_task_metrics(metrics)?;
            if let Some(execution) = &self.execution {
                for node in &metrics.pre_order_plan_metrics {
                    ParallelScanMetrics::merge_worker_report(node, execution)?;
                }
            }
            self.store.insert(
                TaskKey {
                    stage_id: stage_id as usize,
                    task_number: task_number as usize,
                    ..self.key
                },
                metrics,
            );
            // Each worker sends one report per prepared fragment after execution.
            self.received += 1;
        }
        Ok(())
    }

    pub(super) fn complete(&self) -> bool {
        self.received == self.expected
    }

    pub(super) fn record_report_counts(&self) {
        if let Some(execution) = &self.execution {
            execution.record_worker_metric_reports(self.received, self.expected);
        }
    }

    pub(super) async fn plan_with_metrics(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        rewrite_distributed_plan_with_metrics(
            plan,
            DistributedMetricsFormat::Aggregated,
        )
        .await
    }
}
