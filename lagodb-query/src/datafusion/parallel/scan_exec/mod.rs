//! Provider-neutral parallel scan leaf and worker-local Arrow stream.
//!
//! The implemented distribution contract uses unknown partitioning and balanced
//! round-robin assignment of provider work groups to logical tasks.

mod stream;

use std::fmt;
use std::sync::Arc;

use arrow_array::RecordBatchReader;
use arrow_schema::SchemaRef;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan,
    Partitioning, PlanProperties, ReplaceChildrenOptions,
};
use datafusion_distributed::{
    DesiredTaskCountEvent, DesiredTaskCountEventResponse, DistributedLeafExec,
    ScaleUpLeafNodeEvent, ScaleUpLeafNodeEventResponse,
};
use lagodb_core::query_contract::ScanId;
use lagodb_core::runtime_api::SourceWorkId;

use super::host::ParallelWorkerHost;
use super::scan_metrics::ParallelScanMetrics;
use crate::datafusion::scan_callbacks::WorkerTableScanSource;
use stream::ParallelTableScanStream;

pub(in crate::datafusion) struct ParallelTableScanExec {
    scan: ScanId,
    schema: SchemaRef,
    assignments: Arc<[Arc<[SourceWorkId]>]>,
    work_count: u32,
    maximum_batch_rows: u64,
    source: Option<Arc<WorkerTableScanSource>>,
    host: Option<Arc<dyn ParallelWorkerHost>>,
    supported: bool,
    properties: Arc<PlanProperties>,
    scan_metrics: Option<ParallelScanMetrics>,
}

impl ParallelTableScanExec {
    fn properties(schema: SchemaRef, partitions: usize) -> Arc<PlanProperties> {
        Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ))
    }

    pub(super) fn leader(
        scan: ScanId,
        schema: SchemaRef,
        maximum_batch_rows: u64,
        work_count: u32,
    ) -> Self {
        Self {
            scan,
            properties: Self::properties(Arc::clone(&schema), work_count as usize),
            schema,
            assignments: Arc::from([]),
            work_count,
            maximum_batch_rows,
            source: None,
            host: None,
            supported: true,
            scan_metrics: None,
        }
    }

    pub(super) fn unsupported(
        scan: ScanId,
        schema: SchemaRef,
        maximum_batch_rows: u64,
    ) -> Self {
        Self {
            scan,
            properties: Self::properties(Arc::clone(&schema), 1),
            schema,
            assignments: Arc::from([]),
            work_count: 0,
            maximum_batch_rows,
            source: None,
            host: None,
            supported: false,
            scan_metrics: None,
        }
    }

    pub(super) fn worker(
        scan: ScanId,
        schema: SchemaRef,
        assignments: Arc<[Arc<[SourceWorkId]>]>,
        maximum_batch_rows: u64,
        source: Arc<WorkerTableScanSource>,
        host: Arc<dyn ParallelWorkerHost>,
        collect_metrics: bool,
    ) -> Result<Self> {
        if assignments.is_empty()
            || assignments.iter().any(|assignment| assignment.is_empty())
            || maximum_batch_rows == 0
        {
            return Err(DataFusionError::Plan(
                "parallel table scan has an empty assignment or zero batch size"
                    .to_owned(),
            ));
        }
        Ok(Self {
            scan,
            properties: Self::properties(Arc::clone(&schema), assignments.len()),
            schema,
            assignments,
            work_count: 0,
            maximum_batch_rows,
            source: Some(source),
            host: Some(host),
            supported: true,
            scan_metrics: collect_metrics.then(|| ParallelScanMetrics::new(scan)),
        })
    }

    pub(super) const fn scan(&self) -> ScanId {
        self.scan
    }

    pub(super) fn assignments(&self) -> &[Arc<[SourceWorkId]>] {
        &self.assignments
    }

    pub(super) const fn maximum_batch_rows(&self) -> u64 {
        self.maximum_batch_rows
    }

    fn task_plan(
        &self,
        task: usize,
        task_count: usize,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let work_ids: Arc<[SourceWorkId]> = (task..self.work_count as usize)
            .step_by(task_count)
            .map(|work| work as SourceWorkId)
            .collect::<Vec<_>>()
            .into();
        if work_ids.is_empty() {
            return Ok(Arc::new(EmptyExec::new(Arc::clone(&self.schema))));
        }
        Ok(Arc::new(Self {
            scan: self.scan,
            properties: Self::properties(Arc::clone(&self.schema), 1),
            schema: Arc::clone(&self.schema),
            assignments: Arc::from([work_ids]),
            work_count: 0,
            maximum_batch_rows: self.maximum_batch_rows,
            source: self.source.as_ref().map(Arc::clone),
            host: self.host.as_ref().map(Arc::clone),
            supported: self.supported,
            scan_metrics: self.scan_metrics.clone(),
        }))
    }
}

impl fmt::Debug for ParallelTableScanExec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ParallelTableScanExec")
            .field("scan", &self.scan)
            .field("assignments", &self.assignments.len())
            .field("work_count", &self.work_count)
            .field("maximum_batch_rows", &self.maximum_batch_rows)
            .field("supported", &self.supported)
            .finish_non_exhaustive()
    }
}

impl ParallelTableScanExec {
    pub(in crate::datafusion) fn desired_task_count(
        event: DesiredTaskCountEvent<'_>,
    ) -> Option<Result<DesiredTaskCountEventResponse>> {
        let scan = event.plan.downcast_ref::<Self>()?;
        Some(Ok(if scan.supported {
            DesiredTaskCountEventResponse::maximum(scan.work_count as usize)
        } else {
            DesiredTaskCountEventResponse::maximum(1)
        }))
    }

    pub(in crate::datafusion) fn scale_up(
        event: ScaleUpLeafNodeEvent<'_>,
    ) -> Option<Result<ScaleUpLeafNodeEventResponse>> {
        let scan = event.plan.downcast_ref::<Self>()?;
        let variants = (0..event.task_count)
            .map(|task| scan.task_plan(task, event.task_count))
            .collect::<Result<Vec<_>>>();
        Some(variants.and_then(|variants| {
            DistributedLeafExec::try_new(Arc::clone(event.plan), variants)
                .map(|leaf| ScaleUpLeafNodeEventResponse::new(Arc::new(leaf)))
        }))
    }
}

impl DisplayAs for ParallelTableScanExec {
    fn fmt_as(
        &self,
        display: DisplayFormatType,
        formatter: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        match display {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(
                formatter,
                "ParallelTableScanExec: scan={}, work_units={}",
                self.scan.index(),
                self.work_count as usize
                    + self
                        .assignments
                        .iter()
                        .map(|assignment| assignment.len())
                        .sum::<usize>(),
            ),
            DisplayFormatType::TreeRender => formatter.write_str("ParallelTableScan"),
        }
    }
}

impl ExecutionPlan for ParallelTableScanExec {
    fn name(&self) -> &'static str {
        "ParallelTableScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn apply_expressions(
        &self,
        _visitor: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Internal(
                "ParallelTableScanExec is a leaf and cannot accept children"
                    .to_owned(),
            ))
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let work_ids = self.assignments.get(partition).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "parallel source {} partition {partition} is outside {} assignments",
                self.scan.index(),
                self.assignments.len(),
            ))
        })?;
        let source = self.source.as_ref().ok_or_else(|| {
            DataFusionError::Internal(
                "leader attempted to execute an undispatched parallel source"
                    .to_owned(),
            )
        })?;
        let host = self.host.as_ref().ok_or_else(|| {
            DataFusionError::Internal(
                "worker parallel table scan has no cancellation host".to_owned(),
            )
        })?;
        let mut reader = source
            .open_stream(work_ids, self.maximum_batch_rows)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        if reader.schema() != self.schema {
            let primary = DataFusionError::Execution(
                "parallel table-scan Arrow C Stream schema differs from its planned schema"
                    .to_owned(),
            );
            return Err(match reader.close() {
                Ok(()) => primary,
                Err(cleanup) => primary.context(format!(
                    "parallel source stream release also failed: {cleanup}",
                )),
            });
        }
        Ok(Box::pin(ParallelTableScanStream::new(
            Arc::clone(&self.schema),
            reader,
            Arc::clone(host),
            self.scan_metrics.clone(),
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.scan_metrics
            .as_ref()
            .map(ParallelScanMetrics::snapshot)
    }
}
