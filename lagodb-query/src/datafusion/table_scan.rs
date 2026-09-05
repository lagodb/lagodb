//! Provider-neutral DataFusion table scan backed by Arrow C Stream.

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DataFusionError, Result, Statistics};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan,
    Partitioning, PlanProperties, RecordBatchStream, ReplaceChildrenOptions,
};
use futures::Stream;
use lagodb_core::diag::PgReportError;
use lagodb_core::query_contract::{ScanEstimate, ScanId};
use pgrx::pg_sys;

use super::metrics::ExecutionMetrics;
use super::scan_callbacks::{PreparedTableScanHandle, ProviderStreamReader};

#[derive(Debug, Clone, Copy)]
pub(super) struct ExternalTableStatistics {
    pub estimated_rows: usize,
}

impl ExternalTableStatistics {
    pub(super) fn from_estimate(estimate: ScanEstimate) -> Self {
        Self {
            estimated_rows: estimate.estimated_rows().min(usize::MAX as f64) as usize,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ExternalTableScanLimits {
    pub maximum_batch_rows: u64,
}

/// DataFusion catalog leaf owned by the engine DSO.
#[derive(Debug, Clone)]
pub(super) struct ExternalTableProvider {
    scan: ScanId,
    schema: SchemaRef,
    positions_by_attno: Box<[Option<usize>]>,
    statistics: ExternalTableStatistics,
    limits: ExternalTableScanLimits,
    prepared: Arc<PreparedTableScanHandle>,
    metrics: Arc<ExecutionMetrics>,
}

impl ExternalTableProvider {
    pub(super) fn new(
        scan: ScanId,
        schema: SchemaRef,
        projected_attnos: Box<[pg_sys::AttrNumber]>,
        statistics: ExternalTableStatistics,
        limits: ExternalTableScanLimits,
        prepared: Arc<PreparedTableScanHandle>,
        metrics: Arc<ExecutionMetrics>,
    ) -> Result<Self> {
        if schema.fields().len() != projected_attnos.len() {
            return Err(DataFusionError::Internal(format!(
                "table scan {} returned {} Arrow fields for {} projected attributes",
                scan.index(),
                schema.fields().len(),
                projected_attnos.len(),
            )));
        }
        let mut positions_by_attno = Vec::new();
        for (position, attno) in projected_attnos.into_vec().into_iter().enumerate() {
            debug_assert!(attno > 0, "query-plan validation rejects invalid attnos");
            let index = (attno - 1) as usize;
            if positions_by_attno.len() <= index {
                positions_by_attno.resize(index + 1, None);
            }
            positions_by_attno[index] = Some(position);
        }
        Ok(Self {
            scan,
            schema,
            positions_by_attno: positions_by_attno.into_boxed_slice(),
            statistics,
            limits,
            prepared,
            metrics,
        })
    }

    pub(super) fn column_name(&self, attno: pg_sys::AttrNumber) -> Option<&str> {
        usize::try_from(attno)
            .ok()
            .and_then(|attno| attno.checked_sub(1))
            .and_then(|index| self.positions_by_attno.get(index))
            .and_then(|position| *position)
            .map(|position| self.schema.field(position).name().as_str())
    }
}

#[async_trait]
impl TableProvider for ExternalTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn statistics(&self) -> Option<Statistics> {
        let statistics = Statistics::default()
            .with_num_rows(Precision::Inexact(self.statistics.estimated_rows));
        // DataFusion defines total_byte_size as Arrow output size, not the
        // provider's physical scan bytes. Only a genuinely empty output schema
        // has a proven exact size here; projected column sizes remain unknown.
        Some(if self.schema.fields().is_empty() {
            statistics.with_total_byte_size(Precision::Exact(0))
        } else {
            statistics
        })
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if projection.is_some_and(|projection| {
            projection
                .iter()
                .any(|position| *position >= self.schema.fields().len())
        }) {
            return Err(DataFusionError::Plan(
                "table scan projection is out of bounds".to_owned(),
            ));
        }
        if !filters.is_empty() {
            // PostgreSQL scan-local predicates have already gone through the
            // typed provider contract. A filter arriving here is optimizer-
            // derived and cannot bypass that negotiation.
            // TODO(query filter pushdown): route it through the ScanId-scoped
            // static-filter contract before accepting it here.
            return Err(DataFusionError::Plan(
                "external table scan does not accept engine filters".to_owned(),
            ));
        }
        let projection = projection
            .cloned()
            .unwrap_or_else(|| (0..self.schema.fields().len()).collect());
        let output_schema =
            Arc::new(self.schema.project(&projection).map_err(|error| {
                DataFusionError::ArrowError(Box::new(error), None)
            })?);
        let identity_projection = projection.len() == self.schema.fields().len()
            && projection
                .iter()
                .enumerate()
                .all(|(position, projected)| position == *projected);
        Ok(Arc::new(ExternalTableScanExec::new(
            self,
            output_schema,
            projection.into_boxed_slice(),
            identity_projection,
        )))
    }
}

#[derive(Debug)]
pub(super) struct ExternalTableScanExec {
    scan: ScanId,
    source_schema: SchemaRef,
    schema: SchemaRef,
    projection: Box<[usize]>,
    identity_projection: bool,
    limits: ExternalTableScanLimits,
    prepared: Arc<PreparedTableScanHandle>,
    metrics: Arc<ExecutionMetrics>,
    properties: Arc<PlanProperties>,
}

impl ExternalTableScanExec {
    fn new(
        provider: &ExternalTableProvider,
        schema: SchemaRef,
        projection: Box<[usize]>,
        identity_projection: bool,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            scan: provider.scan,
            source_schema: Arc::clone(&provider.schema),
            schema,
            projection,
            identity_projection,
            limits: provider.limits,
            prepared: Arc::clone(&provider.prepared),
            metrics: Arc::clone(&provider.metrics),
            properties,
        }
    }
}

impl DisplayAs for ExternalTableScanExec {
    fn fmt_as(
        &self,
        display: DisplayFormatType,
        formatter: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        match display {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    formatter,
                    "ExternalTableScanExec: scan={}",
                    self.scan.index()
                )
            }
            DisplayFormatType::TreeRender => formatter.write_str("ExternalTableScan"),
        }
    }
}

impl ExecutionPlan for ExternalTableScanExec {
    fn name(&self) -> &'static str {
        "ExternalTableScanExec"
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
                "ExternalTableScanExec is a leaf and cannot accept children"
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
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "ExternalTableScanExec partition {partition} is outside its single partition"
            )));
        }
        let reader = self
            .prepared
            .open_serial_stream(self.limits.maximum_batch_rows)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        if reader.schema() != self.source_schema {
            return Err(DataFusionError::Execution(
                "table-scan Arrow C Stream schema differs from its planned schema"
                    .to_owned(),
            ));
        }
        Ok(Box::pin(ExternalTableScanStream {
            schema: Arc::clone(&self.schema),
            projection: self.projection.clone(),
            identity_projection: self.identity_projection,
            reader,
            metrics: Arc::clone(&self.metrics),
        }))
    }
}

struct ExternalTableScanStream {
    schema: SchemaRef,
    projection: Box<[usize]>,
    identity_projection: bool,
    reader: ProviderStreamReader,
    metrics: Arc<ExecutionMetrics>,
}

impl ExternalTableScanStream {
    fn map_error(error: ArrowError) -> DataFusionError {
        match error {
            ArrowError::ExternalError(error) => {
                match error.downcast::<PgReportError>() {
                    Ok(error) => DataFusionError::Context(
                        "table scan batch".to_owned(),
                        Box::new(DataFusionError::External(error)),
                    ),
                    Err(error) => DataFusionError::ArrowError(
                        Box::new(ArrowError::ExternalError(error)),
                        None,
                    ),
                }
            }
            error => DataFusionError::ArrowError(Box::new(error), None),
        }
    }
}

impl Stream for ExternalTableScanStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        // PostgreSQL does not regain control while an aggregate query consumes
        // the input. Check once at each scan batch boundary, never per row.
        pg_sys::check_for_interrupts!();
        let batch = match self.reader.next() {
            Some(Ok(batch)) => {
                self.metrics.record_input(&batch);
                if self.identity_projection {
                    Some(Ok(batch))
                } else {
                    Some(batch.project(&self.projection).map_err(|error| {
                        DataFusionError::ArrowError(Box::new(error), None)
                    }))
                }
            }
            Some(Err(error)) => Some(Err(Self::map_error(error))),
            None => None,
        };
        Poll::Ready(batch)
    }
}

impl RecordBatchStream for ExternalTableScanStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
