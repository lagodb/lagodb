//! Provider-neutral lazy DataFusion source backed by Arrow C Stream.

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
use lagodb_core::query_contract::SourceId;
use pgrx::pg_sys;

use super::metrics::ExecutionMetrics;
use super::source_ffi::{PreparedSourceHandle, ProviderStreamReader};

#[derive(Debug, Clone, Copy)]
pub(super) struct ExternalSourceStatistics {
    pub estimated_rows: usize,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ExternalSourceLimits {
    pub maximum_batch_rows: u64,
}

/// DataFusion catalog leaf owned by the engine DSO.
#[derive(Debug, Clone)]
pub(super) struct ExternalSourceProvider {
    source: SourceId,
    schema: SchemaRef,
    statistics: ExternalSourceStatistics,
    limits: ExternalSourceLimits,
    prepared: Arc<PreparedSourceHandle>,
    metrics: Arc<ExecutionMetrics>,
}

impl ExternalSourceProvider {
    pub(super) fn new(
        source: SourceId,
        schema: SchemaRef,
        statistics: ExternalSourceStatistics,
        limits: ExternalSourceLimits,
        prepared: Arc<PreparedSourceHandle>,
        metrics: Arc<ExecutionMetrics>,
    ) -> Self {
        Self {
            source,
            schema,
            statistics,
            limits,
            prepared,
            metrics,
        }
    }
}

#[async_trait]
impl TableProvider for ExternalSourceProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn statistics(&self) -> Option<Statistics> {
        Some(
            Statistics::default()
                .with_num_rows(Precision::Inexact(self.statistics.estimated_rows))
                // DataFusion defines this as Arrow output size, not physical
                // scan bytes. CountRows has an empty schema and emits no
                // column buffers across this source boundary.
                .with_total_byte_size(Precision::Exact(0)),
        )
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if projection.is_some_and(|projection| !projection.is_empty()) {
            return Err(DataFusionError::Plan(
                "row-count source has no projectable output columns".to_owned(),
            ));
        }
        if !filters.is_empty() {
            return Err(DataFusionError::Plan(
                "S1M external source does not accept engine filters".to_owned(),
            ));
        }
        Ok(Arc::new(ExternalSourceExec::new(
            self.source,
            Arc::clone(&self.schema),
            self.limits,
            Arc::clone(&self.prepared),
            Arc::clone(&self.metrics),
        )))
    }
}

#[derive(Debug)]
struct ExternalSourceExec {
    source: SourceId,
    schema: SchemaRef,
    limits: ExternalSourceLimits,
    prepared: Arc<PreparedSourceHandle>,
    metrics: Arc<ExecutionMetrics>,
    properties: Arc<PlanProperties>,
}

impl ExternalSourceExec {
    fn new(
        source: SourceId,
        schema: SchemaRef,
        limits: ExternalSourceLimits,
        prepared: Arc<PreparedSourceHandle>,
        metrics: Arc<ExecutionMetrics>,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            source,
            schema,
            limits,
            prepared,
            metrics,
            properties,
        }
    }
}

impl DisplayAs for ExternalSourceExec {
    fn fmt_as(
        &self,
        display: DisplayFormatType,
        formatter: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        match display {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    formatter,
                    "ExternalSourceExec: source={}",
                    self.source.index()
                )
            }
            DisplayFormatType::TreeRender => formatter.write_str("ExternalSource"),
        }
    }
}

impl ExecutionPlan for ExternalSourceExec {
    fn name(&self) -> &'static str {
        "ExternalSourceExec"
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
                "ExternalSourceExec is a leaf and cannot accept children".to_owned(),
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
                "ExternalSourceExec partition {partition} is outside its single partition"
            )));
        }
        let reader = self
            .prepared
            .open_serial_stream(self.limits.maximum_batch_rows)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        if reader.schema() != self.schema {
            return Err(DataFusionError::Execution(
                "provider Arrow C Stream schema differs from its planned schema"
                    .to_owned(),
            ));
        }
        Ok(Box::pin(ExternalSourceStream {
            schema: Arc::clone(&self.schema),
            reader,
            metrics: Arc::clone(&self.metrics),
        }))
    }
}

struct ExternalSourceStream {
    schema: SchemaRef,
    reader: ProviderStreamReader,
    metrics: Arc<ExecutionMetrics>,
}

impl ExternalSourceStream {
    fn map_error(error: ArrowError) -> DataFusionError {
        match error {
            ArrowError::ExternalError(error) => {
                match error.downcast::<PgReportError>() {
                    Ok(error) => DataFusionError::Context(
                        "query source batch".to_owned(),
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

impl Stream for ExternalSourceStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        // PostgreSQL does not regain control while a scalar aggregate consumes
        // the input. Check once at each source batch boundary, never per row.
        pg_sys::check_for_interrupts!();
        let batch = match self.reader.next() {
            Some(Ok(batch)) => {
                self.metrics.record_input(&batch);
                Some(Ok(batch))
            }
            Some(Err(error)) => Some(Err(Self::map_error(error))),
            None => None,
        };
        Poll::Ready(batch)
    }
}

impl RecordBatchStream for ExternalSourceStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
