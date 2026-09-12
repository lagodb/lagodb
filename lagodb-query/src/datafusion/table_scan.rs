//! Provider-neutral DataFusion table scan backed by Arrow C Stream.

mod metrics;
mod predicate;
mod runtime_filters;
mod static_filters;
mod stream;

use std::fmt;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::config::ConfigOptions;
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DataFusionError, Result, Statistics};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterPushdownPhase, FilterPushdownPropagation,
};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan,
    Partitioning, PlanProperties, ReplaceChildrenOptions,
};
use lagodb_core::query_contract::{ScanCost, ScanId};
use pgrx::pg_sys;

use self::metrics::ScanExecMetrics;
use self::runtime_filters::RuntimeFilterSet;
use self::static_filters::StaticFilterSet;
use self::stream::ExternalTableScanStream;
use super::metrics::ExecutionMetrics;
use super::scan_callbacks::BoundTableScanHandle;

#[derive(Debug, Clone, Copy)]
pub(super) struct ExternalTableStatistics {
    pub estimated_rows: usize,
}

impl ExternalTableStatistics {
    pub(super) fn from_scan_cost(cost: ScanCost) -> Self {
        Self {
            estimated_rows: cost.rows_read().min(usize::MAX as f64) as usize,
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
    positions_by_attno: Arc<[Option<usize>]>,
    statistics: ExternalTableStatistics,
    limits: ExternalTableScanLimits,
    bound: Arc<BoundTableScanHandle>,
    metrics: Option<Arc<ExecutionMetrics>>,
}

impl ExternalTableProvider {
    pub(super) fn new(
        scan: ScanId,
        schema: SchemaRef,
        projected_attnos: Box<[pg_sys::AttrNumber]>,
        statistics: ExternalTableStatistics,
        limits: ExternalTableScanLimits,
        bound: Arc<BoundTableScanHandle>,
        metrics: Option<Arc<ExecutionMetrics>>,
    ) -> Result<Self> {
        // `bound_schema` is part of the provider's statement-binding contract:
        // its fields correspond positionally to this projection. Validate the
        // structural arity once at Begin, then trust the provider-owned Arrow
        // types rather than repeating PostgreSQL type checks while reading.
        // The final query-output decoder separately binds the destination slot.
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
            positions_by_attno: positions_by_attno.into(),
            statistics,
            limits,
            bound,
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

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        filters
            .iter()
            .map(|filter| {
                StaticFilterSet::support(filter, self.schema.as_ref(), &self.bound)
            })
            .collect()
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
        let static_filters =
            StaticFilterSet::plan(filters, self.schema.as_ref(), &self.bound)?;
        let projection = projection
            .cloned()
            .unwrap_or_else(|| (0..self.schema.fields().len()).collect());
        let output_schema =
            Arc::new(self.schema.project(&projection).map_err(|error| {
                DataFusionError::ArrowError(Box::new(error), None)
            })?);
        Ok(Arc::new(ExternalTableScanExec::new(
            self,
            output_schema,
            projection.into_boxed_slice(),
            static_filters,
        )))
    }
}

#[derive(Debug)]
pub(super) struct ExternalTableScanExec {
    scan: ScanId,
    schema: SchemaRef,
    projection: Box<[usize]>,
    static_filters: StaticFilterSet,
    limits: ExternalTableScanLimits,
    bound: Arc<BoundTableScanHandle>,
    metrics: Option<Arc<ExecutionMetrics>>,
    runtime_filters: RuntimeFilterSet,
    scan_metrics: Option<ScanExecMetrics>,
    properties: Arc<PlanProperties>,
}

impl ExternalTableScanExec {
    fn new(
        provider: &ExternalTableProvider,
        schema: SchemaRef,
        projection: Box<[usize]>,
        static_filters: StaticFilterSet,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            scan: provider.scan,
            schema,
            projection,
            static_filters,
            limits: provider.limits,
            bound: Arc::clone(&provider.bound),
            metrics: provider.metrics.as_ref().map(Arc::clone),
            runtime_filters: RuntimeFilterSet::default(),
            scan_metrics: provider.metrics.as_ref().map(|_| ScanExecMetrics::new()),
            properties,
        }
    }

    fn with_runtime_filters(
        &self,
        runtime_filters: RuntimeFilterSet,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(Self {
            scan: self.scan,
            schema: Arc::clone(&self.schema),
            projection: self.projection.clone(),
            static_filters: self.static_filters.clone(),
            limits: self.limits,
            bound: Arc::clone(&self.bound),
            metrics: self.metrics.as_ref().map(Arc::clone),
            runtime_filters,
            scan_metrics: self.scan_metrics.clone(),
            properties: Arc::clone(&self.properties),
        })
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
                    "ExternalTableScanExec: scan={}, runtime_filters={}",
                    self.scan.index(),
                    self.runtime_filters.len(),
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
        visitor: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.runtime_filters.visit(visitor)
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

    fn handle_child_pushdown_result(
        &self,
        phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        if !matches!(phase, FilterPushdownPhase::Post) {
            return Ok(FilterPushdownPropagation::if_all(child_pushdown_result));
        }
        let (runtime_filters, results, changed) = self
            .runtime_filters
            .merge(&child_pushdown_result.parent_filters);
        let propagation =
            FilterPushdownPropagation::with_parent_pushdown_result(results);
        Ok(if changed {
            propagation.with_updated_node(self.with_runtime_filters(runtime_filters))
        } else {
            propagation
        })
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
        Ok(Box::pin(ExternalTableScanStream::new(self)))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.scan_metrics.as_ref().map(ScanExecMetrics::snapshot)
    }
}
