//! Parallel catalog provider using the shared predicate-lowering contract.

use std::fmt;
use std::ptr;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::stats::Precision;
use datafusion::common::{DataFusionError, Result, Statistics};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use lagodb_core::query_contract::{ScanCost, ScanId};
use pgrx::pg_sys;

use super::catalog::ParallelSourceCatalog;
use super::scan_exec::ParallelTableScanExec;
use crate::datafusion::scan_callbacks::{
    BoundTableScanHandle, WorkerTableScanCallbacks,
};
use crate::datafusion::table_scan::static_filters::StaticFilterSet;

/// Inputs bound once on the owning PostgreSQL backend thread.
pub(in crate::datafusion) struct ParallelTableScanBinding {
    pub scan: ScanId,
    pub schema: SchemaRef,
    pub projected_attnos: Box<[pg_sys::AttrNumber]>,
    pub cost: ScanCost,
    pub bound: Arc<BoundTableScanHandle>,
    pub worker: WorkerTableScanCallbacks,
}

/// The callback context is backend-local even though DataFusion's catalog
/// requires `Send + Sync`. Only the owning current-thread runtime calls it.
struct BackendParallelScan {
    bound: Arc<BoundTableScanHandle>,
    worker: WorkerTableScanCallbacks,
}

// SAFETY: as with BoundTableScanHandle, the backend-thread owner keeps the
// registration and bound handle alive, and polls/drops its DataFusion plans
// exclusively on a current-thread runtime before releasing the binding.
unsafe impl Send for BackendParallelScan {}
// SAFETY: catalog sharing does not authorize concurrent callback execution;
// every preparation callback runs on the same owning backend thread. The
// provider-owned WorkerSource, not this callback context, crosses worker tasks.
unsafe impl Sync for BackendParallelScan {}

pub(in crate::datafusion) struct ParallelTableProvider {
    scan: ScanId,
    schema: SchemaRef,
    positions_by_attno: Arc<[Option<usize>]>,
    estimated_rows: usize,
    maximum_batch_rows: u64,
    backend: BackendParallelScan,
    sources: Arc<ParallelSourceCatalog>,
}

impl ParallelTableProvider {
    pub(in crate::datafusion) fn new(
        binding: ParallelTableScanBinding,
        maximum_batch_rows: u64,
        sources: Arc<ParallelSourceCatalog>,
    ) -> Result<Self> {
        let ParallelTableScanBinding {
            scan,
            schema,
            projected_attnos,
            cost,
            bound,
            worker,
        } = binding;
        if schema.fields().len() != projected_attnos.len() {
            return Err(DataFusionError::Internal(format!(
                "parallel table scan {} returned {} Arrow fields for {} projected attributes",
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
            estimated_rows: cost.rows_read().min(usize::MAX as f64) as usize,
            maximum_batch_rows,
            backend: BackendParallelScan { bound, worker },
            sources,
        })
    }

    pub(in crate::datafusion) fn column_name(
        &self,
        attno: pg_sys::AttrNumber,
    ) -> Option<&str> {
        usize::try_from(attno)
            .ok()
            .and_then(|attno| attno.checked_sub(1))
            .and_then(|index| self.positions_by_attno.get(index))
            .and_then(|position| *position)
            .map(|position| self.schema.field(position).name().as_str())
    }
}

impl fmt::Debug for ParallelTableProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ParallelTableProvider")
            .field("scan", &self.scan)
            .field("schema", &self.schema)
            .field("estimated_rows", &self.estimated_rows)
            .field("maximum_batch_rows", &self.maximum_batch_rows)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl TableProvider for ParallelTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn statistics(&self) -> Option<Statistics> {
        Some(
            Statistics::default()
                .with_num_rows(Precision::Inexact(self.estimated_rows)),
        )
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        filters
            .iter()
            .map(|filter| {
                StaticFilterSet::support(
                    filter,
                    self.schema.as_ref(),
                    &self.backend.bound,
                )
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
                "parallel table scan projection is out of bounds".to_owned(),
            ));
        }
        let filters = StaticFilterSet::plan(
            filters,
            self.schema.as_ref(),
            &self.backend.bound,
        )?;
        let projection = projection
            .cloned()
            .unwrap_or_else(|| (0..self.schema.fields().len()).collect());
        let schema =
            Arc::new(self.schema.project(&projection).map_err(|error| {
                DataFusionError::ArrowError(Box::new(error), None)
            })?);
        let predicate_handles = filters.handles();
        let (planned, _) = self
            .backend
            .bound
            .plan_tasks(&projection, &predicate_handles, ptr::null())
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let prepared = self.backend.worker.prepare(&self.backend.bound, &planned);
        let cleanup = planned.close();
        let prepared = match (prepared, cleanup) {
            (Err(primary), Err(cleanup)) => {
                return Err(DataFusionError::External(Box::new(
                    primary.contextualize(
                        "parallel source preparation failed",
                        Some(format!("planned scan release also failed: {cleanup}")),
                    ),
                )));
            }
            (Err(error), Ok(())) | (Ok(_), Err(error)) => {
                return Err(DataFusionError::External(Box::new(error)));
            }
            (Ok(prepared), Ok(())) => prepared,
        };
        let Some(prepared) = prepared else {
            self.sources.mark_unsupported()?;
            return Ok(Arc::new(ParallelTableScanExec::unsupported(
                self.scan,
                schema,
                self.maximum_batch_rows,
            )));
        };
        let work_count = prepared.work_count();
        self.sources.install(self.scan, prepared)?;
        Ok(Arc::new(ParallelTableScanExec::leader(
            self.scan,
            schema,
            self.maximum_batch_rows,
            work_count,
        )))
    }
}
