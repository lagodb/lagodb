//! Lazy run-local provider stream lifecycle.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, SchemaRef};
use datafusion::common::{DataFusionError, Result};
use datafusion::physical_plan::RecordBatchStream;
use futures::Stream;
use lagodb_core::diag::PgReportError;
use lagodb_core::query_contract::ScanId;
use pgrx::pg_sys;

use super::ExternalTableScanExec;
use super::metrics::ScanExecMetrics;
use super::runtime_filters::{ActiveRuntimeFilters, RuntimeFilterSet};
use super::static_filters::StaticFilterSet;
use crate::datafusion::metrics::ExecutionMetrics;
use crate::datafusion::scan_callbacks::{BoundTableScanHandle, ProviderStreamReader};

pub(super) struct ExternalTableScanStream {
    scan: ScanId,
    schema: SchemaRef,
    projection: Box<[usize]>,
    maximum_batch_rows: u64,
    // Field order is intentional. Rust drops fields in declaration order: the
    // provider reader releases its Arrow stream and planned-task handle before
    // the evolving/fixed predicate owners, then static predicates release
    // before the statement-bound scan they were planned against.
    reader: Option<ProviderStreamReader>,
    runtime_filters: Option<ActiveRuntimeFilters>,
    pending_filters: Option<RuntimeFilterSet>,
    static_filters: StaticFilterSet,
    bound: Arc<BoundTableScanHandle>,
    finished: bool,
    metrics: Option<Arc<ExecutionMetrics>>,
    scan_metrics: Option<ScanExecMetrics>,
}

impl ExternalTableScanStream {
    pub(super) fn new(plan: &ExternalTableScanExec) -> Self {
        Self {
            scan: plan.scan,
            schema: Arc::clone(&plan.schema),
            projection: plan.projection.clone(),
            maximum_batch_rows: plan.limits.maximum_batch_rows,
            reader: None,
            runtime_filters: None,
            pending_filters: Some(plan.runtime_filters.clone()),
            static_filters: plan.static_filters.clone(),
            bound: Arc::clone(&plan.bound),
            finished: false,
            metrics: plan.metrics.as_ref().map(Arc::clone),
            scan_metrics: plan.scan_metrics.clone(),
        }
    }

    fn open_if_needed(&mut self) -> Result<()> {
        if self.reader.is_some() {
            return Ok(());
        }
        let filters = self
            .pending_filters
            .take()
            .expect("a lazy table scan opens exactly once")
            .activate(&self.projection, Arc::clone(&self.schema), &self.bound)?;
        let static_predicates = self.static_filters.handles();
        let (reader, task_metrics) = self
            .bound
            .open_serial_stream(
                &self.projection,
                &static_predicates,
                self.maximum_batch_rows,
                filters.fixed_provider_predicate(),
                filters.evolving_provider_predicate(),
            )
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        if let Some(metrics) = &self.metrics {
            metrics.record_task_plan(self.scan, task_metrics);
        }
        if let Some(scan_metrics) = &self.scan_metrics {
            scan_metrics.record_task_plan(task_metrics);
        }
        if reader.schema() != self.schema {
            return Err(DataFusionError::Execution(
                "table-scan Arrow C Stream schema differs from its planned schema"
                    .to_owned(),
            ));
        }
        self.reader = Some(reader);
        self.runtime_filters = Some(filters);
        Ok(())
    }

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
        pg_sys::check_for_interrupts!();
        if self.finished {
            return Poll::Ready(None);
        }
        if let Err(error) = self.open_if_needed() {
            self.finished = true;
            return Poll::Ready(Some(Err(error)));
        }
        let filters = self
            .runtime_filters
            .as_mut()
            .expect("an open table scan has active runtime filters");
        if let Err(error) = filters.refresh() {
            self.finished = true;
            return Poll::Ready(Some(Err(error)));
        }
        let next = self
            .reader
            .as_mut()
            .expect("an open table scan has a provider reader")
            .next();
        let batch = match next {
            Some(Ok(batch)) => {
                if let Some(metrics) = &self.metrics {
                    metrics.record_input(self.scan, &batch);
                }
                Some((|| {
                    let input_rows = batch.num_rows();
                    let filters = self
                        .runtime_filters
                        .as_ref()
                        .expect("an open table scan has active runtime filters");
                    let filtered = filters.apply(batch)?;
                    if !filters.is_empty()
                        && let Some(scan_metrics) = &self.scan_metrics
                    {
                        scan_metrics
                            .record_dynamic_filter(input_rows, filtered.num_rows());
                    }
                    Ok(filtered)
                })())
            }
            Some(Err(error)) => {
                self.finished = true;
                Some(Err(Self::map_error(error)))
            }
            None => {
                self.finished = true;
                None
            }
        };
        Poll::Ready(batch)
    }
}

impl RecordBatchStream for ExternalTableScanStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
