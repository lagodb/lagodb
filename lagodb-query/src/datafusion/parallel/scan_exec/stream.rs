//! Worker-local stream for one parallel table-scan partition.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use datafusion::physical_plan::RecordBatchStream;
use futures::Stream;

use super::super::host::ParallelWorkerHost;
use super::super::scan_metrics::ParallelScanMetrics;
use crate::datafusion::scan_callbacks::WorkerProviderStreamReader;

pub(super) struct ParallelTableScanStream {
    schema: SchemaRef,
    reader: WorkerProviderStreamReader,
    host: Arc<dyn ParallelWorkerHost>,
    finished: bool,
    metrics: Option<ParallelScanMetrics>,
}

impl ParallelTableScanStream {
    pub(super) fn new(
        schema: SchemaRef,
        reader: WorkerProviderStreamReader,
        host: Arc<dyn ParallelWorkerHost>,
        metrics: Option<ParallelScanMetrics>,
    ) -> Self {
        Self {
            schema,
            reader,
            host,
            finished: false,
            metrics,
        }
    }

    fn poll_cancellation(&mut self) -> Option<Poll<Option<Result<RecordBatch>>>> {
        if self.host.interrupt_pending() {
            self.finished = true;
            let primary =
                DataFusionError::Execution("parallel query interrupted".to_owned());
            let error = match self.reader.close() {
                Ok(()) => primary,
                Err(cleanup) => primary.context(format!(
                    "parallel source stream release also failed: {cleanup}",
                )),
            };
            return Some(Poll::Ready(Some(Err(error))));
        }
        None
    }
}

impl Stream for ParallelTableScanStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }
        if let Some(cancelled) = self.poll_cancellation() {
            return cancelled;
        }
        let next = match self.reader.next() {
            Some(Err(error)) => {
                self.finished = true;
                let primary = DataFusionError::ArrowError(Box::new(error), None);
                let error = match self.reader.close() {
                    Ok(()) => primary,
                    Err(cleanup) => primary.context(format!(
                        "parallel source stream release also failed: {cleanup}",
                    )),
                };
                return Poll::Ready(Some(Err(error)));
            }
            next => next,
        };
        if let Some(cancelled) = self.poll_cancellation() {
            return cancelled;
        }
        match next {
            Some(Ok(batch)) => {
                if let Some(metrics) = &self.metrics {
                    metrics.record(&batch);
                }
                Poll::Ready(Some(Ok(batch)))
            }
            Some(Err(_)) => unreachable!("reader errors return above"),
            None => {
                self.finished = true;
                Poll::Ready(None)
            }
        }
    }
}

impl RecordBatchStream for ParallelTableScanStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
