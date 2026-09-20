//! Statement-owned payloads and worker-local source bindings.

use std::mem;
use std::sync::{Arc, Mutex};

use datafusion::common::{DataFusionError, Result};
use lagodb_core::query_contract::ScanId;
use lagodb_core::runtime_api::TableScanTaskMetrics;

use crate::datafusion::scan_callbacks::{
    PreparedWorkerSource, WorkerTableScanSource,
};

use super::host::ParallelWorkerHost;

pub(in crate::datafusion) struct PreparedParallelSource {
    pub(super) scan: ScanId,
    pub(super) payload: Box<[u8]>,
    pub(super) task_metrics: TableScanTaskMetrics,
}

struct ParallelSourceSlots {
    slots: Box<[Option<PreparedParallelSource>]>,
    unsupported: bool,
}

/// Statement-owned payload catalog populated while DataFusion calls each
/// parallel table provider's `scan` method.
pub(in crate::datafusion) struct ParallelSourceCatalog {
    inner: Mutex<ParallelSourceSlots>,
}

impl ParallelSourceCatalog {
    pub(in crate::datafusion) fn new(source_count: usize) -> Self {
        Self {
            inner: Mutex::new(ParallelSourceSlots {
                slots: (0..source_count)
                    .map(|_| None)
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
                unsupported: false,
            }),
        }
    }

    pub(super) fn install(
        &self,
        scan: ScanId,
        prepared: PreparedWorkerSource,
    ) -> Result<()> {
        let mut inner = self.inner.lock().map_err(|_| {
            DataFusionError::Internal(
                "parallel source catalog was poisoned".to_owned(),
            )
        })?;
        let slot = inner.slots.get_mut(scan.index()).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "parallel source {} is outside the source catalog",
                scan.index(),
            ))
        })?;
        if slot.is_some() {
            return Err(DataFusionError::Internal(format!(
                "parallel source {} was prepared more than once",
                scan.index(),
            )));
        }
        let (payload, _, task_metrics) = prepared.into_parts();
        *slot = Some(PreparedParallelSource {
            scan,
            payload,
            task_metrics,
        });
        Ok(())
    }

    pub(super) fn mark_unsupported(&self) -> Result<()> {
        self.inner
            .lock()
            .map_err(|_| {
                DataFusionError::Internal(
                    "parallel source catalog was poisoned".to_owned(),
                )
            })?
            .unsupported = true;
        Ok(())
    }

    pub(in crate::datafusion) fn take(
        &self,
    ) -> Result<Option<Box<[PreparedParallelSource]>>> {
        let mut inner = self.inner.lock().map_err(|_| {
            DataFusionError::Internal(
                "parallel source catalog was poisoned".to_owned(),
            )
        })?;
        if inner.unsupported {
            return Ok(None);
        }
        let slots = mem::take(&mut inner.slots);
        slots
            .into_vec()
            .into_iter()
            .enumerate()
            .map(|(scan, source)| {
                source.ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "parallel plan did not prepare source {scan}",
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()
            .map(|sources| Some(sources.into_boxed_slice()))
    }
}

/// Worker-local source table indexed directly by `ScanId`.
#[derive(Clone)]
pub(in crate::datafusion) struct WorkerSourceCatalog {
    sources: Arc<Vec<Arc<WorkerTableScanSource>>>,
    host: Arc<dyn ParallelWorkerHost>,
}

impl WorkerSourceCatalog {
    pub(in crate::datafusion) fn new(
        sources: Vec<Arc<WorkerTableScanSource>>,
        host: Arc<dyn ParallelWorkerHost>,
    ) -> Self {
        Self {
            sources: Arc::new(sources),
            host,
        }
    }

    pub(super) fn host(&self) -> Arc<dyn ParallelWorkerHost> {
        Arc::clone(&self.host)
    }

    pub(super) fn get(&self, scan: ScanId) -> Result<Arc<WorkerTableScanSource>> {
        self.sources
            .get(scan.index())
            .map(Arc::clone)
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "worker plan references missing source {}",
                    scan.index(),
                ))
            })
    }

    pub(super) fn close(self) -> Result<()> {
        let sources = Arc::try_unwrap(self.sources).map_err(|_| {
            DataFusionError::Internal(
                "worker source catalog remained shared while closing execution"
                    .to_owned(),
            )
        })?;
        let mut first_error = None;
        for (scan, source) in sources.into_iter().enumerate().rev() {
            let result = Arc::try_unwrap(source)
                .map_err(|_| DataFusionError::Internal(format!(
                    "worker source {scan} remained shared while closing execution",
                )))
                .and_then(|source| source.close().map_err(|error| DataFusionError::External(Box::new(error))));
            if first_error.is_none() {
                first_error = result.err();
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}
