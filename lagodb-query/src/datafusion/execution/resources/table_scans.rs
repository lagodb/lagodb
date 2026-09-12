//! Statement-scoped provider bindings shared by physical-plan rebuilds.

use std::sync::Arc;

use lagodb_core::query_contract::ScanId;
use pgrx::pg_sys;

use super::super::QueryExecutionError;
use crate::datafusion::scan_callbacks::{
    BoundTableScanHandle, SerialTableScanCallbacks,
};
use crate::datafusion::table_scan::{
    ExternalTableProvider, ExternalTableScanLimits, ExternalTableStatistics,
};
use crate::datafusion::{SerialExecutionLimits, metrics::ExecutionMetrics};
use crate::plan::{PlannedTableScan, QueryFragment};

struct BoundScan {
    scan: ScanId,
    statistics: ExternalTableStatistics,
    handle: Arc<BoundTableScanHandle>,
}

/// Dense provider handles corresponding to the fragment scan table.
pub(super) struct BoundTableScans {
    entries: Box<[BoundScan]>,
}

impl BoundTableScans {
    pub(super) fn bind(
        scans: &[PlannedTableScan<'_>],
        callbacks: &[SerialTableScanCallbacks],
    ) -> Result<Self, QueryExecutionError> {
        if scans.len() != callbacks.len() {
            return Err(QueryExecutionError::ScanCallbackCount {
                scans: scans.len(),
                callbacks: callbacks.len(),
            });
        }

        let mut entries = Vec::with_capacity(scans.len());
        for (scan_index, (planned_scan, callbacks)) in
            scans.iter().zip(callbacks).enumerate()
        {
            let scan = ScanId::from_index(scan_index);
            // SAFETY: the selected plan ties every provider payload to the
            // live plan-data input, and registry resolution supplied these
            // callbacks.
            let handle = match unsafe { callbacks.bind(planned_scan.provider_plan()) }
            {
                Ok(handle) => handle,
                Err(error) => {
                    let cleanup = Self {
                        entries: entries.into_boxed_slice(),
                    }
                    .close()
                    .err()
                    .map(Box::new);
                    return Err(QueryExecutionError::Initialization {
                        primary: Box::new(QueryExecutionError::ScanBind(error)),
                        cleanup,
                    });
                }
            };
            entries.push(BoundScan {
                scan,
                statistics: ExternalTableStatistics::from_scan_cost(
                    planned_scan.cost(),
                ),
                handle: Arc::new(handle),
            });
        }
        Ok(Self {
            entries: entries.into_boxed_slice(),
        })
    }

    pub(super) fn providers(
        &self,
        fragment: &QueryFragment,
        limits: SerialExecutionLimits,
        metrics: Option<&Arc<ExecutionMetrics>>,
    ) -> Result<Box<[Arc<ExternalTableProvider>]>, QueryExecutionError> {
        self.entries
            .iter()
            .map(|entry| {
                let schema = entry
                    .handle
                    .schema()
                    .map_err(QueryExecutionError::ScanBind)?;
                let projected_attnos: Box<[pg_sys::AttrNumber]> = fragment
                    .scan(entry.scan)
                    .map(|scan| {
                        scan.columns().iter().map(|column| column.attno).collect()
                    })
                    .ok_or(QueryExecutionError::MissingScanMetadata {
                        scan: entry.scan.index(),
                    })?;
                Ok(Arc::new(ExternalTableProvider::new(
                    entry.scan,
                    schema,
                    projected_attnos,
                    entry.statistics,
                    ExternalTableScanLimits {
                        maximum_batch_rows: limits.maximum_batch_rows() as u64,
                    },
                    Arc::clone(&entry.handle),
                    metrics.map(Arc::clone),
                )?))
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Vec::into_boxed_slice)
    }

    pub(super) fn finish_run(&self) -> Result<(), QueryExecutionError> {
        let mut first_error = None;
        for entry in self.entries.iter().rev() {
            let result = entry
                .handle
                .finish_run()
                .map_err(QueryExecutionError::ScanRelease);
            if first_error.is_none() {
                first_error = result.err();
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    pub(super) fn close(self) -> Result<(), QueryExecutionError> {
        let mut first_error = None;
        // Handles form an acquisition stack and are released in reverse.
        for entry in self.entries.into_vec().into_iter().rev() {
            let result = Arc::try_unwrap(entry.handle)
                .map_err(|_| QueryExecutionError::BoundScanStillShared {
                    scan: entry.scan.index(),
                })
                .and_then(|handle| {
                    handle.close().map_err(QueryExecutionError::ScanRelease)
                });
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
