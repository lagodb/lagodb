use std::ffi::{CString, c_void};
use std::ptr::{self, NonNull};

use datafusion::common::{DataFusionError, Result};
use lagodb_core::query_contract::ScanId;
use lagodb_core::runtime_api::TableScanTaskMetrics;

use super::bootstrap::ParallelSource;
use super::catalog::PreparedParallelSource;
use crate::datafusion::metrics::ExecutionMetrics;

pub(super) struct ParallelSourceRoute {
    pub(super) kind: i32,
    pub(super) name: CString,
}

/// Leader-owned payloads and their provider-neutral flat DSM directory.
pub(super) struct PreparedSourceInventory {
    descriptors: Vec<ParallelSource>,
    payloads: Vec<Box<[u8]>>,
    task_metrics: Box<[(ScanId, TableScanTaskMetrics)]>,
    byte_len: usize,
}

impl PreparedSourceInventory {
    pub(super) fn try_new(
        sources: Box<[PreparedParallelSource]>,
        routes: &[ParallelSourceRoute],
    ) -> Result<Self> {
        let mut byte_len = 0_usize;
        let mut payloads = Vec::with_capacity(sources.len());
        let mut task_metrics = Vec::with_capacity(sources.len());
        let descriptors = sources
            .into_vec()
            .into_iter()
            .map(|source| {
                let route = &routes[source.scan.index()];
                let payload_offset = u64::try_from(byte_len).map_err(|_| {
                    DataFusionError::Plan(
                        "parallel source inventory exceeds the transport address space"
                            .to_owned(),
                    )
                })?;
                let payload_len = u64::try_from(source.payload.len()).map_err(|_| {
                    DataFusionError::Plan(
                        "parallel source payload exceeds the transport address space"
                            .to_owned(),
                    )
                })?;
                byte_len = byte_len.checked_add(source.payload.len()).ok_or_else(|| {
                    DataFusionError::Plan(
                        "parallel source inventory size overflowed usize".to_owned(),
                    )
                })?;
                task_metrics.push((source.scan, source.task_metrics));
                payloads.push(source.payload);
                Ok(ParallelSource {
                    route_kind: route.kind,
                    route_name: route.name.as_bytes_with_nul().to_vec(),
                    payload_offset,
                    payload_len,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            descriptors,
            payloads,
            task_metrics: task_metrics.into_boxed_slice(),
            byte_len,
        })
    }

    pub(super) fn descriptors(&self) -> &[ParallelSource] {
        &self.descriptors
    }

    pub(super) const fn byte_len(&self) -> usize {
        self.byte_len
    }

    pub(super) fn encoded_byte_len(&self) -> Result<u64> {
        u64::try_from(self.byte_len).map_err(|_| {
            DataFusionError::Plan(
                "parallel source inventory exceeds the transport address space"
                    .to_owned(),
            )
        })
    }

    /// Commit task-planning facts only after this inventory belongs to a live
    /// parallel run. A short launch can then fall back to serial execution
    /// without counting an abandoned parallel preparation.
    pub(super) fn record_task_plans(&self, metrics: &ExecutionMetrics) {
        for (scan, task_metrics) in &self.task_metrics {
            metrics.record_task_plan(*scan, *task_metrics);
        }
    }

    /// Copy the immutable inventory into its allocated DSM tail.
    ///
    /// # Safety
    ///
    /// `destination` must provide `self.byte_len()` writable bytes and must not
    /// overlap any source payload.
    pub(super) unsafe fn write_to(&self, mut destination: *mut u8) {
        for payload in &self.payloads {
            unsafe {
                ptr::copy_nonoverlapping(
                    payload.as_ptr(),
                    destination,
                    payload.len(),
                );
                destination = destination.add(payload.len());
            }
        }
    }
}

/// Worker view over the immutable inventory tail of an attached DSM mapping.
pub(super) struct MappedSourceInventory<'a> {
    bytes: &'a [u8],
}

impl<'a> MappedSourceInventory<'a> {
    /// # Safety
    ///
    /// `base` must name a live `mapping_len` byte DSM mapping. Its transport
    /// prefix must occupy `transport_len` bytes, and the mapping must remain
    /// attached for `'a`.
    pub(super) unsafe fn try_new(
        base: NonNull<c_void>,
        mapping_len: usize,
        transport_len: usize,
        inventory_len: usize,
    ) -> Result<Self> {
        let inventory_offset = mapping_len
            .checked_sub(inventory_len)
            .filter(|offset| *offset >= transport_len)
            .ok_or_else(|| {
                DataFusionError::Plan(
                    "parallel source inventory lies outside its DSM mapping"
                        .to_owned(),
                )
            })?;
        let bytes = unsafe {
            core::slice::from_raw_parts(
                base.as_ptr().cast::<u8>().add(inventory_offset),
                inventory_len,
            )
        };
        Ok(Self { bytes })
    }

    pub(super) fn payload(&self, source: &ParallelSource) -> Result<&'a [u8]> {
        let offset = usize::try_from(source.payload_offset).map_err(|_| {
            DataFusionError::Plan(
                "parallel source offset exceeds this platform".to_owned(),
            )
        })?;
        let len = usize::try_from(source.payload_len).map_err(|_| {
            DataFusionError::Plan(
                "parallel source length exceeds this platform".to_owned(),
            )
        })?;
        let end = offset.checked_add(len).ok_or_else(|| {
            DataFusionError::Plan(
                "parallel source byte range overflowed usize".to_owned(),
            )
        })?;
        self.bytes.get(offset..end).ok_or_else(|| {
            DataFusionError::Plan(
                "parallel source lies outside the shared inventory".to_owned(),
            )
        })
    }
}
