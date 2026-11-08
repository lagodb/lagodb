//! Typed ForeignScan lifecycle over the shared Iceberg query reader.

mod begin;
mod execution;
mod mutation;

use core::ffi::c_void;

use lagodb_core::fdw::{
    ForeignScanError, ReScanForeignScanContext, ScanSlotWriter,
    StartForeignScanContext,
};
use lagodb_core::runtime_api::TableScanTaskMetrics;
use pgrx::pg_sys;

use super::super::provider::LagodbIceberg;
use super::ForeignMutationScan;
use crate::predicate::BoundIcebergPredicate;
use crate::scan::{PreparedRowScan, ReaderPredicate};

use execution::{PostgresParallelScan, SerialScan};
use mutation::{MutationTargetScan, PreparedMutationScan};

/// Per-scan provider state inside the FDW framework wrapper.
pub(crate) struct IcebergFdwScanState {
    execution: ScanExecutionState,
}

/// Closed set of ForeignScan execution modes and the lifecycle boundaries
/// PostgreSQL makes externally visible. Begin constructs the first state;
/// Start consumes it and returns its successor.
enum ScanExecutionState {
    SerialAwaitingStart(PreparedRowScan),
    SerialScan(SerialScan),
    /// Spans Start: Gather initializes DSM before the leader runs its subplan,
    /// reinitializes it after a rescan whether or not the leader started, and
    /// a non-participating leader never starts.
    PostgresParallelScan(PostgresParallelScan),
    MutationAwaitingStart(PreparedMutationScan),
    MutationTargetScan(MutationTargetScan),
}

impl IcebergFdwScanState {
    pub(crate) fn task_metrics(&self) -> Option<TableScanTaskMetrics> {
        match &self.execution {
            ScanExecutionState::SerialScan(scan) => scan.task_metrics(),
            ScanExecutionState::PostgresParallelScan(scan) => scan.task_metrics(),
            _ => None,
        }
    }

    pub(crate) fn start(
        self,
        context: StartForeignScanContext<'_, LagodbIceberg>,
    ) -> Result<Self, ForeignScanError> {
        let predicate = ReaderPredicate::new(
            BoundIcebergPredicate::conjoin_row_filter(context.filters.iter()),
        );
        let execution = match self.execution {
            ScanExecutionState::SerialAwaitingStart(prepared) => {
                ScanExecutionState::SerialScan(SerialScan::open(prepared, predicate)?)
            }
            ScanExecutionState::PostgresParallelScan(mut scan) => {
                scan.start(predicate);
                ScanExecutionState::PostgresParallelScan(scan)
            }
            ScanExecutionState::MutationAwaitingStart(prepared) => {
                ScanExecutionState::MutationTargetScan(prepared.start(predicate)?)
            }
            ScanExecutionState::SerialScan(_)
            | ScanExecutionState::MutationTargetScan(_) => {
                unreachable!("the FDW framework starts a provider scan once")
            }
        };
        Ok(Self { execution })
    }

    /// Mutation context of a begun target scan, read by BeginForeignModify
    /// after BeginForeignScan and before any row is produced.
    pub(crate) fn mutation_context(&self) -> Option<ForeignMutationScan> {
        match &self.execution {
            ScanExecutionState::MutationAwaitingStart(prepared) => {
                Some(prepared.context())
            }
            _ => None,
        }
    }

    /// Dispatch one PostgreSQL tuple callback to its fixed execution mode.
    /// This is the only mode branch on the per-row ForeignScan path.
    pub(crate) fn next_slot(
        &mut self,
        output: &mut ScanSlotWriter<'_>,
    ) -> Result<bool, ForeignScanError> {
        match &mut self.execution {
            ScanExecutionState::SerialScan(scan) => scan.next_slot(output),
            ScanExecutionState::PostgresParallelScan(scan) => scan.next_slot(output),
            ScanExecutionState::MutationTargetScan(scan) => scan.next_slot(output),
            ScanExecutionState::SerialAwaitingStart(_)
            | ScanExecutionState::MutationAwaitingStart(_) => {
                unreachable!("the FDW framework produces rows only after Start")
            }
        }
    }

    pub(crate) fn rescan(
        &mut self,
        context: ReScanForeignScanContext<'_, LagodbIceberg>,
    ) -> Result<(), ForeignScanError> {
        let replacement = context
            .filters_changed
            .then(|| {
                BoundIcebergPredicate::conjoin_row_filter(context.filters.iter())
            })
            .map(ReaderPredicate::new);
        match &mut self.execution {
            ScanExecutionState::SerialScan(scan) => scan.rescan(replacement),
            ScanExecutionState::PostgresParallelScan(scan) => {
                scan.rescan(replacement);
                Ok(())
            }
            ScanExecutionState::MutationTargetScan(scan) => scan.rescan(replacement),
            ScanExecutionState::SerialAwaitingStart(_)
            | ScanExecutionState::MutationAwaitingStart(_) => {
                unreachable!("the FDW framework rescans a provider only after Start")
            }
        }
    }

    pub(crate) fn parallel_workers_allowed(&self) -> bool {
        self.parallel().workers_allowed()
    }

    pub(crate) fn estimate_dsm(&self) -> Result<pg_sys::Size, ForeignScanError> {
        self.parallel().estimate_dsm()
    }

    pub(crate) unsafe fn initialize_dsm(
        &mut self,
        coordinate: *mut c_void,
    ) -> Result<(), ForeignScanError> {
        unsafe { self.parallel_mut().initialize_dsm(coordinate) }
    }

    pub(crate) unsafe fn reinitialize_dsm(
        &mut self,
        coordinate: *mut c_void,
    ) -> Result<(), ForeignScanError> {
        unsafe { self.parallel_mut().reinitialize_dsm(coordinate) }
    }

    pub(crate) unsafe fn initialize_worker(
        &mut self,
        coordinate: *mut c_void,
    ) -> Result<(), ForeignScanError> {
        unsafe { self.parallel_mut().initialize_worker(coordinate) }
    }

    /// PostgreSQL shuts down every ForeignScan, parallel-aware or not.
    pub(crate) fn shutdown_parallel(&mut self) {
        if let ScanExecutionState::PostgresParallelScan(scan) = &mut self.execution {
            scan.shutdown();
        }
    }

    /// PostgreSQL invokes the DSM callbacks only for a `parallel_aware` plan,
    /// which the framework emits only for SELECT scans, and Begin builds
    /// exactly those as `PostgresParallelScan`.
    fn parallel(&self) -> &PostgresParallelScan {
        let ScanExecutionState::PostgresParallelScan(scan) = &self.execution else {
            unreachable!("DSM callback reached a non-parallel Iceberg scan")
        };
        scan
    }

    fn parallel_mut(&mut self) -> &mut PostgresParallelScan {
        let ScanExecutionState::PostgresParallelScan(scan) = &mut self.execution
        else {
            unreachable!("DSM callback reached a non-parallel Iceberg scan")
        };
        scan
    }
}
