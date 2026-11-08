//! Serial and PostgreSQL native-parallel ForeignScan read modes.

use core::ffi::c_void;

use lagodb_core::fdw::{ForeignScanError, ScanSlotWriter};
use lagodb_core::runtime_api::TableScanTaskMetrics;
use pgrx::pg_sys;

use crate::scan::parallel::PostgresParallelExecution;
use crate::scan::{PgRowCursor, PreparedRowScan, ReaderPredicate};

/// Serial read over one complete row cursor.
pub(super) struct SerialScan {
    cursor: PgRowCursor,
    prepared: PreparedRowScan,
}

impl SerialScan {
    /// Bind Start's complete reader predicate and open the first cursor.
    pub(super) fn open(
        mut prepared: PreparedRowScan,
        predicate: ReaderPredicate,
    ) -> Result<Self, ForeignScanError> {
        prepared.rebind_reader_filter(predicate);
        let cursor = prepared.open_row_cursor()?;
        Ok(Self { cursor, prepared })
    }

    pub(super) fn task_metrics(&self) -> Option<TableScanTaskMetrics> {
        self.prepared.query_task_metrics()
    }

    pub(super) fn next_slot(
        &mut self,
        output: &mut ScanSlotWriter<'_>,
    ) -> Result<bool, ForeignScanError> {
        // SAFETY: this is the callback's only output. Begin bound the decoder
        // to every output-layout column of this scan slot.
        let mut columns = unsafe { output.datum_columns() };
        // SAFETY: the decoder was bound to the validated scan-slot types;
        // ForeignNext selected the per-tuple context.
        unsafe { self.cursor.next_into_slot(&mut columns) }
            .map_err(ForeignScanError::from)
    }

    pub(super) fn rescan(
        &mut self,
        replacement: Option<ReaderPredicate>,
    ) -> Result<(), ForeignScanError> {
        if let Some(predicate) = replacement {
            self.prepared.rebind_reader_filter(predicate);
        }
        self.cursor = self.prepared.open_row_cursor()?;
        Ok(())
    }
}

/// PostgreSQL native-parallel read under `Gather`.
pub(super) struct PostgresParallelScan {
    execution: PostgresParallelExecution,
    prepared: PreparedRowScan,
    /// Fixed for this execution. When false, core skips every DSM setup
    /// callback and the unattached execution reads the complete input.
    workers_allowed: bool,
}

impl PostgresParallelScan {
    pub(super) fn new(
        mut prepared: PreparedRowScan,
        workers_allowed: bool,
    ) -> Result<Self, ForeignScanError> {
        let mut execution = PostgresParallelExecution::new();
        // Only a leader whose workers may participate publishes the task
        // inventory. Workers attach to it; a leader without workers keeps
        // execution unattached so its fallback opens one complete cursor.
        if workers_allowed && unsafe { pg_sys::ParallelWorkerNumber } < 0 {
            execution
                .prepare(&mut prepared)
                .map_err(ForeignScanError::provider)?;
        }
        Ok(Self {
            execution,
            prepared,
            workers_allowed,
        })
    }

    pub(super) fn workers_allowed(&self) -> bool {
        self.workers_allowed
    }

    pub(super) fn task_metrics(&self) -> Option<TableScanTaskMetrics> {
        self.prepared.query_task_metrics()
    }

    pub(super) fn start(&mut self, predicate: ReaderPredicate) {
        // The DSM task inventory uses Begin's stable pruning predicate.
        // Dynamic values affect only the local reader, not task ownership.
        self.prepared.rebind_reader_filter(predicate);
    }

    pub(super) fn next_slot(
        &mut self,
        output: &mut ScanSlotWriter<'_>,
    ) -> Result<bool, ForeignScanError> {
        // SAFETY: as for SerialScan; every task-group cursor uses the same
        // prepared read and its decoder.
        let mut columns = unsafe { output.datum_columns() };
        loop {
            let Some(cursor) = self
                .execution
                .cursor_or_open(&mut self.prepared)
                .map_err(ForeignScanError::provider)?
            else {
                return Ok(false);
            };
            // SAFETY: as for SerialScan.
            if unsafe { cursor.next_into_slot(&mut columns) }? {
                return Ok(true);
            }
            self.execution.finish_cursor();
        }
    }

    pub(super) fn rescan(&mut self, replacement: Option<ReaderPredicate>) {
        if let Some(predicate) = replacement {
            self.prepared.rebind_reader_filter(predicate);
        }
        self.execution.reset_local();
    }

    pub(super) fn estimate_dsm(&self) -> Result<pg_sys::Size, ForeignScanError> {
        self.execution
            .estimate()
            .map_err(ForeignScanError::provider)
    }

    pub(super) unsafe fn initialize_dsm(
        &mut self,
        coordinate: *mut c_void,
    ) -> Result<(), ForeignScanError> {
        unsafe { self.execution.initialize(coordinate, &self.prepared) }
            .map_err(ForeignScanError::provider)
    }

    pub(super) unsafe fn reinitialize_dsm(
        &mut self,
        coordinate: *mut c_void,
    ) -> Result<(), ForeignScanError> {
        unsafe {
            self.execution
                .reinitialize_shared(coordinate, &self.prepared)
        }
        .map_err(ForeignScanError::provider)
    }

    pub(super) unsafe fn initialize_worker(
        &mut self,
        coordinate: *mut c_void,
    ) -> Result<(), ForeignScanError> {
        unsafe { self.execution.attach_worker(coordinate, &self.prepared) }
            .map_err(ForeignScanError::provider)
    }

    pub(super) fn shutdown(&mut self) {
        self.execution.shutdown();
    }
}
