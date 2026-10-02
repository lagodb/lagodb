//! Concrete CustomScan execution modes.

use core::ffi::c_void;

use crate::managed_table::access::mutation::{
    IcebergModifyQueryState, IcebergModifyScanContext, ManagedMutationCursor,
    PreparedManagedMutationScan,
};
use crate::scan::parallel::PostgresParallelExecution;
use crate::scan::{PgRowCursor, PreparedRowScan, ReaderPredicate};
use lagodb_core::access::mutation::ModifyScanBinding;
use lagodb_core::customscan::provider::{
    CustomScanError, NextSlotAttempt, NextSlotEmitter, NextSlotResult,
};
use lagodb_core::runtime_api::TableScanTaskMetrics;
use pgrx::pg_sys;

/// Serial PostgreSQL CustomScan execution.
pub(super) struct SerialScan {
    // Keep the cursor before its preparation so reader resources close first.
    cursor: PgRowCursor,
    prepared: PreparedRowScan,
}

impl SerialScan {
    pub(super) fn task_metrics(&self) -> Option<TableScanTaskMetrics> {
        self.prepared.query_task_metrics()
    }

    pub(super) fn new(
        mut prepared: PreparedRowScan,
        predicate: ReaderPredicate,
    ) -> Result<Self, CustomScanError> {
        prepared.rebind_reader_filter(predicate);
        let cursor = prepared.open_row_cursor()?;
        Ok(Self { cursor, prepared })
    }

    #[inline]
    pub(super) fn next_slot<'a>(
        &mut self,
        emitter: NextSlotEmitter<'a>,
    ) -> Result<NextSlotResult<'a>, CustomScanError> {
        emitter.emit_columns(&mut self.cursor)
    }

    pub(super) fn rescan(
        &mut self,
        replacement: Option<ReaderPredicate>,
    ) -> Result<(), CustomScanError> {
        if let Some(predicate) = replacement {
            self.prepared.rebind_reader_filter(predicate);
        }
        self.cursor = self.prepared.open_row_cursor()?;
        Ok(())
    }
}

/// PostgreSQL native-parallel CustomScan execution under `Gather`.
pub(super) struct PostgresParallelScan {
    // The worker-local cursor is owned by execution and must close before the
    // prepared read it was opened from.
    execution: PostgresParallelExecution,
    prepared: PreparedRowScan,
}

impl PostgresParallelScan {
    pub(super) fn task_metrics(&self) -> Option<TableScanTaskMetrics> {
        self.prepared.query_task_metrics()
    }

    pub(super) fn new(
        mut prepared: PreparedRowScan,
    ) -> Result<Self, CustomScanError> {
        let mut execution = PostgresParallelExecution::new();
        if unsafe { pg_sys::ParallelWorkerNumber } < 0 {
            execution.prepare(&mut prepared)?;
        }
        Ok(Self {
            execution,
            prepared,
        })
    }

    pub(super) fn start(&mut self, predicate: ReaderPredicate) {
        // DSM task inventory uses Begin's stable pruning predicate. Dynamic
        // values affect only the local reader, not shared task ownership.
        self.prepared.rebind_reader_filter(predicate);
    }

    #[inline]
    pub(super) fn next_slot<'a>(
        &mut self,
        mut emitter: NextSlotEmitter<'a>,
    ) -> Result<NextSlotResult<'a>, CustomScanError> {
        loop {
            if let Some(cursor) = self.execution.cursor() {
                match emitter.try_emit_columns(cursor)? {
                    NextSlotAttempt::Produced(produced) => return Ok(produced),
                    NextSlotAttempt::Exhausted(exhausted) => {
                        emitter = exhausted;
                    }
                }
                self.execution.finish_cursor();
            }
            if !self.execution.open_next(&mut self.prepared)? {
                return Ok(emitter.finish_eof());
            }
        }
    }

    pub(super) fn rescan(
        &mut self,
        replacement: Option<ReaderPredicate>,
    ) -> Result<(), CustomScanError> {
        if let Some(predicate) = replacement {
            self.prepared.rebind_reader_filter(predicate);
        }
        self.execution.reset_local();
        Ok(())
    }

    pub(super) fn estimate_dsm(&self) -> Result<pg_sys::Size, CustomScanError> {
        self.execution.estimate().map_err(Into::into)
    }

    pub(super) unsafe fn initialize_dsm(
        &mut self,
        coordinate: *mut c_void,
    ) -> Result<(), CustomScanError> {
        unsafe { self.execution.initialize(coordinate, &self.prepared) }
            .map_err(Into::into)
    }

    pub(super) unsafe fn reinitialize_dsm(
        &mut self,
        coordinate: *mut c_void,
    ) -> Result<(), CustomScanError> {
        unsafe {
            self.execution
                .reinitialize_shared(coordinate, &self.prepared)
        }
        .map_err(Into::into)
    }

    pub(super) unsafe fn initialize_worker(
        &mut self,
        coordinate: *mut c_void,
    ) -> Result<(), CustomScanError> {
        unsafe { self.execution.attach_worker(coordinate, &self.prepared) }
            .map_err(Into::into)
    }

    pub(super) fn shutdown(&mut self) {
        self.execution.shutdown();
    }
}

/// Bound target-row scan used by PostgreSQL managed-table modification plans.
pub(super) struct MutationTargetScan {
    // The cursor carries a binding into the outer ModifyTable state and must
    // close before the prepared mutation scan and its task inventory.
    cursor: ManagedMutationCursor,
    prepared: PreparedManagedMutationScan,
    binding: ModifyScanBinding<IcebergModifyQueryState>,
}

impl MutationTargetScan {
    pub(super) fn new(
        mut prepared: PreparedManagedMutationScan,
        binding: ModifyScanBinding<IcebergModifyQueryState>,
        predicate: ReaderPredicate,
        relation_oid: pg_sys::Oid,
    ) -> Result<Self, CustomScanError> {
        prepared.rebind_reader_filter(predicate);
        let cursor = prepared.open_cursor(binding.clone(), relation_oid)?;
        Ok(Self {
            cursor,
            prepared,
            binding,
        })
    }

    pub(super) fn binding(&self) -> &ModifyScanBinding<IcebergModifyQueryState> {
        &self.binding
    }

    pub(super) fn context(&self) -> IcebergModifyScanContext {
        self.prepared.context()
    }

    #[inline]
    pub(super) fn next_slot<'a>(
        &mut self,
        emitter: NextSlotEmitter<'a>,
    ) -> Result<NextSlotResult<'a>, CustomScanError> {
        emitter.emit_columns(&mut self.cursor)
    }

    pub(super) fn rescan(
        &mut self,
        replacement: Option<ReaderPredicate>,
        relation_oid: pg_sys::Oid,
    ) -> Result<(), CustomScanError> {
        if let Some(predicate) = replacement {
            self.prepared.rebind_reader_filter(predicate);
        }
        self.cursor = self
            .prepared
            .open_cursor(self.binding.clone(), relation_oid)?;
        Ok(())
    }
}
