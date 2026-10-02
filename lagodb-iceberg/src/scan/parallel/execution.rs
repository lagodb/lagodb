//! Shared native-parallel lifecycle for PostgreSQL query scan adapters.

use std::sync::Arc;

use lagodb_core::parallel_scan::{ParallelScanCoordinator, PreparedParallelScan};
use pgrx::pg_sys;

use super::{TaskGrouping, WorkerSource};
use crate::scan::{PgRowCursor, PreparedRowScan, ScanError};

/// PostgreSQL DSM attachment, task inventory, and worker-local cursor.
pub(crate) struct PostgresParallelExecution {
    cursor: ParallelCursorState,
    source: ScanSource,
    coordinator: ParallelScanCoordinator,
}

enum ParallelCursorState {
    Idle,
    Open(PgRowCursor),
}

enum ScanSource {
    Unattached,
    Shared(WorkerSource),
    SerialComplete,
}

impl PostgresParallelExecution {
    pub(crate) fn new() -> Self {
        Self {
            cursor: ParallelCursorState::Idle,
            source: ScanSource::Unattached,
            coordinator: ParallelScanCoordinator::default(),
        }
    }

    pub(crate) fn prepare(
        &mut self,
        read: &mut PreparedRowScan,
    ) -> Result<(), ScanError> {
        let tasks = read.planned_query_tasks()?;
        let grouped =
            TaskGrouping::from_properties(read.table_properties())?.group(&tasks)?;
        let work_count = u32::try_from(grouped.group_count()).map_err(|_| {
            ScanError::WorkerPayload(
                "native parallel group count exceeds u32".to_owned(),
            )
        })?;
        let bytes =
            WorkerSource::encode(&read.query_arrow_schema()?, &tasks, grouped)?;
        self.coordinator
            .prepare(PreparedParallelScan::new(bytes, work_count)?);
        Ok(())
    }

    pub(crate) fn estimate(&self) -> Result<pg_sys::Size, ScanError> {
        Ok(self.coordinator.estimate()?)
    }

    pub(crate) unsafe fn initialize(
        &mut self,
        coordinate: *mut core::ffi::c_void,
        read: &PreparedRowScan,
    ) -> Result<(), ScanError> {
        unsafe { self.coordinator.initialize(coordinate) }?;
        self.attach_source(read)
    }

    pub(crate) unsafe fn attach_worker(
        &mut self,
        coordinate: *mut core::ffi::c_void,
        read: &PreparedRowScan,
    ) -> Result<(), ScanError> {
        unsafe { self.coordinator.attach(coordinate) }?;
        self.attach_source(read)
    }

    /// Reattach after shutdown and reset shared claims and the local source.
    /// PostgreSQL can call this before or after `ReScan`.
    pub(crate) unsafe fn reinitialize_shared(
        &mut self,
        coordinate: *mut core::ffi::c_void,
        read: &PreparedRowScan,
    ) -> Result<(), ScanError> {
        unsafe { self.coordinator.attach(coordinate) }?;
        self.coordinator.reinitialize()?;
        self.attach_source(read)
    }

    pub(crate) fn reset_local(&mut self) {
        self.cursor = ParallelCursorState::Idle;
        if matches!(self.source, ScanSource::SerialComplete) {
            self.source = ScanSource::Unattached;
        }
    }

    pub(crate) fn cursor(&mut self) -> Option<&mut PgRowCursor> {
        match &mut self.cursor {
            ParallelCursorState::Idle => None,
            ParallelCursorState::Open(cursor) => Some(cursor),
        }
    }

    pub(crate) fn finish_cursor(&mut self) {
        self.cursor = ParallelCursorState::Idle;
    }

    /// Claim and open the next task group. Returns `false` at global EOF.
    pub(crate) fn open_next(
        &mut self,
        read: &mut PreparedRowScan,
    ) -> Result<bool, ScanError> {
        if matches!(self.cursor, ParallelCursorState::Open(_)) {
            return Err(ScanError::WorkerPayload(
                "parallel Iceberg cursor is already open".to_owned(),
            ));
        }
        let source = match &self.source {
            ScanSource::Shared(source) => source,
            ScanSource::SerialComplete => return Ok(false),
            ScanSource::Unattached => {
                // PostgreSQL can execute a parallel-aware plan without
                // entering parallel mode (including SPI and zero-worker
                // execution). No InitializeDSM callback runs in that case;
                // the leader must scan the complete input exactly once.
                self.cursor = ParallelCursorState::Open(read.open_row_cursor()?);
                self.source = ScanSource::SerialComplete;
                return Ok(true);
            }
        };
        let Some(work_id) = self.coordinator.claim()? else {
            return Ok(false);
        };
        let tasks = source.take_tasks(work_id)?;
        self.cursor = ParallelCursorState::Open(
            read.open_row_cursor_with_tasks(Arc::from(tasks.into_boxed_slice()))?,
        );
        Ok(true)
    }

    pub(crate) fn shutdown(&mut self) {
        self.cursor = ParallelCursorState::Idle;
        self.source = ScanSource::Unattached;
        self.coordinator.detach();
    }

    fn attach_source(&mut self, read: &PreparedRowScan) -> Result<(), ScanError> {
        // SAFETY: the coordinator's immutable payload remains attached until
        // `shutdown`; `source` is cleared before the coordinator detaches.
        let source = unsafe {
            WorkerSource::decode_shared(self.coordinator.payload()?, read.file_io())
        }?;
        let expected = read.query_arrow_schema()?;
        if source.schema().as_ref() != expected.as_ref() {
            return Err(ScanError::WorkerPayload(
                "worker relation view does not match the leader scan schema"
                    .to_owned(),
            ));
        }
        self.cursor = ParallelCursorState::Idle;
        self.source = ScanSource::Shared(source);
        Ok(())
    }
}

impl Drop for PostgresParallelExecution {
    fn drop(&mut self) {
        self.shutdown();
    }
}
