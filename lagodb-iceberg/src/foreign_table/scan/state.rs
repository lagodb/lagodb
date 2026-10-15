//! Typed ForeignScan lifecycle over the shared Iceberg query reader.

mod begin;

use std::mem;

use lagodb_core::fdw::{
    ForeignScanError, ReScanForeignScanContext, ScanSlotWriter,
    StartForeignScanContext,
};
use lagodb_core::runtime_api::TableScanTaskMetrics;
use pgrx::pg_sys;

use super::super::error::IcebergFdwError;
use super::super::provider::LagodbIceberg;
use super::ForeignMutationScan;
use super::cursor::ForeignMutationCursor;
use crate::predicate::BoundIcebergPredicate;
use crate::scan::parallel::PostgresParallelExecution;
use crate::scan::{PgRowCursor, PreparedRowScan, ReaderPredicate};
use crate::write::RelationRowRegistry;

pub(crate) struct IcebergFdwScanState {
    phase: ForeignScanPhase,
    parallel_workers_allowed: bool,
}

enum ForeignScanPhase {
    PreparedQuery {
        prepared: PreparedRowScan,
    },
    SerialQuery {
        cursor: PgRowCursor,
        prepared: PreparedRowScan,
    },
    ParallelQuery {
        execution: PostgresParallelExecution,
        prepared: PreparedRowScan,
    },
    PreparedMutation {
        prepared: PreparedRowScan,
        registry: RelationRowRegistry,
        context: ForeignMutationScan,
    },
    ForeignMutation {
        cursor: ForeignMutationCursor,
        prepared: PreparedRowScan,
        context: ForeignMutationScan,
    },
    Transitioning,
    Ended,
}

impl IcebergFdwScanState {
    pub(crate) fn parallel_workers_allowed(&self) -> bool {
        self.parallel_workers_allowed
    }

    pub(crate) fn task_metrics(&self) -> Option<TableScanTaskMetrics> {
        match &self.phase {
            ForeignScanPhase::SerialQuery { prepared, .. }
            | ForeignScanPhase::ParallelQuery { prepared, .. } => {
                prepared.query_task_metrics()
            }
            _ => None,
        }
    }

    pub(crate) fn start(
        &mut self,
        context: StartForeignScanContext<'_, LagodbIceberg>,
    ) -> Result<(), ForeignScanError> {
        let row_filter = ReaderPredicate::new(BoundIcebergPredicate::conjoin(
            context.filters.iter(),
        ));
        let phase = mem::replace(&mut self.phase, ForeignScanPhase::Transitioning);
        self.phase = match phase {
            ForeignScanPhase::PreparedQuery { mut prepared } => {
                prepared.rebind_reader_filter(row_filter);
                let cursor =
                    prepared.open_row_cursor().map_err(IcebergFdwError::from)?;
                ForeignScanPhase::SerialQuery { cursor, prepared }
            }
            ForeignScanPhase::ParallelQuery {
                execution,
                mut prepared,
            } => {
                prepared.rebind_reader_filter(row_filter);
                ForeignScanPhase::ParallelQuery {
                    execution,
                    prepared,
                }
            }
            ForeignScanPhase::PreparedMutation {
                mut prepared,
                registry,
                context,
            } => {
                prepared.rebind_reader_filter(row_filter);
                let cursor = ForeignMutationCursor::new(
                    context
                        .open_row_location_scan(&prepared)
                        .map_err(IcebergFdwError::from)?,
                    registry,
                );
                ForeignScanPhase::ForeignMutation {
                    cursor,
                    prepared,
                    context,
                }
            }
            active => {
                self.phase = active;
                return Err(IcebergFdwError::InvalidPlan {
                    detail: "Iceberg scan was started more than once",
                }
                .into());
            }
        };
        Ok(())
    }

    pub(crate) fn mutation_context(&self) -> Option<ForeignMutationScan> {
        match &self.phase {
            ForeignScanPhase::PreparedMutation { context, .. }
            | ForeignScanPhase::ForeignMutation { context, .. } => {
                Some(context.clone())
            }
            _ => None,
        }
    }

    pub(crate) fn next_slot(
        &mut self,
        output: &mut ScanSlotWriter<'_>,
    ) -> Result<bool, ForeignScanError> {
        match &mut self.phase {
            ForeignScanPhase::SerialQuery { cursor, .. } => cursor
                .next_with(|decoder, batch, row_index| {
                    let mut columns = unsafe { output.datum_columns() };
                    unsafe {
                        decoder.write_row_unchecked(batch, row_index, &mut columns)
                    }?;
                    Ok(())
                })
                .map_err(ForeignScanError::from),
            ForeignScanPhase::ParallelQuery {
                execution,
                prepared,
            } => loop {
                if let Some(cursor) = execution.cursor() {
                    let produced = cursor
                        .next_with(|decoder, batch, row_index| {
                            let mut columns = unsafe { output.datum_columns() };
                            unsafe {
                                decoder.write_row_unchecked(
                                    batch,
                                    row_index,
                                    &mut columns,
                                )
                            }?;
                            Ok(())
                        })
                        .map_err(ForeignScanError::from)?;
                    if produced {
                        return Ok(true);
                    }
                    execution.finish_cursor();
                }
                if !execution
                    .open_next(prepared)
                    .map_err(ForeignScanError::provider)?
                {
                    return Ok(false);
                }
            },
            ForeignScanPhase::ForeignMutation { cursor, .. } => {
                cursor.next_slot(output)
            }
            ForeignScanPhase::PreparedQuery { .. }
            | ForeignScanPhase::PreparedMutation { .. } => {
                Err(IcebergFdwError::InvalidPlan {
                    detail: "Iceberg scan cursor was not started",
                }
                .into())
            }
            ForeignScanPhase::Transitioning | ForeignScanPhase::Ended => {
                Err(IcebergFdwError::InvalidPlan {
                    detail: "Iceberg scan is in an invalid lifecycle transition",
                }
                .into())
            }
        }
    }

    pub(crate) fn rescan(
        &mut self,
        context: ReScanForeignScanContext<'_, LagodbIceberg>,
    ) -> Result<(), ForeignScanError> {
        let replacement = context
            .filters_changed
            .then(|| BoundIcebergPredicate::conjoin(context.filters.iter()))
            .map(ReaderPredicate::new);
        match &mut self.phase {
            ForeignScanPhase::SerialQuery { cursor, prepared } => {
                if let Some(predicate) = replacement {
                    prepared.rebind_reader_filter(predicate);
                }
                *cursor =
                    prepared.open_row_cursor().map_err(IcebergFdwError::from)?;
            }
            ForeignScanPhase::ParallelQuery {
                execution,
                prepared,
            } => {
                if let Some(predicate) = replacement {
                    prepared.rebind_reader_filter(predicate);
                }
                execution.reset_local();
            }
            ForeignScanPhase::ForeignMutation {
                cursor,
                prepared,
                context,
            } => {
                if let Some(predicate) = replacement {
                    prepared.rebind_reader_filter(predicate);
                }
                let registry = cursor.registry();
                *cursor = ForeignMutationCursor::new(
                    context
                        .open_row_location_scan(prepared)
                        .map_err(IcebergFdwError::from)?,
                    registry,
                );
            }
            ForeignScanPhase::PreparedQuery { .. }
            | ForeignScanPhase::PreparedMutation { .. }
            | ForeignScanPhase::Transitioning
            | ForeignScanPhase::Ended => {
                return Err(IcebergFdwError::InvalidPlan {
                    detail: "Iceberg scan was rescanned before start",
                }
                .into());
            }
        }
        Ok(())
    }

    pub(crate) fn estimate_dsm(&self) -> Result<pg_sys::Size, ForeignScanError> {
        self.parallel()?
            .estimate()
            .map_err(ForeignScanError::provider)
    }

    pub(crate) unsafe fn initialize_dsm(
        &mut self,
        coordinate: *mut core::ffi::c_void,
    ) -> Result<(), ForeignScanError> {
        let ForeignScanPhase::ParallelQuery {
            execution,
            prepared,
        } = &mut self.phase
        else {
            return Err(Self::parallel_state_error());
        };
        unsafe { execution.initialize(coordinate, prepared) }
            .map_err(ForeignScanError::provider)
    }

    pub(crate) unsafe fn reinitialize_dsm(
        &mut self,
        coordinate: *mut core::ffi::c_void,
    ) -> Result<(), ForeignScanError> {
        let ForeignScanPhase::ParallelQuery {
            execution,
            prepared,
        } = &mut self.phase
        else {
            return Err(Self::parallel_state_error());
        };
        unsafe { execution.reinitialize_shared(coordinate, prepared) }
            .map_err(ForeignScanError::provider)
    }

    pub(crate) unsafe fn initialize_worker(
        &mut self,
        coordinate: *mut core::ffi::c_void,
    ) -> Result<(), ForeignScanError> {
        let ForeignScanPhase::ParallelQuery {
            execution,
            prepared,
        } = &mut self.phase
        else {
            return Err(Self::parallel_state_error());
        };
        unsafe { execution.attach_worker(coordinate, prepared) }
            .map_err(ForeignScanError::provider)
    }

    pub(crate) fn shutdown_parallel(&mut self) {
        if let ForeignScanPhase::ParallelQuery { execution, .. } = &mut self.phase {
            execution.shutdown();
        }
    }

    pub(crate) fn end(&mut self) {
        let phase = mem::replace(&mut self.phase, ForeignScanPhase::Ended);
        match phase {
            ForeignScanPhase::SerialQuery { cursor, prepared } => {
                drop(cursor);
                drop(prepared);
            }
            ForeignScanPhase::ParallelQuery {
                execution,
                prepared,
            } => {
                drop(execution);
                drop(prepared);
            }
            ForeignScanPhase::ForeignMutation {
                cursor, prepared, ..
            } => {
                drop(cursor);
                drop(prepared);
            }
            ForeignScanPhase::PreparedQuery { prepared }
            | ForeignScanPhase::PreparedMutation { prepared, .. } => drop(prepared),
            ForeignScanPhase::Transitioning | ForeignScanPhase::Ended => {}
        }
    }

    fn parallel(&self) -> Result<&PostgresParallelExecution, ForeignScanError> {
        let ForeignScanPhase::ParallelQuery { execution, .. } = &self.phase else {
            return Err(Self::parallel_state_error());
        };
        Ok(execution)
    }

    fn parallel_state_error() -> ForeignScanError {
        ForeignScanError::provider(IcebergFdwError::InvalidPlan {
            detail: "parallel callback received a non-parallel Iceberg scan",
        })
    }
}
