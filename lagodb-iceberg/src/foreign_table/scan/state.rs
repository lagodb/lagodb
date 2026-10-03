//! Typed ForeignScan lifecycle over the shared Iceberg query reader.

use std::mem;

use lagodb_core::fdw::{
    BeginForeignScanContext, ForeignRowIdentityRequirement, ForeignScanError,
    ReScanForeignScanContext, ScanSlotWriter, StartForeignScanContext,
};
use lagodb_core::runtime_api::TableScanTaskMetrics;
use pgrx::pg_sys;

use super::super::error::IcebergFdwError;
use super::super::provider::LagodbIceberg;
use super::super::relation::RestForeignTable;
use super::super::schema::ForeignSchemaBinding;
use super::super::source_identity::PlanSourceIdentity;
use super::super::transaction::ForeignTransaction;
use super::ForeignMutationScan;
use super::cursor::ForeignMutationCursor;
use crate::predicate::BoundIcebergPredicate;
use crate::scan::parallel::PostgresParallelExecution;
use crate::scan::{
    IcebergReadSnapshot, PgRowCursor, PreparedRowScan, ReaderPredicate,
    ScanPredicates, StablePruningPredicate,
};
use crate::schema::projection::{ProjectedAttribute, SlotProjection};
use crate::write::RelationRowRegistry;

pub(crate) struct IcebergFdwScanState {
    phase: ForeignScanPhase,
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
    pub(crate) fn task_metrics(&self) -> Option<TableScanTaskMetrics> {
        match &self.phase {
            ForeignScanPhase::SerialQuery { prepared, .. }
            | ForeignScanPhase::ParallelQuery { prepared, .. } => {
                prepared.query_task_metrics()
            }
            _ => None,
        }
    }

    pub(crate) fn begin(
        context: BeginForeignScanContext<'_, LagodbIceberg>,
    ) -> Result<Self, ForeignScanError> {
        let resolved = RestForeignTable::resolve(
            context.relation.oid(),
            context.effective_user_id(),
        )?;
        if resolved.identity() != context.private_data.identity() {
            return Err(IcebergFdwError::PlanIdentityChanged.into());
        }
        let source = PlanSourceIdentity::from_table(resolved.table());
        if context
            .private_data
            .source()
            .is_some_and(|planned| planned != &source)
        {
            return Err(IcebergFdwError::PlanSourceChanged.into());
        }

        let mutation = matches!(
            context.row_identity_requirement,
            ForeignRowIdentityRequirement::ItemPointer
        );
        if mutation && context.parallel_aware {
            return Err(IcebergFdwError::InvalidPlan {
                detail: "mutation ForeignScan cannot be parallel-aware",
            }
            .into());
        }
        let view = if mutation {
            ForeignTransaction::begin_write(resolved)?
        } else {
            ForeignTransaction::scan_view(resolved)?
        };
        let table = view.table;
        let mutation_table = mutation.then(|| table.clone());
        let schema = table.metadata().current_schema();
        let layout = ForeignSchemaBinding::bind(&context.relation, schema)?
            .into_relation_layout();
        let projection = SlotProjection::from_outputs(
            context
                .output_layout
                .columns()
                .iter()
                .map(|column| {
                    ProjectedAttribute::new(column.attno(), column.destination())
                })
                .collect(),
        );
        let planning_filter =
            BoundIcebergPredicate::conjoin(context.filters.rescan_stable());
        let predicates = ScanPredicates::new(
            StablePruningPredicate::new(planning_filter.clone()),
            ReaderPredicate::new(planning_filter),
        );
        let mut prepared = PreparedRowScan::projected(
            IcebergReadSnapshot::new(table, view.delta),
            projection,
            predicates,
            &layout,
            context.output_layout.slot_types(),
        )
        .map_err(IcebergFdwError::from)?;

        let phase = if mutation {
            let tasks = prepared
                .plan_row_location_tasks()
                .map_err(IcebergFdwError::from)?;
            let registry = ForeignTransaction::row_registry(&view.key)?;
            let mutation_table =
                mutation_table.ok_or_else(|| IcebergFdwError::InvalidPlan {
                    detail: "mutation scan did not retain its transaction table",
                })?;
            let mutation_context = ForeignMutationScan::new(
                context.private_data.identity().clone(),
                view.key,
                mutation_table,
                layout,
                prepared.starting_snapshot_id(),
                tasks,
            );
            ForeignScanPhase::PreparedMutation {
                prepared,
                registry,
                context: mutation_context,
            }
        } else if context.parallel_aware {
            let mut execution = PostgresParallelExecution::new();
            if unsafe { pg_sys::ParallelWorkerNumber } < 0 {
                execution
                    .prepare(&mut prepared)
                    .map_err(ForeignScanError::provider)?;
            }
            ForeignScanPhase::ParallelQuery {
                execution,
                prepared,
            }
        } else {
            ForeignScanPhase::PreparedQuery { prepared }
        };
        Ok(Self { phase })
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
