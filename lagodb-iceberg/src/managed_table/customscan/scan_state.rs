//! Iceberg-specific CustomScan lifecycle routing.

use core::ffi::c_void;
use std::mem;

use crate::error::IcebergError;
use crate::managed_table::ManagedTableSnapshot;
use crate::managed_table::access::mutation::{
    IcebergModifyScanContext, PreparedManagedMutationScan,
};
use crate::predicate::BoundIcebergPredicate;
use crate::scan::{
    PreparedRowScan, ReaderPredicate, ScanPredicates, StablePruningPredicate,
};
use crate::schema::relation::RelationShape;
use iceberg_lite::expr::Predicate;
use lagodb_core::customscan::modify::ModifyBindContext;
use lagodb_core::customscan::provider::{
    BeginContext, CustomScanError, EndContext, NextSlotContext, NextSlotResult,
    ReScanContext,
};
use pgrx::pg_sys;

use super::IcebergCustomScanProvider;
use super::execution::{MutationTargetScan, PostgresParallelScan, SerialScan};
use super::projection::ProjectionResolver;

/// Per-scan runtime state inside the framework wrapper.
pub(crate) struct IcebergScanState {
    execution: ScanExecutionState,
}

/// Closed set of CustomScan execution modes and externally visible lifecycle
/// boundaries.
enum ScanExecutionState {
    NotStarted,
    SerialScan(SerialScan),
    PostgresParallelScan(PostgresParallelScan),
    MutationAwaitingBinding(PreparedManagedMutationScan),
    MutationTargetScan(MutationTargetScan),
    Finished,
}

impl Default for IcebergScanState {
    fn default() -> Self {
        Self {
            execution: ScanExecutionState::NotStarted,
        }
    }
}

impl IcebergScanState {
    pub(super) fn begin(
        ctx: BeginContext<'_, IcebergCustomScanProvider>,
    ) -> Result<(), CustomScanError> {
        let relation_oid = ctx.relation.oid();
        let tablespace_oid = ctx.relation.tablespace_oid();
        let scan_tuple = ctx.scan_tuple();
        let projection =
            ProjectionResolver.resolve(ctx.required_columns(), scan_tuple)?;
        let shape = RelationShape::from_relation(&ctx.relation)?;
        let snapshot =
            ManagedTableSnapshot::load_query(relation_oid, tablespace_oid)?
                .into_read_snapshot();

        let mut prepared = match projection {
            None => {
                PreparedRowScan::full(snapshot, ScanPredicates::unfiltered(), &shape)?
            }
            Some(projection) => PreparedRowScan::projected(
                snapshot,
                projection,
                ScanPredicates::unfiltered(),
                &shape,
                &scan_tuple.attr_types(),
            )?,
        };

        BoundIcebergPredicate::validate_schema(
            ctx.filters.iter(),
            prepared.schema_id(),
        )
        .map_err(CustomScanError::provider)?;
        let row_filter = BoundIcebergPredicate::conjoin(ctx.filters.iter());
        let planning_filter =
            BoundIcebergPredicate::conjoin(ctx.filters.rescan_stable());
        prepared.replace_predicates(ScanPredicates::new(
            StablePruningPredicate::new(planning_filter),
            ReaderPredicate::new(row_filter),
        ));

        ctx.state.execution = if ctx.purpose.is_modify() {
            let conflict_filter =
                BoundIcebergPredicate::conjoin(ctx.filters.static_values())
                    .unwrap_or(Predicate::AlwaysTrue);
            ScanExecutionState::MutationAwaitingBinding(
                PreparedManagedMutationScan::prepare(prepared, conflict_filter)?,
            )
        } else if ctx.parallel_aware {
            ScanExecutionState::PostgresParallelScan(PostgresParallelScan::new(
                prepared,
            )?)
        } else {
            ScanExecutionState::SerialScan(SerialScan::new(prepared)?)
        };
        Ok(())
    }

    pub(super) fn modify_scan_context(&self) -> Option<IcebergModifyScanContext> {
        match &self.execution {
            ScanExecutionState::MutationAwaitingBinding(prepared) => {
                Some(prepared.context())
            }
            ScanExecutionState::MutationTargetScan(scan) => Some(scan.context()),
            _ => None,
        }
    }

    pub(super) fn bind_modify(
        ctx: ModifyBindContext<'_, IcebergCustomScanProvider>,
    ) -> Result<(), CustomScanError> {
        let current =
            mem::replace(&mut ctx.state.execution, ScanExecutionState::Finished);
        match current {
            ScanExecutionState::MutationAwaitingBinding(prepared) => {
                let binding = ctx.binding;
                let cursor =
                    match prepared.open_cursor(binding.clone(), ctx.relation.oid()) {
                        Ok(cursor) => cursor,
                        Err(error) => {
                            ctx.state.execution =
                                ScanExecutionState::MutationAwaitingBinding(prepared);
                            return Err(error.into());
                        }
                    };
                ctx.state.execution = ScanExecutionState::MutationTargetScan(
                    MutationTargetScan::new(cursor, prepared, binding),
                );
                Ok(())
            }
            ScanExecutionState::MutationTargetScan(scan) => {
                let result = if scan.binding() == &ctx.binding {
                    Ok(())
                } else {
                    Err(CustomScanError::provider(IcebergError::InvariantViolated(
                        "Modify scan was bound to two relation states",
                    )))
                };
                ctx.state.execution = ScanExecutionState::MutationTargetScan(scan);
                result
            }
            other => {
                ctx.state.execution = other;
                Err(CustomScanError::provider(IcebergError::InvariantViolated(
                    "Modify scan binding has no managed mutation state",
                )))
            }
        }
    }

    /// Dispatch one PostgreSQL tuple callback to its fixed execution mode.
    ///
    /// This is the only mode branch on the per-row CustomScan path. Serial and
    /// mutation scans terminate directly; native parallel delegates its real
    /// task-boundary loop to `PostgresParallelScan`.
    #[inline]
    pub(super) fn next_slot<'a>(
        ctx: NextSlotContext<'a, IcebergCustomScanProvider>,
    ) -> Result<NextSlotResult<'a>, CustomScanError> {
        let (state, emitter) = ctx.split();
        match &mut state.execution {
            ScanExecutionState::SerialScan(scan) => scan.next_slot(emitter),
            ScanExecutionState::PostgresParallelScan(scan) => scan.next_slot(emitter),
            ScanExecutionState::MutationTargetScan(scan) => scan.next_slot(emitter),
            ScanExecutionState::MutationAwaitingBinding(_) => {
                Err(CustomScanError::provider(IcebergError::InvariantViolated(
                    "Modify scan executed before outer binding",
                )))
            }
            ScanExecutionState::NotStarted | ScanExecutionState::Finished => {
                Ok(emitter.finish_eof())
            }
        }
    }

    pub(super) fn rescan(
        ctx: ReScanContext<'_, IcebergCustomScanProvider>,
    ) -> Result<(), CustomScanError> {
        let replacement = ctx
            .filters_changed
            .then(|| BoundIcebergPredicate::conjoin(ctx.filters.iter()))
            .map(ReaderPredicate::new);

        match &mut ctx.state.execution {
            ScanExecutionState::SerialScan(scan) => scan.rescan(replacement),
            ScanExecutionState::PostgresParallelScan(scan) => {
                scan.rescan(replacement)
            }
            ScanExecutionState::MutationTargetScan(scan) => {
                scan.rescan(replacement, ctx.relation.oid())
            }
            ScanExecutionState::MutationAwaitingBinding(_) => {
                Err(CustomScanError::provider(IcebergError::InvariantViolated(
                    "Modify rescan occurred before outer binding",
                )))
            }
            ScanExecutionState::NotStarted | ScanExecutionState::Finished => Ok(()),
        }
    }

    pub(super) fn end(
        ctx: EndContext<'_, IcebergCustomScanProvider>,
    ) -> Result<(), CustomScanError> {
        drop(mem::replace(
            &mut ctx.state.execution,
            ScanExecutionState::Finished,
        ));
        Ok(())
    }

    pub(super) fn estimate_dsm(&mut self) -> Result<pg_sys::Size, CustomScanError> {
        self.postgres_parallel_mut()?.estimate_dsm()
    }

    pub(super) unsafe fn initialize_dsm(
        &mut self,
        coordinate: *mut c_void,
    ) -> Result<(), CustomScanError> {
        unsafe { self.postgres_parallel_mut()?.initialize_dsm(coordinate) }
    }

    pub(super) unsafe fn reinitialize_dsm(
        &mut self,
        coordinate: *mut c_void,
    ) -> Result<(), CustomScanError> {
        unsafe { self.postgres_parallel_mut()?.reinitialize_dsm(coordinate) }
    }

    pub(super) unsafe fn initialize_worker(
        &mut self,
        coordinate: *mut c_void,
    ) -> Result<(), CustomScanError> {
        unsafe { self.postgres_parallel_mut()?.initialize_worker(coordinate) }
    }

    pub(super) fn shutdown_parallel(&mut self) {
        if let ScanExecutionState::PostgresParallelScan(scan) = &mut self.execution {
            scan.shutdown();
        }
    }

    fn postgres_parallel_mut(
        &mut self,
    ) -> Result<&mut PostgresParallelScan, CustomScanError> {
        let ScanExecutionState::PostgresParallelScan(scan) = &mut self.execution
        else {
            return Err(CustomScanError::provider(IcebergError::InvariantViolated(
                "PostgreSQL parallel callback received a non-parallel Iceberg scan",
            )));
        };
        Ok(scan)
    }
}
