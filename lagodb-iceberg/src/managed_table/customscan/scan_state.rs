//! Iceberg-specific CustomScan lifecycle routing.

use core::ffi::c_void;

use crate::error::IcebergError;
use crate::managed_table::ManagedTableReadView;
use crate::managed_table::access::mutation::{
    IcebergModifyQueryState, IcebergModifyScanContext, ManagedMutationScan,
    PreparedManagedMutationScan,
};
use crate::predicate::BoundIcebergPredicate;
use crate::scan::{
    PreparedRowScan, ReaderPredicate, ScanPredicates, StablePruningPredicate,
};
use crate::schema::relation::RelationLayout;
use iceberg_lite::expr::Predicate;
use lagodb_core::customscan::modify::ModifyBindContext;
use lagodb_core::customscan::provider::{
    BeginContext, CustomScanError, EndContext, NextSlotContext, NextSlotResult,
    ReScanContext, StartContext,
};
use lagodb_core::runtime_api::TableScanTaskMetrics;
use lagodb_core::table_am::ModifyScanBinding;
use pgrx::pg_sys;

use super::IcebergCustomScanProvider;
use super::execution::{PostgresParallelScan, SerialScan};
use super::projection::ProjectionResolver;

/// Per-scan runtime state inside the framework wrapper.
pub(crate) struct IcebergScanState {
    execution: ScanExecutionState,
}

/// Closed set of CustomScan execution modes and the lifecycle boundaries
/// PostgreSQL makes externally visible. Begin constructs the first state; Start
/// and the outer ModifyTable binding consume a state and return its successor.
enum ScanExecutionState {
    SerialAwaitingStart(PreparedRowScan),
    SerialScan(SerialScan),
    /// Spans Start: Gather initializes DSM before the leader runs its subplan,
    /// reinitializes it after a rescan whether or not the leader started, and
    /// a non-participating leader never starts.
    PostgresParallelScan(PostgresParallelScan),
    MutationAwaitingBinding(PreparedManagedMutationScan),
    MutationAwaitingStart {
        prepared: PreparedManagedMutationScan,
        binding: ModifyScanBinding<IcebergModifyQueryState>,
    },
    MutationTargetScan(ManagedMutationScan),
}

impl IcebergScanState {
    pub(super) fn task_metrics(&self) -> Option<TableScanTaskMetrics> {
        match &self.execution {
            ScanExecutionState::SerialScan(scan) => scan.task_metrics(),
            ScanExecutionState::PostgresParallelScan(scan) => scan.task_metrics(),
            _ => None,
        }
    }

    pub(super) fn begin(
        ctx: BeginContext<'_, IcebergCustomScanProvider>,
    ) -> Result<Self, CustomScanError> {
        let relation_oid = ctx.relation.oid();
        let scan_tuple = ctx.scan_tuple();
        let projection =
            ProjectionResolver.resolve(ctx.required_columns(), scan_tuple)?;
        let layout = RelationLayout::from_relation(&ctx.relation)?;
        let snapshot =
            ManagedTableReadView::for_snapshot(relation_oid, &ctx.snapshot)?
                .into_read_snapshot()?;
        let schema_id = snapshot.schema().schema_id();
        let predicates = ScanPredicates::new(
            StablePruningPredicate::new(BoundIcebergPredicate::conjoin(
                ctx.filters.rescan_stable(),
            )),
            // Start installs the complete row predicate before opening readers.
            ReaderPredicate::new(None),
        );

        let prepared = match projection {
            None => PreparedRowScan::full(snapshot, predicates, &layout)?,
            Some(projection) => PreparedRowScan::projected(
                snapshot,
                projection,
                predicates,
                &layout,
                &scan_tuple.attr_types(),
            )?,
        };
        // Validate after binding the read plan so column-binding errors keep
        // their own SQLSTATE ahead of a planned-predicate schema mismatch.
        BoundIcebergPredicate::validate_schema(ctx.filters.iter(), schema_id)
            .map_err(CustomScanError::provider)?;

        let execution = if ctx.purpose.is_modify_target() {
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
            ScanExecutionState::SerialAwaitingStart(prepared)
        };
        Ok(Self { execution })
    }

    pub(super) fn start(
        self,
        ctx: StartContext<'_, IcebergCustomScanProvider>,
    ) -> Result<Self, CustomScanError> {
        let predicate = ReaderPredicate::new(
            BoundIcebergPredicate::conjoin_row_filter(ctx.filters.iter()),
        );
        let execution = match self.execution {
            ScanExecutionState::SerialAwaitingStart(prepared) => {
                ScanExecutionState::SerialScan(SerialScan::new(prepared, predicate)?)
            }
            ScanExecutionState::PostgresParallelScan(mut scan) => {
                scan.start(predicate);
                ScanExecutionState::PostgresParallelScan(scan)
            }
            ScanExecutionState::MutationAwaitingStart {
                mut prepared,
                binding,
            } => {
                prepared.rebind_reader_filter(predicate);
                ScanExecutionState::MutationTargetScan(ManagedMutationScan::new(
                    prepared,
                    binding,
                    ctx.relation.oid(),
                )?)
            }
            // The outer ModifyTable Begin binds every surviving target scan
            // before any node executes; this is the one check of that contract.
            ScanExecutionState::MutationAwaitingBinding(_) => {
                return Err(CustomScanError::provider(
                    IcebergError::InvariantViolated(
                        "ModifyTarget CustomScan started without a ModifyTable binding",
                    ),
                ));
            }
            ScanExecutionState::SerialScan(_)
            | ScanExecutionState::MutationTargetScan(_) => {
                unreachable!("the CustomScan framework starts a provider scan once")
            }
        };
        Ok(Self { execution })
    }

    /// Storage context of a begun ModifyTarget scan, read by the outer
    /// ModifyTable Begin immediately before [`Self::bind_modify`].
    pub(super) fn modify_scan_context(&self) -> Option<IcebergModifyScanContext> {
        match &self.execution {
            ScanExecutionState::MutationAwaitingBinding(prepared) => {
                Some(prepared.context())
            }
            _ => None,
        }
    }

    pub(super) fn bind_modify(
        self,
        ctx: ModifyBindContext<'_, IcebergCustomScanProvider>,
    ) -> Result<Self, CustomScanError> {
        let ScanExecutionState::MutationAwaitingBinding(prepared) = self.execution
        else {
            unreachable!(
                "the outer ModifyTable binds each begun ModifyTarget scan once"
            )
        };
        // Binding establishes ownership only. Start opens the cursor after the
        // first dynamic predicates have been evaluated.
        Ok(Self {
            execution: ScanExecutionState::MutationAwaitingStart {
                prepared,
                binding: ctx.binding,
            },
        })
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
            ScanExecutionState::MutationTargetScan(scan) => {
                emitter.emit_columns(|columns| scan.next_into_slot(columns))
            }
            ScanExecutionState::SerialAwaitingStart(_)
            | ScanExecutionState::MutationAwaitingBinding(_)
            | ScanExecutionState::MutationAwaitingStart { .. } => {
                unreachable!(
                    "the CustomScan framework produces rows only after Start"
                )
            }
        }
    }

    pub(super) fn rescan(
        ctx: ReScanContext<'_, IcebergCustomScanProvider>,
    ) -> Result<(), CustomScanError> {
        let replacement = ctx
            .filters_changed
            .then(|| BoundIcebergPredicate::conjoin_row_filter(ctx.filters.iter()))
            .map(ReaderPredicate::new);

        match &mut ctx.state.execution {
            ScanExecutionState::SerialScan(scan) => scan.rescan(replacement),
            ScanExecutionState::PostgresParallelScan(scan) => {
                scan.rescan(replacement)
            }
            ScanExecutionState::MutationTargetScan(scan) => {
                scan.rescan(replacement).map_err(Into::into)
            }
            ScanExecutionState::SerialAwaitingStart(_)
            | ScanExecutionState::MutationAwaitingBinding(_)
            | ScanExecutionState::MutationAwaitingStart { .. } => {
                unreachable!(
                    "the CustomScan framework rescans a provider only after Start"
                )
            }
        }
    }

    /// The framework drops this state right after End, before the relation
    /// closes; the wrapper's Drop releases it when ERROR skips End.
    pub(super) fn end(
        _ctx: EndContext<'_, IcebergCustomScanProvider>,
    ) -> Result<(), CustomScanError> {
        Ok(())
    }

    pub(super) fn estimate_dsm(&mut self) -> Result<pg_sys::Size, CustomScanError> {
        self.postgres_parallel_mut().estimate_dsm()
    }

    pub(super) unsafe fn initialize_dsm(
        &mut self,
        coordinate: *mut c_void,
    ) -> Result<(), CustomScanError> {
        unsafe { self.postgres_parallel_mut().initialize_dsm(coordinate) }
    }

    pub(super) unsafe fn reinitialize_dsm(
        &mut self,
        coordinate: *mut c_void,
    ) -> Result<(), CustomScanError> {
        unsafe { self.postgres_parallel_mut().reinitialize_dsm(coordinate) }
    }

    pub(super) unsafe fn initialize_worker(
        &mut self,
        coordinate: *mut c_void,
    ) -> Result<(), CustomScanError> {
        unsafe { self.postgres_parallel_mut().initialize_worker(coordinate) }
    }

    /// PostgreSQL shuts down every CustomScan, parallel-aware or not.
    pub(super) fn shutdown_parallel(&mut self) {
        if let ScanExecutionState::PostgresParallelScan(scan) = &mut self.execution {
            scan.shutdown();
        }
    }

    /// PostgreSQL invokes the DSM callbacks only for a `parallel_aware` plan,
    /// which the framework emits only for Read scans, and Begin builds exactly
    /// those as `PostgresParallelScan`.
    fn postgres_parallel_mut(&mut self) -> &mut PostgresParallelScan {
        let ScanExecutionState::PostgresParallelScan(scan) = &mut self.execution
        else {
            unreachable!(
                "PostgreSQL DSM callback reached a non-parallel Iceberg scan"
            )
        };
        scan
    }
}
