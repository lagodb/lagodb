//! Begin-time binding of complete query read views and mutation tasks.

use lagodb_core::fdw::{
    BeginForeignScanContext, ForeignRowIdentityRequirement, ForeignScanError,
};
use lagodb_core::handles::ExecutorHandle;

use super::execution::PostgresParallelScan;
use super::mutation::PreparedMutationScan;
use super::{IcebergFdwScanState, ScanExecutionState};
use crate::foreign_table::scan::ForeignMutationScan;
use crate::foreign_table::{
    ForeignSchemaBinding, ForeignTransaction, IcebergFdwError, LagodbIceberg,
    PlanSourceIdentity, ResolvedForeignTableBinding,
};
use crate::predicate::BoundIcebergPredicate;
use crate::scan::{
    IcebergReadSnapshot, PreparedRowScan, ReaderPredicate, ScanPredicates,
    StablePruningPredicate,
};
use crate::schema::projection::{ProjectedAttribute, SlotProjection};
use crate::schema::relation::RelationLayout;

impl IcebergFdwScanState {
    pub(crate) fn begin(
        context: BeginForeignScanContext<'_, LagodbIceberg>,
    ) -> Result<Self, ForeignScanError> {
        // SAFETY: BeginForeignScan borrows the initialized owning executor.
        let executor = unsafe { ExecutorHandle::from_raw(context.estate) };
        let resolved = ResolvedForeignTableBinding::resolve_for_execution(
            context.relation.oid(),
            context.effective_user_id(),
            &executor,
        )?;
        if resolved.identity() != context.private_data.identity() {
            return Err(IcebergFdwError::PlanIdentityChanged.into());
        }
        let execution = match context.row_identity_requirement {
            ForeignRowIdentityRequirement::ItemPointer => {
                Self::begin_mutation(&context, resolved)?
            }
            ForeignRowIdentityRequirement::None => {
                Self::begin_read(&context, resolved)?
            }
        };
        Ok(Self { execution })
    }

    fn begin_read(
        context: &BeginForeignScanContext<'_, LagodbIceberg>,
        resolved: ResolvedForeignTableBinding,
    ) -> Result<ScanExecutionState, ForeignScanError> {
        // A cached partial plan can outlive its planner-time eligibility.
        // Capture this execution's state before consuming the resolved table.
        let workers_allowed = context.parallel_aware
            && !ForeignTransaction::has_local_actions(resolved.key());
        let view = ForeignTransaction::scan_view(
            resolved.into_catalog_table(),
            context.snapshot.command_id(),
        )?;
        if context.private_data.source().is_some_and(|planned| {
            planned != &PlanSourceIdentity::from_metadata(view.metadata())
        }) {
            return Err(IcebergFdwError::PlanSourceChanged.into());
        }
        let binding = ForeignSchemaBinding::new(&context.relation)?;
        if context.required_columns.needs_all_columns() {
            binding.validate_read(
                view.metadata(),
                context.private_data.identity(),
                binding
                    .layout()
                    .live_columns()
                    .iter()
                    .map(|column| column.attno),
            )?;
        } else {
            binding.validate_read(
                view.metadata(),
                context.private_data.identity(),
                context.required_columns.user_columns(),
            )?;
        }
        let layout = binding.into_relation_layout();
        let (_, table, delta) = view.into_parts();
        let prepared = Self::prepare_rows(
            context,
            IcebergReadSnapshot::new(table, delta),
            &layout,
        )?;
        Ok(if context.parallel_aware {
            ScanExecutionState::PostgresParallelScan(PostgresParallelScan::new(
                prepared,
                workers_allowed,
            )?)
        } else {
            ScanExecutionState::SerialAwaitingStart(prepared)
        })
    }

    fn begin_mutation(
        context: &BeginForeignScanContext<'_, LagodbIceberg>,
        resolved: ResolvedForeignTableBinding,
    ) -> Result<ScanExecutionState, ForeignScanError> {
        let view = ForeignTransaction::begin_write(
            resolved.into_catalog_table(),
            context.snapshot.command_id(),
            context.private_data.source(),
        )?;
        let binding = ForeignSchemaBinding::new(&context.relation)?;
        binding.validate_write(view.metadata().current_schema())?;
        let layout = binding.into_relation_layout();
        let (key, table, delta) = view.into_parts();
        let prepared = Self::prepare_rows(
            context,
            IcebergReadSnapshot::new(table.clone(), delta),
            &layout,
        )?;
        let tasks = prepared.plan_row_location_tasks()?;
        let registry = ForeignTransaction::row_registry(&key)?;
        let mutation = ForeignMutationScan::new(
            context.private_data.identity().clone(),
            key,
            table,
            layout,
            prepared.starting_snapshot_id(),
            tasks,
        );
        Ok(ScanExecutionState::MutationAwaitingStart(
            PreparedMutationScan::new(prepared, registry, mutation),
        ))
    }

    /// Bind this scan's output projection and statement-stable predicates to
    /// the read snapshot. Start installs the complete reader predicate.
    fn prepare_rows(
        context: &BeginForeignScanContext<'_, LagodbIceberg>,
        snapshot: IcebergReadSnapshot,
        layout: &RelationLayout,
    ) -> Result<PreparedRowScan, ForeignScanError> {
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
        let predicates = ScanPredicates::new(
            StablePruningPredicate::new(BoundIcebergPredicate::conjoin(
                context.filters.rescan_stable(),
            )),
            ReaderPredicate::new(BoundIcebergPredicate::conjoin_row_filter(
                context.filters.rescan_stable(),
            )),
        );
        Ok(PreparedRowScan::projected(
            snapshot,
            projection,
            predicates,
            layout,
            context.output_layout.slot_types(),
        )?)
    }
}
