//! Begin-time binding of complete query read views and mutation tasks.

use lagodb_core::fdw::{
    BeginForeignScanContext, ForeignRowIdentityRequirement, ForeignScanError,
};
use lagodb_core::handles::ExecutorHandle;
use pgrx::pg_sys;

use super::{ForeignScanPhase, IcebergFdwScanState};
use crate::foreign_table::scan::ForeignMutationScan;
use crate::foreign_table::{
    ForeignSchemaBinding, ForeignTransaction, IcebergFdwError, LagodbIceberg,
    PlanSourceIdentity, ResolvedForeignTableBinding,
};
use crate::predicate::BoundIcebergPredicate;
use crate::scan::parallel::PostgresParallelExecution;
use crate::scan::{
    IcebergReadSnapshot, PreparedRowScan, ReaderPredicate, ScanPredicates,
    StablePruningPredicate,
};
use crate::schema::projection::{ProjectedAttribute, SlotProjection};

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
        // A cached partial plan can outlive its planner-time eligibility.
        // Capture this execution's state before consuming the resolved table.
        let parallel_workers_allowed = !context.parallel_aware
            || !ForeignTransaction::has_local_actions(resolved.key());
        let command_id = context.snapshot.command_id();
        let catalog_table = resolved.into_catalog_table();
        let view = if mutation {
            ForeignTransaction::begin_write(
                catalog_table,
                command_id,
                context.private_data.source(),
            )?
        } else {
            ForeignTransaction::scan_view(catalog_table, command_id)?
        };
        if !mutation
            && context.private_data.source().is_some_and(|planned| {
                planned != &PlanSourceIdentity::from_metadata(view.metadata())
            })
        {
            return Err(IcebergFdwError::PlanSourceChanged.into());
        }
        let binding = ForeignSchemaBinding::new(&context.relation)?;
        if mutation {
            binding.validate_write(view.metadata().current_schema())?;
        } else if context.required_columns.needs_all_columns() {
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
        let (key, table, delta) = view.into_parts();
        let mutation_table = mutation.then(|| table.clone());
        let mut prepared = PreparedRowScan::projected(
            IcebergReadSnapshot::new(table, delta),
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
            let registry = ForeignTransaction::row_registry(&key)?;
            let mutation_table =
                mutation_table.ok_or_else(|| IcebergFdwError::InvalidPlan {
                    detail: "mutation scan did not retain its transaction table",
                })?;
            let mutation_context = ForeignMutationScan::new(
                context.private_data.identity().clone(),
                key,
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
            // Without workers, core skips provider DSM setup. Keep execution
            // unattached so its existing fallback opens one complete cursor.
            if parallel_workers_allowed && unsafe { pg_sys::ParallelWorkerNumber } < 0
            {
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
        Ok(Self {
            phase,
            parallel_workers_allowed,
        })
    }
}
