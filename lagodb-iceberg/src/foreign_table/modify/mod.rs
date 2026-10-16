//! PostgreSQL FDW modify adapter for the shared Iceberg write engine.

mod private;
mod state;

use lagodb_core::fdw::{
    FdwModify, FdwScan, ForeignInsertBeginContext, ForeignModifyBeginContext,
    ForeignModifyCapabilities, ForeignModifyError, ForeignModifyOperation,
    ForeignModifyPlanContext, ForeignModifyPlanSpec, ForeignModifyRelationContext,
    ForeignUpdateTargetContext,
};
use lagodb_core::handles::{ExecutorHandle, RelationHandle};
use pgrx::pg_sys;

use super::options::ForeignTableIdentity;
use super::provider::LagodbIceberg;
use super::relation::IcebergForeignTable;
use super::scan::ForeignMutationScan;
use super::schema::ForeignSchemaBinding;
use super::transaction::ForeignTransaction;
use private::IcebergFdwModifyPrivate;
use state::IcebergFdwModifyState;

impl FdwModify for LagodbIceberg {
    type ModifyPrivateData = IcebergFdwModifyPrivate;
    type ModifyState = IcebergFdwModifyState;
    type TargetScanContext = ForeignMutationScan;

    fn capabilities(
        _context: &ForeignModifyRelationContext<'_>,
    ) -> Result<ForeignModifyCapabilities, ForeignModifyError> {
        Ok(ForeignModifyCapabilities::insert_update_delete())
    }

    fn add_update_targets(
        context: &mut ForeignUpdateTargetContext<'_>,
    ) -> Result<(), ForeignModifyError> {
        context.add_item_pointer_identity()?;
        if matches!(context.operation(), ForeignModifyOperation::Delete) {
            let returning = context.returning_columns().to_vec();
            for attno in returning {
                context.add_returning_column(attno)?;
            }
        }
        Ok(())
    }

    fn plan_modify(
        context: &ForeignModifyPlanContext<'_>,
    ) -> Result<ForeignModifyPlanSpec<Self::ModifyPrivateData>, ForeignModifyError>
    {
        let identity = ForeignTableIdentity::resolve(context.relation().oid())?;
        Ok(ForeignModifyPlanSpec::new(IcebergFdwModifyPrivate::new(
            identity,
        )))
    }

    fn begin_modify(
        context: ForeignModifyBeginContext<'_, Self::ModifyPrivateData>,
        target_scan: Option<Self::TargetScanContext>,
    ) -> Result<Self::ModifyState, ForeignModifyError> {
        Self::begin_state(
            context.relation(),
            Some(context.executor()),
            context.effective_user_id(),
            context.operation(),
            Some(context.private_data()),
            target_scan,
            context.command_id(),
        )
    }

    fn target_scan_context(
        state: &<Self as FdwScan>::State,
    ) -> Option<Self::TargetScanContext> {
        state.mutation_context()
    }

    fn begin_insert(
        context: &mut ForeignInsertBeginContext<'_>,
    ) -> Result<Self::ModifyState, ForeignModifyError> {
        Self::begin_state(
            context.relation(),
            context.executor(),
            context.effective_user_id(),
            ForeignModifyOperation::Insert,
            None,
            None,
            context.command_id()?,
        )
    }
}

impl LagodbIceberg {
    fn begin_state(
        relation: &RelationHandle<'_>,
        executor: Option<&ExecutorHandle<'_>>,
        effective_user: pg_sys::Oid,
        operation: ForeignModifyOperation,
        planned: Option<&IcebergFdwModifyPrivate>,
        mutation_scan: Option<ForeignMutationScan>,
        command_id: pg_sys::CommandId,
    ) -> Result<IcebergFdwModifyState, ForeignModifyError> {
        if matches!(
            operation,
            ForeignModifyOperation::Update | ForeignModifyOperation::Delete
        ) {
            let scan = mutation_scan.as_ref().ok_or_else(|| {
                super::error::IcebergFdwError::InvalidPlan {
                    detail: "foreign UPDATE/DELETE has no mutation target scan",
                }
            })?;
            let planned = planned.ok_or_else(|| {
                super::error::IcebergFdwError::InvalidPlan {
                    detail: "foreign UPDATE/DELETE has no modify plan data",
                }
            })?;
            if planned.identity() != scan.identity() {
                return Err(super::error::IcebergFdwError::PlanIdentityChanged.into());
            }
            return IcebergFdwModifyState::new(
                scan.key(),
                operation,
                scan.table(),
                scan.layout(),
                Some(scan),
                command_id,
            );
        }

        let resolved = match executor {
            Some(executor) => IcebergForeignTable::resolve_for_execution(
                relation.oid(),
                effective_user,
                executor,
            )?,
            // COPY has no snapshot-bearing executor and keeps its own binding.
            None => IcebergForeignTable::resolve(relation.oid(), effective_user)?,
        };
        if planned.is_some_and(|private| private.identity() != resolved.identity()) {
            return Err(super::error::IcebergFdwError::PlanIdentityChanged.into());
        }
        let view = ForeignTransaction::begin_write(resolved, None)?;
        let binding = ForeignSchemaBinding::new(relation)?;
        binding.validate_write(view.table.metadata().current_schema())?;
        let layout = binding.into_relation_layout();
        IcebergFdwModifyState::new(
            &view.key,
            operation,
            &view.table,
            &layout,
            None,
            command_id,
        )
    }
}
