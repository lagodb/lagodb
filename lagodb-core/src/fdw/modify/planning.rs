//! PostgreSQL planner callbacks for foreign INSERT/UPDATE/DELETE.

use core::ffi::c_int;

use pgrx::pg_guard;
use pgrx::pg_sys;

use super::super::row_identity_registry::RowIdentityRegistry;
use super::super::system_column::SystemColumnRequirement;
use super::contract::{FdwModify, ForeignModifyOperation};
use super::error::{ForeignModifyError, ForeignModifyPhase};
use super::plan_private::ModifyPrivateEnvelope;
use super::planning_context::{
    ForeignModifyPlanContext, ForeignModifyRelationContext,
    ForeignUpdateTargetContext,
};
use super::return_requirements::ForeignModifyReturnRequirements;

/// # Safety
///
/// PostgreSQL supplies a live planner root, result relation, target RTE and
/// relation for the duration of this callback.
#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn add_foreign_update_targets<P: FdwModify>(
    root: *mut pg_sys::PlannerInfo,
    rtindex: pg_sys::Index,
    _target_rte: *mut pg_sys::RangeTblEntry,
    target_relation: pg_sys::Relation,
) {
    let result = (|| {
        let mut context = unsafe {
            ForeignUpdateTargetContext::from_raw(root, rtindex, target_relation)
        }?;
        P::add_update_targets(&mut context)?;
        context.register_returning_columns();
        Ok::<(), ForeignModifyError>(())
    })();

    if let Err(error) = result {
        error
            .with_provider_phase::<P>(ForeignModifyPhase::AddUpdateTargets)
            .report();
    }
}

/// # Safety
///
/// `relation` is the live foreign relation supplied by PostgreSQL.
#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn is_foreign_rel_updatable<P: FdwModify>(
    relation: pg_sys::Relation,
) -> c_int {
    let context = unsafe { ForeignModifyRelationContext::from_raw(relation) };
    match P::capabilities(&context) {
        Ok(capabilities) => capabilities.flags(),
        Err(error) => error
            .with_provider_phase::<P>(ForeignModifyPhase::Capabilities)
            .report(),
    }
}

/// # Safety
///
/// PostgreSQL supplies live planner nodes for the duration of this callback.
#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn plan_foreign_modify<P: FdwModify>(
    root: *mut pg_sys::PlannerInfo,
    plan: *mut pg_sys::ModifyTable,
    result_relation: pg_sys::Index,
    subplan_index: c_int,
) -> *mut pg_sys::List {
    let result = (|| {
        let operation = unsafe { modify_operation(plan) }?;
        let rte = unsafe { pg_sys::planner_rt_fetch!(result_relation, root).get() };
        let relation =
            unsafe { pg_sys::relation_open((*rte).relid, pg_sys::NoLock as i32) };

        let result = (|| {
            let relation_context =
                unsafe { ForeignModifyRelationContext::from_raw(relation) };
            let capabilities = P::capabilities(&relation_context)?;
            if !capabilities.supports(operation) {
                return Err(ForeignModifyError::unsupported(
                    "foreign provider does not support this modify operation",
                ));
            }
            let updated_columns =
                unsafe { updated_columns(plan, operation, subplan_index) };
            let context = unsafe {
                ForeignModifyPlanContext::from_raw(
                    plan,
                    relation,
                    result_relation,
                    subplan_index,
                    operation,
                    &updated_columns,
                )
            }?;
            let returned_item_pointer_required = unsafe {
                analyze_returning_system_columns(
                    context.returning_list(),
                    result_relation,
                    operation,
                )?
            };
            let row_identities = if matches!(
                operation,
                ForeignModifyOperation::Update | ForeignModifyOperation::Delete
            ) {
                let identities = unsafe {
                    RowIdentityRegistry::new(root, result_relation).identities()
                }?;
                if identities.is_empty() {
                    return Err(ForeignModifyError::unsupported(
                        "foreign UPDATE/DELETE provider did not register a row identity",
                    ));
                }
                identities
            } else {
                Vec::new().into_boxed_slice()
            };
            let plan_spec = P::plan_modify(&context)?;
            if returned_item_pointer_required
                && !plan_spec.returned_identity.supports_item_pointer()
            {
                return Err(ForeignModifyError::unsupported(
                    "foreign provider does not support target-table ctid in modify RETURNING",
                ));
            }
            ModifyPrivateEnvelope::<P> {
                relation_oid: unsafe { (*relation).rd_id },
                operation,
                updated_columns: &updated_columns,
                row_identities: &row_identities,
                returned_identity: plan_spec.returned_identity,
                returned_item_pointer_required,
                return_requirements: context.return_requirements(),
                private_data: &plan_spec.private_data,
            }
            .encode()
        })();
        unsafe { pg_sys::relation_close(relation, pg_sys::NoLock as i32) };
        result
    })();

    match result {
        Ok(private_data) => private_data,
        Err(error) => error
            .with_provider_phase::<P>(ForeignModifyPhase::Plan)
            .report(),
    }
}

/// Resolve the operation and reject modify features outside framework v1.
unsafe fn modify_operation(
    plan: *mut pg_sys::ModifyTable,
) -> Result<ForeignModifyOperation, ForeignModifyError> {
    let operation = ForeignModifyOperation::from_pg(unsafe { (*plan).operation })?;
    if unsafe { (*plan).onConflictAction }
        != pg_sys::OnConflictAction::ONCONFLICT_NONE
    {
        return Err(ForeignModifyError::unsupported(
            "FDW framework v1 does not support ON CONFLICT",
        ));
    }
    if unsafe { (*plan).partColsUpdated }
        || !unsafe { (*plan).mergeActionLists }.is_null()
        || !unsafe { (*plan).fdwDirectModifyPlans }.is_null()
    {
        return Err(ForeignModifyError::unsupported(
            "FDW framework v1 does not support partition movement, MERGE, or direct modify",
        ));
    }
    Ok(operation)
}

unsafe fn updated_columns(
    plan: *mut pg_sys::ModifyTable,
    operation: ForeignModifyOperation,
    subplan_index: c_int,
) -> Box<[pg_sys::AttrNumber]> {
    if matches!(
        operation,
        ForeignModifyOperation::Insert | ForeignModifyOperation::Delete
    ) {
        return Vec::new().into_boxed_slice();
    }
    let columns =
        (unsafe { pg_sys::list_nth((*plan).updateColnosLists, subplan_index) })
            as *mut pg_sys::List;
    let length = unsafe { pg_sys::list_length(columns) };
    let mut result = Vec::with_capacity(length as usize);
    for index in 0..length {
        let attno =
            unsafe { pg_sys::list_nth_int(columns, index) } as pg_sys::AttrNumber;
        result.push(attno);
    }
    result.into_boxed_slice()
}

/// Analyze target-table system columns used by the modify `RETURNING` list.
/// `tableoid` is synthesized by PostgreSQL; the provider-returned ItemPointer
/// identity is accepted only when the provider explicitly declares it in the
/// modify plan. Other header fields are unsupported.
///
/// # Safety
///
/// `returning` is NULL or a live planner list of `TargetEntry` nodes, and
/// `result_relation` is the target relation RTI for this modify plan.
pub(crate) unsafe fn analyze_returning_system_columns(
    returning: *mut pg_sys::List,
    result_relation: pg_sys::Index,
    operation: ForeignModifyOperation,
) -> Result<bool, ForeignModifyError> {
    if returning.is_null() {
        return Ok(false);
    }
    let attributes = unsafe {
        ForeignModifyReturnRequirements::referenced_attributes(
            returning,
            result_relation,
            operation,
        )
    }?;
    let mut item_pointer_required = false;
    for attno in attributes {
        if attno >= 0 {
            // Positive attributes and attno 0 (whole-row) are ordinary
            // target-table values, not system columns.
            continue;
        }
        match SystemColumnRequirement::from_attno(attno) {
            SystemColumnRequirement::CoreSynthesizedTableOid => {}
            SystemColumnRequirement::ProviderReturnedItemPointer => {
                item_pointer_required = true;
            }
            SystemColumnRequirement::UnsupportedHeaderField(_) => {
                return Err(ForeignModifyError::unsupported(
                    "FDW framework v1 does not support this target-table system column in modify RETURNING",
                ));
            }
        }
    }
    Ok(item_pointer_required)
}
