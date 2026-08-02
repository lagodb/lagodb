//! PostgreSQL planner callbacks for foreign INSERT/UPDATE/DELETE.

use core::ffi::c_int;
use core::ptr;

use pgrx::pg_guard;
use pgrx::pg_sys;

use super::super::row_identity::RowIdentityLayout;
use super::super::system_column::SystemColumnRequirement;
use super::contract::{FdwModify, ForeignModifyOperation};
use super::error::{ForeignModifyError, ForeignModifyPhase};
use super::planning_context::{
    ForeignModifyPlanContext, ForeignModifyRelationContext,
    ForeignUpdateTargetContext,
};
use super::private::encode_modify_private;

/// # Safety
///
/// PostgreSQL supplies a live planner root, result relation, target RTE and
/// relation for the duration of this callback.
#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn add_foreign_update_targets<P: FdwModify>(
    root: *mut pg_sys::PlannerInfo,
    rtindex: pg_sys::Index,
    target_rte: *mut pg_sys::RangeTblEntry,
    target_relation: pg_sys::Relation,
) {
    let prior_context = unsafe { pg_sys::CurrentMemoryContext };
    let result = (|| {
        let mut context = unsafe {
            ForeignUpdateTargetContext::from_raw(
                root,
                rtindex,
                target_rte,
                target_relation,
            )
        }?;
        P::add_update_targets(&mut context)
    })();

    if let Err(error) = result {
        error
            .with_provider_phase::<P>(ForeignModifyPhase::AddUpdateTargets)
            .report_after_switch(prior_context);
    }
}

/// # Safety
///
/// `relation` is the live foreign relation supplied by PostgreSQL.
#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn is_foreign_rel_updatable<P: FdwModify>(
    relation: pg_sys::Relation,
) -> c_int {
    let prior_context = unsafe { pg_sys::CurrentMemoryContext };
    let result = (|| {
        let context = unsafe { ForeignModifyRelationContext::from_raw(relation) }?;
        P::capabilities(&context)
    })();

    match result {
        Ok(capabilities) => capabilities.flags(),
        Err(error) => error
            .with_provider_phase::<P>(ForeignModifyPhase::Capabilities)
            .report_after_switch(prior_context),
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
    let prior_context = unsafe { pg_sys::CurrentMemoryContext };
    let result = (|| {
        let operation =
            unsafe { validate_modify_plan(plan, result_relation, subplan_index) }?;
        let rte = unsafe { planner_rte(root, result_relation)? };
        let relation =
            unsafe { pg_sys::relation_open((*rte).relid, pg_sys::NoLock as i32) };
        if relation.is_null() {
            return Err(ForeignModifyError::framework(
                "PlanForeignModify could not open the result relation",
            ));
        }

        let result = (|| {
            let relation_context =
                unsafe { ForeignModifyRelationContext::from_raw(relation) }?;
            let capabilities = P::capabilities(&relation_context)?;
            if !capabilities.supports(operation) {
                return Err(ForeignModifyError::unsupported(
                    "foreign provider does not support this modify operation",
                ));
            }
            let updated_columns =
                unsafe { updated_columns(plan, operation, subplan_index)? };
            let context = unsafe {
                ForeignModifyPlanContext::from_raw(
                    root,
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
                )?
            };
            if matches!(
                operation,
                ForeignModifyOperation::Update | ForeignModifyOperation::Delete
            ) {
                let identity_layout = unsafe {
                    RowIdentityLayout::from_targetlist(
                        context.subplan_targetlist(),
                        relation,
                        result_relation,
                    )
                }?;
                if identity_layout.is_empty() {
                    return Err(ForeignModifyError::unsupported(
                        "foreign UPDATE/DELETE provider did not register a row identity",
                    ));
                }
            }
            let plan_spec = P::plan_modify(&context)?;
            if returned_item_pointer_required
                && !plan_spec.returned_identity.supports_item_pointer()
            {
                return Err(ForeignModifyError::unsupported(
                    "foreign provider does not support target-table ctid in modify RETURNING",
                ));
            }
            encode_modify_private::<P>(
                P::NAME,
                unsafe { (*relation).rd_id },
                operation,
                &updated_columns,
                plan_spec.returned_identity,
                returned_item_pointer_required,
                &plan_spec.private_data,
            )
        })();
        unsafe { pg_sys::relation_close(relation, pg_sys::NoLock as i32) };
        result
    })();

    match result {
        Ok(private_data) => private_data,
        Err(error) => error
            .with_provider_phase::<P>(ForeignModifyPhase::Plan)
            .report_after_switch(prior_context),
    }
}

/// Validate the deliberately narrow first modify scope before provider code
/// sees any planner data.
unsafe fn validate_modify_plan(
    plan: *mut pg_sys::ModifyTable,
    result_relation: pg_sys::Index,
    subplan_index: c_int,
) -> Result<ForeignModifyOperation, ForeignModifyError> {
    if plan.is_null() || subplan_index != 0 {
        return Err(ForeignModifyError::framework(
            "PlanForeignModify received an invalid plan or subplan index",
        ));
    }
    if unsafe { (*plan).plan.type_ } != pg_sys::NodeTag::T_ModifyTable {
        return Err(ForeignModifyError::framework(
            "PlanForeignModify received a non-ModifyTable plan",
        ));
    }
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
    let result_relations = unsafe { (*plan).resultRelations };
    if result_relations.is_null()
        || unsafe { pg_sys::list_length(result_relations) } != 1
        || unsafe { pg_sys::list_nth_int(result_relations, 0) as pg_sys::Index }
            != result_relation
        || unsafe { (*plan).rootRelation } != 0
        || unsafe { (*plan).nominalRelation } != result_relation
    {
        return Err(ForeignModifyError::unsupported(
            "FDW framework v1 supports one non-inherited result relation only",
        ));
    }
    if unsafe { (*plan).plan.lefttree }.is_null() {
        return Err(ForeignModifyError::framework(
            "PlanForeignModify has no modify subplan",
        ));
    }
    Ok(operation)
}

unsafe fn planner_rte(
    root: *mut pg_sys::PlannerInfo,
    result_relation: pg_sys::Index,
) -> Result<*mut pg_sys::RangeTblEntry, ForeignModifyError> {
    if root.is_null() || result_relation == 0 {
        return Err(ForeignModifyError::framework(
            "PlanForeignModify has no planner root or result relation",
        ));
    }
    let rte = unsafe { pg_sys::planner_rt_fetch(result_relation, root) };
    if rte.is_null() || unsafe { (*rte).relid == pg_sys::InvalidOid } {
        return Err(ForeignModifyError::framework(
            "PlanForeignModify result relation has no catalog relation",
        ));
    }
    Ok(rte)
}

unsafe fn updated_columns(
    plan: *mut pg_sys::ModifyTable,
    operation: ForeignModifyOperation,
    subplan_index: c_int,
) -> Result<Box<[pg_sys::AttrNumber]>, ForeignModifyError> {
    if matches!(
        operation,
        ForeignModifyOperation::Insert | ForeignModifyOperation::Delete
    ) {
        return Ok(Vec::new().into_boxed_slice());
    }
    let lists = unsafe { (*plan).updateColnosLists };
    if !lists.is_null() && unsafe { pg_sys::list_length(lists) } <= subplan_index {
        return Err(ForeignModifyError::framework(
            "UPDATE modify plan column-list index is outside its plan list",
        ));
    }
    let columns = if lists.is_null() {
        ptr::null_mut()
    } else {
        (unsafe { pg_sys::list_nth(lists, subplan_index) }) as *mut pg_sys::List
    };
    if columns.is_null() {
        return Err(ForeignModifyError::framework(
            "UPDATE modify plan has no update column list",
        ));
    }
    let length = unsafe { pg_sys::list_length(columns) };
    if length < 0 {
        return Err(ForeignModifyError::framework(
            "UPDATE modify plan has a negative update column count",
        ));
    }
    let mut result = Vec::with_capacity(length as usize);
    for index in 0..length {
        let attno =
            unsafe { pg_sys::list_nth_int(columns, index) } as pg_sys::AttrNumber;
        if attno <= 0 || result.contains(&attno) {
            return Err(ForeignModifyError::framework(
                "UPDATE modify plan has an invalid or duplicate update column",
            ));
        }
        result.push(attno);
    }
    Ok(result.into_boxed_slice())
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
) -> Result<bool, ForeignModifyError> {
    if returning.is_null() {
        return Ok(false);
    }
    let mut attributes = ptr::null_mut();
    unsafe {
        pg_sys::pull_varattnos(
            returning.cast::<pg_sys::Node>(),
            result_relation,
            &mut attributes,
        );
    }
    let mut item_pointer_required = false;
    let mut bit = -1;
    loop {
        bit = unsafe { pg_sys::bms_next_member(attributes, bit) };
        if bit < 0 {
            break;
        }
        let attno = match pg_sys::AttrNumber::try_from(
            bit + pg_sys::FirstLowInvalidHeapAttributeNumber,
        ) {
            Ok(attno) => attno,
            Err(_) => {
                if !attributes.is_null() {
                    unsafe { pg_sys::bms_free(attributes) };
                }
                return Err(ForeignModifyError::framework(
                    "PlanForeignModify returning attribute exceeds PostgreSQL range",
                ));
            }
        };
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
                if !attributes.is_null() {
                    unsafe { pg_sys::bms_free(attributes) };
                }
                return Err(ForeignModifyError::unsupported(
                    "FDW framework v1 does not support this target-table system column in modify RETURNING",
                ));
            }
        }
    }
    if !attributes.is_null() {
        unsafe { pg_sys::bms_free(attributes) };
    }
    Ok(item_pointer_required)
}
