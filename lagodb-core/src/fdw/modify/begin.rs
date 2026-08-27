//! Begin-time construction of foreign modify executor state.

use core::ffi::c_int;
use core::ptr;

use pgrx::pg_sys;

use crate::handles::{RelationHandle, SnapshotHandle};

use super::super::row_identity::RowIdentityLayout;
use super::contract::{FdwModify, ForeignModifyOperation};
use super::error::ForeignModifyError;
use super::execution_context::{
    ForeignInsertBeginContext, ForeignModifyBeginContext,
};
use super::executor::{return_slot_required_for_modify, validate_updated_columns};
use super::planning::analyze_returning_system_columns;
use super::planning_context::ForeignModifyRelationContext;
use super::private::decode_modify_private;
use super::return_layout::ForeignModifyReturnLayout;
use super::return_requirements::ForeignModifyReturnRequirements;
use super::state::ForeignModifyStateWrapper;
use super::target_scan::ForeignModifyTargetScan;

impl<P: FdwModify> ForeignModifyStateWrapper<P> {
    /// Build and publish the normal INSERT/UPDATE/DELETE executor state.
    /// All planner/executor invariants used by row callbacks are established
    /// here, before the wrapper is published through `ResultRelInfo`.
    ///
    /// # Safety
    ///
    /// PostgreSQL must supply the live executor objects for one supported
    /// non-explain ModifyTable subplan. The private list must be the matching
    /// output of this framework's `PlanForeignModify` callback.
    pub(crate) unsafe fn begin(
        mtstate: *mut pg_sys::ModifyTableState,
        rinfo: *mut pg_sys::ResultRelInfo,
        fdw_private: *mut pg_sys::List,
        subplan_index: c_int,
        eflags: c_int,
    ) -> Result<(), ForeignModifyError> {
        if (eflags as u32) & pg_sys::EXEC_FLAG_EXPLAIN_ONLY != 0 {
            return Ok(());
        }

        let relation = unsafe { (*rinfo).ri_RelationDesc };
        let estate = unsafe { (*mtstate).ps.state };
        let plan = unsafe { (*mtstate).ps.plan } as *mut pg_sys::ModifyTable;
        let operation =
            ForeignModifyOperation::from_pg(unsafe { (*mtstate).operation })?;
        let query_context = unsafe { (*estate).es_query_cxt };
        let snapshot = unsafe { (*estate).es_snapshot };

        let decoded = unsafe { decode_modify_private::<P>(fdw_private) }?;
        if decoded.operation != operation {
            return Err(ForeignModifyError::framework(
                "FDW modify private data operation does not match the executor",
            ));
        }
        if decoded.relation_oid != unsafe { (*relation).rd_id } {
            return Err(ForeignModifyError::framework(
                "FDW modify private data relation does not match the executor",
            ));
        }
        unsafe { validate_updated_columns(&decoded.updated_columns, relation) }?;

        let (row_identity_layout, plan_tuple_desc) = match operation {
            ForeignModifyOperation::Insert => {
                (RowIdentityLayout::empty(), ptr::null_mut())
            }
            ForeignModifyOperation::Update | ForeignModifyOperation::Delete => {
                let subplan_state = unsafe { (*mtstate).ps.lefttree };
                let plan_slot = unsafe { (*subplan_state).ps_ResultTupleSlot };
                let plan_tuple_desc = unsafe { (*plan_slot).tts_tupleDescriptor };
                let targetlist = unsafe { (*(*subplan_state).plan).targetlist };
                let layout = unsafe {
                    RowIdentityLayout::from_targetlist(
                        targetlist,
                        relation,
                        (*rinfo).ri_RangeTableIndex,
                    )
                }?;
                unsafe { layout.validate_tuple_desc(plan_tuple_desc) }?;
                if layout.is_empty() {
                    return Err(ForeignModifyError::framework(
                        "UPDATE/DELETE modify subplan has no provider row identity",
                    ));
                }
                (layout, plan_tuple_desc)
            }
        };
        let return_requirements =
            if matches!(operation, ForeignModifyOperation::Delete) {
                unsafe {
                    ForeignModifyReturnRequirements::from_modify_plan(
                        plan,
                        relation,
                        (*rinfo).ri_RangeTableIndex,
                        subplan_index,
                    )
                }?
            } else {
                ForeignModifyReturnRequirements::default()
            };
        let return_layout = if matches!(operation, ForeignModifyOperation::Delete) {
            let subplan_state = unsafe { (*mtstate).ps.lefttree };
            let targetlist = unsafe { (*(*subplan_state).plan).targetlist };
            let layout = unsafe {
                ForeignModifyReturnLayout::from_targetlist(
                    targetlist,
                    relation,
                    (*rinfo).ri_RangeTableIndex,
                    &return_requirements,
                )
            }?;
            unsafe { layout.validate_tuple_desc(plan_tuple_desc) }?;
            layout
        } else {
            ForeignModifyReturnLayout::empty()
        };
        let return_slot_required =
            if matches!(operation, ForeignModifyOperation::Delete) {
                return_requirements.requires_row()
                    || decoded.returned_item_pointer_required
            } else {
                unsafe {
                    return_slot_required_for_modify(mtstate, rinfo, plan, operation)
                }
            };
        let per_tuple_context = unsafe { Self::per_tuple_context(estate) };
        let effective_user_id =
            unsafe { pg_sys::ExecGetResultRelCheckAsUser(rinfo, estate) };
        let target_scan = match operation {
            ForeignModifyOperation::Insert => None,
            ForeignModifyOperation::Update | ForeignModifyOperation::Delete => {
                unsafe {
                    ForeignModifyTargetScan::<P>::find(
                        (*mtstate).ps.lefttree,
                        (*relation).rd_id,
                        (*rinfo).ri_RangeTableIndex,
                    )
                }?
            }
        };
        // SAFETY: PostgreSQL keeps the live result relation open for the modify
        // state.
        let mut wrapper = unsafe {
            Self::new(
                decoded.private_data,
                relation,
                operation,
                (*estate).es_output_cid,
                decoded.updated_columns,
                row_identity_layout,
                plan_tuple_desc,
                per_tuple_context,
                decoded.returned_identity,
                decoded.returned_item_pointer_required,
                return_requirements,
                return_layout,
                return_slot_required,
            )
        };
        let context = ForeignModifyBeginContext::new(
            wrapper.private_data(),
            unsafe { RelationHandle::from_raw(relation) },
            unsafe { SnapshotHandle::from_raw(snapshot) },
            operation,
            &wrapper.updated_columns,
            wrapper.row_identity_layout.len(),
            wrapper.returned_identity,
            wrapper.returned_item_pointer_required,
            wrapper.return_requirements.columns(),
            wrapper.return_requirements.all_columns(),
            return_slot_required,
            subplan_index,
            eflags,
            effective_user_id,
            unsafe { (*estate).es_output_cid },
        );
        let entry_context = unsafe { pg_sys::MemoryContextSwitchTo(query_context) };
        let provider_state = P::begin_modify(context, target_scan);
        unsafe { pg_sys::MemoryContextSwitchTo(entry_context) };
        wrapper.install_provider_state(provider_state?);
        let wrapper_ptr = wrapper.leak_in(query_context);
        unsafe { (*rinfo).ri_FdwState = wrapper_ptr.cast() };
        Ok(())
    }

    /// Build and publish the state used by routed and COPY INSERT.
    ///
    /// # Safety
    ///
    /// PostgreSQL must supply the live executor objects for a supported INSERT
    /// route.
    pub(crate) unsafe fn begin_insert(
        mtstate: *mut pg_sys::ModifyTableState,
        rinfo: *mut pg_sys::ResultRelInfo,
    ) -> Result<(), ForeignModifyError> {
        if unsafe { (*mtstate).operation } != pg_sys::CmdType::CMD_INSERT {
            return Err(ForeignModifyError::unsupported(
                "FDW framework v1 does not support partition-routing inserts created by UPDATE or MERGE",
            ));
        }
        let plan = unsafe { (*mtstate).ps.plan } as *mut pg_sys::ModifyTable;
        if !plan.is_null()
            && (unsafe { (*plan).onConflictAction }
                != pg_sys::OnConflictAction::ONCONFLICT_NONE
                || !unsafe { (*plan).mergeActionLists }.is_null())
        {
            return Err(ForeignModifyError::unsupported(
                "FDW framework v1 does not support ON CONFLICT or MERGE",
            ));
        }
        let relation = unsafe { (*rinfo).ri_RelationDesc };
        let estate = unsafe { (*mtstate).ps.state };
        let relation_context =
            unsafe { ForeignModifyRelationContext::from_raw(relation) }?;
        if !P::capabilities(&relation_context)?.supports_insert() {
            return Err(ForeignModifyError::unsupported(
                "foreign provider does not support INSERT",
            ));
        }
        let query_context = unsafe { (*estate).es_query_cxt };
        let operation = ForeignModifyOperation::Insert;
        let returned_item_pointer_required = if plan.is_null() {
            false
        } else {
            let returning_lists = unsafe { (*plan).returningLists };
            if returning_lists.is_null() {
                false
            } else if unsafe { pg_sys::list_length(returning_lists) } != 1 {
                return Err(ForeignModifyError::framework(
                    "BeginForeignInsert has an invalid returning-list layout",
                ));
            } else {
                let root_result_relation = unsafe { (*mtstate).rootResultRelInfo };
                let returning = unsafe { pg_sys::list_nth(returning_lists, 0) }
                    as *mut pg_sys::List;
                unsafe {
                    analyze_returning_system_columns(
                        returning,
                        (*root_result_relation).ri_RangeTableIndex,
                    )?
                }
            }
        };
        let return_slot_required = unsafe {
            return_slot_required_for_modify(mtstate, rinfo, plan, operation)
        };
        let per_tuple_context = unsafe { Self::per_tuple_context(estate) };
        let effective_user_id =
            unsafe { pg_sys::ExecGetResultRelCheckAsUser(rinfo, estate) };
        let mut context = ForeignInsertBeginContext::new(
            unsafe { RelationHandle::from_raw(relation) },
            returned_item_pointer_required,
            effective_user_id,
        );
        let entry_context = unsafe { pg_sys::MemoryContextSwitchTo(query_context) };
        let provider_state = P::begin_insert(&mut context);
        unsafe { pg_sys::MemoryContextSwitchTo(entry_context) };
        let provider_state = provider_state?;
        if returned_item_pointer_required
            && !context.returned_identity().supports_item_pointer()
        {
            return Err(ForeignModifyError::unsupported(
                "foreign provider did not declare routed INSERT ctid support",
            ));
        }
        // SAFETY: PostgreSQL keeps the live routed-insert result relation open
        // for the insert state.
        let mut wrapper = unsafe {
            Self::new_insert(
                relation,
                context.returned_identity(),
                returned_item_pointer_required,
                return_slot_required,
                per_tuple_context,
                (*estate).es_output_cid,
            )
        };
        wrapper.install_provider_state(provider_state);
        let wrapper_ptr = wrapper.leak_in(query_context);
        unsafe { (*rinfo).ri_FdwState = wrapper_ptr.cast() };
        Ok(())
    }

    /// # Safety
    ///
    /// `estate` must be the live executor state for this Begin callback.
    unsafe fn per_tuple_context(
        estate: *mut pg_sys::EState,
    ) -> pg_sys::MemoryContext {
        let econtext = unsafe {
            if (*estate).es_per_tuple_exprcontext.is_null() {
                pg_sys::MakePerTupleExprContext(estate)
            } else {
                (*estate).es_per_tuple_exprcontext
            }
        };
        unsafe { (*econtext).ecxt_per_tuple_memory }
    }
}
