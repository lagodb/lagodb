//! Begin-time construction of foreign modify executor state.

use core::ffi::c_int;
use core::marker::PhantomData;

use pgrx::pg_sys;

use crate::handles::{ExecutorHandle, RelationHandle};

use super::super::payload::ProviderPayload;
use super::super::row_identity::RowIdentityLayout;
use super::contract::{FdwModify, ForeignModifyOperation};
use super::error::ForeignModifyError;
use super::execution_context::{ForeignInsertBeginContext, InsertCommand};
use super::executor::{return_slot_required_for_modify, validate_updated_columns};
use super::plan_private::decode_modify_private;
use super::planning::analyze_returning_system_columns;
use super::planning_context::ForeignModifyRelationContext;
use super::return_layout::ForeignModifyReturnLayout;
use super::row_layout::ModifyRowLayout;
use super::state::ForeignModifyStateWrapper;
use super::target_scan::{ForeignModifyTargetScan, ForeignModifyTargetScanMatch};

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

        let target_scan = match operation {
            ForeignModifyOperation::Insert => None,
            ForeignModifyOperation::Update | ForeignModifyOperation::Delete => {
                let target_scan = unsafe {
                    ForeignModifyTargetScan::<P>::find(
                        (*mtstate).ps.lefttree,
                        (*relation).rd_id,
                        (*rinfo).ri_RangeTableIndex,
                    )
                }?;
                match target_scan {
                    ForeignModifyTargetScanMatch::NotInitialized => {
                        // PostgreSQL can retain a result relation whose target
                        // scan was removed by planning or initial pruning. No
                        // initialized matching scan means this relation cannot
                        // supply a modify row; EndForeignModify already treats
                        // a NULL ri_FdwState as a no-op.
                        return Ok(());
                    }
                    ForeignModifyTargetScanMatch::Initialized(context) => context,
                }
            }
        };
        let row_identity_layout = match operation {
            ForeignModifyOperation::Insert => RowIdentityLayout::empty(),
            ForeignModifyOperation::Update | ForeignModifyOperation::Delete => {
                let subplan_state = unsafe { (*mtstate).ps.lefttree };
                let targetlist = unsafe { (*(*subplan_state).plan).targetlist };
                let layout = unsafe {
                    RowIdentityLayout::from_targetlist(
                        targetlist,
                        relation,
                        &decoded.row_identities,
                    )
                }?;
                if layout.is_empty() {
                    return Err(ForeignModifyError::framework(
                        "UPDATE/DELETE modify subplan has no provider row identity",
                    ));
                }
                layout
            }
        };
        let return_requirements = decoded.return_requirements;
        let return_layout = if matches!(operation, ForeignModifyOperation::Delete) {
            let subplan_state = unsafe { (*mtstate).ps.lefttree };
            let targetlist = unsafe { (*(*subplan_state).plan).targetlist };
            unsafe {
                ForeignModifyReturnLayout::from_targetlist(
                    targetlist,
                    relation,
                    &return_requirements,
                )
            }?
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
            unsafe { ExecutorHandle::from_raw(estate).result_relation_user(rinfo) };
        // SAFETY: PostgreSQL keeps the live result relation open for the modify
        // state.
        let row_layout = unsafe { ModifyRowLayout::from_relation(relation) };
        let mut wrapper = Self {
            payload: ProviderPayload::with_private(decoded.private_data),
            row_layout,
            operation,
            command_id: Some(unsafe { (*estate).es_output_cid }),
            updated_columns: decoded.updated_columns,
            row_identity_layout,
            per_tuple_context,
            returned_identity: decoded.returned_identity,
            returned_item_pointer_required: decoded.returned_item_pointer_required,
            return_requirements,
            return_layout,
            return_slot_required,
            _marker: PhantomData,
        };
        let context = wrapper.begin_context(
            unsafe { RelationHandle::from_raw(relation) },
            // SAFETY: normal ModifyTable initialization supplies a live
            // executor with its registered statement snapshot.
            unsafe { ExecutorHandle::from_raw(estate) },
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
                        operation,
                    )?
                }
            }
        };
        let return_slot_required = unsafe {
            return_slot_required_for_modify(mtstate, rinfo, plan, operation)
        };
        let per_tuple_context = unsafe { Self::per_tuple_context(estate) };
        let effective_user_id =
            unsafe { ExecutorHandle::from_raw(estate).result_relation_user(rinfo) };
        // COPY builds a plan-less temporary ModifyTable/EState without
        // es_output_cid or es_snapshot. Only normal tuple routing exposes an
        // ExecutorHandle; COPY providers request their CID independently.
        let command = if plan.is_null() {
            InsertCommand::CopyFrom
        } else {
            InsertCommand::ModifyTable {
                // SAFETY: this route belongs to a normal ModifyTable with
                // a live executor and registered statement snapshot.
                executor: unsafe { ExecutorHandle::from_raw(estate) },
                command_id: unsafe { (*estate).es_output_cid },
            }
        };
        let mut context = ForeignInsertBeginContext::new(
            unsafe { RelationHandle::from_raw(relation) },
            returned_item_pointer_required,
            effective_user_id,
            command,
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
