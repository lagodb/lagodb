//! PostgreSQL executor callbacks for the FDW modify capability.

use core::ffi::c_int;

use pgrx::pg_guard;
use pgrx::pg_sys;

use super::super::row_identity::ModifyPlanSlot;
use super::contract::FdwModify;
use super::contract::ForeignModifyState;
use super::error::{ForeignModifyError, ForeignModifyPhase};
use super::executor::{
    end_modify, map_outcome, state_wrapper_unchecked, with_modify_per_tuple_context,
};
use super::slot::{ForeignInsertBatch, ModifySlot};
use super::state::ForeignModifyStateWrapper;

#[pg_guard]
/// # Safety
///
/// PostgreSQL supplies live executor nodes and a private-data list produced by
/// the matching `PlanForeignModify` callback.
pub(crate) unsafe extern "C-unwind" fn begin_foreign_modify<P: FdwModify>(
    mtstate: *mut pg_sys::ModifyTableState,
    rinfo: *mut pg_sys::ResultRelInfo,
    fdw_private: *mut pg_sys::List,
    subplan_index: c_int,
    eflags: c_int,
) {
    let result = unsafe {
        ForeignModifyStateWrapper::<P>::begin(
            mtstate,
            rinfo,
            fdw_private,
            subplan_index,
            eflags,
        )
    };

    if let Err(error) = result {
        error
            .with_provider_phase::<P>(ForeignModifyPhase::Begin)
            .report();
    }
}

#[pg_guard]
/// # Safety
///
/// PostgreSQL supplies live executor objects for a foreign relation that is
/// being inserted through COPY or tuple routing.
pub(crate) unsafe extern "C-unwind" fn begin_foreign_insert<P: FdwModify>(
    mtstate: *mut pg_sys::ModifyTableState,
    rinfo: *mut pg_sys::ResultRelInfo,
) {
    let result =
        unsafe { ForeignModifyStateWrapper::<P>::begin_insert(mtstate, rinfo) };

    if let Err(error) = result {
        error
            .with_provider_phase::<P>(ForeignModifyPhase::BeginInsert)
            .report();
    }
}

#[pg_guard]
/// # Safety
///
/// PostgreSQL supplies live slots from the current ModifyTable execution. The
/// state pointer was installed by `begin_foreign_modify` or
/// `begin_foreign_insert` for this provider.
pub(crate) unsafe extern "C-unwind" fn exec_foreign_insert<P: FdwModify>(
    _estate: *mut pg_sys::EState,
    rinfo: *mut pg_sys::ResultRelInfo,
    slot: *mut pg_sys::TupleTableSlot,
    _plan_slot: *mut pg_sys::TupleTableSlot,
) -> *mut pg_sys::TupleTableSlot {
    let result = {
        let wrapper = unsafe { &mut *state_wrapper_unchecked::<P>(rinfo) };
        let state_ptr = unsafe { wrapper.provider_state_ptr_unchecked() };
        let per_tuple_context = wrapper.per_tuple_context;
        unsafe {
            with_modify_per_tuple_context(per_tuple_context, |conversion_context| {
                let return_slot_required = wrapper.return_slot_required;
                let operation = wrapper.operation;
                let command_id = wrapper.command_id;
                let mut row = {
                    ModifySlot::from_raw(
                        slot,
                        conversion_context,
                        &mut wrapper.row_layout,
                        wrapper.returned_item_pointer_required,
                    )
                };
                let outcome = {
                    let state = &mut *state_ptr;
                    state.prepare_insert(&mut row)?;
                    state.insert(&mut row)?
                };
                map_outcome(
                    operation,
                    command_id,
                    return_slot_required,
                    &mut row,
                    outcome,
                )
            })
        }
    };

    match result {
        Ok(slot) => slot,
        Err(error) => error
            .with_provider_phase::<P>(ForeignModifyPhase::Insert)
            .report(),
    }
}

#[pg_guard]
/// # Safety
///
/// PostgreSQL supplies a live input-slot array and count for a foreign INSERT
/// state initialized by `BeginForeignModify` or `BeginForeignInsert`.
pub(crate) unsafe extern "C-unwind" fn exec_foreign_batch_insert<P: FdwModify>(
    _estate: *mut pg_sys::EState,
    rinfo: *mut pg_sys::ResultRelInfo,
    slots: *mut *mut pg_sys::TupleTableSlot,
    _plan_slots: *mut *mut pg_sys::TupleTableSlot,
    num_slots: *mut c_int,
) -> *mut *mut pg_sys::TupleTableSlot {
    let result = {
        let wrapper = unsafe { &mut *state_wrapper_unchecked::<P>(rinfo) };
        let state_ptr = unsafe { wrapper.provider_state_ptr_unchecked() };
        let per_tuple_context = wrapper.per_tuple_context;
        let input_count = unsafe { *num_slots };
        let input_count = input_count as usize;
        unsafe {
            with_modify_per_tuple_context(per_tuple_context, |conversion_context| {
                let mut batch = ForeignInsertBatch::from_raw(
                    slots,
                    input_count,
                    &mut wrapper.row_layout,
                    conversion_context,
                    wrapper.returned_item_pointer_required,
                    wrapper.return_slot_required,
                );
                (&mut *state_ptr).insert_batch(&mut batch)?;
                Ok(batch.finish())
            })
        }
    };

    match result {
        Ok((slots, output_count)) => {
            // `output_count` cannot exceed the input count supplied by
            // PostgreSQL, which is represented by c_int at this ABI.
            let output_count = c_int::try_from(output_count)
                .expect("foreign batch output count exceeds PostgreSQL c_int");
            unsafe { *num_slots = output_count };
            slots
        }
        Err(error) => error
            .with_provider_phase::<P>(ForeignModifyPhase::BatchInsert)
            .report(),
    }
}

#[pg_guard]
/// # Safety
///
/// PostgreSQL invokes this callback after the matching foreign INSERT state
/// has been initialized. In `EXPLAIN` without execution PostgreSQL may leave
/// `ri_FdwState` NULL; that path intentionally returns the conservative
/// single-row size without calling the provider.
pub(crate) unsafe extern "C-unwind" fn get_foreign_modify_batch_size<P: FdwModify>(
    rinfo: *mut pg_sys::ResultRelInfo,
) -> c_int {
    let result = (|| {
        let state = unsafe { (*rinfo).ri_FdwState };
        if state.is_null() {
            return Ok(1);
        }

        let wrapper = unsafe { &mut *(state as *mut ForeignModifyStateWrapper<P>) };
        let state_ptr = unsafe { wrapper.provider_state_ptr_unchecked() };
        let requested = unsafe { (&*state_ptr).batch_size()? };
        if requested < 1 {
            return Err(ForeignModifyError::framework(
                "foreign provider returned a batch size smaller than one",
            ));
        }

        // BEFORE ROW triggers must observe rows already inserted by this
        // statement, and INSTEAD OF ROW triggers replace the FDW insert path.
        // Keep those paths on PostgreSQL's ordinary row execution. AFTER ROW
        // triggers do support batching: the executor queues one event for each
        // slot returned by ExecForeignBatchInsert, and the wrapper's
        // `return_slot_required` contract preserves those relation-shaped slots.
        let has_pre_fdw_row_trigger = unsafe {
            let triggers = (*rinfo).ri_TrigDesc;
            !triggers.is_null()
                && ((*triggers).trig_insert_before_row
                    || (*triggers).trig_insert_instead_row)
        };
        let batching_forbidden = unsafe {
            !(*rinfo).ri_projectReturning.is_null()
                || !(*rinfo).ri_WithCheckOptions.is_null()
        } || has_pre_fdw_row_trigger
            || !wrapper.row_layout.has_live_attributes()
            || wrapper.returned_item_pointer_required;
        Ok(if batching_forbidden { 1 } else { requested })
    })();

    match result {
        Ok(size) => size,
        Err(error) => error
            .with_provider_phase::<P>(ForeignModifyPhase::BatchInsert)
            .report(),
    }
}

#[pg_guard]
/// # Safety
///
/// PostgreSQL supplies the relation-shaped new row and the non-NULL plan slot
/// produced by this UPDATE's ModifyTable subplan.
pub(crate) unsafe extern "C-unwind" fn exec_foreign_update<P: FdwModify>(
    _estate: *mut pg_sys::EState,
    rinfo: *mut pg_sys::ResultRelInfo,
    slot: *mut pg_sys::TupleTableSlot,
    plan_slot: *mut pg_sys::TupleTableSlot,
) -> *mut pg_sys::TupleTableSlot {
    let result = {
        let wrapper = unsafe { &mut *state_wrapper_unchecked::<P>(rinfo) };
        let state_ptr = unsafe { wrapper.provider_state_ptr_unchecked() };
        let per_tuple_context = wrapper.per_tuple_context;
        unsafe {
            with_modify_per_tuple_context(per_tuple_context, |conversion_context| {
                let return_slot_required = wrapper.return_slot_required;
                let operation = wrapper.operation;
                let command_id = wrapper.command_id;
                let mut row = {
                    ModifySlot::from_raw(
                        slot,
                        conversion_context,
                        &mut wrapper.row_layout,
                        wrapper.returned_item_pointer_required,
                    )
                };
                let plan_view = {
                    ModifyPlanSlot::from_raw_unchecked(
                        plan_slot,
                        &wrapper.row_identity_layout,
                        wrapper.plan_tuple_desc,
                    )
                };
                let outcome = {
                    let state = &mut *state_ptr;
                    state.prepare_update(&mut row, &plan_view)?;
                    state.update(&mut row, &plan_view)?
                };
                map_outcome(
                    operation,
                    command_id,
                    return_slot_required,
                    &mut row,
                    outcome,
                )
            })
        }
    };

    match result {
        Ok(slot) => slot,
        Err(error) => error
            .with_provider_phase::<P>(ForeignModifyPhase::Update)
            .report(),
    }
}

#[pg_guard]
/// # Safety
///
/// PostgreSQL invokes this callback after the modify state has been used. The
/// state pointer, when non-NULL, was installed by this provider's Begin hook.
pub(crate) unsafe extern "C-unwind" fn end_foreign_modify<P: FdwModify>(
    estate: *mut pg_sys::EState,
    rinfo: *mut pg_sys::ResultRelInfo,
) {
    unsafe { end_modify::<P>(estate, rinfo, ForeignModifyPhase::End) };
}

#[pg_guard]
/// # Safety
///
/// PostgreSQL invokes this callback after a routed/COPY insert state has been
/// used. The state pointer, when non-NULL, was installed by BeginForeignInsert.
pub(crate) unsafe extern "C-unwind" fn end_foreign_insert<P: FdwModify>(
    estate: *mut pg_sys::EState,
    rinfo: *mut pg_sys::ResultRelInfo,
) {
    unsafe { end_modify::<P>(estate, rinfo, ForeignModifyPhase::EndInsert) };
}
