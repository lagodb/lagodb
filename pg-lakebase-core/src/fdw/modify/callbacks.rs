//! PostgreSQL executor callbacks for the FDW modify capability.

use core::ffi::c_int;

use pgrx::pg_guard;
use pgrx::pg_sys;

use super::super::row_identity::ModifyPlanSlot;
use super::contract::ForeignModifyState;
use super::contract::{FdwModify, ForeignModifyOperation};
use super::error::ForeignModifyPhase;
use super::executor::{
    end_modify, map_outcome, state_wrapper_unchecked, with_modify_per_tuple_context,
};
use super::slot::ModifySlot;
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
    let prior_context = unsafe { pg_sys::CurrentMemoryContext };
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
            .report_after_switch(prior_context);
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
    let prior_context = unsafe { pg_sys::CurrentMemoryContext };
    let result =
        unsafe { ForeignModifyStateWrapper::<P>::begin_insert(mtstate, rinfo) };

    if let Err(error) = result {
        error
            .with_provider_phase::<P>(ForeignModifyPhase::BeginInsert)
            .report_after_switch(prior_context);
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
    let prior_context = unsafe { pg_sys::CurrentMemoryContext };
    let result = {
        let wrapper = unsafe { &mut *state_wrapper_unchecked::<P>(rinfo) };
        debug_assert_eq!(wrapper.operation, ForeignModifyOperation::Insert);
        let state_ptr = unsafe { wrapper.provider_state_ptr_unchecked() };
        let per_tuple_context = wrapper.per_tuple_context;
        unsafe {
            with_modify_per_tuple_context(per_tuple_context, |conversion_context| {
                let return_slot_required = wrapper.return_slot_required;
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
                map_outcome(return_slot_required, &mut row, outcome)
            })
        }
    };

    match result {
        Ok(slot) => slot,
        Err(error) => error
            .with_provider_phase::<P>(ForeignModifyPhase::Insert)
            .report_after_switch(prior_context),
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
    let prior_context = unsafe { pg_sys::CurrentMemoryContext };
    let result = {
        let wrapper = unsafe { &mut *state_wrapper_unchecked::<P>(rinfo) };
        debug_assert_eq!(wrapper.operation, ForeignModifyOperation::Update);
        let state_ptr = unsafe { wrapper.provider_state_ptr_unchecked() };
        let per_tuple_context = wrapper.per_tuple_context;
        unsafe {
            with_modify_per_tuple_context(per_tuple_context, |conversion_context| {
                let return_slot_required = wrapper.return_slot_required;
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
                map_outcome(return_slot_required, &mut row, outcome)
            })
        }
    };

    match result {
        Ok(slot) => slot,
        Err(error) => error
            .with_provider_phase::<P>(ForeignModifyPhase::Update)
            .report_after_switch(prior_context),
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
