//! Non-FFI modify executor orchestration and state validation.

use core::ptr;

use pgrx::memcxt::PgMemoryContexts;
use pgrx::pg_sys;

use super::contract::{
    FdwModify, ForeignModifyOperation, ForeignModifyOutcome, ForeignModifyState,
};
use super::error::{ForeignModifyError, ForeignModifyPhase};
use super::slot::ModifySlot;
use super::state::ForeignModifyStateWrapper;

/// Return the Begin-published wrapper without repeating lifecycle checks.
///
/// # Safety
///
/// `rinfo` must be the live result relation for a callback that completed
/// Begin, and its `ri_FdwState` must point to `ForeignModifyStateWrapper<P>`.
pub(super) unsafe fn state_wrapper_unchecked<P: FdwModify>(
    rinfo: *mut pg_sys::ResultRelInfo,
) -> *mut ForeignModifyStateWrapper<P> {
    debug_assert!(!rinfo.is_null());
    let state = unsafe { (*rinfo).ri_FdwState };
    debug_assert!(!state.is_null());
    state as *mut ForeignModifyStateWrapper<P>
}

/// Run one modify callback in the Begin-cached per-tuple memory context.
///
/// # Safety
///
/// `per_tuple_context` must be a live PostgreSQL per-tuple memory context
/// initialized by `ForeignModifyStateWrapper::begin` or `begin_insert`.
pub(super) unsafe fn with_modify_per_tuple_context<T, F>(
    per_tuple_context: pg_sys::MemoryContext,
    operation: F,
) -> Result<T, ForeignModifyError>
where
    F: FnOnce(pg_sys::MemoryContext) -> Result<T, ForeignModifyError>,
{
    debug_assert!(!per_tuple_context.is_null());

    let entry_context = unsafe { pg_sys::CurrentMemoryContext };
    let result = if entry_context == per_tuple_context {
        operation(per_tuple_context)
    } else {
        // SAFETY: PostgreSQL owns the live per-tuple context, and the
        // callback-scoped operation does not let its row borrow escape.
        unsafe {
            PgMemoryContexts::For(per_tuple_context)
                .switch_to(|_| operation(per_tuple_context))
        }
    };
    if unsafe { pg_sys::CurrentMemoryContext } != entry_context {
        // SAFETY: entry_context was the context active on callback entry.
        unsafe { pg_sys::MemoryContextSwitchTo(entry_context) };
    }
    result
}

pub(super) fn validate_updated_columns(
    columns: &[pg_sys::AttrNumber],
    relation: pg_sys::Relation,
) -> Result<(), ForeignModifyError> {
    if relation.is_null() {
        return Err(ForeignModifyError::framework(
            "foreign modify state has no result relation",
        ));
    }
    let tuple_desc = unsafe { (*relation).rd_att };
    if tuple_desc.is_null() {
        return Err(ForeignModifyError::framework(
            "foreign modify relation has no tuple descriptor",
        ));
    }
    let natts = unsafe { (*tuple_desc).natts };
    if natts < 0 || unsafe { (*tuple_desc).attrs.as_ptr().is_null() } {
        return Err(ForeignModifyError::framework(
            "foreign modify relation has an invalid tuple descriptor",
        ));
    }
    let natts = natts as usize;
    for &attno in columns {
        let index = usize::try_from(attno as i32 - 1).map_err(|_| {
            ForeignModifyError::framework(
                "foreign modify state contains a non-positive updated column",
            )
        })?;
        if index >= natts {
            return Err(ForeignModifyError::framework(
                "foreign modify state contains an updated column outside the relation",
            ));
        }
        if unsafe { (*tuple_desc).attrs.as_ptr().add(index).read().attisdropped } {
            return Err(ForeignModifyError::framework(
                "foreign modify state updates a dropped relation column",
            ));
        }
    }
    Ok(())
}

pub(super) fn map_outcome(
    return_slot_required: bool,
    row: &mut ModifySlot<'_>,
    outcome: ForeignModifyOutcome,
) -> Result<*mut pg_sys::TupleTableSlot, ForeignModifyError> {
    match outcome {
        ForeignModifyOutcome::Applied => {
            let slot = row.as_raw();
            if slot.is_null() {
                return Err(ForeignModifyError::framework(
                    "foreign modify provider applied a NULL row slot",
                ));
            }
            if unsafe { (*slot).tts_flags as u32 & pg_sys::TTS_FLAG_EMPTY != 0 } {
                return Err(ForeignModifyError::framework(
                    "foreign modify provider applied an empty row slot",
                ));
            }
            row.finish(return_slot_required)?;
            Ok(slot)
        }
        ForeignModifyOutcome::Skipped => Ok(ptr::null_mut()),
    }
}

pub(super) fn return_slot_required_for_modify(
    mtstate: *mut pg_sys::ModifyTableState,
    rinfo: *mut pg_sys::ResultRelInfo,
    plan: *mut pg_sys::ModifyTable,
    operation: ForeignModifyOperation,
) -> bool {
    if rinfo.is_null() {
        return false;
    }
    // BeginForeignModify runs before ExecInitModifyTable initializes
    // ResultRelInfo.ri_WithCheckOptions and ri_projectReturning.  The plan
    // lists are already available at that boundary and are also the only
    // source available for routed/COPY setup where ResultRelInfo is initialized
    // separately.
    let plan_requires_stored_row = !plan.is_null()
        && unsafe {
            !(*plan).returningLists.is_null()
                || !(*plan).withCheckOptionLists.is_null()
        };
    let transition_capture =
        !mtstate.is_null() && unsafe { !(*mtstate).mt_transition_capture.is_null() };
    let after_row_trigger = unsafe {
        let trigger = (*rinfo).ri_TrigDesc;
        !trigger.is_null()
            && match operation {
                ForeignModifyOperation::Insert => (*trigger).trig_insert_after_row,
                ForeignModifyOperation::Update => (*trigger).trig_update_after_row,
                ForeignModifyOperation::Delete => false,
            }
    };
    plan_requires_stored_row || after_row_trigger || transition_capture
}

pub(super) unsafe fn end_modify<P: FdwModify>(
    estate: *mut pg_sys::EState,
    rinfo: *mut pg_sys::ResultRelInfo,
    phase: ForeignModifyPhase,
) {
    let _ = estate;
    if rinfo.is_null() {
        return;
    }
    let state = unsafe { (*rinfo).ri_FdwState };
    if state.is_null() {
        return;
    }
    unsafe { (*rinfo).ri_FdwState = ptr::null_mut() };
    let wrapper = unsafe { &mut *(state as *mut ForeignModifyStateWrapper<P>) };
    let prior_context = unsafe { pg_sys::CurrentMemoryContext };
    let result = if wrapper.payload.provider_state_initialized() {
        let state_ptr = wrapper
            .payload
            .provider_state_ptr()
            .expect("provider state flag must have a pointer");
        unsafe { (&mut *state_ptr).finish() }
    } else {
        Ok(())
    };
    unsafe { pg_sys::MemoryContextSwitchTo(prior_context) };
    wrapper.cleanup_payloads();
    if let Err(error) = result {
        error
            .with_provider_phase::<P>(phase)
            .report_after_switch(prior_context);
    }
}
