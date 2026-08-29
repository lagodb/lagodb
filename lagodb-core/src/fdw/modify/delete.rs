//! PostgreSQL DELETE callback bridge.

use core::ptr;

use pgrx::pg_guard;
use pgrx::pg_sys;

use super::super::row_identity::ModifyPlanSlot;
use super::contract::{FdwModify, ForeignModifyOutcome, ForeignModifyState};
use super::error::{ForeignModifyError, ForeignModifyPhase};
use super::executor::{state_wrapper_unchecked, with_modify_per_tuple_context};
use super::slot::ModifySlot;

/// # Safety
///
/// PostgreSQL supplies the relation-shaped returning slot, the DELETE
/// ModifyTable plan slot, and state initialized by `BeginForeignModify`.
#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn exec_foreign_delete<P: FdwModify>(
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
                let operation = wrapper.operation;
                let command_id = wrapper.command_id;
                let plan_view = {
                    ModifyPlanSlot::from_raw_unchecked(
                        plan_slot,
                        &wrapper.row_identity_layout,
                        wrapper.plan_tuple_desc,
                    )
                };

                if wrapper.return_slot_required {
                    let mut row = {
                        ModifySlot::from_delete_raw(
                            slot,
                            conversion_context,
                            &mut wrapper.row_layout,
                            wrapper.returned_item_pointer_required,
                        )
                    };
                    if wrapper.return_layout.has_plan_values() {
                        wrapper
                            .return_layout
                            .populate_from_plan_slot(&plan_view, &mut row);
                    }
                    let outcome = {
                        let state = &mut *state_ptr;
                        state.prepare_delete(Some(&mut row), &plan_view)?;
                        state.delete(Some(&mut row), &plan_view)?
                    };
                    match outcome {
                        ForeignModifyOutcome::Applied => {
                            row.finish(true)?;
                            let returned_slot = row.as_raw();
                            if (*returned_slot).tts_flags as u32
                                & pg_sys::TTS_FLAG_EMPTY
                                != 0
                            {
                                return Err(ForeignModifyError::framework(
                                    "foreign DELETE provider applied an empty returned row",
                                ));
                            }
                            Ok(returned_slot)
                        }
                        ForeignModifyOutcome::Skipped => Ok(ptr::null_mut()),
                        ForeignModifyOutcome::SelfModified {
                            modifying_command_id,
                        } if modifying_command_id == command_id => {
                            Ok(ptr::null_mut())
                        }
                        ForeignModifyOutcome::SelfModified { .. } => {
                            Err(ForeignModifyError::self_modified(operation))
                        }
                    }
                } else {
                    let outcome = {
                        let state = &mut *state_ptr;
                        state.prepare_delete(None, &plan_view)?;
                        state.delete(None, &plan_view)?
                    };
                    match outcome {
                        // PostgreSQL initializes this empty slot itself when it
                        // evaluates DELETE RETURNING or tableoid.
                        ForeignModifyOutcome::Applied => Ok(slot),
                        ForeignModifyOutcome::Skipped => Ok(ptr::null_mut()),
                        ForeignModifyOutcome::SelfModified {
                            modifying_command_id,
                        } if modifying_command_id == command_id => {
                            Ok(ptr::null_mut())
                        }
                        ForeignModifyOutcome::SelfModified { .. } => {
                            Err(ForeignModifyError::self_modified(operation))
                        }
                    }
                }
            })
        }
    };

    match result {
        Ok(slot) => slot,
        Err(error) => error
            .with_provider_phase::<P>(ForeignModifyPhase::Delete)
            .report(),
    }
}
