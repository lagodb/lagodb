//! Data modification callback wrappers for AmDml trait
//!
//! This module provides wrapper functions for INSERT, UPDATE, DELETE operations
//! with lazy initialization and automatic cleanup via memory context callbacks.

use crate::api::AmDml;
use crate::data::Row;
use crate::handles::{
    BulkInsertStateHandle, ItemPointer, SnapshotHandle, TM_FailureData,
};
use crate::diag::{report_warning, ReportableError};
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::*;
use std::any::Any;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;

struct ModifySession {
    state: Box<dyn Any>,
    row_buffer: Row,
}

thread_local! {
    static MODIFY_STATES: RefCell<HashMap<pg_sys::Oid, Box<ModifySession>>> = RefCell::new(HashMap::new());
    static LAST_USED_SESSION: Cell<Option<(pg_sys::Oid, *mut ModifySession)>> = const { Cell::new(None) };
}

#[pg_guard]
extern "C-unwind" fn cleanup_modify_state<E, T>(arg: *mut ::core::ffi::c_void)
where
    E: Into<ErrorReport>,
    T: AmDml<E> + 'static,
{
    let relid = pg_sys::Oid::from(arg as usize as u32);

    LAST_USED_SESSION.with(|last| {
        if let Some((cached_relid, _)) = last.get() {
            if cached_relid == relid {
                last.set(None);
            }
        }
    });

    MODIFY_STATES.with(|states| {
        let mut map = states.borrow_mut();
        if let Some(boxed_session) = map.remove(&relid) {
            let boxed_state = boxed_session.state;
            // Downcast to actual type
            if let Ok(mut state) = boxed_state.downcast::<T>() {
                if let Err(e) = state.end_modify() {
                    report_warning(&format!(
                        "end_modify failed for relation {}: {:?}",
                        relid,
                        e.into()
                    ));
                }
            }
        }
    });
    // State is dropped here, calling Drop if implemented
}

fn get_or_create_session<E, T>(rel: pg_sys::Relation) -> Result<*mut ModifySession, E>
where
    E: Into<ErrorReport>,
    T: AmDml<E> + 'static,
{
    unsafe {
        let relid = (*rel).rd_id;

        if let Some((cached_relid, cached_ptr)) =
            LAST_USED_SESSION.with(|last| last.get())
        {
            if cached_relid == relid {
                return Ok(cached_ptr);
            }
        }

        // Slow path: check HashMap and initialize if needed
        let needs_init =
            MODIFY_STATES.with(|states| !states.borrow().contains_key(&relid));

        if needs_init {
            let mut instance = T::new(rel)?;
            instance.begin_modify()?;

            let session = Box::new(ModifySession {
                state: Box::new(instance),
                row_buffer: Row::new(),
            });

            MODIFY_STATES.with(|states| {
                states.borrow_mut().insert(relid, session);
            });

            let callback = pg_sys::palloc0(std::mem::size_of::<
                pg_sys::MemoryContextCallback,
            >()) as *mut pg_sys::MemoryContextCallback;

            (*callback).func = Some(cleanup_modify_state::<E, T>);
            (*callback).arg = relid.to_u32() as usize as *mut ::core::ffi::c_void;

            pg_sys::MemoryContextRegisterResetCallback(
                pg_sys::CurrentMemoryContext,
                callback,
            );
        }

        let session_ptr = MODIFY_STATES.with(|states| {
            let mut map = states.borrow_mut();
            let boxed_session = map
                .get_mut(&relid)
                .expect("Session should exist after initialization");
            boxed_session.as_mut() as *mut ModifySession
        });

        LAST_USED_SESSION.with(|last| last.set(Some((relid, session_ptr))));
        Ok(session_ptr)
    }
}

#[pg_guard]
pub extern "C-unwind" fn tuple_insert<E, T>(
    rel: pg_sys::Relation,
    slot: *mut pg_sys::TupleTableSlot,
    cid: pg_sys::CommandId,
    options: ::core::ffi::c_int,
    bistate: *mut pg_sys::BulkInsertStateData,
) where
    E: Into<ErrorReport>,
    T: AmDml<E> + 'static,
{
    unsafe {
        let session = get_or_create_session::<E, T>(rel).report_unwrap();

        // Convert bistate to Handle if not null
        let mut bistate_handle =
            (!bistate.is_null()).then(|| BulkInsertStateHandle::from_raw(bistate));

        // Update reused row buffer from slot
        (*session).row_buffer.update_from_slot(slot);

        let state = (*session)
            .state
            .downcast_mut::<T>()
            .expect("State type mismatch - this is a bug");

        state
            .tuple_insert(
                &(*session).row_buffer,
                cid,
                options,
                bistate_handle.as_mut(),
            )
            .report_unwrap();
    }
}

#[pg_guard]
pub extern "C-unwind" fn tuple_insert_speculative<E, T>(
    rel: pg_sys::Relation,
    slot: *mut pg_sys::TupleTableSlot,
    cid: pg_sys::CommandId,
    options: ::core::ffi::c_int,
    bistate: *mut pg_sys::BulkInsertStateData,
    _spec_token: u32,
) where
    E: Into<ErrorReport>,
    T: AmDml<E> + 'static,
{
    unsafe {
        let session = get_or_create_session::<E, T>(rel).report_unwrap();

        let mut bistate_handle =
            (!bistate.is_null()).then(|| BulkInsertStateHandle::from_raw(bistate));

        // Update reused row buffer from slot
        (*session).row_buffer.update_from_slot(slot);

        let state = (*session)
            .state
            .downcast_mut::<T>()
            .expect("State type mismatch - this is a bug");

        state
            .tuple_insert(
                &(*session).row_buffer,
                cid,
                options,
                bistate_handle.as_mut(),
            )
            .report_unwrap();
    }
}

#[pg_guard]
pub extern "C-unwind" fn tuple_complete_speculative<E, T>(
    rel: pg_sys::Relation,
    slot: *mut pg_sys::TupleTableSlot,
    spec_token: u32,
    succeeded: bool,
) where
    E: Into<ErrorReport>,
    T: AmDml<E> + 'static,
{
    unsafe {
        let session = get_or_create_session::<E, T>(rel).report_unwrap();

        // Update reused row buffer from slot
        (*session).row_buffer.update_from_slot(slot);

        let state = (*session)
            .state
            .downcast_mut::<T>()
            .expect("State type mismatch - this is a bug");

        state
            .tuple_complete_speculative(&(*session).row_buffer, spec_token, succeeded)
            .report_unwrap();
    }
}

#[pg_guard]
pub extern "C-unwind" fn multi_insert<E, T>(
    rel: pg_sys::Relation,
    slots: *mut *mut pg_sys::TupleTableSlot,
    nslots: ::core::ffi::c_int,
    cid: pg_sys::CommandId,
    options: ::core::ffi::c_int,
    bistate: *mut pg_sys::BulkInsertStateData,
) where
    E: Into<ErrorReport>,
    T: AmDml<E> + 'static,
{
    unsafe {
        let session = get_or_create_session::<E, T>(rel).report_unwrap();

        // For multi_insert, we still need to allocate vector of Rows as the trait expects slice.
        // Optimization: we could reuse a Vec<Row> in ModifySession if multi_insert is frequent,
        // but each Row inside still needs own storage unless we redesign multi_insert interface.
        // For now, we keep existing behavior for multi_insert but use Row::from_slot which is now safer.
        let slots_slice = std::slice::from_raw_parts(slots, nslots as usize);
        let rows: Vec<Row> = slots_slice
            .iter()
            .map(|&slot| Row::from_slot(slot))
            .collect();

        let mut bistate_handle =
            (!bistate.is_null()).then(|| BulkInsertStateHandle::from_raw(bistate));

        let state = (*session)
            .state
            .downcast_mut::<T>()
            .expect("State type mismatch - this is a bug");

        state
            .multi_insert(&rows, cid, options, bistate_handle.as_mut())
            .report_unwrap();
    }
}

#[pg_guard]
pub extern "C-unwind" fn tuple_delete<E, T>(
    rel: pg_sys::Relation,
    tid: pg_sys::ItemPointer,
    cid: pg_sys::CommandId,
    snapshot: pg_sys::Snapshot,
    crosscheck: pg_sys::Snapshot,
    wait: bool,
    tmfd: *mut pg_sys::TM_FailureData,
    changing_part: bool,
) -> pg_sys::TM_Result::Type
where
    E: Into<ErrorReport>,
    T: AmDml<E> + 'static,
{
    unsafe {
        let session = get_or_create_session::<E, T>(rel).report_unwrap();

        let tid = ItemPointer::from_raw(tid);
        let snapshot_handle = SnapshotHandle::from_raw(snapshot);
        let crosscheck_handle =
            (!crosscheck.is_null()).then(|| SnapshotHandle::from_raw(crosscheck));
        let mut tmfd_rust = TM_FailureData::default();

        let state = (*session)
            .state
            .downcast_mut::<T>()
            .expect("State type mismatch - this is a bug");

        let result = state
            .tuple_delete(
                &tid,
                cid,
                &snapshot_handle,
                crosscheck_handle.as_ref(),
                wait,
                &mut tmfd_rust,
                changing_part,
            )
            .report_unwrap();

        tmfd_rust.write_to_ptr(tmfd);

        result
    }
}

#[pg_guard]
pub extern "C-unwind" fn tuple_update<E, T>(
    rel: pg_sys::Relation,
    otid: pg_sys::ItemPointer,
    slot: *mut pg_sys::TupleTableSlot,
    cid: pg_sys::CommandId,
    snapshot: pg_sys::Snapshot,
    crosscheck: pg_sys::Snapshot,
    wait: bool,
    tmfd: *mut pg_sys::TM_FailureData,
    lockmode: *mut pg_sys::LockTupleMode::Type,
    update_indexes: *mut pg_sys::TU_UpdateIndexes::Type,
) -> pg_sys::TM_Result::Type
where
    E: Into<ErrorReport>,
    T: AmDml<E> + 'static,
{
    unsafe {
        let session = get_or_create_session::<E, T>(rel).report_unwrap();

        let otid = ItemPointer::from_raw(otid);
        // Update buffer from slot
        let snapshot_handle = SnapshotHandle::from_raw(snapshot);
        let crosscheck_handle =
            (!crosscheck.is_null()).then(|| SnapshotHandle::from_raw(crosscheck));
        let mut tmfd_rust = TM_FailureData::default();

        (*session).row_buffer.update_from_slot(slot);

        let state = (*session)
            .state
            .downcast_mut::<T>()
            .expect("State type mismatch - this is a bug");

        let result = state
            .tuple_update(
                &otid,
                &(*session).row_buffer,
                cid,
                &snapshot_handle,
                crosscheck_handle.as_ref(),
                wait,
                &mut tmfd_rust,
                &mut *lockmode,
                &mut *update_indexes,
            )
            .report_unwrap();

        tmfd_rust.write_to_ptr(tmfd);

        result
    }
}

#[pg_guard]
pub extern "C-unwind" fn tuple_lock<E, T>(
    rel: pg_sys::Relation,
    tid: pg_sys::ItemPointer,
    snapshot: pg_sys::Snapshot,
    slot: *mut pg_sys::TupleTableSlot,
    cid: pg_sys::CommandId,
    mode: pg_sys::LockTupleMode::Type,
    wait_policy: pg_sys::LockWaitPolicy::Type,
    flags: u8,
    tmfd: *mut pg_sys::TM_FailureData,
) -> pg_sys::TM_Result::Type
where
    E: Into<ErrorReport>,
    T: AmDml<E> + 'static,
{
    unsafe {
        let session = get_or_create_session::<E, T>(rel).report_unwrap();

        let tid = ItemPointer::from_raw(tid);
        let snapshot_handle = SnapshotHandle::from_raw(snapshot);
        let mut tmfd_rust = TM_FailureData::default();

        // Note: tuple_lock might modify the row (e.g. stores current version), so passing mut ref is correct
        // But for consistency with update_from_slot, we first fill it
        (*session).row_buffer.update_from_slot(slot);

        let state = (*session)
            .state
            .downcast_mut::<T>()
            .expect("State type mismatch - this is a bug");

        let result = state
            .tuple_lock(
                &tid,
                &snapshot_handle,
                &mut (*session).row_buffer,
                cid,
                mode,
                wait_policy,
                flags,
                &mut tmfd_rust,
            )
            .report_unwrap();

        tmfd_rust.write_to_ptr(tmfd);

        result
    }
}

#[pg_guard]
pub extern "C-unwind" fn finish_bulk_insert<E, T>(
    rel: pg_sys::Relation,
    options: ::core::ffi::c_int,
) where
    E: Into<ErrorReport>,
    T: AmDml<E> + 'static,
{
    unsafe {
        let relid = (*rel).rd_id;

        // Fast path: check if this is the cached session
        if let Some((cached_relid, cached_ptr)) =
            LAST_USED_SESSION.with(|last| last.get())
        {
            if cached_relid == relid {
                if let Some(state) = (*cached_ptr).state.downcast_mut::<T>() {
                    state.finish_bulk_insert(options).report_unwrap();
                }
                return;
            }
        }

        // Slow path: lookup in HashMap
        // Only call finish if we have an active state
        // We don't want to create a new state just to finish it
        MODIFY_STATES.with(|states| {
            let mut map = states.borrow_mut();
            if let Some(session) = map.get_mut(&relid) {
                if let Some(state) = session.state.downcast_mut::<T>() {
                    state.finish_bulk_insert(options).report_unwrap();
                }
            }
        });
    }
}

#[pg_guard]
pub extern "C-unwind" fn index_delete_tuples<E, T>(
    rel: pg_sys::Relation,
    delstate: *mut pg_sys::TM_IndexDeleteOp,
) -> pg_sys::TransactionId
where
    E: Into<ErrorReport>,
    T: AmDml<E> + 'static,
{
    unsafe {
        // Try to get existing state, or create if needed (VACUUM typically runs as separate operation).
        // We silently ignore initialization errors here as index deletion is often a best-effort
        // maintenance operation (like VACUUM).
        if let Ok(session) = get_or_create_session::<E, T>(rel) {
            if let Some(state) = (*session).state.downcast_mut::<T>() {
                // Default to InvalidTransactionId if implementation fails/returns error
                return state
                    .index_delete_tuples(&mut *delstate)
                    .unwrap_or(pg_sys::InvalidTransactionId);
            }
        }
        pg_sys::InvalidTransactionId
    }
}

pub fn register<E, T>(routine: &mut pg_sys::TableAmRoutine)
where
    E: Into<ErrorReport>,
    T: AmDml<E> + 'static,
{
    routine.tuple_insert = Some(tuple_insert::<E, T>);
    routine.tuple_insert_speculative = Some(tuple_insert_speculative::<E, T>);
    routine.tuple_complete_speculative = Some(tuple_complete_speculative::<E, T>);
    routine.multi_insert = Some(multi_insert::<E, T>);
    routine.tuple_delete = Some(tuple_delete::<E, T>);
    routine.tuple_update = Some(tuple_update::<E, T>);
    routine.tuple_lock = Some(tuple_lock::<E, T>);
    routine.finish_bulk_insert = Some(finish_bulk_insert::<E, T>);
    routine.index_delete_tuples = Some(index_delete_tuples::<E, T>);
}
