//! `extern "C-unwind"` shims that PostgreSQL's TableAm calls into.
//!
//! Each shim resolves the per-relation [`ModifySession`](super::session::ModifySession),
//! converts raw FFI inputs into typed handles, and dispatches to the erased
//! DML session. Errors are reported via `report_unwrap`, which raises a
//! PostgreSQL ERROR through `pgrx`.
//!
//! These callbacks intentionally do not own lifecycle decisions.  PostgreSQL may
//! call the same callback many times inside one ModifyTable/COPY frame, and
//! MERGE may call different callbacks inside the same frame.  The callbacks only
//! translate arguments and ask `with_current_relation_session` for the relation-local AM
//! session that belongs to the current frame.

use crate::api::TableAccessMethod;
use crate::diag::ReportableError;
use crate::handles::{
    BulkInsertStateHandle, ItemPointer, SnapshotHandle, TM_FailureData,
};
use crate::tuple::{TupleSlotBatch, TupleSlotRow};
use pgrx::pg_sys;
use pgrx::prelude::*;

use super::session::with_current_relation_session;

#[pg_guard]
pub(super) extern "C-unwind" fn tuple_insert<A>(
    rel: pg_sys::Relation,
    slot: *mut pg_sys::TupleTableSlot,
    cid: pg_sys::CommandId,
    options: ::core::ffi::c_int,
    bistate: *mut pg_sys::BulkInsertStateData,
) where
    A: TableAccessMethod,
{
    with_current_relation_session::<A, _>(rel, |session| unsafe {
        // Convert bistate to Handle if not null
        let bistate_handle =
            (!bistate.is_null()).then(|| BulkInsertStateHandle::from_raw(bistate));

        let row = TupleSlotRow::from_raw(slot);

        session
            .state
            .tuple_insert_slot(row, cid, options, bistate_handle.as_ref())
    })
    .report_unwrap();
}

#[pg_guard]
pub(super) extern "C-unwind" fn tuple_insert_speculative<A>(
    rel: pg_sys::Relation,
    slot: *mut pg_sys::TupleTableSlot,
    cid: pg_sys::CommandId,
    options: ::core::ffi::c_int,
    bistate: *mut pg_sys::BulkInsertStateData,
    spec_token: u32,
) where
    A: TableAccessMethod,
{
    with_current_relation_session::<A, _>(rel, |session| unsafe {
        let bistate_handle =
            (!bistate.is_null()).then(|| BulkInsertStateHandle::from_raw(bistate));

        let row = TupleSlotRow::from_raw(slot);

        session.state.tuple_insert_speculative_slot(
            row,
            cid,
            options,
            bistate_handle.as_ref(),
            spec_token,
        )
    })
    .report_unwrap();
}

#[pg_guard]
pub(super) extern "C-unwind" fn tuple_complete_speculative<A>(
    rel: pg_sys::Relation,
    slot: *mut pg_sys::TupleTableSlot,
    spec_token: u32,
    succeeded: bool,
) where
    A: TableAccessMethod,
{
    with_current_relation_session::<A, _>(rel, |session| unsafe {
        let row = TupleSlotRow::from_raw(slot);

        session
            .state
            .tuple_complete_speculative_slot(row, spec_token, succeeded)
    })
    .report_unwrap();
}

#[pg_guard]
pub(super) extern "C-unwind" fn multi_insert<A>(
    rel: pg_sys::Relation,
    slots: *mut *mut pg_sys::TupleTableSlot,
    nslots: ::core::ffi::c_int,
    cid: pg_sys::CommandId,
    options: ::core::ffi::c_int,
    bistate: *mut pg_sys::BulkInsertStateData,
) where
    A: TableAccessMethod,
{
    with_current_relation_session::<A, _>(rel, |session| unsafe {
        let rows = TupleSlotBatch::from_raw(slots, nslots as usize);

        let bistate_handle =
            (!bistate.is_null()).then(|| BulkInsertStateHandle::from_raw(bistate));

        session
            .state
            .multi_insert_slots(rows, cid, options, bistate_handle.as_ref())
    })
    .report_unwrap();
}

#[pg_guard]
pub(super) extern "C-unwind" fn tuple_delete<A>(
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
    A: TableAccessMethod,
{
    with_current_relation_session::<A, _>(rel, |session| unsafe {
        let tid = ItemPointer::from_raw(tid);
        let snapshot_handle = SnapshotHandle::from_raw(snapshot);
        let crosscheck_handle =
            (!crosscheck.is_null()).then(|| SnapshotHandle::from_raw(crosscheck));
        let mut tmfd_rust = TM_FailureData::default();

        let result = session.state.tuple_delete(
            &tid,
            cid,
            &snapshot_handle,
            crosscheck_handle.as_ref(),
            wait,
            &mut tmfd_rust,
            changing_part,
        )?;

        tmfd_rust.write_to_ptr(tmfd);

        Ok(result)
    })
    .report_unwrap()
}

#[pg_guard]
pub(super) extern "C-unwind" fn tuple_update<A>(
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
    A: TableAccessMethod,
{
    with_current_relation_session::<A, _>(rel, |session| unsafe {
        let otid = ItemPointer::from_raw(otid);
        // Update buffer from slot
        let snapshot_handle = SnapshotHandle::from_raw(snapshot);
        let crosscheck_handle =
            (!crosscheck.is_null()).then(|| SnapshotHandle::from_raw(crosscheck));
        let mut tmfd_rust = TM_FailureData::default();

        let row = TupleSlotRow::from_raw(slot);

        let result = session.state.tuple_update_slot(
            &otid,
            row,
            cid,
            &snapshot_handle,
            crosscheck_handle.as_ref(),
            wait,
            &mut tmfd_rust,
            &mut *lockmode,
            &mut *update_indexes,
        )?;

        tmfd_rust.write_to_ptr(tmfd);

        Ok(result)
    })
    .report_unwrap()
}

#[pg_guard]
pub(super) extern "C-unwind" fn tuple_lock<A>(
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
    A: TableAccessMethod,
{
    with_current_relation_session::<A, _>(rel, |session| unsafe {
        let tid = ItemPointer::from_raw(tid);
        let snapshot_handle = SnapshotHandle::from_raw(snapshot);
        let mut tmfd_rust = TM_FailureData::default();

        // Note: tuple_lock might modify the row (e.g. stores current version), so
        // passing mut ref is correct. But for consistency with update_from_slot,
        // we first fill it.
        session.row_buffer.update_from_slot(slot);

        let result = session.state.tuple_lock(
            &tid,
            &snapshot_handle,
            &mut session.row_buffer,
            cid,
            mode,
            wait_policy,
            flags,
            &mut tmfd_rust,
        )?;

        tmfd_rust.write_to_ptr(tmfd);

        Ok(result)
    })
    .report_unwrap()
}

#[pg_guard]
pub(super) extern "C-unwind" fn finish_bulk_insert<A>(
    rel: pg_sys::Relation,
    options: ::core::ffi::c_int,
) where
    A: TableAccessMethod,
{
    with_current_relation_session::<A, _>(rel, |session| {
        session.finish_bulk_insert(options)
    })
    .report_unwrap();
}
