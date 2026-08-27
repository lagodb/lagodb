//! Table-AM callbacks used by COPY FROM.
//!
//! PG17 ModifyTable mutation is intercepted by `LagoDBModifyTable` and calls the
//! slot-first Rust SPI directly. Reaching a row-mutation table-AM callback is
//! therefore an unsupported executor path, not a compatibility fallback.

use crate::api::TableAccessMethod;
use crate::diag::ReportableError;
use crate::handles::BulkInsertStateHandle;
use crate::tuple::{TupleSlotBatch, TupleSlotRow};
use pgrx::{pg_guard, pg_sys};

use super::session::with_current_relation_session;

fn custom_modifytable_only(callback: &'static str) -> ! {
    pgrx::error!("{callback} reached the Iceberg table AM outside LagoDBModifyTable")
}

#[pg_guard]
pub(super) extern "C-unwind" fn tuple_insert<A: TableAccessMethod>(
    rel: pg_sys::Relation,
    slot: *mut pg_sys::TupleTableSlot,
    cid: pg_sys::CommandId,
    options: i32,
    bistate: *mut pg_sys::BulkInsertStateData,
) {
    with_current_relation_session::<A, _>(rel, |session| unsafe {
        let bistate =
            (!bistate.is_null()).then(|| BulkInsertStateHandle::from_raw(bistate));
        session.state.tuple_insert_slot(
            TupleSlotRow::from_raw(slot),
            cid,
            options,
            bistate.as_ref(),
        )
    })
    .report_unwrap();
}

#[pg_guard]
pub(super) extern "C-unwind" fn multi_insert<A>(
    rel: pg_sys::Relation,
    slots: *mut *mut pg_sys::TupleTableSlot,
    nslots: i32,
    cid: pg_sys::CommandId,
    options: i32,
    bistate: *mut pg_sys::BulkInsertStateData,
) where
    A: TableAccessMethod,
{
    with_current_relation_session::<A, _>(rel, |session| unsafe {
        let bistate =
            (!bistate.is_null()).then(|| BulkInsertStateHandle::from_raw(bistate));
        session.state.multi_insert_slots(
            TupleSlotBatch::from_raw(slots, nslots as usize),
            cid,
            options,
            bistate.as_ref(),
        )
    })
    .report_unwrap();
}

#[pg_guard]
pub(super) extern "C-unwind" fn finish_bulk_insert<A: TableAccessMethod>(
    rel: pg_sys::Relation,
    options: i32,
) {
    with_current_relation_session::<A, _>(rel, |session| {
        session.finish_bulk_insert(options)
    })
    .report_unwrap();
}

#[pg_guard]
pub(super) extern "C-unwind" fn tuple_insert_speculative(
    _rel: pg_sys::Relation,
    _slot: *mut pg_sys::TupleTableSlot,
    _cid: pg_sys::CommandId,
    _options: i32,
    _bistate: *mut pg_sys::BulkInsertStateData,
    _spec_token: u32,
) {
    custom_modifytable_only("tuple_insert_speculative")
}

#[pg_guard]
pub(super) extern "C-unwind" fn tuple_complete_speculative(
    _rel: pg_sys::Relation,
    _slot: *mut pg_sys::TupleTableSlot,
    _spec_token: u32,
    _succeeded: bool,
) {
    custom_modifytable_only("tuple_complete_speculative")
}

#[pg_guard]
pub(super) extern "C-unwind" fn tuple_delete(
    _rel: pg_sys::Relation,
    _tid: pg_sys::ItemPointer,
    _cid: pg_sys::CommandId,
    _snapshot: pg_sys::Snapshot,
    _crosscheck: pg_sys::Snapshot,
    _wait: bool,
    _tmfd: *mut pg_sys::TM_FailureData,
    _changing_part: bool,
) -> pg_sys::TM_Result::Type {
    custom_modifytable_only("tuple_delete")
}

#[pg_guard]
pub(super) extern "C-unwind" fn tuple_update(
    _rel: pg_sys::Relation,
    _otid: pg_sys::ItemPointer,
    _slot: *mut pg_sys::TupleTableSlot,
    _cid: pg_sys::CommandId,
    _snapshot: pg_sys::Snapshot,
    _crosscheck: pg_sys::Snapshot,
    _wait: bool,
    _tmfd: *mut pg_sys::TM_FailureData,
    _lockmode: *mut pg_sys::LockTupleMode::Type,
    _update_indexes: *mut pg_sys::TU_UpdateIndexes::Type,
) -> pg_sys::TM_Result::Type {
    custom_modifytable_only("tuple_update")
}

#[pg_guard]
pub(super) extern "C-unwind" fn tuple_lock(
    _rel: pg_sys::Relation,
    _tid: pg_sys::ItemPointer,
    _snapshot: pg_sys::Snapshot,
    _slot: *mut pg_sys::TupleTableSlot,
    _cid: pg_sys::CommandId,
    _mode: pg_sys::LockTupleMode::Type,
    _wait_policy: pg_sys::LockWaitPolicy::Type,
    _flags: u8,
    _tmfd: *mut pg_sys::TM_FailureData,
) -> pg_sys::TM_Result::Type {
    custom_modifytable_only("tuple_lock")
}
