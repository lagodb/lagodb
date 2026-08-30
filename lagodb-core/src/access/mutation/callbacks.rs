//! Table-AM callbacks used by COPY FROM.
//!
//! PG17 ModifyTable mutation is intercepted by `LagoDBModifyTable` and calls the
//! slot-first Rust SPI directly. Speculative insertion can only arrive from
//! `nodeModifyTable`, so reaching its callbacks violates that routing invariant.
//! PostgreSQL also invokes delete/update/lock callbacks from independent paths
//! such as logical replication, triggers, and `LockRows`; those remain normal
//! unsupported table-AM capabilities.
//!
//! The PostgreSQL ABI still supplies `BulkInsertStateData` to the insert
//! callbacks. It remains at this FFI boundary because the provider owns its
//! insertion session state and does not consume PostgreSQL's heap-private bulk
//! buffer state.

use std::error::Error;
use std::fmt::{self, Display, Formatter};

use crate::api::{TableAccessMethod, unsupported_callback};
use crate::diag::{PgReportError, ReportableError, SqlStateError};
use crate::tuple::{TupleSlotBatch, TupleSlotRow};
use pgrx::prelude::PgSqlErrorCode;
use pgrx::{pg_guard, pg_sys};

use super::session::with_current_relation_session;

#[derive(Debug, Clone, Copy)]
struct SpeculativeInsertRoutingError {
    callback: &'static str,
}

impl SpeculativeInsertRoutingError {
    const fn outside_modify_table(callback: &'static str) -> Self {
        Self { callback }
    }
}

impl Display for SpeculativeInsertRoutingError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} bypassed the registered Custom ModifyTable executor",
            self.callback
        )
    }
}

impl Error for SpeculativeInsertRoutingError {}

impl SqlStateError for SpeculativeInsertRoutingError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        PgSqlErrorCode::ERRCODE_INTERNAL_ERROR
    }
}

#[pg_guard]
pub(super) extern "C-unwind" fn tuple_insert<A: TableAccessMethod>(
    rel: pg_sys::Relation,
    slot: *mut pg_sys::TupleTableSlot,
    cid: pg_sys::CommandId,
    options: i32,
    _bistate: *mut pg_sys::BulkInsertStateData,
) {
    with_current_relation_session::<A, _>(rel, |session| unsafe {
        session.tuple_insert_slot(TupleSlotRow::from_raw(slot), cid, options)
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
    _bistate: *mut pg_sys::BulkInsertStateData,
) where
    A: TableAccessMethod,
{
    with_current_relation_session::<A, _>(rel, |session| unsafe {
        session.multi_insert_slots(
            TupleSlotBatch::from_raw(slots, nslots as usize),
            cid,
            options,
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
    PgReportError::from(SpeculativeInsertRoutingError::outside_modify_table(
        "tuple_insert_speculative",
    ))
    .report()
}

#[pg_guard]
pub(super) extern "C-unwind" fn tuple_complete_speculative(
    _rel: pg_sys::Relation,
    _slot: *mut pg_sys::TupleTableSlot,
    _spec_token: u32,
    _succeeded: bool,
) {
    PgReportError::from(SpeculativeInsertRoutingError::outside_modify_table(
        "tuple_complete_speculative",
    ))
    .report()
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
    unsupported_callback("tuple_delete").report_unwrap()
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
    unsupported_callback("tuple_update").report_unwrap()
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
    unsupported_callback("tuple_lock").report_unwrap()
}
