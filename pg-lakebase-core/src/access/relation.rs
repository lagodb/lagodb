//! Relation-level callback wrappers for AmRelation trait
//!
//! This module provides FFI boundary functions for relation-level operations.
//! All unsafe operations are handled here, keeping the AmRelation trait implementation safe.

use crate::api::TableAccessMethod;
use crate::diag::{PgReportError, ReportableError};
use crate::handles::{
    AttrWidthsHandle, BufferAccessStrategyHandle, ItemPointer, RelationHandle,
    SnapshotHandle, TupleTableSlotHandle, VacuumParamsHandle, VarlenaHandle,
};
use crate::tuple::{Row, RowDatumCodec, TupleSlotWriter};
use pgrx::prelude::*;

#[pg_guard]
pub extern "C-unwind" fn relation_estimate_size<A>(
    rel: pg_sys::Relation,
    attr_widths: *mut i32,
    pages: *mut pg_sys::BlockNumber,
    tuples: *mut f64,
    allvisfrac: *mut f64,
) where
    A: TableAccessMethod,
{
    let rel_handle = unsafe { RelationHandle::from_raw(rel) };
    let mut attr_widths_handle = if attr_widths.is_null() {
        None
    } else {
        unsafe {
            let natts = (*(*rel).rd_att).natts as usize;
            AttrWidthsHandle::from_raw(attr_widths, natts)
        }
    };

    let (est_pages, est_tuples, est_allvisfrac) =
        A::relation_estimate_size(&rel_handle, attr_widths_handle.as_mut())
            .report_unwrap();

    unsafe {
        *pages = est_pages;
        *tuples = est_tuples;
        *allvisfrac = est_allvisfrac;
    }
}

#[pg_guard]
pub extern "C-unwind" fn relation_size<A>(
    rel: pg_sys::Relation,
    fork_number: pg_sys::ForkNumber::Type,
) -> u64
where
    A: TableAccessMethod,
{
    let rel_handle = unsafe { RelationHandle::from_raw(rel) };
    A::relation_size(&rel_handle, fork_number).report_unwrap()
}

#[pg_guard]
pub extern "C-unwind" fn relation_needs_toast_table<A>(rel: pg_sys::Relation) -> bool
where
    A: TableAccessMethod,
{
    let rel_handle = unsafe { RelationHandle::from_raw(rel) };
    A::relation_needs_toast_table(&rel_handle).report_unwrap()
}

#[pg_guard]
pub extern "C-unwind" fn relation_toast_am<A>(rel: pg_sys::Relation) -> pg_sys::Oid
where
    A: TableAccessMethod,
{
    let rel_handle = unsafe { RelationHandle::from_raw(rel) };
    A::relation_toast_am(&rel_handle).report_unwrap()
}

#[pg_guard]
pub extern "C-unwind" fn relation_fetch_toast_slice<A>(
    toastrel: pg_sys::Relation,
    valueid: pg_sys::Oid,
    attrsize: i32,
    sliceoffset: i32,
    slicelength: i32,
    result: *mut pg_sys::varlena,
) where
    A: TableAccessMethod,
{
    let toastrel_handle = unsafe { RelationHandle::from_raw(toastrel) };
    let result_handle = unsafe { VarlenaHandle::from_raw(result) };

    A::relation_fetch_toast_slice(
        &toastrel_handle,
        valueid,
        attrsize,
        sliceoffset,
        slicelength,
        &result_handle,
    )
    .report_unwrap()
}

#[pg_guard]
pub extern "C-unwind" fn tuple_fetch_row_version<A>(
    rel: pg_sys::Relation,
    tid: pg_sys::ItemPointer,
    snapshot: pg_sys::Snapshot,
    slot: *mut pg_sys::TupleTableSlot,
) -> bool
where
    A: TableAccessMethod,
{
    unsafe {
        pg_sys::ExecClearTuple(slot);

        let rel_handle = RelationHandle::from_raw(rel);
        let tid = ItemPointer::from_raw(tid);
        let snapshot_handle = SnapshotHandle::from_raw(snapshot);

        let tup_desc = (*slot).tts_tupleDescriptor;
        let natts = (*tup_desc).natts as usize;

        match crate::access::mutation::trigger_rows::fetch::<A>(
            rel_handle.oid(),
            tid,
            slot,
        ) {
            crate::access::mutation::trigger_rows::FetchResult::Found => {
                (*slot).tts_tid = tid.to_pg_sys();
                return true;
            }
            crate::access::mutation::trigger_rows::FetchResult::Missing => {
                error!(
                    "Lakebase AFTER-trigger row identity has no live query-level row store"
                );
            }
            crate::access::mutation::trigger_rows::FetchResult::PhysicalRow => {}
        }

        let mut row = Row::with_capacity(natts);
        if !A::tuple_fetch_row_version(&rel_handle, &tid, &snapshot_handle, &mut row)
            .report_unwrap()
        {
            return false;
        }

        let row_codec = RowDatumCodec::from_relation(rel)
            .map_err(PgReportError::from_domain_error)
            .report_unwrap();
        TupleSlotWriter::new(slot, (*slot).tts_mcxt, &row_codec)
            .write_row(&mut row)
            .report_unwrap();
        (*slot).tts_tid = tid.to_pg_sys();

        true
    }
}

#[pg_guard]
pub extern "C-unwind" fn tuple_satisfies_snapshot<A>(
    rel: pg_sys::Relation,
    slot: *mut pg_sys::TupleTableSlot,
    snapshot: pg_sys::Snapshot,
) -> bool
where
    A: TableAccessMethod,
{
    let rel_handle = unsafe { RelationHandle::from_raw(rel) };
    let slot_handle = unsafe { TupleTableSlotHandle::from_raw(slot) };
    let snapshot_handle = unsafe { SnapshotHandle::from_raw(snapshot) };

    A::tuple_satisfies_snapshot(&rel_handle, &slot_handle, &snapshot_handle)
        .report_unwrap()
}

#[pg_guard]
pub extern "C-unwind" fn relation_vacuum<A>(
    rel: pg_sys::Relation,
    params: *mut pg_sys::VacuumParams,
    bstrategy: pg_sys::BufferAccessStrategy,
) where
    A: TableAccessMethod,
{
    let rel_handle = unsafe { RelationHandle::from_raw(rel) };
    let params_handle = unsafe { VacuumParamsHandle::from_raw(params) };
    let bstrategy_handle = unsafe { BufferAccessStrategyHandle::from_raw(bstrategy) };

    A::relation_vacuum(&rel_handle, &params_handle, &bstrategy_handle).report_unwrap()
}

pub fn register<A>(routine: &mut pg_sys::TableAmRoutine)
where
    A: TableAccessMethod,
{
    routine.relation_estimate_size = Some(relation_estimate_size::<A>);
    routine.relation_size = Some(relation_size::<A>);
    routine.relation_needs_toast_table = Some(relation_needs_toast_table::<A>);
    routine.relation_toast_am = Some(relation_toast_am::<A>);
    routine.relation_fetch_toast_slice = Some(relation_fetch_toast_slice::<A>);
    routine.tuple_fetch_row_version = Some(tuple_fetch_row_version::<A>);
    routine.tuple_satisfies_snapshot = Some(tuple_satisfies_snapshot::<A>);
    routine.relation_vacuum = Some(relation_vacuum::<A>);
}
