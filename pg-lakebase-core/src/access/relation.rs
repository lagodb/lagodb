//! Relation-level callback wrappers for AmRelation trait
//!
//! This module provides FFI boundary functions for relation-level operations.
//! All unsafe operations are handled here, keeping the AmRelation trait implementation safe.

use crate::api::AmRelation;
use crate::data::Row;
use crate::handles::{
    BufferAccessStrategyHandle, ItemPointer, RelationHandle, SnapshotHandle,
    TupleTableSlotHandle, VacuumParamsHandle, VarlenaHandle,
};
use crate::diag::ReportableError;
use pgrx::memcxt::PgMemoryContexts;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::*;

#[pg_guard]
pub extern "C-unwind" fn relation_estimate_size<E, T>(
    rel: pg_sys::Relation,
    attr_widths: *mut i32,
    pages: *mut pg_sys::BlockNumber,
    tuples: *mut f64,
    allvisfrac: *mut f64,
) where
    E: Into<ErrorReport>,
    T: AmRelation<E>,
{
    let rel_handle = unsafe { RelationHandle::from_raw(rel) };
    let attr_widths_slice = if attr_widths.is_null() {
        None
    } else {
        unsafe {
            let natts = (*(*rel).rd_att).natts as usize;
            Some(std::slice::from_raw_parts_mut(attr_widths, natts))
        }
    };

    let (est_pages, est_tuples, est_allvisfrac) =
        T::relation_estimate_size(&rel_handle, attr_widths_slice).report_unwrap();

    unsafe {
        *pages = est_pages;
        *tuples = est_tuples;
        *allvisfrac = est_allvisfrac;
    }
}

#[pg_guard]
pub extern "C-unwind" fn relation_size<E, T>(
    rel: pg_sys::Relation,
    fork_number: pg_sys::ForkNumber::Type,
) -> u64
where
    E: Into<ErrorReport>,
    T: AmRelation<E>,
{
    let rel_handle = unsafe { RelationHandle::from_raw(rel) };
    T::relation_size(&rel_handle, fork_number).report_unwrap()
}

#[pg_guard]
pub extern "C-unwind" fn relation_needs_toast_table<E, T>(
    rel: pg_sys::Relation,
) -> bool
where
    E: Into<ErrorReport>,
    T: AmRelation<E>,
{
    let rel_handle = unsafe { RelationHandle::from_raw(rel) };
    T::relation_needs_toast_table(&rel_handle).report_unwrap()
}

#[pg_guard]
pub extern "C-unwind" fn relation_toast_am<E, T>(rel: pg_sys::Relation) -> pg_sys::Oid
where
    E: Into<ErrorReport>,
    T: AmRelation<E>,
{
    let rel_handle = unsafe { RelationHandle::from_raw(rel) };
    T::relation_toast_am(&rel_handle).report_unwrap()
}

#[pg_guard]
pub extern "C-unwind" fn relation_fetch_toast_slice<E, T>(
    toastrel: pg_sys::Relation,
    valueid: pg_sys::Oid,
    attrsize: i32,
    sliceoffset: i32,
    slicelength: i32,
    result: *mut pg_sys::varlena,
) where
    E: Into<ErrorReport>,
    T: AmRelation<E>,
{
    let toastrel_handle = unsafe { RelationHandle::from_raw(toastrel) };
    let result_handle = unsafe { VarlenaHandle::from_raw(result) };

    T::relation_fetch_toast_slice(
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
pub extern "C-unwind" fn tuple_fetch_row_version<E, T>(
    rel: pg_sys::Relation,
    tid: pg_sys::ItemPointer,
    snapshot: pg_sys::Snapshot,
    slot: *mut pg_sys::TupleTableSlot,
) -> bool
where
    E: Into<ErrorReport>,
    T: AmRelation<E>,
{
    unsafe {
        pg_sys::ExecClearTuple(slot);

        let rel_handle = RelationHandle::from_raw(rel);
        let tid = ItemPointer::from_raw(tid);
        let snapshot_handle = SnapshotHandle::from_raw(snapshot);

        let tup_desc = (*slot).tts_tupleDescriptor;
        let natts = (*tup_desc).natts as usize;

        let mut row = Row::with_capacity(natts);

        let found =
            T::tuple_fetch_row_version(&rel_handle, &tid, &snapshot_handle, &mut row)
                .report_unwrap();

        if !found {
            return false;
        }

        // Use the slot's own pre-allocated buffers
        let slot_values = std::slice::from_raw_parts_mut((*slot).tts_values, natts);
        let slot_nulls = std::slice::from_raw_parts_mut((*slot).tts_isnull, natts);

        PgMemoryContexts::For((*slot).tts_mcxt).switch_to(|_| {
            for i in 0..natts {
                let cell = row.cells.get_unchecked_mut(i);
                match cell.take() {
                    Some(cell) => {
                        slot_values[i] = cell
                            .into_datum()
                            .expect("Failed to convert cell to datum");
                        slot_nulls[i] = false;
                    }
                    None => {
                        slot_nulls[i] = true;
                    }
                }
            }

            pg_sys::ExecStoreVirtualTuple(slot);
        });

        true
    }
}

#[pg_guard]
pub extern "C-unwind" fn tuple_satisfies_snapshot<E, T>(
    rel: pg_sys::Relation,
    slot: *mut pg_sys::TupleTableSlot,
    snapshot: pg_sys::Snapshot,
) -> bool
where
    E: Into<ErrorReport>,
    T: AmRelation<E>,
{
    let rel_handle = unsafe { RelationHandle::from_raw(rel) };
    let slot_handle = unsafe { TupleTableSlotHandle::from_raw(slot) };
    let snapshot_handle = unsafe { SnapshotHandle::from_raw(snapshot) };

    T::tuple_satisfies_snapshot(&rel_handle, &slot_handle, &snapshot_handle)
        .report_unwrap()
}

#[pg_guard]
pub extern "C-unwind" fn relation_vacuum<E, T>(
    rel: pg_sys::Relation,
    params: *mut pg_sys::VacuumParams,
    bstrategy: pg_sys::BufferAccessStrategy,
) where
    E: Into<ErrorReport>,
    T: AmRelation<E>,
{
    let rel_handle = unsafe { RelationHandle::from_raw(rel) };
    let mut params_handle = unsafe { VacuumParamsHandle::from_raw(params) };
    let bstrategy_handle = unsafe { BufferAccessStrategyHandle::from_raw(bstrategy) };

    T::relation_vacuum(&rel_handle, &mut params_handle, &bstrategy_handle)
        .report_unwrap()
}

pub fn register<E, T>(routine: &mut pg_sys::TableAmRoutine)
where
    E: Into<ErrorReport>,
    T: AmRelation<E>,
{
    routine.relation_estimate_size = Some(relation_estimate_size::<E, T>);
    routine.relation_size = Some(relation_size::<E, T>);
    routine.relation_needs_toast_table = Some(relation_needs_toast_table::<E, T>);
    routine.relation_toast_am = Some(relation_toast_am::<E, T>);
    routine.relation_fetch_toast_slice = Some(relation_fetch_toast_slice::<E, T>);
    routine.tuple_fetch_row_version = Some(tuple_fetch_row_version::<E, T>);
    routine.tuple_satisfies_snapshot = Some(tuple_satisfies_snapshot::<E, T>);
    routine.relation_vacuum = Some(relation_vacuum::<E, T>);
}
