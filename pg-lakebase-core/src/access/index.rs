//! Index scan callback wrappers for AmIndex trait
//!
//! This module provides FFI boundary functions for index scan operations.
//! All unsafe operations are handled here, keeping the AmIndex trait implementation safe.

use crate::api::AmIndex;
use crate::data::Row;
use crate::handles::{
    CallbackStateHandle, IndexBuildCallbackHandle, IndexInfoHandle, ItemPointer,
    RelationHandle, SnapshotHandle, TableScanDescHandle, ValidateIndexStateHandle,
};
use crate::diag::ReportableError;
use pgrx::memcxt::PgMemoryContexts;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::*;

/// C-compatible wrapper for IndexFetchTableData
#[repr(C)]
pub struct CustomIndexFetchData<T> {
    pub base: pg_sys::IndexFetchTableData,
    pub state: *mut IndexFetchState<T>,
}

pub struct IndexFetchState<T> {
    pub am_instance: T,
    pub tmp_ctx: pg_sys::MemoryContext,
    pub row: Row,
}

impl<T> IndexFetchState<T> {
    pub unsafe fn new(
        am_instance: T,
        tmp_ctx: pg_sys::MemoryContext,
        natts: usize,
    ) -> Self {
        let row = Row::with_capacity(natts);
        Self {
            am_instance,
            tmp_ctx,
            row,
        }
    }
}

#[pg_guard]
pub extern "C-unwind" fn index_fetch_begin<E, T>(
    rel: pg_sys::Relation,
) -> *mut pg_sys::IndexFetchTableData
where
    E: Into<ErrorReport>,
    T: AmIndex<E>,
{
    unsafe {
        let fetch_data = pgrx::pg_sys::palloc0(std::mem::size_of::<
            CustomIndexFetchData<T>,
        >()) as *mut CustomIndexFetchData<T>;

        (*fetch_data).base.rel = rel;

        // Create temporary memory context
        let rel_oid = (*(*rel).rd_rel).oid;
        let ctx_name = format!("IndexFetch_{}", rel_oid);
        let tmp_ctx = crate::memory::create_tam_memctx(&ctx_name);

        let rel_handle = RelationHandle::from_raw(rel);
        let mut instance = T::new(&rel_handle).report_unwrap();
        instance.index_fetch_begin().report_unwrap();

        let tup_desc = (*rel).rd_att;
        let natts = (*tup_desc).natts as usize;

        let state = IndexFetchState::new(instance, tmp_ctx, natts);
        (*fetch_data).state = Box::into_raw(Box::new(state));

        fetch_data as *mut pg_sys::IndexFetchTableData
    }
}

#[pg_guard]
pub extern "C-unwind" fn index_fetch_reset<E, T>(
    data: *mut pg_sys::IndexFetchTableData,
) where
    E: Into<ErrorReport>,
    T: AmIndex<E>,
{
    unsafe {
        let custom_data = data as *mut CustomIndexFetchData<T>;
        if !(*custom_data).state.is_null() {
            let state = &mut *(*custom_data).state;
            state.am_instance.index_fetch_reset().report_unwrap();
        }
    }
}

#[pg_guard]
pub extern "C-unwind" fn index_fetch_end<E, T>(data: *mut pg_sys::IndexFetchTableData)
where
    E: Into<ErrorReport>,
    T: AmIndex<E>,
{
    unsafe {
        let custom_data = data as *mut CustomIndexFetchData<T>;
        if !(*custom_data).state.is_null() {
            let mut state = Box::from_raw((*custom_data).state);
            state.am_instance.index_fetch_end().report_unwrap();

            crate::memory::delete_tam_memctx(state.tmp_ctx);
            drop(state);
        }

        pgrx::pg_sys::pfree(custom_data as *mut ::core::ffi::c_void);
    }
}

#[pg_guard]
pub extern "C-unwind" fn index_fetch_tuple<E, T>(
    data: *mut pg_sys::IndexFetchTableData,
    tid: pg_sys::ItemPointer,
    snapshot: pg_sys::Snapshot,
    slot: *mut pg_sys::TupleTableSlot,
    call_again: *mut bool,
    all_dead: *mut bool,
) -> bool
where
    E: Into<ErrorReport>,
    T: AmIndex<E>,
{
    unsafe {
        pg_sys::ExecClearTuple(slot);

        let custom_data = data as *mut CustomIndexFetchData<T>;
        let state = &mut *(*custom_data).state;

        let tid = ItemPointer::from_raw(tid);
        let snapshot_handle = SnapshotHandle::from_raw(snapshot);
        let found = state
            .am_instance
            .index_fetch_tuple(
                &tid,
                &snapshot_handle,
                &mut state.row,
                &mut *call_again,
                &mut *all_dead,
            )
            .report_unwrap();

        if !found {
            return false;
        }

        let tup_desc = (*slot).tts_tupleDescriptor;
        let natts = (*tup_desc).natts as usize;

        // Use the slot's own pre-allocated buffers
        let slot_values = std::slice::from_raw_parts_mut((*slot).tts_values, natts);
        let slot_nulls = std::slice::from_raw_parts_mut((*slot).tts_isnull, natts);

        PgMemoryContexts::For(state.tmp_ctx).switch_to(|_| {
            for i in 0..natts {
                let cell = state.row.cells.get_unchecked_mut(i);
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
pub extern "C-unwind" fn index_build_range_scan<E, T>(
    table_rel: pg_sys::Relation,
    index_rel: pg_sys::Relation,
    index_info: *mut pg_sys::IndexInfo,
    allow_sync: bool,
    anyvisible: bool,
    progress: bool,
    start_blockno: pg_sys::BlockNumber,
    numblocks: pg_sys::BlockNumber,
    callback: pg_sys::IndexBuildCallback,
    callback_state: *mut ::core::ffi::c_void,
    scan: pg_sys::TableScanDesc,
) -> f64
where
    E: Into<ErrorReport>,
    T: AmIndex<E>,
{
    let table_rel_handle = unsafe { RelationHandle::from_raw(table_rel) };
    let index_rel_handle = unsafe { RelationHandle::from_raw(index_rel) };
    let index_info_handle = unsafe { IndexInfoHandle::from_raw(index_info) };
    let callback_handle = unsafe { IndexBuildCallbackHandle::from_raw(callback) };
    let callback_state_handle =
        unsafe { CallbackStateHandle::from_raw(callback_state) };
    let scan_handle = unsafe { TableScanDescHandle::from_raw(scan) };

    T::index_build_range_scan(
        &table_rel_handle,
        &index_rel_handle,
        &index_info_handle,
        allow_sync,
        anyvisible,
        progress,
        start_blockno,
        numblocks,
        &callback_handle,
        &callback_state_handle,
        &scan_handle,
    )
    .report_unwrap()
}

#[pg_guard]
pub extern "C-unwind" fn index_validate_scan<E, T>(
    table_rel: pg_sys::Relation,
    index_rel: pg_sys::Relation,
    index_info: *mut pg_sys::IndexInfo,
    snapshot: pg_sys::Snapshot,
    state: *mut pg_sys::ValidateIndexState,
) where
    E: Into<ErrorReport>,
    T: AmIndex<E>,
{
    let table_rel_handle = unsafe { RelationHandle::from_raw(table_rel) };
    let index_rel_handle = unsafe { RelationHandle::from_raw(index_rel) };
    let index_info_handle = unsafe { IndexInfoHandle::from_raw(index_info) };
    let snapshot_handle = unsafe { SnapshotHandle::from_raw(snapshot) };
    let state_handle = unsafe { ValidateIndexStateHandle::from_raw(state) };

    T::index_validate_scan(
        &table_rel_handle,
        &index_rel_handle,
        &index_info_handle,
        &snapshot_handle,
        &state_handle,
    )
    .report_unwrap()
}

pub fn register<E, T>(routine: &mut pg_sys::TableAmRoutine)
where
    E: Into<ErrorReport>,
    T: AmIndex<E>,
{
    routine.index_fetch_begin = Some(index_fetch_begin::<E, T>);
    routine.index_fetch_reset = Some(index_fetch_reset::<E, T>);
    routine.index_fetch_end = Some(index_fetch_end::<E, T>);
    routine.index_fetch_tuple = Some(index_fetch_tuple::<E, T>);
    routine.index_build_range_scan = Some(index_build_range_scan::<E, T>);
    routine.index_validate_scan = Some(index_validate_scan::<E, T>);
}
