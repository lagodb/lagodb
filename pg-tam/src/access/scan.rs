//! Sequential scan callback wrappers for AmScan trait
//!
//! This module provides wrapper functions that bridge PostgreSQL's sequential
//! scan callbacks with the AmScan trait implementation.

use crate::api::AmScan;
use crate::data::Row;
use crate::handles::{
    ItemPointer, ParallelTableScanDescHandle, ReadStreamHandle, RelationHandle,
    SampleScanStateHandle, ScanDirection, ScanKeyHandle, SnapshotHandle,
    TBMIterateResultHandle,
};
use crate::diag::ReportableError;
use pgrx::memcxt::PgMemoryContexts;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::*;

#[repr(C)]
pub struct CustomScanDesc<T> {
    pub base: pg_sys::TableScanDescData,
    pub am_state: *mut ScanState<T>,
}

pub struct ScanState<T> {
    pub am_instance: T,
    pub tmp_ctx: pg_sys::MemoryContext,
    pub row: Row,
}

impl<T> ScanState<T> {
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

    pub unsafe fn row_to_slot(&mut self, slot: *mut pg_sys::TupleTableSlot) {
        unsafe {
            let tup_desc = (*slot).tts_tupleDescriptor;
            let natts = (*tup_desc).natts as usize;
            let attrs = std::slice::from_raw_parts((*tup_desc).attrs.as_ptr(), natts);

            // Use the slot's own pre-allocated buffers
            let slot_values = std::slice::from_raw_parts_mut((*slot).tts_values, natts);
            let slot_nulls = std::slice::from_raw_parts_mut((*slot).tts_isnull, natts);

            PgMemoryContexts::For(self.tmp_ctx).switch_to(|_| {
                for i in 0..natts {
                    let cell = self.row.cells.get_unchecked_mut(i);
                    match cell.take() {
                        Some(cell) => {
                            // Get target type information from TupleDesc
                            let attr = &attrs[i];
                            let typoid = attr.atttypid;
                            let typmod = attr.atttypmod;

                            // Use into_datum_typed to handle composite types correctly
                            slot_values[i] = cell
                                .into_datum_typed(typoid, typmod)
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
        }
    }
}

#[pg_guard]
pub extern "C-unwind" fn slot_callbacks<E, T>(
    _rel: pg_sys::Relation,
) -> *const pg_sys::TupleTableSlotOps
where
    E: Into<ErrorReport>,
    T: AmScan<E>,
{
    T::slot_callbacks()
}

#[pg_guard]
pub extern "C-unwind" fn scan_begin<E, T>(
    rel: pg_sys::Relation,
    snapshot: pg_sys::Snapshot,
    nkeys: ::core::ffi::c_int,
    key: *mut pg_sys::ScanKeyData,
    pscan: pg_sys::ParallelTableScanDesc,
    flags: u32,
) -> pg_sys::TableScanDesc
where
    E: Into<ErrorReport>,
    T: AmScan<E>,
{
    unsafe {
        let scan_desc = pgrx::pg_sys::palloc0(std::mem::size_of::<CustomScanDesc<T>>())
            as *mut CustomScanDesc<T>;

        (*scan_desc).base.rs_rd = rel;
        (*scan_desc).base.rs_snapshot = snapshot;
        (*scan_desc).base.rs_nkeys = nkeys;
        (*scan_desc).base.rs_key = key;
        (*scan_desc).base.rs_flags = flags;
        (*scan_desc).base.rs_parallel = pscan;

        // Create temporary memory context
        let rel_oid = (*(*rel).rd_rel).oid;
        let ctx_name = format!("TableScan_{}", rel_oid);
        let tmp_ctx = crate::memory::create_tam_memctx(&ctx_name);

        // Convert raw C pointers to safe Handle types
        let rel_handle = RelationHandle::from_raw(rel);
        let snapshot_handle = SnapshotHandle::from_raw(snapshot);
        let key_handle = if key.is_null() {
            None
        } else {
            Some(ScanKeyHandle::from_raw(key, nkeys))
        };
        let pscan_handle = if pscan.is_null() {
            None
        } else {
            Some(ParallelTableScanDescHandle::from_raw(pscan))
        };

        let mut instance = T::new(
            &rel_handle,
            &snapshot_handle,
            key_handle.as_ref(),
            pscan_handle.as_ref(),
            flags,
        )
        .report_unwrap();
        instance.scan_begin().report_unwrap();

        let tup_desc = (*rel).rd_att;
        let natts = (*tup_desc).natts as usize;

        let state = ScanState::new(instance, tmp_ctx, natts);
        (*scan_desc).am_state = Box::into_raw(Box::new(state));

        scan_desc as pg_sys::TableScanDesc
    }
}

#[pg_guard]
pub extern "C-unwind" fn scan_end<E, T>(scan: pg_sys::TableScanDesc)
where
    E: Into<ErrorReport>,
    T: AmScan<E>,
{
    unsafe {
        let custom_scan = scan as *mut CustomScanDesc<T>;
        if !(*custom_scan).am_state.is_null() {
            let mut state = Box::from_raw((*custom_scan).am_state);
            state.am_instance.scan_end().report_unwrap();

            crate::memory::delete_tam_memctx(state.tmp_ctx);
            drop(state);
        }

        pgrx::pg_sys::pfree(custom_scan as *mut ::core::ffi::c_void);
    }
}

#[pg_guard]
pub extern "C-unwind" fn scan_rescan<E, T>(
    scan: pg_sys::TableScanDesc,
    key: *mut pg_sys::ScanKeyData,
    set_params: bool,
    allow_strat: bool,
    allow_sync: bool,
    allow_pagemode: bool,
) where
    E: Into<ErrorReport>,
    T: AmScan<E>,
{
    unsafe {
        let custom_scan = scan as *mut CustomScanDesc<T>;
        if !(*custom_scan).am_state.is_null() {
            let state = &mut *(*custom_scan).am_state;

            // Convert raw pointer to safe Handle type
            let nkeys = (*custom_scan).base.rs_nkeys;
            let key_handle = if key.is_null() {
                None
            } else {
                Some(ScanKeyHandle::from_raw(key, nkeys))
            };

            state
                .am_instance
                .scan_rescan(
                    key_handle.as_ref(),
                    set_params,
                    allow_strat,
                    allow_sync,
                    allow_pagemode,
                )
                .report_unwrap()
        }
    }
}

#[pg_guard]
pub extern "C-unwind" fn scan_getnextslot<E, T>(
    scan: pg_sys::TableScanDesc,
    direction: pg_sys::ScanDirection::Type,
    slot: *mut pg_sys::TupleTableSlot,
) -> bool
where
    E: Into<ErrorReport>,
    T: AmScan<E>,
{
    unsafe {
        let custom_scan = scan as *mut CustomScanDesc<T>;
        pg_sys::ExecClearTuple(slot);

        let state = &mut *(*custom_scan).am_state;

        pg_sys::MemoryContextReset(state.tmp_ctx);

        let direction_handle = ScanDirection::from_raw(direction);
        let found = state
            .am_instance
            .scan_getnextslot(direction_handle, &mut state.row)
            .report_unwrap();

        if !found {
            return false;
        }

        state.row_to_slot(slot);
        true
    }
}

#[pg_guard]
pub extern "C-unwind" fn scan_set_tidrange<E, T>(
    scan: pg_sys::TableScanDesc,
    mintid: pg_sys::ItemPointer,
    maxtid: pg_sys::ItemPointer,
) where
    E: Into<ErrorReport>,
    T: AmScan<E>,
{
    unsafe {
        let custom_scan = scan as *mut CustomScanDesc<T>;
        let state = &mut *(*custom_scan).am_state;

        // Convert raw pointers to safe Handle types
        let mintid_handle = ItemPointer::from_raw(mintid);
        let maxtid_handle = ItemPointer::from_raw(maxtid);

        state
            .am_instance
            .scan_set_tidrange(&mintid_handle, &maxtid_handle)
            .report_unwrap()
    }
}

#[pg_guard]
pub extern "C-unwind" fn scan_getnextslot_tidrange<E, T>(
    scan: pg_sys::TableScanDesc,
    direction: pg_sys::ScanDirection::Type,
    slot: *mut pg_sys::TupleTableSlot,
) -> bool
where
    E: Into<ErrorReport>,
    T: AmScan<E>,
{
    unsafe {
        let custom_scan = scan as *mut CustomScanDesc<T>;

        pg_sys::ExecClearTuple(slot);

        let state = &mut *(*custom_scan).am_state;
        pg_sys::MemoryContextReset(state.tmp_ctx);

        let direction_handle = ScanDirection::from_raw(direction);
        let found = state
            .am_instance
            .scan_getnextslot_tidrange(direction_handle, &mut state.row)
            .report_unwrap();

        if !found {
            return false;
        }

        state.row_to_slot(slot);
        true
    }
}

#[pg_guard]
pub extern "C-unwind" fn scan_bitmap_next_block<E, T>(
    scan: pg_sys::TableScanDesc,
    tbmres: *mut pg_sys::TBMIterateResult,
) -> bool
where
    E: Into<ErrorReport>,
    T: AmScan<E>,
{
    unsafe {
        let custom_scan = scan as *mut CustomScanDesc<T>;
        let state = &mut *(*custom_scan).am_state;

        // Convert raw pointer to safe Handle type
        let tbmres_handle = TBMIterateResultHandle::from_raw(tbmres);

        state
            .am_instance
            .scan_bitmap_next_block(&tbmres_handle)
            .report_unwrap()
    }
}

#[pg_guard]
pub extern "C-unwind" fn scan_bitmap_next_tuple<E, T>(
    scan: pg_sys::TableScanDesc,
    tbmres: *mut pg_sys::TBMIterateResult,
    slot: *mut pg_sys::TupleTableSlot,
) -> bool
where
    E: Into<ErrorReport>,
    T: AmScan<E>,
{
    unsafe {
        let custom_scan = scan as *mut CustomScanDesc<T>;
        pg_sys::ExecClearTuple(slot);

        let state = &mut *(*custom_scan).am_state;
        pg_sys::MemoryContextReset(state.tmp_ctx);

        let tbmres_handle = TBMIterateResultHandle::from_raw(tbmres);
        let found = state
            .am_instance
            .scan_bitmap_next_tuple(&tbmres_handle, &mut state.row)
            .report_unwrap();

        if !found {
            return false;
        }

        state.row_to_slot(slot);
        true
    }
}

#[pg_guard]
pub extern "C-unwind" fn scan_sample_next_block<E, T>(
    scan: pg_sys::TableScanDesc,
    scanstate: *mut pg_sys::SampleScanState,
) -> bool
where
    E: Into<ErrorReport>,
    T: AmScan<E>,
{
    unsafe {
        let custom_scan = scan as *mut CustomScanDesc<T>;
        let state = &mut *(*custom_scan).am_state;
        let scanstate_handle = SampleScanStateHandle::from_raw(scanstate);
        state
            .am_instance
            .scan_sample_next_block(&scanstate_handle)
            .report_unwrap()
    }
}

#[pg_guard]
pub extern "C-unwind" fn scan_sample_next_tuple<E, T>(
    scan: pg_sys::TableScanDesc,
    scanstate: *mut pg_sys::SampleScanState,
    slot: *mut pg_sys::TupleTableSlot,
) -> bool
where
    E: Into<ErrorReport>,
    T: AmScan<E>,
{
    unsafe {
        let custom_scan = scan as *mut CustomScanDesc<T>;
        pg_sys::ExecClearTuple(slot);

        let state = &mut *(*custom_scan).am_state;
        pg_sys::MemoryContextReset(state.tmp_ctx);

        let scanstate_handle = SampleScanStateHandle::from_raw(scanstate);
        let found = state
            .am_instance
            .scan_sample_next_tuple(&scanstate_handle, &mut state.row)
            .report_unwrap();

        if !found {
            return false;
        }

        state.row_to_slot(slot);
        true
    }
}

#[pg_guard]
pub extern "C-unwind" fn parallelscan_estimate<E, T>(
    rel: pg_sys::Relation,
) -> pg_sys::Size
where
    E: Into<ErrorReport>,
    T: AmScan<E>,
{
    // Convert raw pointer to safe Handle type
    let rel_handle = unsafe { RelationHandle::from_raw(rel) };
    T::parallelscan_estimate(&rel_handle).report_unwrap()
}

#[pg_guard]
pub extern "C-unwind" fn parallelscan_initialize<E, T>(
    rel: pg_sys::Relation,
    pscan: pg_sys::ParallelTableScanDesc,
) -> pg_sys::Size
where
    E: Into<ErrorReport>,
    T: AmScan<E>,
{
    // Convert raw pointers to safe Handle types
    let rel_handle = unsafe { RelationHandle::from_raw(rel) };
    let pscan_handle = unsafe { ParallelTableScanDescHandle::from_raw(pscan) };
    T::parallelscan_initialize(&rel_handle, &pscan_handle).report_unwrap()
}

#[pg_guard]
pub extern "C-unwind" fn parallelscan_reinitialize<E, T>(
    rel: pg_sys::Relation,
    pscan: pg_sys::ParallelTableScanDesc,
) where
    E: Into<ErrorReport>,
    T: AmScan<E>,
{
    // Convert raw pointers to safe Handle types
    let rel_handle = unsafe { RelationHandle::from_raw(rel) };
    let pscan_handle = unsafe { ParallelTableScanDescHandle::from_raw(pscan) };
    T::parallelscan_reinitialize(&rel_handle, &pscan_handle).report_unwrap()
}

#[pg_guard]
pub extern "C-unwind" fn tuple_tid_valid<E, T>(
    scan: pg_sys::TableScanDesc,
    tid: pg_sys::ItemPointer,
) -> bool
where
    E: Into<ErrorReport>,
    T: AmScan<E>,
{
    unsafe {
        let custom_scan = scan as *mut CustomScanDesc<T>;
        let state = &mut *(*custom_scan).am_state;
        let tid_handle = ItemPointer::from_raw(tid);
        state
            .am_instance
            .tuple_tid_valid(&tid_handle)
            .report_unwrap()
    }
}

#[pg_guard]
pub extern "C-unwind" fn tuple_get_latest_tid<E, T>(
    scan: pg_sys::TableScanDesc,
    tid: pg_sys::ItemPointer,
) where
    E: Into<ErrorReport>,
    T: AmScan<E>,
{
    unsafe {
        let custom_scan = scan as *mut CustomScanDesc<T>;
        let state = &mut *(*custom_scan).am_state;
        let tid_handle = ItemPointer::from_raw(tid);

        state
            .am_instance
            .tuple_get_latest_tid(&tid_handle)
            .report_unwrap();
    }
}

#[pg_guard]
pub extern "C-unwind" fn scan_analyze_next_block<E, T>(
    scan: pg_sys::TableScanDesc,
    stream: *mut pg_sys::ReadStream,
) -> bool
where
    E: Into<ErrorReport>,
    T: AmScan<E>,
{
    unsafe {
        let custom_scan = scan as *mut CustomScanDesc<T>;
        let state = &mut *(*custom_scan).am_state;
        let stream_handle = ReadStreamHandle::from_raw(stream);

        state
            .am_instance
            .scan_analyze_next_block(&stream_handle)
            .report_unwrap()
    }
}

#[pg_guard]
pub extern "C-unwind" fn scan_analyze_next_tuple<E, T>(
    scan: pg_sys::TableScanDesc,
    oldest_xmin: pg_sys::TransactionId,
    liverows: *mut f64,
    deadrows: *mut f64,
    slot: *mut pg_sys::TupleTableSlot,
) -> bool
where
    E: Into<ErrorReport>,
    T: AmScan<E>,
{
    unsafe {
        let custom_scan = scan as *mut CustomScanDesc<T>;
        pg_sys::ExecClearTuple(slot);

        let state = &mut *(*custom_scan).am_state;
        pg_sys::MemoryContextReset(state.tmp_ctx);

        let (found, live, dead) = state
            .am_instance
            .scan_analyze_next_tuple(oldest_xmin, &mut state.row)
            .report_unwrap();

        *liverows = live;
        *deadrows = dead;

        if !found {
            return false;
        }

        state.row_to_slot(slot);
        true
    }
}

pub fn register<E, T>(routine: &mut pg_sys::TableAmRoutine)
where
    E: Into<ErrorReport>,
    T: AmScan<E>,
{
    routine.slot_callbacks = Some(slot_callbacks::<E, T>);
    routine.scan_begin = Some(scan_begin::<E, T>);
    routine.scan_end = Some(scan_end::<E, T>);
    routine.scan_rescan = Some(scan_rescan::<E, T>);
    routine.scan_getnextslot = Some(scan_getnextslot::<E, T>);
    routine.scan_set_tidrange = Some(scan_set_tidrange::<E, T>);
    routine.scan_getnextslot_tidrange = Some(scan_getnextslot_tidrange::<E, T>);

    routine.scan_bitmap_next_block = Some(scan_bitmap_next_block::<E, T>);
    routine.scan_bitmap_next_tuple = Some(scan_bitmap_next_tuple::<E, T>);
    routine.scan_sample_next_block = Some(scan_sample_next_block::<E, T>);
    routine.scan_sample_next_tuple = Some(scan_sample_next_tuple::<E, T>);

    routine.parallelscan_estimate = Some(parallelscan_estimate::<E, T>);
    routine.parallelscan_initialize = Some(parallelscan_initialize::<E, T>);
    routine.parallelscan_reinitialize = Some(parallelscan_reinitialize::<E, T>);

    routine.tuple_tid_valid = Some(tuple_tid_valid::<E, T>);
    routine.tuple_get_latest_tid = Some(tuple_get_latest_tid::<E, T>);

    routine.scan_analyze_next_block = Some(scan_analyze_next_block::<E, T>);
    routine.scan_analyze_next_tuple = Some(scan_analyze_next_tuple::<E, T>);
}
