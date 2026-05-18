//! Sequential scan callback wrappers for table-AM scan traits
//!
//! This module provides wrapper functions that bridge PostgreSQL's sequential
//! scan callbacks with the AM-level scan facet and scan session implementation.

use super::common::FfiContainer;
use crate::api::{AmScanSession, TableAccessMethod};
use crate::diag::ReportableError;
use crate::handles::{
    ItemPointer, ParallelTableScanDescHandle, ReadStreamHandle, RelationHandle,
    SampleScanStateHandle, ScanDirection, ScanKeyHandle, SnapshotHandle,
    TBMIterateResultHandle,
};
use pgrx::prelude::*;

type CustomScanDesc<T> = FfiContainer<pg_sys::TableScanDescData, T>;

#[pg_guard]
pub extern "C-unwind" fn slot_callbacks<A>(
    _rel: pg_sys::Relation,
) -> *const pg_sys::TupleTableSlotOps
where
    A: TableAccessMethod,
{
    A::slot_callbacks()
}

#[pg_guard]
pub extern "C-unwind" fn scan_begin<A>(
    rel: pg_sys::Relation,
    snapshot: pg_sys::Snapshot,
    nkeys: ::core::ffi::c_int,
    key: *mut pg_sys::ScanKeyData,
    pscan: pg_sys::ParallelTableScanDesc,
    flags: u32,
) -> pg_sys::TableScanDesc
where
    A: TableAccessMethod,
{
    unsafe {
        // PostgreSQL may skip scan_end after ERROR, so the Rust session state
        // is owned by this context's reset/delete callback.  The descriptor is
        // allocated in the same context so the C wrapper and Rust state share
        // one lifetime boundary.
        const LIFECYCLE_CTX_NAME: &core::ffi::CStr =
            c"pg-lakebase TableScan lifecycle";
        let scan_desc = CustomScanDesc::<A::ScanSession>::alloc(
            pg_sys::CurrentMemoryContext,
            LIFECYCLE_CTX_NAME,
        );
        {
            let base = (*scan_desc).base_mut();
            base.rs_rd = rel;
            base.rs_snapshot = snapshot;
            base.rs_nkeys = nkeys;
            base.rs_key = key;
            base.rs_flags = flags;
            base.rs_parallel = pscan;
        }

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

        let mut instance = <A::ScanSession as AmScanSession>::new(
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

        const TMP_CTX_NAME: &core::ffi::CStr = c"pg-lakebase TableScan tmp";
        (*scan_desc).init_session(instance, TMP_CTX_NAME, natts);

        scan_desc as pg_sys::TableScanDesc
    }
}

#[pg_guard]
pub extern "C-unwind" fn scan_end<A>(scan: pg_sys::TableScanDesc)
where
    A: TableAccessMethod,
{
    unsafe {
        let custom_scan = CustomScanDesc::<A::ScanSession>::from_base_ptr(scan);
        if let Some(end_res) =
            CustomScanDesc::<A::ScanSession>::finish_with(custom_scan, |state| {
                state.am_instance.scan_end()
            })
        {
            end_res.report_unwrap();
        }
    }
}

#[pg_guard]
pub extern "C-unwind" fn scan_rescan<A>(
    scan: pg_sys::TableScanDesc,
    key: *mut pg_sys::ScanKeyData,
    set_params: bool,
    allow_strat: bool,
    allow_sync: bool,
    allow_pagemode: bool,
) where
    A: TableAccessMethod,
{
    unsafe {
        let custom_scan = CustomScanDesc::<A::ScanSession>::from_base_ptr(scan);
        let nkeys = (*custom_scan).base().rs_nkeys;
        if let Some(state) = (*custom_scan).session_mut_if_initialized() {
            // Convert raw pointer to safe Handle type
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
pub extern "C-unwind" fn scan_getnextslot<A>(
    scan: pg_sys::TableScanDesc,
    direction: pg_sys::ScanDirection::Type,
    slot: *mut pg_sys::TupleTableSlot,
) -> bool
where
    A: TableAccessMethod,
{
    unsafe {
        let custom_scan = CustomScanDesc::<A::ScanSession>::from_base_ptr(scan);
        pg_sys::ExecClearTuple(slot);

        let state = (*custom_scan).session_mut();
        state.reset_tmp_context();

        let direction_handle = ScanDirection::from_raw(direction);
        let found = state
            .am_instance
            .scan_getnextslot(direction_handle, &mut state.row)
            .report_unwrap();

        if !found {
            return false;
        }

        state.write_row_to_slot(slot).report_unwrap();
        true
    }
}

#[pg_guard]
pub extern "C-unwind" fn scan_set_tidrange<A>(
    scan: pg_sys::TableScanDesc,
    mintid: pg_sys::ItemPointer,
    maxtid: pg_sys::ItemPointer,
) where
    A: TableAccessMethod,
{
    unsafe {
        let custom_scan = CustomScanDesc::<A::ScanSession>::from_base_ptr(scan);
        let state = (*custom_scan).session_mut();

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
pub extern "C-unwind" fn scan_getnextslot_tidrange<A>(
    scan: pg_sys::TableScanDesc,
    direction: pg_sys::ScanDirection::Type,
    slot: *mut pg_sys::TupleTableSlot,
) -> bool
where
    A: TableAccessMethod,
{
    unsafe {
        let custom_scan = CustomScanDesc::<A::ScanSession>::from_base_ptr(scan);

        pg_sys::ExecClearTuple(slot);

        let state = (*custom_scan).session_mut();
        state.reset_tmp_context();

        let direction_handle = ScanDirection::from_raw(direction);
        let found = state
            .am_instance
            .scan_getnextslot_tidrange(direction_handle, &mut state.row)
            .report_unwrap();

        if !found {
            return false;
        }

        state.write_row_to_slot(slot).report_unwrap();
        true
    }
}

#[pg_guard]
pub extern "C-unwind" fn scan_bitmap_next_block<A>(
    scan: pg_sys::TableScanDesc,
    tbmres: *mut pg_sys::TBMIterateResult,
) -> bool
where
    A: TableAccessMethod,
{
    unsafe {
        let custom_scan = CustomScanDesc::<A::ScanSession>::from_base_ptr(scan);
        let state = (*custom_scan).session_mut();

        // Convert raw pointer to safe Handle type
        let tbmres_handle = TBMIterateResultHandle::from_raw(tbmres);

        state
            .am_instance
            .scan_bitmap_next_block(&tbmres_handle)
            .report_unwrap()
    }
}

#[pg_guard]
pub extern "C-unwind" fn scan_bitmap_next_tuple<A>(
    scan: pg_sys::TableScanDesc,
    tbmres: *mut pg_sys::TBMIterateResult,
    slot: *mut pg_sys::TupleTableSlot,
) -> bool
where
    A: TableAccessMethod,
{
    unsafe {
        let custom_scan = CustomScanDesc::<A::ScanSession>::from_base_ptr(scan);
        pg_sys::ExecClearTuple(slot);

        let state = (*custom_scan).session_mut();
        state.reset_tmp_context();

        let tbmres_handle = TBMIterateResultHandle::from_raw(tbmres);
        let found = state
            .am_instance
            .scan_bitmap_next_tuple(&tbmres_handle, &mut state.row)
            .report_unwrap();

        if !found {
            return false;
        }

        state.write_row_to_slot(slot).report_unwrap();
        true
    }
}

#[pg_guard]
pub extern "C-unwind" fn scan_sample_next_block<A>(
    scan: pg_sys::TableScanDesc,
    scanstate: *mut pg_sys::SampleScanState,
) -> bool
where
    A: TableAccessMethod,
{
    unsafe {
        let custom_scan = CustomScanDesc::<A::ScanSession>::from_base_ptr(scan);
        let state = (*custom_scan).session_mut();
        let scanstate_handle = SampleScanStateHandle::from_raw(scanstate);
        state
            .am_instance
            .scan_sample_next_block(&scanstate_handle)
            .report_unwrap()
    }
}

#[pg_guard]
pub extern "C-unwind" fn scan_sample_next_tuple<A>(
    scan: pg_sys::TableScanDesc,
    scanstate: *mut pg_sys::SampleScanState,
    slot: *mut pg_sys::TupleTableSlot,
) -> bool
where
    A: TableAccessMethod,
{
    unsafe {
        let custom_scan = CustomScanDesc::<A::ScanSession>::from_base_ptr(scan);
        pg_sys::ExecClearTuple(slot);

        let state = (*custom_scan).session_mut();
        state.reset_tmp_context();

        let scanstate_handle = SampleScanStateHandle::from_raw(scanstate);
        let found = state
            .am_instance
            .scan_sample_next_tuple(&scanstate_handle, &mut state.row)
            .report_unwrap();

        if !found {
            return false;
        }

        state.write_row_to_slot(slot).report_unwrap();
        true
    }
}

#[pg_guard]
pub extern "C-unwind" fn parallelscan_estimate<A>(
    rel: pg_sys::Relation,
) -> pg_sys::Size
where
    A: TableAccessMethod,
{
    // Convert raw pointer to safe Handle type
    let rel_handle = unsafe { RelationHandle::from_raw(rel) };
    A::parallelscan_estimate(&rel_handle).report_unwrap()
}

#[pg_guard]
pub extern "C-unwind" fn parallelscan_initialize<A>(
    rel: pg_sys::Relation,
    pscan: pg_sys::ParallelTableScanDesc,
) -> pg_sys::Size
where
    A: TableAccessMethod,
{
    // Convert raw pointers to safe Handle types
    let rel_handle = unsafe { RelationHandle::from_raw(rel) };
    let pscan_handle = unsafe { ParallelTableScanDescHandle::from_raw(pscan) };
    A::parallelscan_initialize(&rel_handle, &pscan_handle).report_unwrap()
}

#[pg_guard]
pub extern "C-unwind" fn parallelscan_reinitialize<A>(
    rel: pg_sys::Relation,
    pscan: pg_sys::ParallelTableScanDesc,
) where
    A: TableAccessMethod,
{
    // Convert raw pointers to safe Handle types
    let rel_handle = unsafe { RelationHandle::from_raw(rel) };
    let pscan_handle = unsafe { ParallelTableScanDescHandle::from_raw(pscan) };
    A::parallelscan_reinitialize(&rel_handle, &pscan_handle).report_unwrap()
}

#[pg_guard]
pub extern "C-unwind" fn tuple_tid_valid<A>(
    scan: pg_sys::TableScanDesc,
    tid: pg_sys::ItemPointer,
) -> bool
where
    A: TableAccessMethod,
{
    unsafe {
        let custom_scan = CustomScanDesc::<A::ScanSession>::from_base_ptr(scan);
        let state = (*custom_scan).session_mut();
        let tid_handle = ItemPointer::from_raw(tid);
        state
            .am_instance
            .tuple_tid_valid(&tid_handle)
            .report_unwrap()
    }
}

#[pg_guard]
pub extern "C-unwind" fn tuple_get_latest_tid<A>(
    scan: pg_sys::TableScanDesc,
    tid: pg_sys::ItemPointer,
) where
    A: TableAccessMethod,
{
    unsafe {
        let custom_scan = CustomScanDesc::<A::ScanSession>::from_base_ptr(scan);
        let state = (*custom_scan).session_mut();
        let mut tid_handle = ItemPointer::from_raw(tid);

        state
            .am_instance
            .tuple_get_latest_tid(&mut tid_handle)
            .report_unwrap();

        tid_handle.write_to_raw(tid);
    }
}

#[pg_guard]
pub extern "C-unwind" fn scan_analyze_next_block<A>(
    scan: pg_sys::TableScanDesc,
    stream: *mut pg_sys::ReadStream,
) -> bool
where
    A: TableAccessMethod,
{
    unsafe {
        let custom_scan = CustomScanDesc::<A::ScanSession>::from_base_ptr(scan);
        let state = (*custom_scan).session_mut();
        let stream_handle = ReadStreamHandle::from_raw(stream);

        state
            .am_instance
            .scan_analyze_next_block(&stream_handle)
            .report_unwrap()
    }
}

#[pg_guard]
pub extern "C-unwind" fn scan_analyze_next_tuple<A>(
    scan: pg_sys::TableScanDesc,
    oldest_xmin: pg_sys::TransactionId,
    liverows: *mut f64,
    deadrows: *mut f64,
    slot: *mut pg_sys::TupleTableSlot,
) -> bool
where
    A: TableAccessMethod,
{
    unsafe {
        let custom_scan = CustomScanDesc::<A::ScanSession>::from_base_ptr(scan);
        pg_sys::ExecClearTuple(slot);

        let state = (*custom_scan).session_mut();
        state.reset_tmp_context();

        let (found, live, dead) = state
            .am_instance
            .scan_analyze_next_tuple(oldest_xmin, &mut state.row)
            .report_unwrap();

        *liverows = live;
        *deadrows = dead;

        if !found {
            return false;
        }

        state.write_row_to_slot(slot).report_unwrap();
        true
    }
}

pub fn register<A>(routine: &mut pg_sys::TableAmRoutine)
where
    A: TableAccessMethod,
{
    let capabilities = A::SCAN_CAPABILITIES;

    routine.slot_callbacks = Some(slot_callbacks::<A>);
    routine.scan_begin = Some(scan_begin::<A>);
    routine.scan_end = Some(scan_end::<A>);
    routine.scan_rescan = Some(scan_rescan::<A>);
    routine.scan_getnextslot = Some(scan_getnextslot::<A>);
    if capabilities.tid_range {
        routine.scan_set_tidrange = Some(scan_set_tidrange::<A>);
        routine.scan_getnextslot_tidrange = Some(scan_getnextslot_tidrange::<A>);
    }

    if capabilities.bitmap {
        routine.scan_bitmap_next_block = Some(scan_bitmap_next_block::<A>);
        routine.scan_bitmap_next_tuple = Some(scan_bitmap_next_tuple::<A>);
    }
    routine.scan_sample_next_block = Some(scan_sample_next_block::<A>);
    routine.scan_sample_next_tuple = Some(scan_sample_next_tuple::<A>);

    routine.parallelscan_estimate = Some(parallelscan_estimate::<A>);
    routine.parallelscan_initialize = Some(parallelscan_initialize::<A>);
    routine.parallelscan_reinitialize = Some(parallelscan_reinitialize::<A>);

    routine.tuple_tid_valid = Some(tuple_tid_valid::<A>);
    routine.tuple_get_latest_tid = Some(tuple_get_latest_tid::<A>);

    routine.scan_analyze_next_block = Some(scan_analyze_next_block::<A>);
    routine.scan_analyze_next_tuple = Some(scan_analyze_next_tuple::<A>);
}
