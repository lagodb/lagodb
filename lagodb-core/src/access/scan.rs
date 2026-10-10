//! Sequential scan callback wrappers for table-AM scan traits
//!
//! This module provides wrapper functions that bridge PostgreSQL's sequential
//! scan callbacks with the AM-level scan facet and scan session implementation.

mod descriptor;

use crate::api::{AmResult, AmScanSession, ScanFlags, TableAccessMethod};
use crate::batch::ScanBatchDriver;
use crate::diag::ReportableError;
use crate::handles::{
    AnalyzeReadStreamHandle, ItemPointer, OwnedScanKeys, ParallelTableScanDescHandle,
    RelationHandle, SampleScanStateHandle, ScanDirection, SnapshotHandle,
    TBMIterateResultHandle,
};
use crate::tuple::SlotColumns;
pub(crate) use descriptor::TableAmScanDesc;
use pgrx::memcxt::PgMemoryContexts;
use pgrx::prelude::*;
use std::sync::OnceLock;

/// Virtual-slot operations that preserve `tts_tid` when PostgreSQL copies an
/// ANALYZE sample into a HeapTuple.
///
/// PostgreSQL's stock `tts_virtual_copy_heap_tuple()` forms only attribute
/// data, leaving `HeapTuple.t_self` invalid. ANALYZE later sorts its reservoir
/// by `t_self` to compute column correlation, so a columnar AM must retain the
/// synthetic physical order supplied in the slot.
pub fn virtual_slot_callbacks_with_tid() -> *const pg_sys::TupleTableSlotOps {
    static OPS: OnceLock<pg_sys::TupleTableSlotOps> = OnceLock::new();
    OPS.get_or_init(|| {
        // SAFETY: PostgreSQL exposes TTSOpsVirtual as immutable process-lifetime
        // data. Copying the POD function table and replacing one callback keeps
        // every other virtual-slot invariant and gives this crate stable-owned
        // process-lifetime storage.
        let mut ops = unsafe { pg_sys::TTSOpsVirtual };
        ops.copy_heap_tuple = Some(copy_virtual_heap_tuple_with_tid);
        ops
    })
}

#[pg_guard]
unsafe extern "C-unwind" fn copy_virtual_heap_tuple_with_tid(
    slot: *mut pg_sys::TupleTableSlot,
) -> pg_sys::HeapTuple {
    // SAFETY: PostgreSQL calls a TupleTableSlotOps callback with a non-null,
    // non-empty slot matching this ops table. slot_getallattrs materializes all
    // virtual attributes before heap_form_tuple borrows the value/null arrays.
    unsafe {
        pg_sys::slot_getallattrs(slot);
        let tuple = pg_sys::heap_form_tuple(
            (*slot).tts_tupleDescriptor,
            (*slot).tts_values,
            (*slot).tts_isnull,
        );
        if !tuple.is_null() {
            (*tuple).t_self = (*slot).tts_tid;
        }
        tuple
    }
}

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
    // This is the sole PostgreSQL reporting boundary for scan initialization.
    unsafe { begin_scan::<A>(rel, snapshot, nkeys, key, pscan, flags) }
        .report_unwrap()
}

unsafe fn begin_scan<A: TableAccessMethod>(
    rel: pg_sys::Relation,
    snapshot: pg_sys::Snapshot,
    nkeys: i32,
    key: *mut pg_sys::ScanKeyData,
    pscan: pg_sys::ParallelTableScanDesc,
    flags: u32,
) -> AmResult<pg_sys::TableScanDesc> {
    // SAFETY: PG supplies live callback arguments. ANALYZE passes no snapshot;
    // the parallel descriptor is absent for serial scans.
    unsafe {
        let keys = OwnedScanKeys::copy_from_raw(key, nkeys);
        let relation = RelationHandle::from_raw(rel);
        let snapshot_handle =
            (!snapshot.is_null()).then(|| SnapshotHandle::from_raw(snapshot));
        let parallel =
            (!pscan.is_null()).then(|| ParallelTableScanDescHandle::from_raw(pscan));
        let instance = A::ScanSession::new(
            &relation,
            snapshot_handle.as_ref(),
            parallel.as_ref(),
            ScanFlags::from_bits(flags),
        )?;
        let desc = TableAmScanDesc::<A::ScanSession>::create(
            pg_sys::CurrentMemoryContext,
            rel,
            snapshot,
            pscan,
            flags,
            keys,
            instance,
        );
        (*desc).begin()?;
        Ok(desc.cast())
    }
}

#[pg_guard]
pub extern "C-unwind" fn scan_end<A>(scan: pg_sys::TableScanDesc)
where
    A: TableAccessMethod,
{
    unsafe {
        let custom_scan = TableAmScanDesc::<A::ScanSession>::from_base_ptr(scan);
        if let Some(end_res) =
            TableAmScanDesc::<A::ScanSession>::finish_with(custom_scan, |state| {
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
        let custom_scan = TableAmScanDesc::<A::ScanSession>::from_base_ptr(scan);
        let nkeys = (*custom_scan).base().rs_nkeys;
        if let Some(state) = (*custom_scan).session_mut_if_initialized() {
            // PostgreSQL heap-AM rescan semantics: a non-null `key` argument
            // *replaces* the previously stored keys (memcpy in
            // `initscan`); a null `key` keeps the prior keys. We apply the
            // same rule on the dispatcher-owned buffer before handing it to
            // the AM, so AM implementations only ever see the *effective*
            // key set for the upcoming scan.
            if !key.is_null() {
                state.scan_keys.replace_with(key, nkeys);
            }

            state
                .am_instance
                .scan_rescan(
                    &state.scan_keys,
                    set_params,
                    allow_strat,
                    allow_sync,
                    allow_pagemode,
                )
                .report_unwrap();

            // Re-publish the AM-owned buffer to the descriptor. The heap-AM
            // contract fixes `rs_nkeys` at `scan_begin` time and never
            // changes it, so the length here is unchanged; we only need to
            // refresh `rs_key` because `Vec` may reallocate inside
            // `replace_with`. `rs_key_ptr` returns `NULL` when the buffer
            // is empty, matching PG heap AM convention.
            let new_nkeys = state.scan_keys.len() as core::ffi::c_int;
            let new_ptr = state.scan_keys.rs_key_ptr();
            let base = (*custom_scan).base_mut();
            base.rs_nkeys = new_nkeys;
            base.rs_key = new_ptr;
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
        let custom_scan = TableAmScanDesc::<A::ScanSession>::from_base_ptr(scan);
        pg_sys::ExecClearTuple(slot);

        let state = (*custom_scan).session_mut();
        state.reset_tmp_context();

        let direction = ScanDirection::try_from_raw(direction).report_unwrap();

        // Copied out before borrowing `am_instance`: the slot-fill path needs
        // both the session's context/width and a `&mut` driver at once.
        let tmp_ctx = state.tmp_ctx;

        // One uniform slot-filling path: ask the session's driver for the next
        // tuple. row-vs-column is the driver's own concern. Switch the current
        // context to the just-reset `tmp_ctx` so the driver's varlena palloc
        // lands there and is freed on the next fetch's reset.
        let found = PgMemoryContexts::For(tmp_ctx)
            .switch_to(|_| {
                let mut cols = SlotColumns::new(slot, tmp_ctx);
                state
                    .am_instance
                    .scan_driver()
                    .next_into_slot(direction, &mut cols)
            })
            .report_unwrap();

        // Exactly once on a produced row, never on end-of-scan, so the
        // slot-non-empty invariant holds without delegating it to the AM.
        if found {
            pg_sys::ExecStoreVirtualTuple(slot);
        }
        found
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
        let custom_scan = TableAmScanDesc::<A::ScanSession>::from_base_ptr(scan);
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
        let custom_scan = TableAmScanDesc::<A::ScanSession>::from_base_ptr(scan);

        pg_sys::ExecClearTuple(slot);

        let state = (*custom_scan).session_mut();
        state.reset_tmp_context();

        let direction_handle = ScanDirection::try_from_raw(direction).report_unwrap();
        let found = state
            .am_instance
            .scan_getnextslot_tidrange(direction_handle, &mut state.row)
            .report_unwrap();

        if !found {
            return false;
        }

        state.write_row_to_slot(slot).report_unwrap();
        pg_sys::ExecStoreVirtualTuple(slot);
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
        let custom_scan = TableAmScanDesc::<A::ScanSession>::from_base_ptr(scan);
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
        let custom_scan = TableAmScanDesc::<A::ScanSession>::from_base_ptr(scan);
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
        pg_sys::ExecStoreVirtualTuple(slot);
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
        let custom_scan = TableAmScanDesc::<A::ScanSession>::from_base_ptr(scan);
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
        let custom_scan = TableAmScanDesc::<A::ScanSession>::from_base_ptr(scan);
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
        pg_sys::ExecStoreVirtualTuple(slot);
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
        let custom_scan = TableAmScanDesc::<A::ScanSession>::from_base_ptr(scan);
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
        let custom_scan = TableAmScanDesc::<A::ScanSession>::from_base_ptr(scan);
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
        let custom_scan = TableAmScanDesc::<A::ScanSession>::from_base_ptr(scan);
        let state = (*custom_scan).session_mut();
        // SAFETY: PostgreSQL invokes this callback only from the active
        // acquire_sample_rows() loop. On PG17 that loop owns both `stream` and
        // the BlockSamplerData installed as its callback-private data until
        // table_endscan() and read_stream_end() after the callback loop.
        let stream_handle = AnalyzeReadStreamHandle::from_raw(stream);

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
        let custom_scan = TableAmScanDesc::<A::ScanSession>::from_base_ptr(scan);
        pg_sys::ExecClearTuple(slot);

        let state = (*custom_scan).session_mut();
        state.reset_tmp_context();
        let mut columns = SlotColumns::new(slot, state.tmp_ctx);

        let outcome = state
            .am_instance
            .scan_analyze_next_tuple(oldest_xmin, &mut columns)
            .report_unwrap();

        // PostgreSQL owns these running totals.  A table AM reports the
        // contribution of the tuple it just inspected; replacing the totals
        // makes ANALYZE observe at most one row regardless of sample size.
        *liverows += outcome.live_delta;
        *deadrows += outcome.dead_delta;

        if !outcome.found {
            return false;
        }

        // ANALYZE may need copy behavior that is irrelevant to ordinary
        // executor scans. Keep the relation's normal slot ops during planning
        // and allocation so PostgreSQL can retain its exact virtual-slot fast
        // paths, then switch only a produced ANALYZE sample to a layout-
        // compatible callback table.
        let analyze_ops = A::analyze_slot_callbacks();
        debug_assert_eq!(
            (*(*slot).tts_ops).base_slot_size,
            (*analyze_ops).base_slot_size,
            "ANALYZE slot callbacks must preserve the allocated slot layout"
        );
        (*slot).tts_ops = analyze_ops;
        pg_sys::ExecStoreVirtualTuple(slot);
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
