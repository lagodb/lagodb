//! Index callback wrappers for table-AM index traits
//!
//! This module provides FFI boundary functions for index scan operations.
//! All unsafe operations are handled here, keeping trait implementations safe.

use super::common::FfiContainer;
use crate::am_lifecycle::SessionContext;
use crate::backend::MemoryContextHandle;
use crate::diag::PgReportError;
use crate::diag::ReportableError;
use crate::handles::{
    IndexBuildCallbackHandle, IndexInfoHandle, ItemPointer, RelationHandle,
    SnapshotHandle, TMIndexDeleteOpHandle, TableScanDescHandle,
    ValidateIndexStateHandle,
};
use crate::table_am::{AmIndexFetchSession, IndexBuildContext, TableAccessMethod};
use crate::tuple::{Row, RowDatumCodec, SlotDatumWriter};
use pgrx::prelude::*;
use std::ffi::c_void;
use std::ptr::NonNull;

struct IndexFetchState<T> {
    am_instance: T,
    row: Row,
    tmp_ctx: MemoryContextHandle,
    /// Bound only when the index callback returns an owned Row. Keeping this
    /// lazy avoids imposing semantic UTF-8 requirements on unrelated paths.
    row_codec: Option<RowDatumCodec>,
}

type CustomIndexFetchData<T> =
    FfiContainer<pg_sys::IndexFetchTableData, IndexFetchState<T>>;

impl<T> IndexFetchState<T> {
    fn new(am_instance: T, tmp_ctx: MemoryContextHandle, natts: usize) -> Self {
        Self {
            am_instance,
            row: Row::with_width(natts),
            tmp_ctx,
            row_codec: None,
        }
    }

    /// Free the previous fetch's datums.
    ///
    /// # Safety
    ///
    /// Call at callback entry, before anything switches into `tmp_ctx`, and
    /// only once the previous fetch's slot contents are no longer used.
    unsafe fn reset_tmp_context(&mut self) {
        // SAFETY: the lifecycle context is live while PostgreSQL holds the
        // fetch. PostgreSQL calls in with its own context current, and this
        // framework enters `tmp_ctx` only through scoped switches.
        unsafe { self.tmp_ctx.reset() };
    }

    unsafe fn write_row_to_slot(
        &mut self,
        slot: *mut pg_sys::TupleTableSlot,
    ) -> Result<(), PgReportError> {
        // Index fetch is a Row-returning path; bind only when it actually
        // produces a row, rather than during the common begin callback.
        if self.row_codec.is_none() {
            self.row_codec = Some(
                // SAFETY: the executor passes an initialized slot whose tuple
                // descriptor remains valid for this fetch callback.
                unsafe { RowDatumCodec::from_slot(slot) }
                    .map_err(PgReportError::from_domain_error)?,
            );
        }
        let mut columns =
            unsafe { SlotDatumWriter::new(slot, self.tmp_ctx.as_ptr()) };
        unsafe {
            columns.fill_from_row(
                &mut self.row,
                self.row_codec
                    .as_ref()
                    .expect("row codec initialized before row write"),
            )
        }
    }
}

#[pg_guard]
pub extern "C-unwind" fn index_fetch_begin<A>(
    rel: pg_sys::Relation,
) -> *mut pg_sys::IndexFetchTableData
where
    A: TableAccessMethod,
{
    // SAFETY: PG passes the open table relation; the session is installed
    // before the container is allocated and published.
    unsafe {
        let rel_handle = RelationHandle::from_raw(rel);
        let mut instance =
            <A::IndexFetchSession as AmIndexFetchSession>::new(&rel_handle)
                .report_unwrap();
        instance.index_fetch_begin().report_unwrap();

        let natts = (*(*rel).rd_att).natts as usize;
        let context = MemoryContextHandle::create(
            pg_sys::CurrentMemoryContext,
            c"lagodb IndexFetch lifecycle",
        );
        let tmp_ctx = context.create_child(c"lagodb IndexFetch tmp");
        let state = IndexFetchState::new(instance, tmp_ctx, natts);
        let fetch_data = CustomIndexFetchData::<A::IndexFetchSession>::alloc(
            SessionContext::install(context, state),
        );
        (*fetch_data).base_mut().rel = rel;

        fetch_data as *mut pg_sys::IndexFetchTableData
    }
}

#[pg_guard]
pub extern "C-unwind" fn index_fetch_reset<A>(data: *mut pg_sys::IndexFetchTableData)
where
    A: TableAccessMethod,
{
    unsafe {
        let custom_data =
            CustomIndexFetchData::<A::IndexFetchSession>::from_base_ptr(data);
        (*custom_data)
            .session_mut()
            .am_instance
            .index_fetch_reset()
            .report_unwrap();
    }
}

#[pg_guard]
pub extern "C-unwind" fn index_fetch_end<A>(data: *mut pg_sys::IndexFetchTableData)
where
    A: TableAccessMethod,
{
    unsafe {
        let custom_data =
            CustomIndexFetchData::<A::IndexFetchSession>::from_base_ptr(data);
        CustomIndexFetchData::<A::IndexFetchSession>::finish_with(
            custom_data,
            |state| state.am_instance.index_fetch_end(),
        )
        .report_unwrap();
    }
}

#[pg_guard]
pub extern "C-unwind" fn index_fetch_tuple<A>(
    data: *mut pg_sys::IndexFetchTableData,
    tid: pg_sys::ItemPointer,
    snapshot: pg_sys::Snapshot,
    slot: *mut pg_sys::TupleTableSlot,
    call_again: *mut bool,
    all_dead: *mut bool,
) -> bool
where
    A: TableAccessMethod,
{
    unsafe {
        pg_sys::ExecClearTuple(slot);

        let custom_data =
            CustomIndexFetchData::<A::IndexFetchSession>::from_base_ptr(data);
        let state = (*custom_data).session_mut();
        state.reset_tmp_context();

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

        state.write_row_to_slot(slot).report_unwrap();
        pg_sys::ExecStoreVirtualTuple(slot);

        true
    }
}

#[pg_guard]
#[allow(clippy::too_many_arguments)] // PG TableAmRoutine::index_build_range_scan ABI.
pub extern "C-unwind" fn index_build_range_scan<A>(
    table_rel: pg_sys::Relation,
    index_rel: pg_sys::Relation,
    index_info: *mut pg_sys::IndexInfo,
    allow_sync: bool,
    anyvisible: bool,
    progress: bool,
    start_blockno: pg_sys::BlockNumber,
    numblocks: pg_sys::BlockNumber,
    callback: pg_sys::IndexBuildCallback,
    callback_state: *mut c_void,
    scan: pg_sys::TableScanDesc,
) -> f64
where
    A: TableAccessMethod,
{
    let table_rel_handle = unsafe { RelationHandle::from_raw(table_rel) };
    let index_rel_handle = unsafe { RelationHandle::from_raw(index_rel) };
    let index_info_handle = unsafe { IndexInfoHandle::from_raw(index_info) };
    let mut callback_handle = unsafe {
        IndexBuildCallbackHandle::from_raw(
            &index_rel_handle,
            callback,
            callback_state,
        )
    };
    let scan_handle = NonNull::new(scan)
        .map(|scan| unsafe { TableScanDescHandle::from_non_null(scan) });

    let context = IndexBuildContext {
        table_rel: &table_rel_handle,
        index_rel: &index_rel_handle,
        index_info: &index_info_handle,
        allow_sync,
        anyvisible,
        progress,
        start_blockno,
        numblocks,
        scan: scan_handle.as_ref(),
    };
    A::index_build_range_scan(&context, &mut callback_handle).report_unwrap()
}

#[pg_guard]
pub extern "C-unwind" fn index_validate_scan<A>(
    table_rel: pg_sys::Relation,
    index_rel: pg_sys::Relation,
    index_info: *mut pg_sys::IndexInfo,
    snapshot: pg_sys::Snapshot,
    state: *mut pg_sys::ValidateIndexState,
) where
    A: TableAccessMethod,
{
    let table_rel_handle = unsafe { RelationHandle::from_raw(table_rel) };
    let index_rel_handle = unsafe { RelationHandle::from_raw(index_rel) };
    let index_info_handle = unsafe { IndexInfoHandle::from_raw(index_info) };
    let snapshot_handle = unsafe { SnapshotHandle::from_raw(snapshot) };
    let state_handle = unsafe { ValidateIndexStateHandle::from_raw(state) };

    A::index_validate_scan(
        &table_rel_handle,
        &index_rel_handle,
        &index_info_handle,
        &snapshot_handle,
        &state_handle,
    )
    .report_unwrap()
}

#[pg_guard]
pub extern "C-unwind" fn index_delete_tuples<A>(
    rel: pg_sys::Relation,
    delstate: *mut pg_sys::TM_IndexDeleteOp,
) -> pg_sys::TransactionId
where
    A: TableAccessMethod,
{
    let rel_handle = unsafe { RelationHandle::from_raw(rel) };
    let mut delstate_handle = unsafe { TMIndexDeleteOpHandle::from_raw(delstate) };

    A::index_delete_tuples(&rel_handle, &mut delstate_handle).report_unwrap()
}

pub fn register<A>(routine: &mut pg_sys::TableAmRoutine)
where
    A: TableAccessMethod,
{
    routine.index_fetch_begin = Some(index_fetch_begin::<A>);
    routine.index_fetch_reset = Some(index_fetch_reset::<A>);
    routine.index_fetch_end = Some(index_fetch_end::<A>);
    routine.index_fetch_tuple = Some(index_fetch_tuple::<A>);
    routine.index_build_range_scan = Some(index_build_range_scan::<A>);
    routine.index_validate_scan = Some(index_validate_scan::<A>);
    routine.index_delete_tuples = Some(index_delete_tuples::<A>);
}
