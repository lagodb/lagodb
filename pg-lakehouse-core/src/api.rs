//! Provides interface types and trait to develop Postgres table access method
//!

use crate::data::Row;
use crate::handles::{
    BufferAccessStrategyHandle, BulkInsertStateHandle, CallbackStateHandle,
    IndexBuildCallbackHandle, IndexInfoHandle, ItemPointer,
    ParallelTableScanDescHandle, ReadStreamHandle, RelFileLocator, RelationHandle,
    SampleScanStateHandle, ScanDirection, ScanKeyHandle, SnapshotHandle,
    TBMIterateResultHandle, TM_FailureData, TableScanDescHandle,
    TupleTableSlotHandle, VacuumParamsHandle, ValidateIndexStateHandle,
    VarlenaHandle,
};
use crate::TableAmRoutine;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::pg_sys::{self};

pub trait TableAccessMethod<E: Into<ErrorReport>> {
    type ScanState: AmScan<E>;
    type RelationState: AmRelation<E>;
    type IndexState: AmIndex<E>;
    type DdlState: AmDdl<E>;
    type ModifyState: AmDml<E> + 'static;

    fn am_routine() -> TableAmRoutine
    where
        Self: Sized,
    {
        crate::registry::make_table_am_routine::<E, Self>()
    }
}

pub trait AmScan<E: Into<ErrorReport>> {
    fn new(
        rel: &RelationHandle,
        snapshot: &SnapshotHandle,
        key: Option<&ScanKeyHandle>,
        pscan: Option<&ParallelTableScanDescHandle>,
        flags: u32,
    ) -> Result<Self, E>
    where
        Self: Sized;

    fn scan_begin(&mut self) -> Result<(), E>;

    fn scan_getnextslot(
        &mut self,
        direction: ScanDirection,
        row: &mut Row,
    ) -> Result<bool, E>;

    fn scan_rescan(
        &mut self,
        key: Option<&ScanKeyHandle>,
        set_params: bool,
        allow_strat: bool,
        allow_sync: bool,
        allow_pagemode: bool,
    ) -> Result<(), E>;

    fn scan_end(&mut self) -> Result<(), E>;

    fn scan_set_tidrange(
        &mut self,
        mintid: &ItemPointer,
        maxtid: &ItemPointer,
    ) -> Result<(), E> {
        let _ = (mintid, maxtid);
        Ok(())
    }

    fn scan_getnextslot_tidrange(
        &mut self,
        direction: ScanDirection,
        row: &mut Row,
    ) -> Result<bool, E> {
        let _ = (direction, row);
        Ok(false)
    }

    fn scan_bitmap_next_block(
        &mut self,
        tbmres: &TBMIterateResultHandle,
    ) -> Result<bool, E>;

    fn scan_bitmap_next_tuple(
        &mut self,
        tbmres: &TBMIterateResultHandle,
        row: &mut Row,
    ) -> Result<bool, E>;

    fn scan_sample_next_block(
        &mut self,
        scanstate: &SampleScanStateHandle,
    ) -> Result<bool, E> {
        let _ = scanstate;
        Ok(false)
    }

    fn scan_sample_next_tuple(
        &mut self,
        scanstate: &SampleScanStateHandle,
        row: &mut Row,
    ) -> Result<bool, E> {
        let _ = (scanstate, row);
        Ok(false)
    }

    fn scan_analyze_next_block(
        &mut self,
        stream: &ReadStreamHandle,
    ) -> Result<bool, E> {
        let _ = stream;
        Ok(false)
    }

    fn scan_analyze_next_tuple(
        &mut self,
        oldest_xmin: pg_sys::TransactionId,
        row: &mut Row,
    ) -> Result<(bool, f64, f64), E> {
        let _ = (oldest_xmin, row);
        Ok((false, 0.0, 0.0))
    }

    fn parallelscan_estimate(rel: &RelationHandle) -> Result<pg_sys::Size, E>
    where
        Self: Sized,
    {
        let _ = rel;
        Ok(0)
    }

    fn parallelscan_initialize(
        rel: &RelationHandle,
        pscan: &ParallelTableScanDescHandle,
    ) -> Result<pg_sys::Size, E>
    where
        Self: Sized,
    {
        let _ = (rel, pscan);
        Ok(0)
    }

    fn parallelscan_reinitialize(
        rel: &RelationHandle,
        pscan: &ParallelTableScanDescHandle,
    ) -> Result<(), E>
    where
        Self: Sized,
    {
        let _ = (rel, pscan);
        Ok(())
    }

    fn tuple_tid_valid(&mut self, tid: &ItemPointer) -> Result<bool, E> {
        let _ = tid;
        Ok(false)
    }

    fn tuple_get_latest_tid(&mut self, tid: &ItemPointer) -> Result<(), E> {
        let _ = tid;
        Ok(())
    }

    fn slot_callbacks() -> *const pg_sys::TupleTableSlotOps {
        unsafe { &pg_sys::TTSOpsVirtual }
    }
}

pub trait AmRelation<E: Into<ErrorReport>> {
    fn relation_estimate_size(
        rel: &RelationHandle,
        attr_widths: Option<&mut [i32]>,
    ) -> Result<(pg_sys::BlockNumber, f64, f64), E>
    where
        Self: Sized;

    fn relation_size(
        rel: &RelationHandle,
        fork_number: pg_sys::ForkNumber::Type,
    ) -> Result<u64, E>
    where
        Self: Sized;

    fn relation_needs_toast_table(rel: &RelationHandle) -> Result<bool, E>
    where
        Self: Sized,
    {
        let _ = rel;
        Ok(false)
    }

    fn relation_toast_am(rel: &RelationHandle) -> Result<pg_sys::Oid, E>
    where
        Self: Sized,
    {
        let _ = rel;
        Ok(pg_sys::HEAP_TABLE_AM_OID)
    }

    fn relation_fetch_toast_slice(
        toastrel: &RelationHandle,
        valueid: pg_sys::Oid,
        attrsize: i32,
        sliceoffset: i32,
        slicelength: i32,
        result: &VarlenaHandle,
    ) -> Result<(), E>
    where
        Self: Sized,
    {
        let _ = (
            toastrel,
            valueid,
            attrsize,
            sliceoffset,
            slicelength,
            result,
        );
        Ok(())
    }

    fn tuple_fetch_row_version(
        rel: &RelationHandle,
        tid: &ItemPointer,
        snapshot: &SnapshotHandle,
        row: &mut Row,
    ) -> Result<bool, E>
    where
        Self: Sized,
    {
        let _ = (rel, tid, snapshot, row);
        Ok(false)
    }

    fn tuple_satisfies_snapshot(
        rel: &RelationHandle,
        slot: &TupleTableSlotHandle,
        snapshot: &SnapshotHandle,
    ) -> Result<bool, E>
    where
        Self: Sized,
    {
        let _ = (rel, slot, snapshot);
        Ok(true)
    }

    fn relation_vacuum(
        rel: &RelationHandle,
        params: &mut VacuumParamsHandle,
        bstrategy: &BufferAccessStrategyHandle,
    ) -> Result<(), E>
    where
        Self: Sized,
    {
        let _ = (rel, params, bstrategy);
        Ok(())
    }
}

pub trait AmIndex<E: Into<ErrorReport>> {
    fn new(rel: &RelationHandle) -> Result<Self, E>
    where
        Self: Sized;

    fn index_fetch_begin(&mut self) -> Result<(), E>;
    fn index_fetch_reset(&mut self) -> Result<(), E> {
        Ok(())
    }

    fn index_fetch_tuple(
        &mut self,
        tid: &ItemPointer,
        snapshot: &SnapshotHandle,
        row: &mut Row,
        call_again: &mut bool,
        all_dead: &mut bool,
    ) -> Result<bool, E>;

    fn index_fetch_end(&mut self) -> Result<(), E>;

    fn index_build_range_scan(
        table_rel: &RelationHandle,
        index_rel: &RelationHandle,
        index_info: &IndexInfoHandle,
        allow_sync: bool,
        anyvisible: bool,
        progress: bool,
        start_blockno: pg_sys::BlockNumber,
        numblocks: pg_sys::BlockNumber,
        callback: &IndexBuildCallbackHandle,
        callback_state: &CallbackStateHandle,
        scan: &TableScanDescHandle,
    ) -> Result<f64, E>
    where
        Self: Sized,
    {
        let _ = (
            table_rel,
            index_rel,
            index_info,
            allow_sync,
            anyvisible,
            progress,
            start_blockno,
            numblocks,
            callback,
            callback_state,
            scan,
        );
        Ok(0.0)
    }

    fn index_validate_scan(
        table_rel: &RelationHandle,
        index_rel: &RelationHandle,
        index_info: &IndexInfoHandle,
        snapshot: &SnapshotHandle,
        state: &ValidateIndexStateHandle,
    ) -> Result<(), E>
    where
        Self: Sized,
    {
        let _ = (table_rel, index_rel, index_info, snapshot, state);
        Ok(())
    }
}

pub trait AmDml<E: Into<ErrorReport>> {
    fn new(rel: pg_sys::Relation) -> Result<Self, E>
    where
        Self: Sized;

    fn begin_modify(&mut self) -> Result<(), E>;
    fn end_modify(&mut self) -> Result<(), E>;

    fn tuple_insert(
        &mut self,
        row: &Row,
        cid: pg_sys::CommandId,
        options: i32,
        bistate: Option<&mut BulkInsertStateHandle>,
    ) -> Result<(), E>;

    fn multi_insert(
        &mut self,
        rows: &[Row],
        cid: pg_sys::CommandId,
        options: i32,
        bistate: Option<&mut BulkInsertStateHandle>,
    ) -> Result<(), E>;

    fn tuple_delete(
        &mut self,
        tid: &ItemPointer,
        cid: pg_sys::CommandId,
        snapshot: &SnapshotHandle,
        crosscheck: Option<&SnapshotHandle>,
        wait: bool,
        tmfd: &mut TM_FailureData,
        changing_part: bool,
    ) -> Result<pg_sys::TM_Result::Type, E>;

    fn tuple_update(
        &mut self,
        otid: &ItemPointer,
        row: &Row,
        cid: pg_sys::CommandId,
        snapshot: &SnapshotHandle,
        crosscheck: Option<&SnapshotHandle>,
        wait: bool,
        tmfd: &mut TM_FailureData,
        lockmode: &mut pg_sys::LockTupleMode::Type,
        update_indexes: &mut pg_sys::TU_UpdateIndexes::Type,
    ) -> Result<pg_sys::TM_Result::Type, E>;

    fn tuple_lock(
        &mut self,
        tid: &ItemPointer,
        snapshot: &SnapshotHandle,
        row: &mut Row,
        cid: pg_sys::CommandId,
        mode: pg_sys::LockTupleMode::Type,
        wait_policy: pg_sys::LockWaitPolicy::Type,
        flags: u8,
        tmfd: &mut TM_FailureData,
    ) -> Result<pg_sys::TM_Result::Type, E>;

    fn finish_bulk_insert(&mut self, options: i32) -> Result<(), E> {
        let _ = options;
        Ok(())
    }

    fn tuple_complete_speculative(
        &mut self,
        row: &Row,
        spec_token: u32,
        succeeded: bool,
    ) -> Result<(), E> {
        let _ = (row, spec_token, succeeded);
        Ok(())
    }

    fn index_delete_tuples(
        &mut self,
        delstate: &mut pg_sys::TM_IndexDeleteOp,
    ) -> Result<pg_sys::TransactionId, E> {
        let _ = delstate;
        Ok(pg_sys::InvalidTransactionId)
    }
}

pub trait AmDdl<E: Into<ErrorReport>> {
    fn relation_set_new_filelocator(
        rel: &RelationHandle,
        newrlocator: &RelFileLocator,
        persistence: u8,
    ) -> Result<(pg_sys::TransactionId, pg_sys::MultiXactId), E>
    where
        Self: Sized;

    fn relation_nontransactional_truncate(rel: &RelationHandle) -> Result<(), E>
    where
        Self: Sized;

    fn relation_copy_data(
        rel: &RelationHandle,
        newrlocator: &RelFileLocator,
    ) -> Result<(), E>
    where
        Self: Sized;

    fn relation_copy_for_cluster(
        old_table: &RelationHandle,
        new_table: &RelationHandle,
        old_index: &RelationHandle,
        use_sort: bool,
        oldest_xmin: pg_sys::TransactionId,
    ) -> Result<(pg_sys::TransactionId, pg_sys::MultiXactId, f64, f64, f64), E>
    where
        Self: Sized;
}
