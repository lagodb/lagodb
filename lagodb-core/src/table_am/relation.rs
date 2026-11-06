//! Stateless relation-level callbacks: sizing, TOAST, row versions, VACUUM.

use pgrx::pg_sys;

use super::{AmResult, unsupported_callback};
use crate::handles::{
    AttrWidthsHandle, BufferAccessStrategyHandle, ItemPointer, RelationHandle,
    SnapshotHandle, TupleTableSlotHandle, VacuumParamsHandle, VarlenaHandle,
};
use crate::tuple::Row;

pub trait AmRelation {
    fn relation_estimate_size(
        rel: &RelationHandle,
        attr_widths: Option<&mut AttrWidthsHandle>,
    ) -> AmResult<(pg_sys::BlockNumber, f64, f64)>
    where
        Self: Sized;

    fn relation_size(
        rel: &RelationHandle,
        fork_number: pg_sys::ForkNumber::Type,
    ) -> AmResult<u64>
    where
        Self: Sized;

    fn relation_needs_toast_table(rel: &RelationHandle) -> AmResult<bool>
    where
        Self: Sized,
    {
        let _ = rel;
        Ok(false)
    }

    fn relation_toast_am(rel: &RelationHandle) -> AmResult<pg_sys::Oid>
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
    ) -> AmResult<()>
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
        unsupported_callback("relation_fetch_toast_slice")
    }

    fn tuple_fetch_row_version(
        rel: &RelationHandle,
        tid: &ItemPointer,
        snapshot: &SnapshotHandle,
        row: &mut Row,
    ) -> AmResult<bool>
    where
        Self: Sized,
    {
        let _ = (rel, tid, snapshot, row);
        unsupported_callback("tuple_fetch_row_version")
    }

    fn tuple_satisfies_snapshot(
        rel: &RelationHandle,
        slot: &TupleTableSlotHandle,
        snapshot: &SnapshotHandle,
    ) -> AmResult<bool>
    where
        Self: Sized,
    {
        let _ = (rel, slot, snapshot);
        unsupported_callback("tuple_satisfies_snapshot")
    }

    fn relation_vacuum(
        rel: &RelationHandle,
        params: &VacuumParamsHandle,
        bstrategy: &BufferAccessStrategyHandle,
    ) -> AmResult<()>
    where
        Self: Sized,
    {
        let _ = (rel, params, bstrategy);
        Ok(())
    }
}
