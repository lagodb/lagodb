//! Uniqueness checks for `amcanunique` access methods: the operations
//! `_bt_check_unique` and `_bt_doinsert` use. Finding candidate entries and
//! the retry loop belong to the access method.

use std::mem;
use std::ptr;

use pgrx::pg_sys;
use pgrx::pg_sys::ffi::pg_guard_ffi_boundary;

use super::IndexInsertContext;
use crate::handles::ValidItemPointer;
use crate::index_am::values::IndexValues;

unsafe extern "C-unwind" {
    fn lagodb_index_unique_violation(
        index: pg_sys::Relation,
        heap: pg_sys::Relation,
        values: *const pg_sys::Datum,
        isnull: *const bool,
    );
}

/// The table row behind an existing entry with the same key, under a dirty
/// snapshot.
#[derive(Debug, Clone, Copy)]
pub enum HeapConflictProbe {
    /// No conflict; with `all_dead` the entry may be marked dead.
    Absent { all_dead: bool },
    /// A conflict, unless [`IndexInsertContext::inserted_row_is_live`] is
    /// false.
    Live,
    /// Wait with [`IndexInsertContext::wait_for`], then search again.
    InProgress(HeapTupleWait),
}

#[derive(Debug, Clone, Copy)]
pub struct HeapTupleWait {
    xid: pg_sys::TransactionId,
    speculative_token: u32,
}

impl<A> IndexInsertContext<'_, A> {
    pub fn probe_conflict(&self, tid: ValidItemPointer) -> HeapConflictProbe {
        // SAFETY: `InitDirtySnapshot` sets only the type; the visibility
        // check initializes xmin, xmax and the speculative token.
        let mut snapshot: pg_sys::SnapshotData = unsafe { mem::zeroed() };
        snapshot.snapshot_type = pg_sys::SnapshotType::SNAPSHOT_DIRTY;
        let mut tid = tid.to_pg_sys();
        let mut all_dead = false;
        // SAFETY: the heap relation is live and every pointer outlives the
        // call.
        let found = unsafe {
            pg_sys::table_index_fetch_tuple_check(
                self.heap.as_raw(),
                &mut tid,
                &mut snapshot,
                &mut all_dead,
            )
        };
        if !found {
            return HeapConflictProbe::Absent { all_dead };
        }
        let xid = if snapshot.xmin != pg_sys::TransactionId::INVALID {
            snapshot.xmin
        } else {
            snapshot.xmax
        };
        if xid == pg_sys::TransactionId::INVALID {
            HeapConflictProbe::Live
        } else {
            HeapConflictProbe::InProgress(HeapTupleWait {
                xid,
                speculative_token: snapshot.speculativeToken,
            })
        }
    }

    /// `_bt_check_unique` skips a conflict when the inserted row is no longer
    /// live (`SnapshotSelf`), which CREATE INDEX CONCURRENTLY relies on.
    pub fn inserted_row_is_live(&self, tid: ValidItemPointer) -> bool {
        let mut tid = tid.to_pg_sys();
        // SAFETY: SnapshotSelfData is PostgreSQL's static self snapshot.
        unsafe {
            pg_sys::table_index_fetch_tuple_check(
                self.heap.as_raw(),
                &mut tid,
                &raw mut pg_sys::SnapshotSelfData,
                ptr::null_mut(),
            )
        }
    }

    /// `inserting` is named in PostgreSQL's wait context message.
    pub fn wait_for(&self, wait: HeapTupleWait, inserting: ValidItemPointer) {
        let mut inserting = inserting.to_pg_sys();
        // SAFETY: both waits only take locks and may raise an ERROR.
        unsafe {
            if wait.speculative_token != 0 {
                pg_sys::SpeculativeInsertionWait(wait.xid, wait.speculative_token);
            } else {
                pg_sys::XactLockTableWait(
                    wait.xid,
                    self.index.as_raw(),
                    &mut inserting,
                    pg_sys::XLTW_Oper::XLTW_InsertIndex,
                );
            }
        }
    }

    /// Raises instead of returning an error: the report carries
    /// `errtableconstraint` fields that an error value cannot transport.
    pub fn raise_unique_violation(&self, values: IndexValues<'_>) -> ! {
        let (datums, nulls) = values.as_ptrs();
        let index = self.index.as_raw();
        let heap = self.heap.as_raw();
        // SAFETY: the relations are live and the arrays hold one entry per
        // index column; the bridge only raises.
        unsafe {
            pg_guard_ffi_boundary(|| {
                lagodb_index_unique_violation(index, heap, datums, nulls)
            });
        }
        unreachable!("lagodb_index_unique_violation raises an ERROR")
    }
}
