//! Type erasure for COPY FROM relation sessions.

use crate::api::{AmCopySession, AmResult};
use crate::handles::BulkInsertStateHandle;
use crate::tuple::{TupleSlotBatch, TupleSlotRow};
use pgrx::pg_sys;

pub(super) struct ErasedCopySessionAdapter<T> {
    inner: T,
}

impl<T> ErasedCopySessionAdapter<T> {
    pub(super) fn new(inner: T) -> Self {
        Self { inner }
    }
}

pub(super) trait ErasedCopySession {
    fn end_copy(&mut self) -> AmResult<()>;
    fn tuple_insert_slot(
        &mut self,
        row: TupleSlotRow<'_>,
        cid: pg_sys::CommandId,
        options: i32,
        bistate: Option<&BulkInsertStateHandle>,
    ) -> AmResult<()>;
    fn multi_insert_slots(
        &mut self,
        rows: TupleSlotBatch<'_>,
        cid: pg_sys::CommandId,
        options: i32,
        bistate: Option<&BulkInsertStateHandle>,
    ) -> AmResult<()>;
    fn finish_bulk_insert(&mut self, options: i32) -> AmResult<()>;
    fn abort_copy(&mut self);
}

impl<T> ErasedCopySession for ErasedCopySessionAdapter<T>
where
    T: AmCopySession + 'static,
{
    fn end_copy(&mut self) -> AmResult<()> {
        self.inner.end_copy()
    }

    fn tuple_insert_slot(
        &mut self,
        row: TupleSlotRow<'_>,
        cid: pg_sys::CommandId,
        options: i32,
        bistate: Option<&BulkInsertStateHandle>,
    ) -> AmResult<()> {
        self.inner.tuple_insert_slot(row, cid, options, bistate)
    }

    fn multi_insert_slots(
        &mut self,
        rows: TupleSlotBatch<'_>,
        cid: pg_sys::CommandId,
        options: i32,
        bistate: Option<&BulkInsertStateHandle>,
    ) -> AmResult<()> {
        self.inner.multi_insert_slots(rows, cid, options, bistate)
    }

    fn finish_bulk_insert(&mut self, options: i32) -> AmResult<()> {
        self.inner.finish_bulk_insert(options)
    }

    fn abort_copy(&mut self) {
        self.inner.abort_copy();
    }
}
