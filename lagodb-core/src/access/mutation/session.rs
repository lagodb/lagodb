//! Provider-owned relation binding behind the explicit COPY bridge.

use pgrx::pg_sys;

use crate::api::{AmCopySession, AmResult, TableAccessMethod};
use crate::handles::RelationHandle;
use crate::tuple::{TupleSlotBatch, TupleSlotRow};

use super::copy_trigger_rows::CopyTriggerRows;
use super::trigger_policy::RelationTriggerPolicy;

pub(crate) struct CopyRelationSession<A: TableAccessMethod> {
    // Borrowed only during executor calls; completion and Drop do not use it.
    relation: pg_sys::Relation,
    command_id: pg_sys::CommandId,
    state: Option<A::CopySession>,
    trigger_rows: Option<CopyTriggerRows>,
    completed: bool,
}

impl<A: TableAccessMethod> CopyRelationSession<A> {
    pub(crate) unsafe fn bind(
        relation: pg_sys::Relation,
        command_id: pg_sys::CommandId,
    ) -> AmResult<Self> {
        let handle = unsafe { RelationHandle::from_raw(relation) };
        let preserve_rows =
            RelationTriggerPolicy::new(&handle, A::OWNS_PARTITIONED_TABLE)
                .bind_copy_session()?;
        Ok(Self {
            relation,
            command_id,
            state: None,
            trigger_rows: preserve_rows.then(|| CopyTriggerRows::new::<A>(&handle)),
            completed: false,
        })
    }

    fn writer(&mut self) -> AmResult<&mut A::CopySession> {
        if self.state.is_none() {
            // Binding precedes BEFORE triggers; only actual writes open storage.
            let relation = unsafe { RelationHandle::from_raw(self.relation) };
            self.state =
                Some(A::CopySession::begin_copy(&relation, self.command_id)?);
        }
        Ok(self.state.as_mut().expect("COPY writer was initialized"))
    }

    pub(crate) unsafe fn insert(
        &mut self,
        slot: *mut pg_sys::TupleTableSlot,
        options: i32,
    ) -> AmResult<()> {
        let row = unsafe { TupleSlotRow::from_raw(slot) };
        let cid = self.command_id;
        self.writer()?.tuple_insert_slot(row, cid, options)?;
        if let Some(rows) = &mut self.trigger_rows {
            rows.preserve(row)?;
        }
        unsafe { pg_sys::pgstat_count_heap_insert(self.relation, 1) };
        Ok(())
    }

    pub(crate) unsafe fn insert_batch(
        &mut self,
        slots: *mut *mut pg_sys::TupleTableSlot,
        count: i32,
        options: i32,
    ) -> AmResult<()> {
        let batch = unsafe { TupleSlotBatch::from_raw(slots, count as usize) };
        let cid = self.command_id;
        self.writer()?.multi_insert_slots(batch, cid, options)?;
        if let Some(rows) = &mut self.trigger_rows {
            // SAFETY: successful multi_insert_slots retains every input row
            // with all attributes populated, as required by AmCopySession.
            unsafe { rows.preserve_batch(batch)? };
        }
        unsafe { pg_sys::pgstat_count_heap_insert(self.relation, count.into()) };
        Ok(())
    }

    pub(crate) fn finish_bulk(&mut self, options: i32) -> AmResult<()> {
        if let Some(state) = &mut self.state {
            state.finish_bulk_insert(options)?;
        }
        Ok(())
    }

    /// Called once after the last insert, before queued AFTER trigger execution.
    /// AFTER ROW WHEN conditions have already been evaluated during event queuing.
    pub(crate) fn publish(&mut self) -> AmResult<()> {
        if let Some(state) = &mut self.state {
            state.end_copy()?;
        }
        Ok(())
    }

    /// Input shutdown succeeded; retained rows can now be released normally.
    pub(crate) fn complete(&mut self) {
        if let Some(rows) = self.trigger_rows.take() {
            rows.finish();
        }
        self.completed = true;
    }
}

impl<A: TableAccessMethod> Drop for CopyRelationSession<A> {
    fn drop(&mut self) {
        if !self.completed
            && let Some(state) = &mut self.state
        {
            state.abort_copy();
        }
        // CopyTriggerRows abandons PG pointers on failure. Transaction cleanup
        // owns staged files and deltas, including failures after publication.
    }
}
