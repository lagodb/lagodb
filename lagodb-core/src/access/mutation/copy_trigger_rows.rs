//! COPY-owned AFTER ROW images. This owner survives executor and buffer
//! cleanup, but never extends trigger-row lifetime beyond its COPY command.

use pgrx::memcxt::PgMemoryContexts;
use pgrx::pg_sys;

use crate::access::lifecycle;
use crate::api::{AmResult, TableAccessMethod};
use crate::handles::RelationHandle;
use crate::tuple::{TupleSlotBatch, TupleSlotRow};

use super::trigger_rows::TriggerRelationRows;

pub(super) struct CopyTriggerRows {
    context: pg_sys::MemoryContext,
    rows: TriggerRelationRows,
}

impl CopyTriggerRows {
    pub(super) fn new<A: TableAccessMethod>(relation: &RelationHandle<'_>) -> Self {
        // COPY invokes inserts in per-tuple contexts and closes its executor
        // before the driver completes input shutdown. A transaction child and a descriptor
        // copy keep all PG objects independent of both lifetimes. On abort PG
        // owns this child's reclamation, including during savepoint rollback.
        let context = unsafe {
            lifecycle::create_child_context(
                pg_sys::CurTransactionContext,
                c"LagoDB COPY trigger rows",
            )
        };
        let rows = unsafe {
            PgMemoryContexts::For(context).switch_to(|_| {
                let tuple_desc =
                    pg_sys::CreateTupleDescCopy((*relation.as_raw()).rd_att);
                TriggerRelationRows::new::<A>(relation.oid(), tuple_desc)
            })
        };
        Self { context, rows }
    }

    pub(super) fn preserve(&mut self, row: TupleSlotRow<'_>) -> AmResult<()> {
        let rows = &mut self.rows;
        // SAFETY: the input slot is callback-scoped; all retained tuples and
        // the store's read slots are allocated in the COPY-owned context.
        unsafe {
            PgMemoryContexts::For(self.context).switch_to(|_| {
                let slot = row.as_raw();
                (*slot).tts_tid = rows.preserve(slot)?.to_pg_sys();
                Ok(())
            })
        }
    }

    /// # Safety
    ///
    /// Every slot must retain all populated attributes after a successful
    /// `AmCopySession::multi_insert_slots` call.
    pub(super) unsafe fn preserve_batch(
        &mut self,
        batch: TupleSlotBatch<'_>,
    ) -> AmResult<()> {
        let rows = &mut self.rows;
        // Switch once for the batch. The session skips this entire traversal
        // when the relation has no AFTER INSERT ROW triggers.
        // SAFETY: the caller guarantees populated, live input slots; retained
        // tuples are allocated in the COPY-owned context.
        unsafe {
            PgMemoryContexts::For(self.context).switch_to(|_| {
                for &slot in batch.raw_slots() {
                    (*slot).tts_tid = rows.preserve(slot)?.to_pg_sys();
                }
                Ok(())
            })
        }
    }

    /// Called only after COPY and its queued immediate triggers succeeded.
    pub(super) fn finish(mut self) {
        // Normal shutdown closes tuplestore spill files before deleting the
        // allocations. Closing clears the PG pointers before our Drop runs.
        self.rows.finish();
        unsafe { pg_sys::MemoryContextDelete(self.context) };
    }
}

impl Drop for CopyTriggerRows {
    fn drop(&mut self) {
        // The COPY owner's ResourceOwner callback runs after locks and
        // PG resources have been released. Do not call tuplestore_end or
        // touch stale slots here. PG reclaims our transaction child;
        // dropping the handle removes the store's strong Rust ownership.
        self.rows.abandon();
    }
}
