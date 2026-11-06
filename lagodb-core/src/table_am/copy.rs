//! Relation-local COPY FROM sessions.

use pgrx::pg_sys;

use super::{AmResult, unsupported_callback};
use crate::handles::RelationHandle;
use crate::tuple::{TupleSlotBatch, TupleSlotRow};

/// Relation-local COPY FROM state. COPY bypasses ModifyTable and therefore has
/// a separate driver-owned lifecycle and callback surface. PostgreSQL's
/// heap-specific `BulkInsertStateData` remains at the FFI boundary; providers
/// own any state needed by their insertion session.
pub trait AmCopySession {
    /// Begin and construct the complete relation-local COPY session.
    ///
    /// The relation handle is borrowed only during construction. A successful
    /// return is a fully usable session; a failed construction is never
    /// installed in the relation binding. `command_id` is COPY's original output
    /// CID and must be retained even if nested SPI advances the backend CID.
    fn begin_copy(
        rel: &RelationHandle,
        command_id: pg_sys::CommandId,
    ) -> AmResult<Self>
    where
        Self: Sized;

    /// Finish and publish this session's writes into transaction-local visibility.
    /// Called once after all COPY inserts, before AFTER triggers. Later failures
    /// (including triggers and PROGRAM/input shutdown) must still roll back the
    /// staged data. Core retains the session and trigger rows through shutdown.
    fn end_copy(&mut self) -> AmResult<()>;

    fn abort_copy(&mut self) {}

    fn tuple_insert_slot(
        &mut self,
        row: TupleSlotRow<'_>,
        cid: pg_sys::CommandId,
        options: i32,
    ) -> AmResult<()> {
        let _ = (row, cid, options);
        unsupported_callback("copy tuple_insert_slot")
    }

    /// Insert the entire batch. On success, every input slot must retain its
    /// row with all attributes populated and valid for subsequent core and PG
    /// processing. Iterating through `rows.iter()` prepares those attributes;
    /// providers must not clear or invalidate the slots afterward. Core reuses
    /// their Datum arrays to preserve AFTER ROW images without preparing them
    /// again.
    fn multi_insert_slots(
        &mut self,
        rows: TupleSlotBatch<'_>,
        cid: pg_sys::CommandId,
        options: i32,
    ) -> AmResult<()> {
        let _ = (rows, cid, options);
        unsupported_callback("copy multi_insert_slots")
    }

    fn finish_bulk_insert(&mut self, options: i32) -> AmResult<()> {
        let _ = options;
        Ok(())
    }
}
