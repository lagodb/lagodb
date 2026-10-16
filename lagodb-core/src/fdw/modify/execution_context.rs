//! Provider-facing executor contexts for foreign modify callbacks.

use core::ffi::c_int;

use pgrx::pg_sys;

use crate::access::mutation::current_copy_command_id;
use crate::handles::{ExecutorHandle, RelationHandle, SnapshotHandle};

use super::contract::{
    FdwModify, ForeignModifyOperation, ForeignModifyPrivate, ForeignReturnedIdentity,
};
use super::error::ForeignModifyError;
use super::state::ForeignModifyStateWrapper;

/// Executor context passed before a normal INSERT, UPDATE, or DELETE state is created.
pub struct ForeignModifyBeginContext<'a, D: ForeignModifyPrivate> {
    private_data: &'a D,
    relation: RelationHandle<'a>,
    executor: ExecutorHandle<'a>,
    operation: ForeignModifyOperation,
    updated_columns: &'a [pg_sys::AttrNumber],
    row_identity_count: usize,
    returned_identity: ForeignReturnedIdentity,
    returned_item_pointer_required: bool,
    returning_columns: &'a [pg_sys::AttrNumber],
    returning_all_columns: bool,
    return_slot_required: bool,
    subplan_index: c_int,
    eflags: c_int,
    effective_user_id: pg_sys::Oid,
    command_id: pg_sys::CommandId,
}

impl<P: FdwModify> ForeignModifyStateWrapper<P> {
    pub(super) fn begin_context<'a>(
        &'a self,
        relation: RelationHandle<'a>,
        executor: ExecutorHandle<'a>,
        subplan_index: c_int,
        eflags: c_int,
        effective_user_id: pg_sys::Oid,
        command_id: pg_sys::CommandId,
    ) -> ForeignModifyBeginContext<'a, P::ModifyPrivateData> {
        ForeignModifyBeginContext {
            private_data: self.private_data(),
            relation,
            executor,
            operation: self.operation,
            updated_columns: &self.updated_columns,
            row_identity_count: self.row_identity_layout.len(),
            returned_identity: self.returned_identity,
            returned_item_pointer_required: self.returned_item_pointer_required,
            returning_columns: self.return_requirements.columns(),
            returning_all_columns: self.return_requirements.all_columns(),
            return_slot_required: self.return_slot_required,
            subplan_index,
            eflags,
            effective_user_id,
            command_id,
        }
    }
}

impl<'a, D: ForeignModifyPrivate> ForeignModifyBeginContext<'a, D> {
    #[inline]
    pub fn private_data(&self) -> &D {
        self.private_data
    }

    #[inline]
    pub fn relation(&self) -> &RelationHandle<'a> {
        &self.relation
    }

    #[inline]
    pub fn snapshot(&self) -> SnapshotHandle<'_> {
        self.executor.snapshot()
    }

    /// Owning executor shared with source scans and other modify targets.
    #[inline]
    pub fn executor(&self) -> &ExecutorHandle<'a> {
        &self.executor
    }

    #[inline]
    pub fn operation(&self) -> ForeignModifyOperation {
        self.operation
    }

    #[inline]
    pub fn updated_columns(&self) -> &[pg_sys::AttrNumber] {
        self.updated_columns
    }

    #[inline]
    pub fn row_identity_count(&self) -> usize {
        self.row_identity_count
    }

    #[inline]
    pub fn returned_identity(&self) -> ForeignReturnedIdentity {
        self.returned_identity
    }

    #[inline]
    pub fn returned_item_pointer_required(&self) -> bool {
        self.returned_item_pointer_required
    }

    #[inline]
    pub fn returning_columns(&self) -> &[pg_sys::AttrNumber] {
        self.returning_columns
    }

    #[inline]
    pub fn returning_all_columns(&self) -> bool {
        self.returning_all_columns
    }

    #[inline]
    pub fn return_slot_required(&self) -> bool {
        self.return_slot_required
    }

    #[inline]
    pub fn subplan_index(&self) -> c_int {
        self.subplan_index
    }

    #[inline]
    pub fn eflags(&self) -> c_int {
        self.eflags
    }

    /// The role PostgreSQL selected for the foreign modify's user mapping.
    #[inline]
    pub fn effective_user_id(&self) -> pg_sys::Oid {
        self.effective_user_id
    }

    /// Command identifier fixed by PostgreSQL for this ModifyTable statement.
    #[inline]
    pub fn command_id(&self) -> pg_sys::CommandId {
        self.command_id
    }
}

pub(super) enum InsertCommand<'a> {
    ModifyTable {
        executor: ExecutorHandle<'a>,
        command_id: pg_sys::CommandId,
    },
    CopyFrom,
}

/// Executor context for INSERTs started through PostgreSQL routed or COPY
/// callbacks.
pub struct ForeignInsertBeginContext<'a> {
    relation: RelationHandle<'a>,
    returned_identity: ForeignReturnedIdentity,
    returned_item_pointer_required: bool,
    effective_user_id: pg_sys::Oid,
    command: InsertCommand<'a>,
}

impl<'a> ForeignInsertBeginContext<'a> {
    pub(super) fn new(
        relation: RelationHandle<'a>,
        returned_item_pointer_required: bool,
        effective_user_id: pg_sys::Oid,
        command: InsertCommand<'a>,
    ) -> Self {
        Self {
            relation,
            returned_identity: ForeignReturnedIdentity::None,
            returned_item_pointer_required,
            effective_user_id,
            command,
        }
    }

    #[inline]
    pub fn relation(&self) -> &RelationHandle<'a> {
        &self.relation
    }

    /// Owning executor for partition routing under ModifyTable.
    /// COPY uses a temporary EState without an executor snapshot, so it does
    /// not provide the snapshot-bearing ExecutorHandle.
    pub fn executor(&self) -> Option<&ExecutorHandle<'a>> {
        match &self.command {
            InsertCommand::ModifyTable { executor, .. } => Some(executor),
            InsertCommand::CopyFrom => None,
        }
    }

    /// Output CID fixed by the owning ModifyTable or COPY command.
    ///
    /// Providers that need a COPY CID must register
    /// [`crate::access::mutation::init_lifecycle_hooks`] in their own extension
    /// during initialization, before hooks are published. COPY context is only
    /// accessed when this method is called; providers that do not use the CID
    /// do not need to register that lifecycle.
    ///
    /// Returns an error if the required COPY command context is absent.
    pub fn command_id(&self) -> Result<pg_sys::CommandId, ForeignModifyError> {
        match &self.command {
            InsertCommand::ModifyTable { command_id, .. } => Ok(*command_id),
            InsertCommand::CopyFrom => {
                current_copy_command_id().map_err(ForeignModifyError::from)
            }
        }
    }

    /// Whether PostgreSQL will evaluate target-table ctid for this routed
    /// insert.
    #[inline]
    pub fn returned_item_pointer_required(&self) -> bool {
        self.returned_item_pointer_required
    }

    /// The role PostgreSQL selected for the foreign insert's user mapping.
    #[inline]
    pub fn effective_user_id(&self) -> pg_sys::Oid {
        self.effective_user_id
    }

    /// Declare that routed inserts can provide the returned ItemPointer needed
    /// by target-table ctid expressions.
    pub fn declare_returned_item_pointer(&mut self) {
        self.returned_identity = ForeignReturnedIdentity::ItemPointer;
    }

    pub(crate) fn returned_identity(&self) -> ForeignReturnedIdentity {
        self.returned_identity
    }
}
