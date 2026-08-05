//! Provider-facing executor contexts for foreign modify callbacks.

use core::ffi::c_int;

use pgrx::pg_sys;

use crate::handles::{RelationHandle, SnapshotHandle};

use super::contract::{
    ForeignModifyOperation, ForeignModifyPrivate, ForeignReturnedIdentity,
};

/// Executor context passed before a normal INSERT, UPDATE, or DELETE state is created.
pub struct ForeignModifyBeginContext<'a, D: ForeignModifyPrivate> {
    private_data: &'a D,
    relation: RelationHandle<'a>,
    snapshot: SnapshotHandle<'a>,
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
}

impl<'a, D: ForeignModifyPrivate> ForeignModifyBeginContext<'a, D> {
    pub(crate) fn new(
        private_data: &'a D,
        relation: RelationHandle<'a>,
        snapshot: SnapshotHandle<'a>,
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
    ) -> Self {
        Self {
            private_data,
            relation,
            snapshot,
            operation,
            updated_columns,
            row_identity_count,
            returned_identity,
            returned_item_pointer_required,
            returning_columns,
            returning_all_columns,
            return_slot_required,
            subplan_index,
            eflags,
            effective_user_id,
        }
    }

    #[inline]
    pub fn private_data(&self) -> &D {
        self.private_data
    }

    #[inline]
    pub fn relation(&self) -> &RelationHandle<'a> {
        &self.relation
    }

    #[inline]
    pub fn snapshot(&self) -> &SnapshotHandle<'a> {
        &self.snapshot
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
}

/// Executor context for INSERTs started through PostgreSQL routed or COPY
/// callbacks.
pub struct ForeignInsertBeginContext<'a> {
    relation: RelationHandle<'a>,
    returned_identity: ForeignReturnedIdentity,
    returned_item_pointer_required: bool,
    effective_user_id: pg_sys::Oid,
}

impl<'a> ForeignInsertBeginContext<'a> {
    pub(crate) fn new(
        relation: RelationHandle<'a>,
        returned_item_pointer_required: bool,
        effective_user_id: pg_sys::Oid,
    ) -> Self {
        Self {
            relation,
            returned_identity: ForeignReturnedIdentity::None,
            returned_item_pointer_required,
            effective_user_id,
        }
    }

    #[inline]
    pub fn relation(&self) -> &RelationHandle<'a> {
        &self.relation
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
