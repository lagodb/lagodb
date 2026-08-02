//! Executor-owned state for one foreign result relation.

use core::marker::PhantomData;
use core::ptr;

use pgrx::memcxt::PgMemoryContexts;
use pgrx::pg_sys;

use super::super::payload::ProviderPayload;
use super::super::row_identity::RowIdentityLayout;
use super::contract::{FdwModify, ForeignModifyOperation, ForeignReturnedIdentity};
use super::return_layout::ForeignModifyReturnLayout;
use super::return_requirements::ForeignModifyReturnRequirements;
use super::row_layout::ModifyRowLayout;

/// Typed payload published behind `ResultRelInfo.ri_FdwState`.
pub(crate) struct ForeignModifyStateWrapper<P: FdwModify> {
    pub(crate) payload: ProviderPayload<P::ModifyPrivateData, P::ModifyState>,
    pub(crate) row_layout: ModifyRowLayout,
    pub(crate) operation: ForeignModifyOperation,
    pub(crate) updated_columns: Box<[pg_sys::AttrNumber]>,
    pub(crate) row_identity_layout: RowIdentityLayout,
    pub(crate) plan_tuple_desc: pg_sys::TupleDesc,
    pub(crate) per_tuple_context: pg_sys::MemoryContext,
    pub(crate) returned_identity: ForeignReturnedIdentity,
    pub(crate) returned_item_pointer_required: bool,
    pub(crate) return_requirements: ForeignModifyReturnRequirements,
    pub(crate) return_layout: ForeignModifyReturnLayout,
    pub(crate) return_slot_required: bool,
    _marker: PhantomData<fn() -> P>,
}

impl<P: FdwModify> ForeignModifyStateWrapper<P> {
    /// # Safety
    ///
    /// `relation` must be the live result relation for this modify state and
    /// retain a stable valid TupleDesc until executor cleanup.
    pub(crate) unsafe fn new(
        private_data: P::ModifyPrivateData,
        relation: pg_sys::Relation,
        operation: ForeignModifyOperation,
        updated_columns: Box<[pg_sys::AttrNumber]>,
        row_identity_layout: RowIdentityLayout,
        plan_tuple_desc: pg_sys::TupleDesc,
        per_tuple_context: pg_sys::MemoryContext,
        returned_identity: ForeignReturnedIdentity,
        returned_item_pointer_required: bool,
        return_requirements: ForeignModifyReturnRequirements,
        return_layout: ForeignModifyReturnLayout,
        return_slot_required: bool,
    ) -> Self {
        let row_layout = unsafe { ModifyRowLayout::from_relation(relation) };
        Self {
            payload: ProviderPayload::with_private(private_data),
            row_layout,
            operation,
            updated_columns,
            row_identity_layout,
            plan_tuple_desc,
            per_tuple_context,
            returned_identity,
            returned_item_pointer_required,
            return_requirements,
            return_layout,
            return_slot_required,
            _marker: PhantomData,
        }
    }

    /// # Safety
    ///
    /// `relation` must be the live result relation for this insert state and
    /// retain a stable valid TupleDesc until executor cleanup.
    pub(crate) unsafe fn new_insert(
        relation: pg_sys::Relation,
        returned_identity: ForeignReturnedIdentity,
        returned_item_pointer_required: bool,
        return_slot_required: bool,
        per_tuple_context: pg_sys::MemoryContext,
    ) -> Self {
        let row_layout = unsafe { ModifyRowLayout::from_relation(relation) };
        Self {
            payload: ProviderPayload::empty(),
            row_layout,
            operation: ForeignModifyOperation::Insert,
            updated_columns: Vec::new().into_boxed_slice(),
            row_identity_layout: RowIdentityLayout::empty(),
            plan_tuple_desc: ptr::null_mut(),
            per_tuple_context,
            returned_identity,
            returned_item_pointer_required,
            return_requirements: ForeignModifyReturnRequirements::default(),
            return_layout: ForeignModifyReturnLayout::empty(),
            return_slot_required,
            _marker: PhantomData,
        }
    }

    pub(crate) fn private_data(&self) -> &P::ModifyPrivateData {
        self.payload.private_data()
    }

    pub(crate) fn install_provider_state(&mut self, state: P::ModifyState) {
        self.payload.install_provider_state(state);
    }

    /// Access the provider state after Begin has installed it.
    ///
    /// # Safety
    ///
    /// The wrapper must have been returned by [`Self::begin`] or
    /// [`Self::begin_insert`] and must not have entered cleanup.
    pub(crate) unsafe fn provider_state_ptr_unchecked(
        &mut self,
    ) -> *mut P::ModifyState {
        debug_assert!(self.payload.provider_state_initialized());
        unsafe { self.payload.provider_state_ptr_unchecked() }
    }

    pub(crate) fn leak_in(self, query_context: pg_sys::MemoryContext) -> *mut Self {
        PgMemoryContexts::For(query_context).leak_and_drop_on_delete(self)
    }

    pub(crate) fn cleanup_payloads(&mut self) {
        self.payload.cleanup();
        self.updated_columns = Vec::new().into_boxed_slice();
        self.row_layout = ModifyRowLayout::empty();
        self.row_identity_layout = RowIdentityLayout::empty();
        self.plan_tuple_desc = ptr::null_mut();
        self.per_tuple_context = ptr::null_mut();
        self.return_requirements = ForeignModifyReturnRequirements::default();
        self.return_layout = ForeignModifyReturnLayout::empty();
    }
}

impl<P: FdwModify> Drop for ForeignModifyStateWrapper<P> {
    fn drop(&mut self) {
        self.cleanup_payloads();
    }
}
