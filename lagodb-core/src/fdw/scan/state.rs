//! Executor-owned FDW wrapper and reusable per-scan buffers.

use core::ffi::c_int;
use core::marker::PhantomData;
use core::ptr;

use pgrx::memcxt::PgMemoryContexts;
use pgrx::pg_sys;

use super::super::payload::ProviderPayload;
use super::super::row_identity::ForeignRowIdentityRequirement;
use super::contract::FdwScan;
use super::error::ForeignScanError;
use super::filter::ForeignScanFilters;
use super::private::DecodedScanPrivate;
use super::projection::{ColumnRequirements, ScanProjection};
use super::pushdown::{
    BeginForeignScanContext, ForeignExpressionValue, RuntimeExpressionValues,
    StartForeignScanContext,
};
use super::slot::{ScanOutputLayout, ScanSlotWriter, SlotWriteLayout};
use crate::handles::{RelationHandle, SnapshotHandle};

/// Rust-owned payload kept behind `ForeignScanState.fdw_state`.
pub(crate) struct ForeignScanStateWrapper<P: FdwScan> {
    pub(crate) payload: ProviderPayload<P::PrivateData, P::State>,
    pub(crate) projection: ScanProjection,
    pub(crate) row_identity_requirement: ForeignRowIdentityRequirement,
    pub(crate) required_columns: ColumnRequirements,
    pub(crate) write_layout: SlotWriteLayout,
    pub(crate) datum_defaults_initialized: bool,
    pub(crate) filters: Option<ForeignScanFilters<P>>,
    pub(crate) fdw_expr_states: *mut pg_sys::List,
    pub(crate) eflags: c_int,
    pub(crate) runtime_values: Vec<ForeignExpressionValue>,
    provider_started: bool,
    _marker: PhantomData<fn() -> P>,
}

impl<P: FdwScan> ForeignScanStateWrapper<P> {
    pub(crate) fn new(
        decoded: DecodedScanPrivate<P::PrivateData>,
        filters: ForeignScanFilters<P>,
        fdw_expr_states: *mut pg_sys::List,
        write_layout: SlotWriteLayout,
        eflags: c_int,
    ) -> Self {
        let expression_count = if fdw_expr_states.is_null() {
            0
        } else {
            // SAFETY: PostgreSQL returned this live executor-owned list.
            unsafe { pg_sys::list_length(fdw_expr_states) as usize }
        };
        Self {
            payload: ProviderPayload::with_private(decoded.private_data),
            projection: decoded.projection,
            row_identity_requirement: decoded.row_identity,
            required_columns: decoded.requirements,
            datum_defaults_initialized: false,
            write_layout,
            filters: Some(filters),
            fdw_expr_states,
            eflags,
            runtime_values: Vec::with_capacity(expression_count),
            provider_started: false,
            _marker: PhantomData,
        }
    }

    pub(crate) fn leak(self) -> *mut Self {
        PgMemoryContexts::CurrentMemoryContext.leak_and_drop_on_delete(self)
    }

    #[inline]
    pub(crate) fn private_data(&self) -> &P::PrivateData {
        self.payload.private_data()
    }

    pub(crate) fn install_provider_state(&mut self, state: P::State) {
        self.payload.install_provider_state(state);
    }

    #[inline]
    pub(crate) fn provider_started(&self) -> bool {
        self.provider_started
    }

    /// Initialize provider state from PostgreSQL's BeginForeignScan callback.
    ///
    /// # Safety
    ///
    /// `node` must be the live ForeignScanState that owns this wrapper.  The
    /// executor plan, relation, slot, expression context, and snapshot must
    /// remain valid for the synchronous provider callback.
    pub(crate) unsafe fn initialize_provider(
        &mut self,
        node: *mut pg_sys::ForeignScanState,
    ) -> Result<(), ForeignScanError> {
        if self.payload.provider_state_initialized() {
            return Ok(());
        }
        let plan = unsafe { (*node).ss.ps.plan } as *mut pg_sys::ForeignScan;
        let relation = unsafe { (*node).ss.ss_currentRelation };
        let estate = unsafe { (*node).ss.ps.state };
        let econtext = unsafe { (*node).ss.ps.ps_ExprContext };
        let snapshot = unsafe { (*estate).es_snapshot };
        let query_context = unsafe { (*estate).es_query_cxt };
        let effective_user_id = unsafe {
            if (*plan).checkAsUser != pg_sys::InvalidOid {
                (*plan).checkAsUser
            } else {
                pg_sys::GetUserId()
            }
        };
        let entry_context = unsafe { pg_sys::CurrentMemoryContext };

        unsafe {
            self.filters
                .as_mut()
                .expect("FDW filters must remain installed until EndForeignScan")
                .bind_stable(econtext)
        }?;

        let provider_state = {
            let private_data = self.private_data();
            let begin_context = BeginForeignScanContext {
                private_data,
                relation: unsafe { RelationHandle::from_raw(relation) },
                snapshot: unsafe { SnapshotHandle::from_raw(snapshot) },
                projection: &self.projection,
                required_columns: &self.required_columns,
                output_layout: ScanOutputLayout::new(&self.write_layout),
                row_identity_requirement: self.row_identity_requirement,
                filters: self
                    .filters
                    .as_ref()
                    .expect("FDW filters must remain installed until EndForeignScan")
                    .bound(),
                estate,
                eflags: self.eflags,
                effective_user_id,
            };
            unsafe { pg_sys::MemoryContextSwitchTo(query_context) };
            let result = P::begin(begin_context);
            unsafe { pg_sys::MemoryContextSwitchTo(entry_context) };
            result?
        };
        self.install_provider_state(provider_state);
        Ok(())
    }

    /// Bind dynamic parameters and start the provider after PostgreSQL has
    /// populated the first valid outer-tuple values.
    ///
    /// # Safety
    ///
    /// `node` must be the live ForeignScanState that owns this wrapper.
    pub(crate) unsafe fn start_provider(
        &mut self,
        node: *mut pg_sys::ForeignScanState,
    ) -> Result<(), ForeignScanError> {
        if self.provider_started {
            return Ok(());
        }
        let relation = unsafe { (*node).ss.ss_currentRelation };
        let estate = unsafe { (*node).ss.ps.state };
        let econtext = unsafe { (*node).ss.ps.ps_ExprContext };
        let snapshot = unsafe { (*estate).es_snapshot };
        let query_context = unsafe { (*estate).es_query_cxt };
        unsafe {
            self.filters
                .as_mut()
                .expect("FDW filters must remain installed until EndForeignScan")
                .bind_dynamic_initial(econtext)
        }?;
        unsafe { self.refresh_runtime_values(econtext) }?;

        let state_ptr = unsafe { self.payload.provider_state_ptr_unchecked() };
        let context = StartForeignScanContext {
            private_data: self.private_data(),
            relation: unsafe { RelationHandle::from_raw(relation) },
            snapshot: unsafe { SnapshotHandle::from_raw(snapshot) },
            projection: &self.projection,
            required_columns: &self.required_columns,
            output_layout: ScanOutputLayout::new(&self.write_layout),
            row_identity_requirement: self.row_identity_requirement,
            filters: self
                .filters
                .as_ref()
                .expect("FDW filters must remain installed until EndForeignScan")
                .bound(),
            expressions: self.runtime_values(),
            estate,
            econtext,
        };
        let entry_context = unsafe { pg_sys::MemoryContextSwitchTo(query_context) };
        // SAFETY: provider initialization installed P::State at state_ptr. The
        // callback is synchronous and the context borrows only sibling wrapper
        // fields that remain live for its duration.
        let result = unsafe { P::start(&mut *state_ptr, context) };
        unsafe { pg_sys::MemoryContextSwitchTo(entry_context) };
        self.provider_started = result.is_ok();
        result
    }

    /// Evaluate all provider runtime expressions in their plan-list order.
    /// This runs only when the provider starts and during ReScan, never per
    /// row.
    ///
    /// # Safety
    ///
    /// `econtext` must be a live executor expression context.  The wrapper's
    /// expression-state list and PostgreSQL per-tuple context must remain
    /// valid for the duration of evaluation.
    pub(crate) unsafe fn refresh_runtime_values(
        &mut self,
        econtext: *mut pg_sys::ExprContext,
    ) -> Result<(), ForeignScanError> {
        self.runtime_values.clear();
        if self.fdw_expr_states.is_null() {
            return Ok(());
        }

        // SAFETY: fdw_expr_states is non-NULL and executor-owned for this scan.
        let length = unsafe { pg_sys::list_length(self.fdw_expr_states) };

        for index in 0..length {
            // SAFETY: index is bounded by the live PostgreSQL list length.
            let state = unsafe { pg_sys::list_nth(self.fdw_expr_states, index) }
                as *mut pg_sys::ExprState;
            let mut is_null = false;
            // SAFETY: state is a live ExprState. PostgreSQL switches to
            // econtext->ecxt_per_tuple_memory for this evaluation and restores
            // the caller's current context before returning.
            let datum = unsafe {
                pg_sys::ExecEvalExprSwitchContext(state, econtext, &mut is_null)
            };
            self.runtime_values
                .push(ForeignExpressionValue { datum, is_null });
        }
        Ok(())
    }

    pub(crate) fn runtime_values(&self) -> RuntimeExpressionValues<'_> {
        RuntimeExpressionValues::new(&self.runtime_values)
    }

    /// # Safety
    ///
    /// `slot` must be the live HeapTuple scan slot validated during Begin, and
    /// the wrapper's projection, slot output layout, and datum-default state
    /// must still belong to this scan state.
    pub(crate) unsafe fn output_writer<'a>(
        &'a mut self,
        slot: *mut pg_sys::TupleTableSlot,
    ) -> ScanSlotWriter<'a> {
        // SAFETY: Begin compiled the slot layout; the datum-default state
        // belongs to this wrapper.
        unsafe {
            ScanSlotWriter::new(
                slot,
                &self.write_layout,
                &mut self.datum_defaults_initialized,
                self.row_identity_requirement,
            )
        }
    }

    pub(crate) fn reset_datum_defaults_for_rescan(&mut self) {
        self.datum_defaults_initialized = false;
    }

    pub(crate) fn cleanup_payloads(&mut self) {
        self.payload.cleanup();
        self.filters = None;
        self.runtime_values.clear();
        self.provider_started = false;
        self.datum_defaults_initialized = false;
        self.fdw_expr_states = ptr::null_mut();
        self.projection = ScanProjection::Relation;
        self.row_identity_requirement = ForeignRowIdentityRequirement::None;
        self.required_columns = ColumnRequirements::default();
        self.write_layout = SlotWriteLayout::default();
    }
}

impl<P: FdwScan> Drop for ForeignScanStateWrapper<P> {
    fn drop(&mut self) {
        // EndForeignScan performs the same typed cleanup before warning on an
        // end error.  This Drop path covers ERROR/panic before End is called.
        self.cleanup_payloads();
    }
}
