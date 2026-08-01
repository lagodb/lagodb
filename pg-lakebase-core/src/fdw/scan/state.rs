//! Executor-owned FDW wrapper and reusable per-scan buffers.

use core::ffi::c_int;
use core::marker::PhantomData;
use core::ptr;

use pgrx::memcxt::PgMemoryContexts;
use pgrx::pg_sys;

use crate::expr::contract::{ColumnRef, PushdownContract};

use super::super::payload::ProviderPayload;
use super::super::row_identity::ForeignRowIdentityRequirement;
use super::contract::FdwScan;
use super::error::ForeignScanError;
use super::private::DecodedScanPrivate;
use super::projection::{ColumnRequirements, ScanProjection};
use super::pushdown::{
    BeginForeignScanContext, ForeignExprList, ForeignExpressionValue,
    ForeignPushdown, RuntimeExpressionValues,
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
    pub(crate) contracts: Vec<PushdownContract>,
    pub(crate) column_refs: Vec<ColumnRef>,
    pub(crate) fdw_expr_states: *mut pg_sys::List,
    pub(crate) eflags: c_int,
    pub(crate) runtime_values: Vec<ForeignExpressionValue>,
    _marker: PhantomData<fn() -> P>,
}

impl<P: FdwScan> ForeignScanStateWrapper<P> {
    pub(crate) fn new(
        decoded: DecodedScanPrivate<P::PrivateData>,
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
            contracts: decoded.contracts,
            column_refs: decoded.column_refs,
            fdw_expr_states,
            eflags,
            runtime_values: Vec::with_capacity(expression_count),
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

    /// Start the provider after PostgreSQL has populated any outer-scan
    /// `PARAM_EXEC` values.  PostgreSQL initializes an inner ForeignScan before
    /// the first outer tuple is available, so this must not run from the
    /// `BeginForeignScan` callback itself.
    ///
    /// # Safety
    ///
    /// `node` must be the live ForeignScanState that owns this wrapper.  The
    /// executor plan, relation, slot, expression context, and snapshot must
    /// remain valid for the synchronous provider callback.
    pub(crate) unsafe fn begin_provider(
        &mut self,
        node: *mut pg_sys::ForeignScanState,
    ) -> Result<(), ForeignScanError> {
        if self.payload.provider_state_initialized() {
            return Ok(());
        }
        if node.is_null() {
            return Err(ForeignScanError::framework(
                "provider start received a NULL ForeignScanState",
            ));
        }
        let plan = unsafe { (*node).ss.ps.plan } as *mut pg_sys::ForeignScan;
        let relation = unsafe { (*node).ss.ss_currentRelation };
        let estate = unsafe { (*node).ss.ps.state };
        let econtext = unsafe { (*node).ss.ps.ps_ExprContext };
        if plan.is_null()
            || unsafe { (*plan).scan.plan.type_ } != pg_sys::NodeTag::T_ForeignScan
            || relation.is_null()
            || estate.is_null()
            || econtext.is_null()
        {
            return Err(ForeignScanError::framework(
                "provider start has incomplete executor state",
            ));
        }
        let snapshot = unsafe { (*estate).es_snapshot };
        let query_context = unsafe { (*estate).es_query_cxt };
        let per_tuple_context = unsafe { (*econtext).ecxt_per_tuple_memory };
        if snapshot.is_null()
            || query_context.is_null()
            || per_tuple_context.is_null()
        {
            return Err(ForeignScanError::framework(
                "provider start has no snapshot, query, or per-tuple memory context",
            ));
        }
        let entry_context = unsafe { pg_sys::CurrentMemoryContext };

        // The executor has already reset this context before delayed provider
        // start: ExecScan does so for Iterate, and ReScanExprContext does so
        // for a parameterized ReScan.  Runtime expression values are then
        // evaluated against the current outer tuple, if this scan is the inner
        // side of a Nested Loop.
        unsafe { self.refresh_runtime_values(econtext) }?;

        let provider_state = {
            let private_data = self.private_data();
            let pushdown = unsafe { self.pushdown((*plan).fdw_recheck_quals) };
            let begin_context = BeginForeignScanContext {
                private_data,
                relation: unsafe { RelationHandle::from_raw(relation) },
                snapshot: unsafe { SnapshotHandle::from_raw(snapshot) },
                projection: &self.projection,
                required_columns: &self.required_columns,
                output_layout: ScanOutputLayout::new(&self.write_layout),
                row_identity_requirement: self.row_identity_requirement,
                pushdown,
                expressions: self.runtime_values(),
                estate,
                econtext,
                eflags: self.eflags,
            };
            unsafe { pg_sys::MemoryContextSwitchTo(query_context) };
            let result = P::begin(begin_context);
            unsafe { pg_sys::MemoryContextSwitchTo(entry_context) };
            result?
        };
        self.install_provider_state(provider_state);
        Ok(())
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
        if econtext.is_null() {
            return Err(ForeignScanError::framework(
                "FDW runtime expressions have no ExprContext",
            ));
        }

        // SAFETY: fdw_expr_states is non-NULL and executor-owned for this scan.
        let length = unsafe { pg_sys::list_length(self.fdw_expr_states) };
        // SAFETY: econtext was checked non-NULL immediately above.
        if unsafe { (*econtext).ecxt_per_tuple_memory.is_null() } {
            return Err(ForeignScanError::framework(
                "FDW runtime expressions have no per-tuple memory context",
            ));
        }

        for index in 0..length {
            // SAFETY: index is bounded by the live PostgreSQL list length.
            let state = unsafe { pg_sys::list_nth(self.fdw_expr_states, index) }
                as *mut pg_sys::ExprState;
            if state.is_null() {
                return Err(ForeignScanError::framework(
                    "fdw_exprs initialization produced a NULL ExprState",
                ));
            }
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
    /// `raw_recheck_quals` must be a PostgreSQL-owned plan list that remains
    /// live for the returned borrowed view.
    pub(crate) unsafe fn pushdown<'a>(
        &'a self,
        raw_recheck_quals: *mut pg_sys::List,
    ) -> ForeignPushdown<'a> {
        // SAFETY: the list is PostgreSQL-owned plan data borrowed for this scan.
        let expressions = unsafe { ForeignExprList::from_raw(raw_recheck_quals) };
        ForeignPushdown::new(expressions, &self.contracts, &self.column_refs)
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
        // SAFETY: Begin validated the slot layout; the datum-default state
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
        self.runtime_values.clear();
        self.contracts.clear();
        self.column_refs.clear();
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
