//! Backend-only harnesses for exercising the core CustomScan callbacks.

use core::marker::PhantomData;
use core::ptr;

use pg_lakebase_core::customscan::exec_params::RuntimeParamRefs;
use pg_lakebase_core::customscan::provider::{
    LakebaseCustomScanProvider, ScanTupleLayout,
};
use pg_lakebase_core::customscan::state::{
    CachedEnvelope, CustomScanStateWrapper, create_custom_scan_state_trampoline,
};
use pg_lakebase_core::customscan::{ScanPurpose, custom_exprs};
use pg_lakebase_core::expr::{
    ColumnRef, ExecParamRef, ExternParamRef, PushdownContract,
};
use pgrx::pg_sys;

/// Backend test owner for a PostgreSQL-allocated `CustomScanStateWrapper`.
pub(crate) struct TestScanState<P: LakebaseCustomScanProvider> {
    wrapper: *mut pg_sys::CustomScanState,
    _provider: PhantomData<fn() -> P>,
}

impl<P: LakebaseCustomScanProvider> TestScanState<P> {
    /// Allocate the same wrapper used by PostgreSQL's CreateCustomScanState.
    ///
    /// # Safety
    ///
    /// The returned state must only be used while the current PostgreSQL memory
    /// context remains alive.
    pub(crate) unsafe fn new() -> Self {
        Self {
            wrapper: unsafe {
                create_custom_scan_state_trampoline::<P>(ptr::null_mut()).cast()
            },
            _provider: PhantomData,
        }
    }

    pub(crate) fn node_ptr(&self) -> *mut pg_sys::CustomScanState {
        self.wrapper
    }

    unsafe fn wrapper_mut(&mut self) -> &mut CustomScanStateWrapper<P> {
        unsafe { CustomScanStateWrapper::from_node_ptr(self.wrapper) }
    }

    unsafe fn wrapper_ref(&self) -> &CustomScanStateWrapper<P> {
        unsafe { CustomScanStateWrapper::from_node_ptr(self.wrapper) }
    }

    pub(crate) unsafe fn base_mut(&mut self) -> &mut pg_sys::CustomScanState {
        unsafe { self.wrapper_mut().test_base_mut() }
    }

    pub(crate) unsafe fn base(&self) -> &pg_sys::CustomScanState {
        unsafe { self.wrapper_ref().test_base() }
    }

    pub(crate) unsafe fn scan_state_ptr(&mut self) -> *mut pg_sys::ScanState {
        unsafe { self.wrapper_mut().test_scan_state_ptr() }
    }

    pub(crate) unsafe fn install_provider_state(&mut self, state: P::State) {
        unsafe { self.wrapper_mut().test_install_provider_state(state) };
    }

    pub(crate) unsafe fn provider_state(&self) -> &P::State {
        unsafe { self.wrapper_ref().test_provider_state() }
    }

    pub(crate) unsafe fn set_cached_envelope(
        &mut self,
        envelope: TestCachedEnvelope,
    ) {
        unsafe {
            self.wrapper_mut().test_set_cached_envelope(CachedEnvelope {
                purpose: envelope.purpose,
                pushed_contracts: envelope.pushed_contracts,
                column_refs: envelope.column_refs,
                tuple_layout: envelope.tuple_layout,
            });
        }
    }

    pub(crate) unsafe fn set_expr_sections(
        &mut self,
        sections: custom_exprs::CustomExprSections,
    ) {
        unsafe { self.wrapper_mut().test_set_expr_sections(sections) };
    }

    pub(crate) unsafe fn set_runtime_params(&mut self, params: RuntimeParamRefs) {
        unsafe { self.wrapper_mut().test_set_runtime_params(params) };
    }

    pub(crate) unsafe fn runtime_params(&self) -> Option<&RuntimeParamRefs> {
        unsafe { self.wrapper_ref().test_runtime_params() }
    }
}

/// Cached framework envelope used by the ReScan and execution tests.
pub(crate) struct TestCachedEnvelope {
    pub(crate) purpose: ScanPurpose,
    pub(crate) pushed_contracts: Vec<PushdownContract>,
    pub(crate) column_refs: Vec<ColumnRef>,
    pub(crate) tuple_layout: ScanTupleLayout,
}

/// Read-only view used by tests without exposing the runtime collector's
/// storage fields.
pub(crate) struct RuntimeParamRefsView<'a>(&'a RuntimeParamRefs);

impl<'a> RuntimeParamRefsView<'a> {
    pub(crate) fn new(refs: &'a RuntimeParamRefs) -> Self {
        Self(refs)
    }

    pub(crate) fn extern_params(&self) -> &[ExternParamRef] {
        self.0.extern_params()
    }

    pub(crate) fn exec_params(&self) -> &[ExecParamRef] {
        self.0.exec_params()
    }

    pub(crate) fn exec_param_ids(&self) -> *mut pg_sys::Bitmapset {
        self.0.exec_param_ids()
    }
}
