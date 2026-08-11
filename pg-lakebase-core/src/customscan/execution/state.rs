//! Executor-owned `#[repr(C)]` `CustomScanStateWrapper<P>` (`base` at offset 0).
//! Typed provider payloads are stored as `Option` so ownership and teardown are
//! represented by the fields themselves.

use core::marker::PhantomData;

use pgrx::PgMemoryContexts;
use pgrx::pg_guard;
use pgrx::pg_sys;

use crate::customscan::ScanPurpose;
use crate::customscan::filter::CustomScanFilters;
use crate::customscan::plan_data::custom_exprs::CustomExprSections;
use crate::customscan::plan_data::tuple_layout::ScanTupleLayout;
use crate::customscan::provider::{LakebaseCustomScanProvider, method_tables_for};

/// `#[repr(C)]` wrapper PostgreSQL holds as `*mut CustomScanState`.
///
/// `base` MUST be the first field for pointer casts via [`Self::from_node_ptr`].
/// `CreateCustomScanState` allocates the wrapper; `BeginCustomScan` fills runtime
/// fields. Typed payloads drop in `EndCustomScan`; [`Drop`] is a fallback when
/// Begin errors before End runs.
#[repr(C)]
pub struct CustomScanStateWrapper<P: LakebaseCustomScanProvider> {
    /// PG's [`pg_sys::CustomScanState`]. **MUST** remain the first field
    /// so `*mut CustomScanState` and `*mut CustomScanStateWrapper<P>`
    /// share an address.
    pub(crate) base: pg_sys::CustomScanState,

    /// Recheck `ExprState` derived from Exact pushed filters; NULL when none.
    pub(crate) recheck_state: *mut pg_sys::ExprState,

    /// Immutable expression sections validated once during Begin.
    pub(crate) expr_sections: Option<CustomExprSections>,

    /// Decoded planned predicates, ExprStates, and the current bound set.
    pub(crate) filters: Option<CustomScanFilters<P>>,

    /// Decoded provider [`PrivateData`](LakebaseCustomScanProvider::PrivateData).
    pub(crate) decoded_private: Option<P::PrivateData>,

    /// Provider-owned runtime state (`P::State`). Present after
    /// `P::create_state` and removed during teardown.
    pub(crate) provider_state: Option<P::State>,

    /// Cached framework envelope fields needed by rescan (avoids re-decoding
    /// the immutable `custom_private` list on every `ReScanCustomScan` call).
    /// Populated once during `BeginCustomScan`; `None` until then.
    pub(crate) cached_envelope: Option<CachedEnvelope>,

    /// Set after `P::begin` succeeds; End calls `P::end` only when true
    /// (EXPLAIN_ONLY may have state without begin).
    pub(crate) provider_began: bool,

    /// Zero-sized marker for provider type `P`.
    _marker: PhantomData<fn() -> P>,
}

/// Subset of `EncodedPrivate` that the rescan trampoline needs on every
/// invocation. Cached in [`CustomScanStateWrapper`] after the first decode
/// in `BeginCustomScan` to avoid re-parsing `custom_private` each rescan.
#[derive(Debug)]
pub struct CachedEnvelope {
    /// Query or modification-target use of the provider scan.
    pub purpose: ScanPurpose,
    /// Decoded once from `custom_private`; shared by provider scan-tuple binding.
    pub tuple_layout: ScanTupleLayout,
}

impl<P: LakebaseCustomScanProvider> Drop for CustomScanStateWrapper<P> {
    fn drop(&mut self) {
        // Normal EndCustomScan runs provider teardown before these owned values
        // are taken. This fallback drops values that survived an ERROR path.
        let _ = self.provider_state.take();
        let _ = self.decoded_private.take();
    }
}

impl<P: LakebaseCustomScanProvider> CustomScanStateWrapper<P> {
    /// Cast `*mut CustomScanState` to `&mut Self` (`base` at offset 0).
    ///
    /// # Safety
    ///
    /// `node` must come from this provider's `CreateCustomScanState` and remain
    /// valid for the per-query memory context lifetime.
    pub unsafe fn from_node_ptr<'a>(
        node: *mut pg_sys::CustomScanState,
    ) -> &'a mut Self {
        // SAFETY: forwarded from this function's contract; the cast is
        // sound under `#[repr(C)]` with `base` as the first field.
        unsafe { &mut *(node as *mut Self) }
    }

    /// Borrow the provider state after a successful BeginCustomScan.
    pub(crate) fn active_provider_state_mut(&mut self) -> Option<&mut P::State> {
        if !self.provider_began {
            return None;
        }
        self.provider_state.as_mut()
    }

    /// Borrow provider state from a callback after PostgreSQL has completed Begin.
    ///
    /// # Safety
    ///
    /// The wrapper must contain a provider state created before the callback.
    /// The caller must not use this accessor after the corresponding End
    /// callback has taken the state.
    pub(crate) unsafe fn provider_state_mut_unchecked(&mut self) -> &mut P::State {
        // SAFETY: guaranteed by this method's callback-lifecycle contract.
        unsafe { self.provider_state.as_mut().unwrap_unchecked() }
    }

    /// # Safety
    ///
    /// `self` must refer to a live wrapper allocated for the current test
    /// backend memory context.
    pub unsafe fn test_base_mut(&mut self) -> &mut pg_sys::CustomScanState {
        &mut self.base
    }

    /// # Safety
    ///
    /// `self` must refer to a live wrapper allocated for the current test
    /// backend memory context.
    pub unsafe fn test_base(&self) -> &pg_sys::CustomScanState {
        &self.base
    }

    /// # Safety
    ///
    /// `self` must refer to a live wrapper allocated for the current test
    /// backend memory context.
    pub unsafe fn test_scan_state_ptr(&mut self) -> *mut pg_sys::ScanState {
        core::ptr::addr_of_mut!(self.base.ss)
    }

    /// # Safety
    ///
    /// `state` must be the provider state created for this wrapper, and the
    /// wrapper must not already contain an installed provider state.
    pub unsafe fn test_install_provider_state(&mut self, state: P::State) {
        self.provider_state = Some(state);
        self.provider_began = true;
    }

    /// # Safety
    ///
    /// The caller must have installed the provider state and must not use the
    /// returned reference after teardown begins.
    pub unsafe fn test_provider_state(&self) -> &P::State {
        self.provider_state
            .as_ref()
            .expect("test provider state was not installed")
    }

    /// # Safety
    ///
    /// `envelope` must describe the live custom scan plan installed in the
    /// wrapper and must be set before invoking the ReScan callback.
    pub unsafe fn test_set_cached_envelope(&mut self, envelope: CachedEnvelope) {
        self.cached_envelope = Some(envelope);
    }

    /// # Safety
    ///
    /// `sections` must reference the live `custom_exprs` plan data for this
    /// wrapper and must be set before invoking the ReScan callback.
    pub unsafe fn test_set_expr_sections(&mut self, sections: CustomExprSections) {
        self.expr_sections = Some(sections);
    }
}

/// `CreateCustomScanState`: allocate wrapper in current memory context,
/// point `base.methods` at cached exec methods. Begin fills runtime fields.
#[pg_guard]
pub unsafe extern "C-unwind" fn create_custom_scan_state_trampoline<
    P: LakebaseCustomScanProvider,
>(
    _cscan: *mut pg_sys::CustomScan,
) -> *mut pg_sys::Node {
    let wrapper = CustomScanStateWrapper::<P> {
        base: pg_sys::CustomScanState {
            ss: pg_sys::ScanState {
                ps: pg_sys::PlanState {
                    type_: pg_sys::NodeTag::T_CustomScanState,
                    ..Default::default()
                },
                ..Default::default()
            },
            methods: method_tables_for::<P>().exec(),
            ..Default::default()
        },
        recheck_state: core::ptr::null_mut(),
        expr_sections: None,
        filters: None,
        decoded_private: None,
        provider_state: None,
        cached_envelope: None,
        provider_began: false,
        _marker: PhantomData,
    };

    // Box allocation honors Rust alignment (may exceed PG MAXALIGN).
    let wrapper_ptr =
        PgMemoryContexts::CurrentMemoryContext.leak_and_drop_on_delete(wrapper);

    // SAFETY: repr(C) first-field chain: wrapper → base → ss → ps → type_.
    wrapper_ptr.cast::<pg_sys::Node>()
}
