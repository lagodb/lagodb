//! Executor-owned `#[repr(C)]` `CustomScanStateWrapper<P>` (`base` at offset 0).
//! `MaybeUninit` provider fields use initialization flags for exception-safe teardown.

use core::marker::PhantomData;
use core::mem::MaybeUninit;

use pgrx::PgMemoryContexts;
use pgrx::pg_guard;
use pgrx::pg_sys;

use crate::customscan::provider::LakebaseCustomScanProvider;

/// `#[repr(C)]` wrapper PostgreSQL holds as `*mut CustomScanState`.
///
/// `base` MUST be the first field for pointer casts via [`Self::from_node_ptr`].
/// `CreateCustomScanState` allocates the wrapper; `BeginCustomScan` fills runtime
/// fields. Typed payloads drop in `EndCustomScan`, gated on `*_initialized` flags;
/// [`Drop`] is a fallback when Begin errors before End runs.
#[repr(C)]
pub struct CustomScanStateWrapper<P: LakebaseCustomScanProvider> {
    /// PG's [`pg_sys::CustomScanState`]. **MUST** remain the first field
    /// so `*mut CustomScanState` and `*mut CustomScanStateWrapper<P>`
    /// share an address.
    pub base: pg_sys::CustomScanState,

    /// Recheck `ExprState` from `ExecInitQual`; NULL when `recheck_count == 0`.
    pub recheck_state: *mut pg_sys::ExprState,

    /// Immutable expression sections validated once during Begin.
    pub expr_sections: Option<crate::customscan::custom_exprs::CustomExprSections>,

    /// Deduplicated parameter references collected once during Begin.
    pub runtime_params: Option<crate::customscan::exec_params::RuntimeParamRefs>,

    /// Decoded provider [`PrivateData`](LakebaseCustomScanProvider::PrivateData).
    /// Read/drop only when [`Self::decoded_private_initialized`] is true.
    pub decoded_private: MaybeUninit<P::PrivateData>,

    /// Provider-owned runtime state (`P::State`). Gated on
    /// [`Self::provider_state_initialized`].
    pub provider_state: MaybeUninit<P::State>,

    /// Cached framework envelope fields needed by rescan (avoids re-decoding
    /// the immutable `custom_private` list on every `ReScanCustomScan` call).
    /// Populated once during `BeginCustomScan`; `None` until then.
    pub cached_envelope: Option<CachedEnvelope>,

    /// Set after `BeginCustomScan` writes [`Self::decoded_private`].
    pub decoded_private_initialized: bool,

    /// Set after `BeginCustomScan` writes [`Self::provider_state`].
    pub provider_state_initialized: bool,

    /// Set after `P::begin` succeeds; End calls `P::end` only when true
    /// (EXPLAIN_ONLY may have state without begin).
    pub provider_began: bool,

    /// Zero-sized marker for provider type `P`.
    _marker: PhantomData<fn() -> P>,
}

/// Subset of `EncodedPrivate` that the rescan trampoline needs on every
/// invocation. Cached in [`CustomScanStateWrapper`] after the first decode
/// in `BeginCustomScan` to avoid re-parsing `custom_private` each rescan.
#[derive(Debug)]
pub struct CachedEnvelope {
    /// Per-pushed-expression pushdown contract (aligned with pushed section).
    pub pushed_contracts: Vec<crate::expr::split::PushdownContract>,
    /// Pre-resolved column metadata for the pushed expressions.
    pub column_refs: Vec<crate::expr::split::ColumnRef>,
    /// Decoded once from `custom_private`; shared by begin/rescan translation
    /// and provider scan-tuple binding.
    pub tuple_layout: crate::customscan::tuple_layout::ScanTupleLayout,
}

impl<P: LakebaseCustomScanProvider> Drop for CustomScanStateWrapper<P> {
    fn drop(&mut self) {
        // Normal EndCustomScan runs provider teardown and clears these flags.
        // This fallback only drops typed Rust payloads that survived an ERROR path.
        if self.provider_state_initialized {
            unsafe {
                core::ptr::drop_in_place(self.provider_state.as_mut_ptr());
            }
            self.provider_state_initialized = false;
        }
        if self.decoded_private_initialized {
            unsafe {
                core::ptr::drop_in_place(self.decoded_private.as_mut_ptr());
            }
            self.decoded_private_initialized = false;
        }
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
        debug_assert!(!node.is_null(), "from_node_ptr called with NULL node");
        // SAFETY: forwarded from this function's contract; the cast is
        // sound under `#[repr(C)]` with `base` as the first field.
        unsafe { &mut *(node as *mut Self) }
    }

    /// Return `*mut CustomScanState` aliasing `self.base`.
    pub fn as_node_ptr(&mut self) -> *mut pg_sys::CustomScanState {
        // SAFETY: `base` is at offset 0 under `#[repr(C)]`.
        &mut self.base as *mut pg_sys::CustomScanState
    }

    /// Upcast to `*mut Node`.
    pub fn as_node(&mut self) -> *mut pg_sys::Node {
        self.as_node_ptr().cast()
    }
}

/// `CreateCustomScanState`: allocate wrapper in current memory context,
/// point `base.methods` at cached exec methods. Begin fills runtime fields.
#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn create_custom_scan_state_trampoline<
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
            methods: crate::customscan::provider::method_tables_for::<P>().exec(),
            ..Default::default()
        },
        recheck_state: core::ptr::null_mut(),
        expr_sections: None,
        runtime_params: None,
        decoded_private: MaybeUninit::uninit(),
        provider_state: MaybeUninit::uninit(),
        cached_envelope: None,
        decoded_private_initialized: false,
        provider_state_initialized: false,
        provider_began: false,
        _marker: PhantomData,
    };

    // Box allocation honors Rust alignment (may exceed PG MAXALIGN).
    let wrapper_ptr =
        PgMemoryContexts::CurrentMemoryContext.leak_and_drop_on_delete(wrapper);

    // SAFETY: repr(C) first-field chain: wrapper → base → ss → ps → type_.
    wrapper_ptr.cast::<pg_sys::Node>()
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestStateA;

    use crate::customscan::test_support::{NoopProvider, NoopProviderSpec};

    struct StateProviderSpec;

    impl NoopProviderSpec for StateProviderSpec {
        const NAME: &'static core::ffi::CStr = c"test-provider-a";
        type State = TestStateA;

        fn state() -> Self::State {
            TestStateA
        }
    }

    type ProviderA = NoopProvider<StateProviderSpec>;

    #[test]
    fn base_field_is_at_offset_zero() {
        let offset = core::mem::offset_of!(CustomScanStateWrapper<ProviderA>, base);
        assert_eq!(
            offset, 0,
            "CustomScanStateWrapper::base must be the first field"
        );
    }

    #[test]
    fn from_node_ptr_and_as_node_ptr_round_trip() {
        let mut storage: MaybeUninit<CustomScanStateWrapper<ProviderA>> =
            MaybeUninit::zeroed();
        // SAFETY: zeroed wrapper; typed payloads never read.
        let wrapper_ptr: *mut CustomScanStateWrapper<ProviderA> =
            storage.as_mut_ptr();
        let node_ptr: *mut pg_sys::CustomScanState = wrapper_ptr.cast();

        // SAFETY: `storage` lives for the test.
        let wrapper_ref =
            unsafe { CustomScanStateWrapper::<ProviderA>::from_node_ptr(node_ptr) };
        let round_tripped = wrapper_ref.as_node_ptr();
        assert_eq!(
            round_tripped, node_ptr,
            "as_node_ptr(from_node_ptr(p)) must equal p",
        );

        let as_node = wrapper_ref.as_node();
        assert_eq!(as_node as *mut pg_sys::CustomScanState, node_ptr);
    }
}
