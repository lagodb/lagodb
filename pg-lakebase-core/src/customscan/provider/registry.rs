//! Process-wide type-erased provider registration used by the planner router.

use core::ffi::CStr;
use core::marker::PhantomData;
use std::sync::{OnceLock, RwLock};

use pgrx::pg_sys;

use crate::customscan::error::CustomScanError;
use crate::expr::predicate::PlanPredicate;
use crate::expr::split::QualPushdownDecision;

use super::{LakebaseCustomScanProvider, PlanTranslateContext, RelPathContext};

/// Type-erased registered provider for the pathlist router.
pub trait ErasedProvider: Sync {
    /// Provider name (`P::NAME`).
    fn name(&self) -> &'static CStr;

    /// Forwards to `P::supports_relation` (framework path-stage gates already applied).
    fn supports_relation(&self, ctx: &RelPathContext) -> bool;

    /// Forwards to `P::classify_predicate`.
    fn classify_predicate(
        &self,
        ctx: &PlanTranslateContext,
        predicate: &PlanPredicate,
    ) -> QualPushdownDecision;

    /// Forwards to [`emit_custom_path`](crate::customscan::builder::emit_custom_path).
    ///
    /// # Safety
    ///
    /// `ctx` must reference live planner-owned structures for the current
    /// pathlist callback, and the underlying provider registration must remain
    /// valid while emitting the path.
    unsafe fn emit_path(
        &self,
        ctx: &crate::customscan::builder::EmitCustomPathContext<'_>,
    ) -> Result<bool, CustomScanError>;
}

/// Phantom wrapper for `P: LakebaseCustomScanProvider` in the registry.
struct ProviderEntry<P: LakebaseCustomScanProvider> {
    _marker: PhantomData<fn() -> P>,
}

impl<P: LakebaseCustomScanProvider> ProviderEntry<P> {
    const fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

// SAFETY: `ProviderEntry<P>` is stateless (`PhantomData` only).
unsafe impl<P: LakebaseCustomScanProvider> Sync for ProviderEntry<P> {}

impl<P: LakebaseCustomScanProvider> ErasedProvider for ProviderEntry<P> {
    fn name(&self) -> &'static CStr {
        P::NAME
    }

    fn supports_relation(&self, ctx: &RelPathContext) -> bool {
        P::supports_relation(ctx)
    }

    fn classify_predicate(
        &self,
        ctx: &PlanTranslateContext,
        predicate: &PlanPredicate,
    ) -> QualPushdownDecision {
        P::classify_predicate(ctx, predicate)
    }

    unsafe fn emit_path(
        &self,
        ctx: &crate::customscan::builder::EmitCustomPathContext<'_>,
    ) -> Result<bool, CustomScanError> {
        // SAFETY: caller upholds emit_custom_path contract.
        unsafe { crate::customscan::builder::emit_custom_path::<P>(ctx) }
    }
}

/// Process-global provider registry (`OnceLock<RwLock<...>>`).
static REGISTRY: OnceLock<RwLock<Vec<&'static dyn ErasedProvider>>> = OnceLock::new();

fn registry() -> &'static RwLock<Vec<&'static dyn ErasedProvider>> {
    REGISTRY.get_or_init(|| RwLock::new(Vec::new()))
}

/// Register provider at `_PG_init`; leaks entry for `'static` registry + calls
/// `RegisterCustomScanMethods`. Duplicate `P::NAME` panics.
pub fn register_provider<P: LakebaseCustomScanProvider>() {
    let entry: &'static ProviderEntry<P> =
        Box::leak(Box::new(ProviderEntry::<P>::new()));
    let mut reg = registry()
        .write()
        .expect("LakebaseCustomScanProvider registry RwLock poisoned");
    if reg.iter().any(|e| e.name() == P::NAME) {
        panic!(
            "LakebaseCustomScanProvider with name {:?} is already registered",
            P::NAME
        );
    }
    reg.push(entry as &'static dyn ErasedProvider);
    drop(reg);

    let methods: *const pg_sys::CustomScanMethods =
        super::method_tables_for::<P>().scan();
    // SAFETY: process-lifetime method table from the provider cache.
    unsafe {
        pg_sys::RegisterCustomScanMethods(methods);
    }
}

/// Find the unique provider claiming this relation, or `None` / multi-match error.
pub fn find_matching_provider(
    ctx: &RelPathContext,
) -> Result<Option<&'static dyn ErasedProvider>, CustomScanError> {
    let reg = registry()
        .read()
        .expect("LakebaseCustomScanProvider registry RwLock poisoned");

    // Collect all matches to detect ambiguity (not first-hit wins).
    let mut iter = reg.iter().copied().filter(|p| p.supports_relation(ctx));
    let Some(first) = iter.next() else {
        // Zero providers match — leave PG's default paths in place.
        return Ok(None);
    };
    if iter.next().is_some() {
        return Err(CustomScanError::multi_provider_match(
            ctx.rel_oid().to_u32(),
        ));
    }
    Ok(Some(first))
}
