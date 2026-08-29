//! Process-wide type-erased provider registration used by the planner router.

use core::ffi::CStr;
use core::marker::PhantomData;
use std::cell::RefCell;

use pgrx::pg_sys;

use crate::customscan::error::CustomScanError;
use crate::customscan::planning::builder::{EmitCustomPathContext, emit_custom_path};
use crate::expr::pushdown::{
    FilterNegotiator, FilterPlanningContext, PathFilterSet, ScanClauseSource,
};

use super::{LagodbCustomScanProvider, RelationContext};

pub(crate) trait ErasedFilterPlanner {
    /// # Safety
    ///
    /// `clauses` must be a live planner-owned `List<RestrictInfo>`.
    unsafe fn negotiate(
        &mut self,
        clauses: *mut pg_sys::List,
        source: ScanClauseSource,
    ) -> Result<PathFilterSet, CustomScanError>;
}

/// Type-erased registered provider for the pathlist router.
pub(crate) trait ErasedProvider: Sync {
    /// Provider name (`P::NAME`).
    fn name(&self) -> &'static CStr;

    /// Forwards to `P::supports_relation` (framework path-stage gates already applied).
    fn supports_relation(&self, ctx: &RelationContext<'_>) -> bool;

    /// Create one relation-scoped typed filter planner behind a planning-only
    /// object-safe bridge.
    fn begin_filter_planning(
        &self,
        context: &FilterPlanningContext,
        baserel: *mut pg_sys::RelOptInfo,
    ) -> Result<Box<dyn ErasedFilterPlanner>, CustomScanError>;

    /// Forwards to [`emit_custom_path`](crate::customscan::planning::builder::emit_custom_path).
    ///
    /// # Safety
    ///
    /// `ctx` must reference live planner-owned structures for the current
    /// pathlist callback, and the underlying provider registration must remain
    /// valid while emitting the path.
    unsafe fn emit_path(
        &self,
        ctx: &EmitCustomPathContext<'_>,
    ) -> Result<bool, CustomScanError>;
}

/// Phantom wrapper for `P: LagodbCustomScanProvider` in the registry.
struct ProviderEntry<P: LagodbCustomScanProvider> {
    _marker: PhantomData<fn() -> P>,
}

struct ProviderFilterPlanner<P: LagodbCustomScanProvider> {
    planner: P::Planner,
    relation_oid: pg_sys::Oid,
    baserel: *mut pg_sys::RelOptInfo,
}

impl<P: LagodbCustomScanProvider> ErasedFilterPlanner for ProviderFilterPlanner<P> {
    unsafe fn negotiate(
        &mut self,
        clauses: *mut pg_sys::List,
        source: ScanClauseSource,
    ) -> Result<PathFilterSet, CustomScanError> {
        let mut negotiator =
            FilterNegotiator::new(&mut self.planner, self.relation_oid, self.baserel);
        match unsafe { negotiator.negotiate(clauses, source) } {
            Ok(filters) => Ok(filters.into_path_set()),
            Err(error) => Err(CustomScanError::provider(error)),
        }
    }
}

impl<P: LagodbCustomScanProvider> ProviderEntry<P> {
    const fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

// SAFETY: `ProviderEntry<P>` is stateless (`PhantomData` only).
unsafe impl<P: LagodbCustomScanProvider> Sync for ProviderEntry<P> {}

impl<P: LagodbCustomScanProvider> ErasedProvider for ProviderEntry<P> {
    fn name(&self) -> &'static CStr {
        P::NAME
    }

    fn supports_relation(&self, ctx: &RelationContext<'_>) -> bool {
        P::supports_relation(ctx)
    }

    fn begin_filter_planning(
        &self,
        context: &FilterPlanningContext,
        baserel: *mut pg_sys::RelOptInfo,
    ) -> Result<Box<dyn ErasedFilterPlanner>, CustomScanError> {
        let planner = match P::begin_filter_planning(context) {
            Ok(planner) => planner,
            Err(error) => return Err(CustomScanError::provider(error)),
        };
        Ok(Box::new(ProviderFilterPlanner::<P> {
            planner,
            relation_oid: context.relation_oid(),
            baserel,
        }))
    }

    unsafe fn emit_path(
        &self,
        ctx: &EmitCustomPathContext<'_>,
    ) -> Result<bool, CustomScanError> {
        // SAFETY: caller upholds emit_custom_path contract.
        unsafe { emit_custom_path::<P>(ctx) }
    }
}

thread_local! {
    /// DSO-local planner providers. Registration and lookup both occur on the
    /// single PostgreSQL backend thread; this is intentionally not cross-DSO.
    static REGISTRY: RefCell<Vec<&'static dyn ErasedProvider>> = const { RefCell::new(Vec::new()) };
}

/// Register provider at `_PG_init`; leaks entry for `'static` registry + calls
/// `RegisterCustomScanMethods`. Duplicate `P::NAME` panics.
pub(super) fn register_provider<P: LagodbCustomScanProvider>() -> bool {
    let entry: &'static ProviderEntry<P> =
        Box::leak(Box::new(ProviderEntry::<P>::new()));
    let first = REGISTRY.with_borrow_mut(|registry| {
        if registry.iter().any(|provider| provider.name() == P::NAME) {
            panic!(
                "LagodbCustomScanProvider with name {:?} is already registered",
                P::NAME
            );
        }
        let first = registry.is_empty();
        registry.push(entry as &'static dyn ErasedProvider);
        first
    });

    let methods: *const pg_sys::CustomScanMethods =
        super::method_tables_for::<P>().scan();
    // SAFETY: process-lifetime method table from the provider cache.
    unsafe {
        pg_sys::RegisterCustomScanMethods(methods);
    }
    first
}

/// Find the unique provider claiming this relation, or `None` / multi-match error.
pub(crate) fn find_matching_provider(
    ctx: &RelationContext<'_>,
) -> Result<Option<&'static dyn ErasedProvider>, CustomScanError> {
    REGISTRY.with_borrow(|registry| {
        let mut matches = registry
            .iter()
            .copied()
            .filter(|provider| provider.supports_relation(ctx));
        let Some(first) = matches.next() else {
            return Ok(None);
        };
        if matches.next().is_some() {
            return Err(CustomScanError::multi_provider_match(
                ctx.rel_oid().to_u32(),
            ));
        }
        Ok(Some(first))
    })
}
