//! Provider-aware planner clause splitting for CustomPath variants.

use pgrx::pg_sys;

use crate::customscan::provider::{ErasedProvider, PlanTranslateContext};
use crate::expr::split::{PlanPushdownSplit, PlanPushdownSplitter, ScanClauseSource};

#[derive(Clone, Copy)]
pub(super) struct ProviderClauseSplitter {
    root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    provider: &'static dyn ErasedProvider,
}

impl ProviderClauseSplitter {
    pub(super) fn new(
        root: *mut pg_sys::PlannerInfo,
        rel: *mut pg_sys::RelOptInfo,
        provider: &'static dyn ErasedProvider,
    ) -> Self {
        Self {
            root,
            rel,
            provider,
        }
    }

    /// # Safety
    ///
    /// Planner pointers and `clauses` must be live planner-owned nodes.
    pub(super) unsafe fn split(
        self,
        clauses: *mut pg_sys::List,
        source: ScanClauseSource,
    ) -> PlanPushdownSplit {
        let translate_ctx = unsafe { PlanTranslateContext::new(self.rel) };
        let mut classify_leaf =
            |predicate: &crate::expr::predicate::PlanPredicate<'_>|
             -> crate::expr::split::QualPushdownDecision {
                self.provider.classify_predicate(&translate_ctx, predicate)
            };
        let mut splitter = PlanPushdownSplitter::new(
            self.root,
            self.rel,
            clauses,
            source,
            &mut classify_leaf,
        );
        unsafe { splitter.split() }
    }
}
