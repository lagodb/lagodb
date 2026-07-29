//! CustomPath planning for one validated relation and provider.

use pgrx::pg_sys;

use crate::customscan::ScanPurpose;
use crate::customscan::error::CustomScanError;
use crate::customscan::planning::builder::EmitCustomPathContext;
use crate::customscan::planning::candidate::CustomScanCandidate;
use crate::customscan::planning::parameterized::{
    ParameterizedPathGroup, ParameterizedPathPlanner, ParameterizedPathResolver,
};
use crate::customscan::provider::{
    ErasedProvider, PathVariantKind, PlanTranslateContext,
};
use crate::expr::contract::QualPushdownDecision;
use crate::expr::split::{PlanPushdownSplit, PlanPushdownSplitter, ScanClauseSource};

/// Plans every CustomPath variant for one validated relation/provider pair.
pub(super) struct CustomScanPathPlanner {
    candidate: CustomScanCandidate,
    provider: &'static dyn ErasedProvider,
    base_split: PlanPushdownSplit,
}

impl CustomScanPathPlanner {
    /// # Safety
    ///
    /// Planner pointers captured by `candidate` must remain live.
    pub(super) unsafe fn new(
        candidate: CustomScanCandidate,
        provider: &'static dyn ErasedProvider,
    ) -> Self {
        let root = candidate.root();
        let rel = candidate.rel();
        let base_split = unsafe {
            PredicatePlanner::new(root, rel, provider)
                .split((*rel).baserestrictinfo, ScanClauseSource::BaseRestriction)
        };
        Self {
            candidate,
            provider,
            base_split,
        }
    }

    /// Emit Plain first, followed by useful JoinParameterized variants.
    pub(super) unsafe fn emit(&self) -> Result<usize, CustomScanError> {
        Ok(usize::from(unsafe { self.emit_plain_variant()? })
            + unsafe { self.emit_parameterized_variants()? })
    }

    unsafe fn emit_plain_variant(&self) -> Result<bool, CustomScanError> {
        let root = self.candidate.root();
        let rel = self.candidate.rel();
        let required_outer = unsafe { pg_sys::bms_copy((*rel).lateral_relids) };

        if required_outer.is_null() {
            return unsafe {
                self.emit_path(
                    PathVariantKind::Plain,
                    required_outer,
                    &self.base_split,
                )
            };
        }

        let lateral_split = unsafe {
            ParameterizedPathResolver::new(root, rel, self.provider)
                .resolve_and_split(required_outer)
        };
        let split = self
            .base_split
            .merged_with_rebased_expr_indexes(&lateral_split);
        unsafe { self.emit_path(PathVariantKind::Plain, required_outer, &split) }
    }

    unsafe fn emit_parameterized_variants(&self) -> Result<usize, CustomScanError> {
        let root = self.candidate.root();
        let rel = self.candidate.rel();
        let groups = unsafe {
            ParameterizedPathPlanner::new(root, rel, self.provider)
                .enumerate((*rel).joininfo)
        };

        let mut emitted = 0;
        for group in groups {
            let Some(split) = ParameterizedVariant::new(
                self.candidate.purpose(),
                &self.base_split,
                &group,
            )
            .merged_split() else {
                continue;
            };
            emitted += usize::from(unsafe {
                self.emit_path(
                    PathVariantKind::JoinParameterized,
                    group.outer_relids,
                    &split,
                )?
            });
        }
        Ok(emitted)
    }

    unsafe fn emit_path(
        &self,
        kind: PathVariantKind,
        required_outer: *mut pg_sys::Bitmapset,
        split: &PlanPushdownSplit,
    ) -> Result<bool, CustomScanError> {
        let ctx = EmitCustomPathContext {
            root: self.candidate.root(),
            baserel: self.candidate.rel(),
            purpose: self.candidate.purpose(),
            kind,
            required_outer,
            split,
        };
        unsafe { self.provider.emit_path(&ctx) }
    }
}

struct ParameterizedVariant<'a> {
    purpose: ScanPurpose,
    base_split: &'a PlanPushdownSplit,
    group: &'a ParameterizedPathGroup,
}

impl<'a> ParameterizedVariant<'a> {
    fn new(
        purpose: ScanPurpose,
        base_split: &'a PlanPushdownSplit,
        group: &'a ParameterizedPathGroup,
    ) -> Self {
        Self {
            purpose,
            base_split,
            group,
        }
    }

    fn merged_split(&self) -> Option<PlanPushdownSplit> {
        (self.purpose.is_modify() || self.group.ppi_split.has_pushed_predicates())
            .then(|| {
                self.base_split
                    .merged_with_rebased_expr_indexes(&self.group.ppi_split)
            })
    }
}

/// Applies core clause gates and asks one provider to classify eligible leaves.
#[derive(Clone, Copy)]
pub(super) struct PredicatePlanner {
    root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    provider: &'static dyn ErasedProvider,
}

impl PredicatePlanner {
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
            |predicate: &crate::expr::predicate::PlanPredicate|
             -> QualPushdownDecision {
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
