//! CustomPath planning for one validated relation and provider.

use pgrx::pg_sys;

use crate::customscan::ScanPurpose;
use crate::customscan::error::CustomScanError;
use crate::customscan::planning::builder::EmitCustomPathContext;
use crate::customscan::planning::candidate::CustomScanCandidate;
use crate::customscan::planning::parameterized::{
    ParameterizedPathPlanner, ParameterizedPathResolver,
};
use crate::customscan::provider::{
    ErasedFilterPlanner, ErasedProvider, PathVariantKind,
};
use crate::expr::pushdown::{FilterPlanningContext, PathFilterSet, ScanClauseSource};

/// Plans every CustomPath variant for one validated relation/provider pair.
pub(super) struct CustomScanPathPlanner {
    candidate: CustomScanCandidate,
    provider: &'static dyn ErasedProvider,
    filter_planner: Box<dyn ErasedFilterPlanner>,
    base_filters: PathFilterSet,
}

impl CustomScanPathPlanner {
    /// # Safety
    ///
    /// Planner pointers captured by `candidate` must remain live.
    pub(super) unsafe fn new(
        candidate: CustomScanCandidate,
        provider: &'static dyn ErasedProvider,
    ) -> Result<Self, CustomScanError> {
        let rel = candidate.rel();
        let relation = unsafe { candidate.relation_context() };
        let context = FilterPlanningContext::new(
            relation.rel_oid(),
            unsafe { (*rel).relid },
            relation.tablespace_oid(),
        );
        let mut filter_planner = provider.begin_filter_planning(&context, rel)?;
        let base_filters = unsafe {
            filter_planner
                .negotiate((*rel).baserestrictinfo, ScanClauseSource::BaseRestriction)
        }?;
        Ok(Self {
            candidate,
            provider,
            filter_planner,
            base_filters,
        })
    }

    /// Emit Plain first, followed by useful JoinParameterized variants.
    pub(super) unsafe fn emit(&mut self) -> Result<usize, CustomScanError> {
        Ok(usize::from(unsafe { self.emit_plain_variant()? })
            + unsafe { self.emit_parameterized_variants()? })
    }

    unsafe fn emit_plain_variant(&mut self) -> Result<bool, CustomScanError> {
        let root = self.candidate.root();
        let rel = self.candidate.rel();
        let required_outer = unsafe { pg_sys::bms_copy((*rel).lateral_relids) };

        if required_outer.is_null() {
            return unsafe {
                self.emit_path(
                    PathVariantKind::Plain,
                    required_outer,
                    &self.base_filters,
                )
            };
        }

        let lateral_filters = unsafe {
            ParameterizedPathResolver::new(root, rel)
                .resolve_and_plan(required_outer, self.filter_planner.as_mut())
        }?;
        let filters = self.base_filters.merged(&lateral_filters);
        unsafe { self.emit_path(PathVariantKind::Plain, required_outer, &filters) }
    }

    unsafe fn emit_parameterized_variants(
        &mut self,
    ) -> Result<usize, CustomScanError> {
        let root = self.candidate.root();
        let rel = self.candidate.rel();
        let groups = unsafe {
            ParameterizedPathPlanner::new(root, rel).enumerate((*rel).joininfo)
        };

        let mut emitted = 0;
        for group in groups {
            let ppi_filters = unsafe {
                ParameterizedPathResolver::new(root, rel).resolve_and_plan(
                    group.outer_relids,
                    self.filter_planner.as_mut(),
                )
            }?;
            let Some(filters) = ParameterizedVariant::new(
                self.candidate.purpose(),
                &self.base_filters,
                &ppi_filters,
            )
            .merged_filters() else {
                continue;
            };
            emitted += usize::from(unsafe {
                self.emit_path(
                    PathVariantKind::JoinParameterized,
                    group.outer_relids,
                    &filters,
                )?
            });
        }
        Ok(emitted)
    }

    unsafe fn emit_path(
        &self,
        kind: PathVariantKind,
        required_outer: *mut pg_sys::Bitmapset,
        filters: &PathFilterSet,
    ) -> Result<bool, CustomScanError> {
        let ctx = EmitCustomPathContext {
            root: self.candidate.root(),
            baserel: self.candidate.rel(),
            purpose: self.candidate.purpose(),
            kind,
            required_outer,
            filters,
        };
        unsafe { self.provider.emit_path(&ctx) }
    }
}

struct ParameterizedVariant<'a> {
    purpose: ScanPurpose,
    base_filters: &'a PathFilterSet,
    ppi_filters: &'a PathFilterSet,
}

impl<'a> ParameterizedVariant<'a> {
    fn new(
        purpose: ScanPurpose,
        base_filters: &'a PathFilterSet,
        ppi_filters: &'a PathFilterSet,
    ) -> Self {
        Self {
            purpose,
            base_filters,
            ppi_filters,
        }
    }

    fn merged_filters(&self) -> Option<PathFilterSet> {
        (self.purpose.is_modify() || self.ppi_filters.has_planned_filters())
            .then(|| self.base_filters.merged(self.ppi_filters))
    }
}
