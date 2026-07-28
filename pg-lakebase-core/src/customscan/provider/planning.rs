//! Planner-facing provider contracts and typed planner contexts.

use core::marker::PhantomData;

use pgrx::pg_sys;

pub use super::context::{PathContext, RelationContext};
use super::contract::LakebaseCustomScanProvider;
pub use crate::customscan::ScanPurpose;
use crate::expr::contract::PushdownContract;

/// Typed builder for [`LakebaseCustomScanProvider::create_path`]: cost overrides
/// and provider-private metadata. `path.rows` is set by the framework, not here.
pub struct CustomPathBuilder<P: LakebaseCustomScanProvider> {
    pub(crate) scanned_pages: Option<f64>,
    pub(crate) scanned_tuples: Option<f64>,
    pub(crate) extra_startup_cost: Option<f64>,
    pub(crate) extra_tuple_width: i32,
    _marker: PhantomData<fn() -> P>,
}

impl<P: LakebaseCustomScanProvider> CustomPathBuilder<P> {
    pub(crate) fn new() -> Self {
        Self {
            scanned_pages: None,
            scanned_tuples: None,
            extra_startup_cost: None,
            extra_tuple_width: 0,
            _marker: PhantomData,
        }
    }

    /// Pruned scan-page count (defaults to `baserel->pages`).
    pub fn scanned_pages(mut self, pages: f64) -> Self {
        debug_assert!(
            pages >= 0.0,
            "CustomPathBuilder::scanned_pages: must be non-negative",
        );
        self.scanned_pages = Some(pages);
        self
    }

    /// Pruned scan-tuple count (defaults to `baserel->tuples`).
    pub fn scanned_tuples(mut self, tuples: f64) -> Self {
        debug_assert!(
            tuples >= 0.0,
            "CustomPathBuilder::scanned_tuples: must be non-negative",
        );
        self.scanned_tuples = Some(tuples);
        self
    }

    /// Additive startup cost (default `0.0`).
    pub fn extra_startup_cost(mut self, cost: f64) -> Self {
        debug_assert!(
            cost >= 0.0,
            "CustomPathBuilder::extra_startup_cost: must be non-negative",
        );
        self.extra_startup_cost = Some(cost);
        self
    }

    /// Additional raw scan-tuple width used by upper-node costing.
    pub fn extra_tuple_width(mut self, width: i32) -> Self {
        debug_assert!(width >= 0);
        self.extra_tuple_width = width;
        self
    }

    /// Finish the path and attach the provider's typed plan data.
    pub fn build(self, private_data: P::PrivateData) -> CustomPathPlan<P> {
        CustomPathPlan {
            scanned_pages: self.scanned_pages,
            scanned_tuples: self.scanned_tuples,
            extra_startup_cost: self.extra_startup_cost,
            extra_tuple_width: self.extra_tuple_width,
            private_data,
            _marker: PhantomData,
        }
    }
}

/// Output of `create_path`; consumed by the framework's path emitter.
pub struct CustomPathPlan<P: LakebaseCustomScanProvider> {
    pub(crate) scanned_pages: Option<f64>,
    pub(crate) scanned_tuples: Option<f64>,
    pub(crate) extra_startup_cost: Option<f64>,
    pub(crate) extra_tuple_width: i32,
    pub(crate) private_data: P::PrivateData,
    _marker: PhantomData<fn() -> P>,
}

/// Nullable PG `Bitmapset *` (Relids). Use `bms_is_empty`, not pointer null test.
pub type Relids = *mut pg_sys::Bitmapset;

/// Plain vs join-parameterized CustomPath variant. Use [`Self::kind`], not
/// `param_info.is_some()`, to tell them apart.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PathVariantKind {
    /// Plain variant (`required_outer = lateral_relids`).
    Plain,

    /// Join-parameterized variant (one per surviving `outer_relids`).
    JoinParameterized,
}

/// Path-stage pushdown metadata for [`PathVariant`]; no raw PG `Expr` pointers.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PathPushdownSummary {
    /// Number of clauses core classified as pushed for this variant.
    pub pushed_count: usize,
    /// Pushed clauses with [`PushdownContract::ExactRowFilter`].
    pub exact_row_filter_count: usize,
    /// Pushed clauses with [`PushdownContract::ConservativePruning`].
    pub conservative_pruning_count: usize,
    /// Pushed clauses with costed pruning.
    pub costed_pruning_count: usize,
    /// Combined selectivity of costed-pruning pushed clauses.
    pub pruning_selectivity: f64,
}

impl PathPushdownSummary {
    /// Whether this variant has any pushed predicates.
    #[inline]
    pub fn has_pushed_predicates(self) -> bool {
        self.pushed_count > 0
    }

    /// Summarize an internal plan split plus planner-computed selectivity.
    pub(crate) fn from_split(
        split: &crate::expr::split::PlanPushdownSplit,
        pruning_selectivity: f64,
    ) -> Self {
        let pushed_count = split.pushed.len();
        let mut exact_row_filter_count = 0;
        let mut conservative_pruning_count = 0;
        let mut costed_pruning_count = 0;
        for pushed in &split.pushed {
            match pushed.contract {
                PushdownContract::ExactRowFilter => exact_row_filter_count += 1,
                PushdownContract::ConservativePruning => {
                    conservative_pruning_count += 1;
                }
            }
            if pushed.costing.is_costed() {
                costed_pruning_count += 1;
            }
        }
        Self {
            pushed_count,
            exact_row_filter_count,
            conservative_pruning_count,
            costed_pruning_count,
            pruning_selectivity: pruning_selectivity.clamp(0.0, 1.0),
        }
    }
}

/// Per-variant input to [`super::contract::LakebaseCustomScanProvider::create_path`].
pub struct PathVariant<'a> {
    /// Query scan or modification-target scan using the same provider.
    pub purpose: ScanPurpose,
    /// Branch on this, not `param_info.is_some()`.
    pub kind: PathVariantKind,
    /// Set when `required_outer` is non-empty.
    pub param_info: Option<&'a pg_sys::ParamPathInfo>,
    /// Required outer relids for this variant.
    pub required_outer: Relids,
    /// Pre-gated pushdown summary for this variant.
    pub pushdown: PathPushdownSummary,
}

/// Plan-stage context for provider predicate classification.
pub struct PlanTranslateContext {
    baserel: *mut pg_sys::RelOptInfo,
}

impl PlanTranslateContext {
    /// Construct from a live planner-owned base relation.
    ///
    /// # Safety
    ///
    /// `baserel` must be a non-NULL planner-owned `RelOptInfo` that remains
    /// live for the duration of use.
    #[inline]
    pub unsafe fn new(baserel: *mut pg_sys::RelOptInfo) -> Self {
        Self { baserel }
    }

    /// The scan relation's range-table index (`baserel->relid`).
    #[inline]
    pub fn scan_relid(&self) -> core::ffi::c_int {
        unsafe { (*self.baserel).relid as core::ffi::c_int }
    }
}
