//! Planner-facing provider contracts and typed planner contexts.

use core::marker::PhantomData;

use pgrx::pg_sys;

pub use super::context::{PathContext, RelationContext};
use super::contract::LagodbCustomScanProvider;
pub use crate::customscan::ScanPurpose;
use crate::expr::pg::expr_list;
use crate::expr::pushdown::{FilterPlanSummary, PathFilterEstimates};

/// Typed builder for [`LagodbCustomScanProvider::create_path`]: cost overrides
/// and provider-private metadata. `path.rows` is set by the framework, not here.
pub struct CustomPathBuilder<P: LagodbCustomScanProvider> {
    pub(crate) cost: CustomPathCost,
    pub(crate) extra_tuple_width: i32,
    pub(crate) parallel_safe_complete: bool,
    pub(crate) native_parallel_partial: bool,
    _marker: PhantomData<fn() -> P>,
}

impl<P: LagodbCustomScanProvider> CustomPathBuilder<P> {
    pub(crate) fn new() -> Self {
        Self {
            cost: CustomPathCost::default(),
            extra_tuple_width: 0,
            parallel_safe_complete: false,
            native_parallel_partial: true,
            _marker: PhantomData,
        }
    }

    /// Pruned scan-page count (defaults to `baserel->pages`).
    pub fn scanned_pages(mut self, pages: f64) -> Self {
        debug_assert!(
            pages >= 0.0,
            "CustomPathBuilder::scanned_pages: must be non-negative",
        );
        self.cost.scanned_pages = Some(pages);
        self
    }

    /// Rows the provider returns to PostgreSQL, each charged `cpu_tuple_cost`
    /// and the residual quals (defaults to `baserel->tuples`, a sequential
    /// scan returning every row it reads).
    pub fn returned_tuples(mut self, tuples: f64) -> Self {
        debug_assert!(
            tuples >= 0.0,
            "CustomPathBuilder::returned_tuples: must be non-negative",
        );
        self.cost.returned_tuples = Some(tuples);
        self
    }

    /// Run cost the provider spends reading and filtering rows before it
    /// returns them (default `0.0`), as an index AM's own cost precedes the
    /// fetched heap tuples in `cost_index`.
    pub fn provider_run_cost(mut self, cost: f64) -> Self {
        debug_assert!(
            cost >= 0.0,
            "CustomPathBuilder::provider_run_cost: must be non-negative",
        );
        self.cost.provider_run_cost = Some(cost);
        self
    }

    /// Additive startup cost (default `0.0`).
    pub fn extra_startup_cost(mut self, cost: f64) -> Self {
        debug_assert!(
            cost >= 0.0,
            "CustomPathBuilder::extra_startup_cost: must be non-negative",
        );
        self.cost.extra_startup_cost = Some(cost);
        self
    }

    /// Additional raw scan-tuple width used by upper-node costing.
    pub fn extra_tuple_width(mut self, width: i32) -> Self {
        debug_assert!(width >= 0);
        self.extra_tuple_width = width;
        self
    }

    /// Allow the complete (non-partial) path to execute independently inside
    /// a PostgreSQL parallel worker.
    ///
    /// This is separate from the provider's native-parallel capability: a
    /// partial path reconstructs the leader's exact scan inventory from DSM,
    /// whereas a complete path receives no provider DSM coordinate and must be
    /// able to rebuild the same relation view using worker-local state alone.
    pub fn parallel_safe_complete(mut self, safe: bool) -> Self {
        self.parallel_safe_complete = safe;
        self
    }

    /// Control whether this path variant may also produce a native
    /// parallel-aware partial sibling. Providers use this when statement-local
    /// state cannot be reconstructed from their DSM payload.
    pub fn native_parallel_partial(mut self, enabled: bool) -> Self {
        self.native_parallel_partial = enabled;
        self
    }

    /// Finish the path and attach the provider's typed plan data.
    pub fn build(self, private_data: P::PrivateData) -> CustomPathPlan<P> {
        CustomPathPlan {
            cost: self.cost,
            extra_tuple_width: self.extra_tuple_width,
            parallel_safe_complete: self.parallel_safe_complete,
            native_parallel_partial: self.native_parallel_partial,
            private_data,
            _marker: PhantomData,
        }
    }
}

/// Output of `create_path`; consumed by the framework's path emitter.
pub struct CustomPathPlan<P: LagodbCustomScanProvider> {
    pub(crate) cost: CustomPathCost,
    pub(crate) extra_tuple_width: i32,
    pub(crate) parallel_safe_complete: bool,
    pub(crate) native_parallel_partial: bool,
    pub(crate) private_data: P::PrivateData,
    _marker: PhantomData<fn() -> P>,
}

/// Cost overrides a provider sets on its CustomPath. Each one it leaves unset
/// takes a sequential scan's relation estimate.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct CustomPathCost {
    pub(crate) scanned_pages: Option<f64>,
    pub(crate) returned_tuples: Option<f64>,
    pub(crate) provider_run_cost: Option<f64>,
    pub(crate) extra_startup_cost: Option<f64>,
}

impl CustomPathCost {
    /// Pages the scan reads (`baserel->pages` unless the provider pruned some).
    ///
    /// # Safety
    ///
    /// `baserel` is a live planner node.
    pub(crate) unsafe fn scanned_pages(
        &self,
        baserel: *const pg_sys::RelOptInfo,
    ) -> f64 {
        self.scanned_pages
            .unwrap_or_else(|| unsafe { (*baserel).pages } as f64)
    }

    /// CustomPath cost: disk + provider run cost + `cpu_tuple_cost` and
    /// residual `cost_qual_eval` per returned tuple + projection.
    ///
    /// # Safety
    ///
    /// Live planner pointers; each `residual` entry is a live planner-owned
    /// `Expr`.
    pub(crate) unsafe fn costs(
        &self,
        root: *mut pg_sys::PlannerInfo,
        baserel: *mut pg_sys::RelOptInfo,
        path_rows: f64,
        residual: &[*mut pg_sys::Expr],
    ) -> (f64, f64) {
        let scanned_pages = unsafe { self.scanned_pages(baserel) };
        let returned_tuples = self
            .returned_tuples
            .unwrap_or_else(|| unsafe { (*baserel).tuples });
        let provider_run_cost = self.provider_run_cost.unwrap_or(0.0);
        let extra_startup_cost = self.extra_startup_cost.unwrap_or(0.0);

        let pathtarget = unsafe { (*baserel).reltarget };
        let target_startup = unsafe { (*pathtarget).cost.startup };
        let target_per_tuple = unsafe { (*pathtarget).cost.per_tuple };

        let residual_list = expr_list(residual.iter().copied());

        let mut qpqual_cost = pg_sys::QualCost {
            startup: 0.0,
            per_tuple: 0.0,
        };
        unsafe {
            pg_sys::cost_qual_eval(
                &mut qpqual_cost as *mut pg_sys::QualCost,
                residual_list,
                root,
            );
        }

        let seq_page_cost = unsafe { pg_sys::seq_page_cost };
        let cpu_tuple_cost = unsafe { pg_sys::cpu_tuple_cost };

        let disk_cost = seq_page_cost * scanned_pages;
        let per_tuple_cpu =
            (cpu_tuple_cost + qpqual_cost.per_tuple) * returned_tuples;
        let projection_per_row = target_per_tuple * path_rows;

        let startup_cost = target_startup + qpqual_cost.startup + extra_startup_cost;
        let total_cost = startup_cost
            + disk_cost
            + provider_run_cost
            + per_tuple_cpu
            + projection_per_row;

        (startup_cost, total_cost)
    }
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

/// Per-variant input to [`super::contract::LagodbCustomScanProvider::create_path`].
pub struct PathVariant<'a> {
    /// Read scan or modification-target scan using the same provider.
    pub purpose: ScanPurpose,
    /// Branch on this, not `param_info.is_some()`.
    pub kind: PathVariantKind,
    /// Set when `required_outer` is non-empty.
    pub param_info: Option<&'a pg_sys::ParamPathInfo>,
    /// Required outer relids for this variant.
    pub required_outer: Relids,
    /// Pre-gated pushdown counts for this variant.
    pub pushdown: FilterPlanSummary,
    /// Estimates of the filters the provider executes for this variant.
    pub filters: PathFilterEstimates<'a>,
}
