//! Small PostgreSQL-version adapters kept out of provider-facing code.

use core::ptr;

use pgrx::pg_sys;

use super::error::ForeignScanError;

/// Construct a base ForeignPath.  PG17 added `fdw_restrictinfo` to the
/// ForeignPath constructor; the framework uses NIL for the base-scan field on
/// both versions because join pushdown is not part of this facet.
///
/// # Safety
///
/// `root` and `baserel` must be live planner-owned nodes from the current
/// `GetForeignPaths` callback.  `pathkeys`, `required_outer`, and
/// `fdw_private` must use PostgreSQL's planner-context ownership rules.
pub(crate) unsafe fn create_foreign_path(
    root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    rows: f64,
    startup_cost: f64,
    total_cost: f64,
    pathkeys: *mut pg_sys::List,
    required_outer: pg_sys::Relids,
    fdw_private: *mut pg_sys::List,
) -> *mut pg_sys::ForeignPath {
    #[cfg(feature = "pg17")]
    let path = {
        // SAFETY: the current planner callback owns all pointer arguments;
        // PostgreSQL retains them in the planner memory context.
        unsafe {
            pg_sys::create_foreignscan_path(
                root,
                baserel,
                ptr::null_mut(),
                rows,
                startup_cost,
                total_cost,
                pathkeys,
                required_outer,
                ptr::null_mut(),
                ptr::null_mut(),
                fdw_private,
            )
        }
    };

    #[cfg(feature = "pg16")]
    let path = {
        // SAFETY: the current planner callback owns all pointer arguments;
        // PostgreSQL retains them in the planner memory context.
        unsafe {
            pg_sys::create_foreignscan_path(
                root,
                baserel,
                ptr::null_mut(),
                rows,
                startup_cost,
                total_cost,
                pathkeys,
                required_outer,
                ptr::null_mut(),
                fdw_private,
            )
        }
    };

    // The base-scan facet deliberately does not implement PostgreSQL's
    // parallel worker callbacks.  PG's constructor copies rel->consider_parallel
    // into ForeignPath.path.parallel_safe, so clear that inherited capability
    // before the path becomes visible to add_path().
    if !path.is_null() {
        // SAFETY: the constructor returned a live ForeignPath in the current
        // planner context, and the null check guards its field access.
        unsafe {
            (*path).path.parallel_safe = false;
            (*path).path.parallel_aware = false;
        }
    }
    path
}

/// Estimate PostgreSQL-local tuple processing performed by the ForeignScan.
///
/// PostgreSQL's `postgres_fdw` charges `cpu_tuple_cost` per row retrieved from
/// the foreign scan for local data manipulation.  The framework uses the same
/// standard cost unit for its scan-slot processing contract.
#[inline]
pub(crate) fn foreignscan_tuple_cost(retrieved_rows: f64) -> f64 {
    // SAFETY: PostgreSQL initializes this planner GUC before FDW callbacks.
    (unsafe { pg_sys::cpu_tuple_cost }) * retrieved_rows
}

/// PostgreSQL-local work performed outside the provider's access cost.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ForeignScanLocalCost {
    pub(crate) startup: f64,
    pub(crate) total: f64,
}

impl ForeignScanLocalCost {
    /// Estimate residual-qual and path-target evaluation using PostgreSQL's
    /// planner costing routines.
    ///
    /// # Safety
    ///
    /// `root` and `reltarget` must be live planner objects from the current
    /// callback.  `residual_quals` must be NULL or a live list of planner
    /// expression nodes owned by the current planner memory context.
    pub(crate) unsafe fn estimate(
        root: *mut pg_sys::PlannerInfo,
        residual_quals: *mut pg_sys::List,
        reltarget: *mut pg_sys::PathTarget,
        retrieved_rows: f64,
        output_rows: f64,
    ) -> Result<Self, ForeignScanError> {
        let mut qual_cost = pg_sys::QualCost {
            startup: 0.0,
            per_tuple: 0.0,
        };
        if !residual_quals.is_null() {
            unsafe { pg_sys::cost_qual_eval(&mut qual_cost, residual_quals, root) };
        }

        let mut startup = qual_cost.startup;
        let mut total = qual_cost.startup + qual_cost.per_tuple * retrieved_rows;
        if !reltarget.is_null() {
            let target_cost = unsafe { &(*reltarget).cost };
            startup += target_cost.startup;
            total += target_cost.startup + target_cost.per_tuple * output_rows;
        }
        if !startup.is_finite() || startup < 0.0 || !total.is_finite() || total < 0.0
        {
            return Err(ForeignScanError::framework(
                "PostgreSQL local foreign-scan cost is invalid",
            ));
        }
        Ok(Self { startup, total })
    }
}
