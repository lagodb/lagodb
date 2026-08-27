//! Provider path alternatives and framework path validation.

use core::ffi::c_void;
use core::ptr;

use pgrx::pg_sys;

use crate::expr::pushdown::PathFilterSet;

use super::context::{
    ForeignPathContext, ForeignPathSpec, ForeignRelContext, PathVariantKind,
};
use super::contract::FdwScan;
use super::error::ForeignScanError;
use super::pathkeys::ForeignPathKeys;
use super::pg;
use super::private::encode_path_private;

/// Path alternatives submitted by one provider path callback.
///
/// A provider can submit no path, one unordered path, or any number of
/// independent ordered and unordered alternatives.  The framework validates
/// each submitted spec separately; it never derives an unordered path from an
/// ordered spec.
pub struct ForeignPathBuilder<D> {
    specs: Vec<ForeignPathSpec<D>>,
}

impl<D> Default for ForeignPathBuilder<D> {
    #[inline]
    fn default() -> Self {
        Self { specs: Vec::new() }
    }
}

impl<D> ForeignPathBuilder<D> {
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Submit one independent path alternative.
    #[inline]
    pub fn push(&mut self, spec: ForeignPathSpec<D>) {
        self.specs.push(spec);
    }

    #[inline]
    pub(crate) fn into_specs(self) -> Vec<ForeignPathSpec<D>> {
        self.specs
    }
}

/// Run one provider callback and add every accepted alternative to
/// PostgreSQL's base relation.
///
/// # Safety
///
/// `root`, `baserel`, `relation`, `param_info`, and `filters` must all refer to
/// live planner state for one `GetForeignPaths` callback. `required_outer`
/// must be a PostgreSQL Bitmapset owned by the planner, and `provider_state`
/// must remain immutably borrowed during path construction.
pub(crate) unsafe fn build_path_variants<P: FdwScan>(
    root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    relation: &ForeignRelContext<'_>,
    provider_state: &P::PlannerState,
    kind: PathVariantKind,
    required_outer: pg_sys::Relids,
    param_info: *mut pg_sys::ParamPathInfo,
    filters: &PathFilterSet,
) -> Result<usize, ForeignScanError> {
    let relids = unsafe { (*baserel).relids };
    if unsafe { pg_sys::bms_overlap(relids, required_outer) } {
        return Err(ForeignScanError::framework(
            "FDW path required_outer overlaps the scanned relation",
        ));
    }
    if !unsafe { pg_sys::bms_is_subset(relation.lateral_relids(), required_outer) } {
        return Err(ForeignScanError::framework(
            "FDW path required_outer does not include the relation's lateral dependencies",
        ));
    }
    if kind == PathVariantKind::JoinParameterized && param_info.is_null() {
        return Err(ForeignScanError::framework(
            "join-parameterized FDW path has a NULL ParamPathInfo",
        ));
    }
    if kind == PathVariantKind::JoinParameterized
        && unsafe {
            pg_sys::bms_membership(required_outer)
                == pg_sys::BMS_Membership::BMS_EMPTY_SET
        }
    {
        return Err(ForeignScanError::framework(
            "join-parameterized FDW path has an empty required_outer set",
        ));
    }

    let context =
        ForeignPathContext::new(*relation, filters, kind, required_outer, param_info);
    let mut builder = ForeignPathBuilder::new();
    P::build_paths(provider_state, &context, &mut builder)?;

    let mut emitted = 0;
    for spec in builder.into_specs() {
        if unsafe {
            add_path_spec::<P>(
                root,
                baserel,
                relation,
                provider_state,
                kind,
                required_outer,
                filters,
                &context,
                spec,
            )
        }? {
            emitted += 1;
        }
    }
    Ok(emitted)
}

/// Validate and add one provider-submitted path spec.
///
/// A structurally unsupported ordered alternative is skipped independently;
/// other alternatives from the same builder remain eligible.
unsafe fn add_path_spec<P: FdwScan>(
    root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    relation: &ForeignRelContext<'_>,
    provider_state: &P::PlannerState,
    kind: PathVariantKind,
    required_outer: pg_sys::Relids,
    filters: &PathFilterSet,
    context: &ForeignPathContext<'_>,
    spec: ForeignPathSpec<P::PrivateData>,
) -> Result<bool, ForeignScanError> {
    let Some(mut pathkeys) = unsafe {
        ForeignPathKeys::analyze(root, relation.baserel(), spec.pathkeys_ptr())
    }?
    else {
        return Ok(false);
    };
    if !pathkeys.is_empty() {
        // PostgreSQL ignores parameterized pathkeys when comparing paths, so a
        // join-parameterized path must not claim an ordering. A Plain path can
        // still have ParamPathInfo when it carries only lateral dependencies.
        if kind == PathVariantKind::JoinParameterized {
            return Ok(false);
        }
        if !P::supports_pathkeys(provider_state, context, &mut pathkeys)? {
            return Ok(false);
        }
    }
    let pathkeys_ptr = if pathkeys.is_empty() {
        ptr::null_mut()
    } else {
        spec.pathkeys_ptr()
    };
    let (startup_cost, total_cost) =
        finalize_path_spec(root, baserel, filters, &spec)?;
    let private = encode_path_private::<P>(P::NAME, kind, &spec.private_data)?;
    let path = unsafe {
        pg::create_foreign_path(
            root,
            baserel,
            spec.rows,
            startup_cost,
            total_cost,
            pathkeys_ptr,
            required_outer,
            private,
        )
    };
    if path.is_null() {
        return Err(ForeignScanError::framework(
            "PostgreSQL returned NULL from create_foreignscan_path",
        ));
    }
    unsafe { pg_sys::add_path(baserel, path.cast()) };
    Ok(true)
}

fn finalize_path_spec<D>(
    root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    filters: &PathFilterSet,
    spec: &ForeignPathSpec<D>,
) -> Result<(f64, f64), ForeignScanError> {
    if !spec.rows.is_finite()
        || spec.rows < 0.0
        || !spec.retrieved_rows.is_finite()
        || spec.retrieved_rows < 0.0
        || !spec.provider_startup_cost.is_finite()
        || spec.provider_startup_cost < 0.0
        || !spec.provider_total_cost.is_finite()
        || spec.provider_total_cost < 0.0
    {
        return Err(ForeignScanError::framework(
            "FDW path estimate contains invalid rows or costs",
        ));
    }
    // PostgreSQL clamps output row estimates to at least one row, while a
    // provider's retrieved-row estimate may come from an independently
    // rounded remote estimate. Every estimated output row still requires a
    // materialized scan tuple, so use the output estimate as the lower bound.
    let materialized_rows = spec.retrieved_rows.max(spec.rows);
    let residual_quals = unsafe { expr_list_from_ptrs(&filters.residual) }?;
    let reltarget = unsafe { (*baserel).reltarget };
    let local_cost = unsafe {
        pg::ForeignScanLocalCost::estimate(
            root,
            residual_quals,
            reltarget,
            materialized_rows,
            spec.rows,
        )
    }?;
    let startup_cost = spec.provider_startup_cost + local_cost.startup;
    let total_cost = spec.provider_total_cost
        + local_cost.total
        + pg::foreignscan_tuple_cost(materialized_rows);
    if !startup_cost.is_finite()
        || startup_cost < 0.0
        || !total_cost.is_finite()
        || total_cost < 0.0
    {
        return Err(ForeignScanError::framework(
            "FDW path estimate overflows after framework tuple cost",
        ));
    }
    Ok((startup_cost, total_cost))
}

/// # Safety
///
/// Every expression pointer in `exprs` must be a non-NULL live planner node
/// valid for the duration of the current planner callback. The returned list
/// is allocated in the current PostgreSQL memory context.
pub(super) unsafe fn expr_list_from_ptrs(
    exprs: &[*mut pg_sys::Expr],
) -> Result<*mut pg_sys::List, ForeignScanError> {
    let mut list = ptr::null_mut();
    for &expr in exprs {
        if expr.is_null() {
            return Err(ForeignScanError::framework(
                "FDW planner produced a NULL expression node",
            ));
        }
        list = unsafe { pg_sys::lappend(list, expr.cast::<c_void>()) };
    }
    Ok(list)
}
