//! Path-stage `CustomPath` construction.

use core::ptr;

use pgrx::pg_sys;

use crate::customscan::ScanPurpose;
use crate::customscan::error::CustomScanError;
use crate::customscan::gucs;
use crate::customscan::plan_data::path_private::encode_path_private;
use crate::customscan::provider::{
    CustomPathBuilder, CustomScanPrivate, LagodbCustomScanProvider, PathContext,
    PathVariant, PathVariantKind, PrivateDataWriter, method_tables_for,
};

use crate::expr::pushdown::{
    FilterPlanSummary, PathFilterEstimates, PathFilterSet, RelationFilterEstimator,
};

/// Context for [`emit_custom_path`].
pub(crate) struct EmitCustomPathContext<'a> {
    pub(crate) root: *mut pg_sys::PlannerInfo,
    pub(crate) baserel: *mut pg_sys::RelOptInfo,
    pub(crate) purpose: ScanPurpose,
    pub(crate) kind: PathVariantKind,
    pub(crate) required_outer: pg_sys::Relids,
    /// Path-stage planned-filter hint; plan-stage recomputes authoritatively.
    pub(crate) filters: &'a PathFilterSet,
    /// The relation's statistics-pruning units, from its filter planner.
    pub(crate) pruning_units: Option<f64>,
}

/// Build and register a `CustomPath` for one variant via `add_path`.
///
/// # Safety
///
/// Planner pointers in `ctx` must be live in the per-query memory context.
/// `ctx.baserel->relid` must identify a non-NULL `RangeTblEntry` in
/// `ctx.root->parse->rtable` from the same planning invocation.
pub(crate) unsafe fn emit_custom_path<P: LagodbCustomScanProvider>(
    ctx: &EmitCustomPathContext<'_>,
) -> Result<bool, CustomScanError> {
    // `bms_membership` (not exported `bms_is_empty`) treats NULL as empty.
    let required_outer_is_empty = unsafe {
        pg_sys::bms_membership(ctx.required_outer)
            == pg_sys::BMS_Membership::BMS_EMPTY_SET
    };
    let param_info: *mut pg_sys::ParamPathInfo = if required_outer_is_empty {
        ptr::null_mut()
    } else {
        unsafe {
            pg_sys::get_baserel_parampathinfo(
                ctx.root,
                ctx.baserel,
                ctx.required_outer,
            )
        }
    };

    // SAFETY: `emit_custom_path` receives live planner nodes, and
    // `resolve_rte` returns the live RTE for `ctx.baserel` under its contract.
    let path_ctx = unsafe {
        PathContext::from_refs(
            &*resolve_rte(ctx.root, ctx.baserel),
            &*ctx.root,
            &*ctx.baserel,
        )
    };
    let pushdown = FilterPlanSummary::from_path_set(ctx.filters);
    let filters = PathFilterEstimates::new(
        &ctx.filters.planned,
        // SAFETY: the same live planner state owns `ctx.baserel`, and planned
        // filters are its planner-owned quals.
        unsafe { RelationFilterEstimator::new(ctx.root, (*ctx.baserel).relid) },
        ctx.pruning_units,
    );
    let variant = PathVariant {
        purpose: ctx.purpose,
        kind: ctx.kind,
        param_info: if param_info.is_null() {
            None
        } else {
            Some(unsafe { &*param_info })
        },
        required_outer: ctx.required_outer,
        pushdown,
        filters,
    };
    let plan =
        match P::create_path(&path_ctx, &variant, CustomPathBuilder::<P>::new()) {
            Some(plan) => plan,
            None => return Ok(false), // provider declined this variant
        };

    let cpath_ptr = unsafe {
        pg_sys::palloc0(core::mem::size_of::<pg_sys::CustomPath>())
            as *mut pg_sys::CustomPath
    };

    unsafe {
        let path = &mut (*cpath_ptr).path;
        path.type_ = pg_sys::NodeTag::T_CustomPath;
        path.pathtype = pg_sys::NodeTag::T_CustomScan;
        path.parent = ctx.baserel;
        let base_target = (*ctx.baserel).reltarget;
        if plan.extra_tuple_width == 0 {
            path.pathtarget = base_target;
        } else {
            let widened = pg_sys::palloc0(core::mem::size_of::<pg_sys::PathTarget>())
                .cast::<pg_sys::PathTarget>();
            core::ptr::copy_nonoverlapping(base_target, widened, 1);
            (*widened).width =
                (*widened).width.saturating_add(plan.extra_tuple_width);
            path.pathtarget = widened;
        }
        path.param_info = param_info;
        let native_parallel_capable = P::NATIVE_PARALLEL
            && (*ctx.baserel).consider_parallel
            && ctx.purpose == ScanPurpose::Read;
        path.parallel_aware = false;
        path.parallel_safe = native_parallel_capable && plan.parallel_safe_complete;
        path.parallel_workers = 0;
        path.rows = if param_info.is_null() {
            (*ctx.baserel).rows
        } else {
            (*param_info).ppi_rows
        };
        path.pathkeys = ptr::null_mut();

        let (startup_cost, total_cost) =
            plan.cost
                .costs(ctx.root, ctx.baserel, path.rows, &ctx.filters.residual);
        // `customscan_mode = force`: override published costs to (0, 1) after baseline compute.
        let (startup_cost, total_cost) = if gucs::force_mode() {
            let _ = (startup_cost, total_cost);
            (0.0_f64, 1.0_f64)
        } else {
            (startup_cost, total_cost)
        };
        path.startup_cost = startup_cost;
        path.total_cost = total_cost;

        (*cpath_ptr).flags = pg_sys::CUSTOMPATH_SUPPORT_PROJECTION;
        (*cpath_ptr).custom_paths = ptr::null_mut();
        #[cfg(any(feature = "pg17", feature = "pg18"))]
        {
            (*cpath_ptr).custom_restrictinfo = ptr::null_mut();
        }

        let provider_metadata = PrivateDataWriter::encode_list(|writer| {
            plan.private_data.encode(writer)
        })?;
        (*cpath_ptr).custom_private = encode_path_private(
            ctx.purpose,
            ctx.purpose.is_modify_target() && path_ctx.modify_requests_wholerow(),
            provider_metadata,
        )?;

        (*cpath_ptr).methods = method_tables_for::<P>().path();

        let partial = if native_parallel_capable
            && plan.native_parallel_partial
            && required_outer_is_empty
        {
            let workers = pg_sys::compute_parallel_worker(
                ctx.baserel,
                plan.cost.scanned_pages(ctx.baserel),
                -1.0,
                pg_sys::max_parallel_workers_per_gather,
            );
            if workers > 0 {
                let partial =
                    pg_sys::palloc0(core::mem::size_of::<pg_sys::CustomPath>())
                        .cast::<pg_sys::CustomPath>();
                core::ptr::copy_nonoverlapping(cpath_ptr, partial, 1);
                let path = &mut (*partial).path;
                path.parallel_aware = true;
                path.parallel_safe = true;
                path.parallel_workers = workers;
                let mut divisor = workers as f64;
                if pg_sys::parallel_leader_participation {
                    divisor += (1.0 - 0.3 * workers as f64).max(0.0);
                }
                path.rows /= divisor;
                path.total_cost = path.startup_cost
                    + (path.total_cost - path.startup_cost) / divisor;
                Some(partial)
            } else {
                None
            }
        } else {
            None
        };

        // Both variants must be fully constructed before ownership of either
        // path is transferred. PostgreSQL may free a dominated path inside
        // add_path/add_partial_path.
        pg_sys::add_path(ctx.baserel, &mut (*cpath_ptr).path as *mut pg_sys::Path);
        if let Some(partial) = partial {
            pg_sys::add_partial_path(ctx.baserel, &mut (*partial).path);
        }
    }
    Ok(true)
}

/// `rt_fetch(rel->relid, root->parse->rtable)`.
///
/// # Safety
///
/// `root` and `rel` must be live planner nodes from the same planning
/// invocation. `root->parse->rtable` must be a valid list, and `rel->relid`
/// must identify a non-NULL `RangeTblEntry` in that list.
unsafe fn resolve_rte(
    root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
) -> *mut pg_sys::RangeTblEntry {
    let parse = unsafe { (*root).parse };
    let rtable = unsafe { (*parse).rtable };
    let relid = unsafe { (*rel).relid };
    unsafe {
        pg_sys::list_nth(rtable, (relid - 1) as core::ffi::c_int)
            as *mut pg_sys::RangeTblEntry
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::customscan::provider::CustomPathPlan;
    use core::ffi::CStr;

    use crate::customscan::provider::{
        BeginContext, CreateStateContext, CustomScanError, EndContext,
        NextSlotContext, NextSlotResult, PathContext, ReScanContext, RelationContext,
        StartContext,
    };
    use crate::customscan::provider::{CustomScanPrivate, PrivateDataReader};
    use crate::expr::RuntimeValueBindings;
    use crate::expr::pushdown::{
        FilterBindResult, FilterPlan, FilterPlanningContext, FilterPushdown,
        FilterPushdownPlanner, PredicateFragment,
    };
    use crate::plan_data::{PlanDataReader, PlanDataWriter};

    struct TestPrivate;

    impl CustomScanPrivate for TestPrivate {
        fn encode(
            &self,
            _writer: &mut PrivateDataWriter,
        ) -> Result<(), CustomScanError> {
            Ok(())
        }

        fn decode(
            _reader: &mut PrivateDataReader<'_>,
        ) -> Result<Self, CustomScanError> {
            Ok(TestPrivate)
        }
    }

    struct TestProviderA;

    struct RejectAllPlanner;

    impl FilterPushdownPlanner for RejectAllPlanner {
        type PlannedPredicate = ();
        type Error = CustomScanError;

        fn try_plan_filter(
            &mut self,
            _fragment: &PredicateFragment,
        ) -> Result<FilterPlan<Self::PlannedPredicate>, Self::Error> {
            Ok(FilterPlan::Unsupported)
        }
    }

    macro_rules! impl_test_provider {
        ($ty:ty, $name:expr) => {
            impl FilterPushdown for $ty {
                type Planner = RejectAllPlanner;
                type PlannedPredicate = ();
                type BoundPredicate = ();
                type Error = CustomScanError;

                fn begin_filter_planning(
                    _context: &FilterPlanningContext,
                ) -> Result<Self::Planner, Self::Error> {
                    Ok(RejectAllPlanner)
                }

                fn encode_planned(
                    _predicate: &Self::PlannedPredicate,
                    _writer: &mut PlanDataWriter,
                ) -> Result<(), Self::Error> {
                    Ok(())
                }

                fn decode_planned(
                    _reader: &mut PlanDataReader<'_>,
                    _binding_count: usize,
                ) -> Result<Self::PlannedPredicate, Self::Error> {
                    Ok(())
                }

                fn bind_filter(
                    _predicate: &Self::PlannedPredicate,
                    _values: RuntimeValueBindings<'_>,
                ) -> Result<FilterBindResult<Self::BoundPredicate>, Self::Error> {
                    Ok(FilterBindResult::ValueNotRepresentable)
                }
            }

            impl LagodbCustomScanProvider for $ty {
                const NAME: &'static CStr = $name;
                type PrivateData = TestPrivate;
                type State = ();

                fn supports_relation(_ctx: &RelationContext<'_>) -> bool {
                    false
                }

                fn create_path(
                    _ctx: &PathContext<'_>,
                    _variant: &PathVariant<'_>,
                    _builder: CustomPathBuilder<Self>,
                ) -> Option<CustomPathPlan<Self>> {
                    None
                }

                fn create_state(_ctx: CreateStateContext<Self>) -> Self::State {
                    unreachable!(
                        "custom scan execution lifecycle is not exercised in builder tests"
                    )
                }

                fn begin(
                    _ctx: BeginContext<'_, Self>,
                ) -> Result<(), CustomScanError> {
                    unreachable!(
                        "custom scan execution lifecycle is not exercised in builder tests"
                    )
                }

                fn start(
                    _ctx: StartContext<'_, Self>,
                ) -> Result<(), CustomScanError> {
                    unreachable!(
                        "custom scan execution lifecycle is not exercised in builder tests"
                    )
                }

                fn next_slot<'a>(
                    _ctx: NextSlotContext<'a, Self>,
                ) -> Result<NextSlotResult<'a>, CustomScanError> {
                    unreachable!(
                        "custom scan execution lifecycle is not exercised in builder tests"
                    )
                }

                fn rescan(
                    _ctx: ReScanContext<'_, Self>,
                ) -> Result<(), CustomScanError> {
                    unreachable!(
                        "custom scan execution lifecycle is not exercised in builder tests"
                    )
                }

                fn end(_ctx: EndContext<'_, Self>) -> Result<(), CustomScanError> {
                    unreachable!(
                        "custom scan execution lifecycle is not exercised in builder tests"
                    )
                }
            }
        };
    }

    impl_test_provider!(TestProviderA, c"test-builder-provider-a");

    /// Builder cost overrides default to `None`, so path emission substitutes
    /// the relation's PostgreSQL estimates.
    #[test]
    fn custom_path_builder_defaults_are_none() {
        let plan = CustomPathBuilder::<TestProviderA>::new().build(TestPrivate);
        assert!(plan.cost.scanned_pages.is_none());
        assert!(plan.cost.returned_tuples.is_none());
        assert!(plan.cost.provider_run_cost.is_none());
        assert!(plan.cost.extra_startup_cost.is_none());
    }

    /// The setters round-trip into the produced [`CustomPathPlan`].
    #[test]
    fn custom_path_builder_setters_round_trip() {
        let plan = CustomPathBuilder::<TestProviderA>::new()
            .scanned_pages(42.0)
            .returned_tuples(100.0)
            .provider_run_cost(7.0)
            .extra_startup_cost(1.5)
            .build(TestPrivate);
        assert_eq!(plan.cost.scanned_pages, Some(42.0));
        assert_eq!(plan.cost.returned_tuples, Some(100.0));
        assert_eq!(plan.cost.provider_run_cost, Some(7.0));
        assert_eq!(plan.cost.extra_startup_cost, Some(1.5));
    }
}
