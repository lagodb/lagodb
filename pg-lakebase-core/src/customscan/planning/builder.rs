//! Planning-side `CustomPath` / `CustomScan` builders: [`CustomPathBuilder`],
//! [`emit_custom_path`], and the `PlanCustomPath` trampoline.
//!
//! Path-stage `custom_private` wraps scan purpose plus provider metadata;
//! plan-stage [`plan_custom_path_trampoline`] re-splits clauses and wraps it via
//! [`encode_split`].

use core::ffi::c_void;
use core::ptr;

use pgrx::pg_guard;
use pgrx::pg_sys;

use crate::customscan::ScanPurpose;
use crate::customscan::error::CustomScanError;
use crate::customscan::gucs;
use crate::customscan::plan_data::custom_private::{
    decode_path_private, encode_path_private, encode_split_with_layout,
};
use crate::customscan::provider::{
    CustomPathBuilder, CustomScanPrivate, LakebaseCustomScanProvider, PathContext,
    PathPushdownSummary, PathVariant, PathVariantKind, PlanTranslateContext,
    PrivateDataWriter, method_tables_for,
};

use crate::diag::ReportableError;
use crate::expr::contract::{PushdownContract, QualPushdownDecision};
use crate::expr::predicate::PlanPredicate;
use crate::expr::relation::PlanRelationResolver;
use crate::expr::split::{PlanPushdownSplit, PlanPushdownSplitter, ScanClauseSource};

use super::tuple_planner::{BaseScanTuplePlanner, PlannedScanTuple};

/// `PlanCustomPath`: re-split `scan_clauses`, build `custom_exprs` / `plan.qual`,
/// encode `custom_private`, and assemble the `CustomScan` node.
///
/// # Safety
///
/// Called from `create_customscan_plan`; all planner pointers are live in the
/// per-query memory context. `clauses` is `List<RestrictInfo>` (not extracted).
#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn plan_custom_path_trampoline<
    P: LakebaseCustomScanProvider,
>(
    root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    best_path: *mut pg_sys::CustomPath,
    tlist: *mut pg_sys::List,
    clauses: *mut pg_sys::List,
    custom_plans: *mut pg_sys::List,
) -> *mut pg_sys::Plan {
    let path_private =
        unsafe { decode_path_private((*best_path).custom_private) }.report_unwrap();
    let provider_metadata = path_private.provider_metadata;
    let final_clause_sources = unsafe { FinalScanClauseSources::new(rel) };

    let translate_ctx = unsafe { PlanTranslateContext::new(rel) };
    let mut classify_leaf = |predicate: &PlanPredicate| -> QualPushdownDecision {
        P::classify_predicate(&translate_ctx, predicate)
    };
    let split = unsafe {
        let mut splitter = PlanPushdownSplitter::new(
            root,
            rel,
            clauses,
            ScanClauseSource::BaseRestriction,
            &mut classify_leaf,
        );
        splitter.split_with_source(|rinfo| final_clause_sources.source_for(rinfo))
    };

    let pushed_count = split.pushed.len();
    let recheck_count = split.recheck.len();
    let pushed_contracts: Vec<PushdownContract> = split.pushed_contracts().collect();
    let custom_exprs = unsafe {
        let mut list: *mut pg_sys::List = ptr::null_mut();
        for p in split.pushed_exprs() {
            list = pg_sys::lappend(list, p.cast::<c_void>());
        }
        for &p in &split.recheck {
            list = pg_sys::lappend(list, p.cast::<c_void>());
        }
        list
    };

    let plan_qual = unsafe {
        let mut list: *mut pg_sys::List = ptr::null_mut();
        for &p in &split.residual {
            list = pg_sys::lappend(list, p.cast::<c_void>());
        }
        list
    };

    let relation_oid =
        unsafe { PlanRelationResolver::new(root).rel_oid((*rel).relid) };
    let tuple_planner =
        BaseScanTuplePlanner::new(unsafe { (*rel).relid }, relation_oid);
    let scan_tuple = if path_private.requires_wholerow {
        PlannedScanTuple::relation()
    } else if path_private.purpose.is_modify() {
        unsafe {
            tuple_planner.plan_relation_scan(
                tlist,
                (*(*best_path).path.pathtarget).exprs,
                plan_qual,
                custom_exprs,
            )
        }
    } else {
        unsafe {
            tuple_planner.plan(
                tlist,
                (*(*best_path).path.pathtarget).exprs,
                plan_qual,
                custom_exprs,
            )
        }
    };
    let custom_private = unsafe {
        encode_split_with_layout(
            P::NAME,
            path_private.purpose,
            relation_oid,
            pushed_count,
            recheck_count,
            &pushed_contracts,
            &split.column_refs,
            provider_metadata,
            &scan_tuple.layout,
        )
    }
    .map_err(CustomScanError::encode_custom_private)
    .report_unwrap();

    let cscan = unsafe {
        pg_sys::palloc0(core::mem::size_of::<pg_sys::CustomScan>())
            as *mut pg_sys::CustomScan
    };

    unsafe {
        let scan = &mut (*cscan).scan;
        let plan = &mut scan.plan;

        plan.type_ = pg_sys::NodeTag::T_CustomScan;
        // Mirror `copy_generic_path_info` (not exposed in pg_sys).
        let path_ptr = best_path as *mut pg_sys::Path;
        plan.startup_cost = (*path_ptr).startup_cost;
        plan.total_cost = (*path_ptr).total_cost;
        plan.plan_rows = (*path_ptr).rows;
        plan.plan_width = (*(*path_ptr).pathtarget).width;
        plan.parallel_aware = (*path_ptr).parallel_aware;
        plan.parallel_safe = (*path_ptr).parallel_safe;
        plan.async_capable = false;
        plan.plan_node_id = 0;
        plan.targetlist = tlist;
        plan.qual = plan_qual;
        plan.lefttree = ptr::null_mut();
        plan.righttree = ptr::null_mut();
        plan.initPlan = ptr::null_mut();
        plan.extParam = ptr::null_mut();
        plan.allParam = ptr::null_mut();

        scan.scanrelid = (*rel).relid;

        (*cscan).flags = (*best_path).flags;
        (*cscan).custom_plans = custom_plans;
        (*cscan).custom_exprs = custom_exprs;
        (*cscan).custom_private = custom_private;
        (*cscan).custom_scan_tlist = scan_tuple.custom_scan_tlist;
        (*cscan).custom_relids =
            pg_sys::bms_make_singleton((*rel).relid as core::ffi::c_int);
        (*cscan).methods = method_tables_for::<P>().scan();
    }

    cscan as *mut pg_sys::Plan
}

/// Source resolver for the final ordered `scan_clauses` passed to
/// `PlanCustomPath`.
///
/// PostgreSQL builds a base scan's final clauses from `baserestrictinfo` plus
/// parameterized-path `ppi_clauses`. Like `postgres_fdw`, we recover the base
/// clauses by pointer membership; anything else is a join/PPI clause and must
/// pass the movability gate before exact pushdown can remove it from
/// `plan.qual`.
#[derive(Debug, Clone, Copy)]
struct FinalScanClauseSources {
    baserestrictinfo: *mut pg_sys::List,
}

impl FinalScanClauseSources {
    /// # Safety
    ///
    /// `rel` must be a live planner-owned node from the current
    /// `PlanCustomPath` call.
    unsafe fn new(rel: *mut pg_sys::RelOptInfo) -> Self {
        Self {
            baserestrictinfo: unsafe { (*rel).baserestrictinfo },
        }
    }

    fn source_for(self, rinfo: *mut pg_sys::RestrictInfo) -> ScanClauseSource {
        if unsafe {
            pg_sys::list_member_ptr(self.baserestrictinfo, rinfo.cast::<c_void>())
        } {
            ScanClauseSource::BaseRestriction
        } else {
            ScanClauseSource::Movable
        }
    }
}

/// Forwards to [`P::reparameterize_private_data`] when a CustomPath is pushed under an appendrel child.
///
/// # Safety
///
/// Called from `reparameterize_path_by_child`; planner pointers are live.
#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn reparameterize_custom_path_by_child_trampoline<
    P: LakebaseCustomScanProvider,
>(
    root: *mut pg_sys::PlannerInfo,
    custom_private: *mut pg_sys::List,
    child_rel: *mut pg_sys::RelOptInfo,
) -> *mut pg_sys::List {
    let path_private = unsafe { decode_path_private(custom_private) }.report_unwrap();
    let provider_metadata = unsafe {
        P::reparameterize_private_data(
            root,
            path_private.provider_metadata,
            child_rel,
        )
    };
    unsafe {
        encode_path_private(
            path_private.purpose,
            path_private.requires_wholerow,
            provider_metadata,
        )
    }
}

/// Context for [`emit_custom_path`].
pub struct EmitCustomPathContext<'a> {
    pub root: *mut pg_sys::PlannerInfo,
    pub baserel: *mut pg_sys::RelOptInfo,
    pub purpose: ScanPurpose,
    pub kind: PathVariantKind,
    pub required_outer: pg_sys::Relids,
    /// Path-stage split hint; plan-stage recomputes authoritatively.
    pub split: &'a PlanPushdownSplit,
}

/// Build and register a `CustomPath` for one variant via `add_path`.
///
/// # Safety
///
/// Planner pointers in `ctx` must be live in the per-query memory context.
/// `ctx.baserel->relid` must identify a non-NULL `RangeTblEntry` in
/// `ctx.root->parse->rtable` from the same planning invocation.
pub unsafe fn emit_custom_path<P: LakebaseCustomScanProvider>(
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
    let costed_pushed: Vec<_> = ctx.split.costed_pruning_exprs().collect();
    let pushdown = PathPushdownSummary::from_split(
        ctx.split,
        path_ctx.clauselist_selectivity_for_exprs(&costed_pushed),
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
        path.parallel_aware = false;
        path.parallel_safe = false;
        path.parallel_workers = 0;
        path.rows = if param_info.is_null() {
            (*ctx.baserel).rows
        } else {
            (*param_info).ppi_rows
        };
        path.pathkeys = ptr::null_mut();

        let (startup_cost, total_cost) = compute_costs(
            ctx.root,
            ctx.baserel,
            path.rows,
            plan.scanned_pages
                .unwrap_or_else(|| (*ctx.baserel).pages as f64),
            plan.scanned_tuples.unwrap_or_else(|| (*ctx.baserel).tuples),
            plan.extra_startup_cost.unwrap_or(0.0),
            &ctx.split.residual,
        );
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
        (*cpath_ptr).custom_restrictinfo = ptr::null_mut();

        let mut writer = PrivateDataWriter::new();
        let provider_metadata = plan
            .private_data
            .encode(&mut writer)
            .and_then(|()| writer.finish())
            .map_err(CustomScanError::encode_custom_private)?;
        (*cpath_ptr).custom_private = encode_path_private(
            ctx.purpose,
            ctx.purpose.is_modify() && path_ctx.modify_requests_wholerow(),
            provider_metadata,
        );

        (*cpath_ptr).methods = method_tables_for::<P>().path();

        pg_sys::add_path(ctx.baserel, &mut (*cpath_ptr).path as *mut pg_sys::Path);
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

/// CustomPath cost: disk + residual `cost_qual_eval` per tuple + projection.
///
/// # Safety
///
/// Live planner pointers; each `residual` entry is NULL or a live `Expr`/`RestrictInfo`.
unsafe fn compute_costs(
    root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    path_rows: f64,
    scanned_pages: f64,
    scanned_tuples: f64,
    extra_startup_cost: f64,
    residual: &[*mut pg_sys::Expr],
) -> (f64, f64) {
    let pathtarget = unsafe { (*baserel).reltarget };
    let target_startup = unsafe { (*pathtarget).cost.startup };
    let target_per_tuple = unsafe { (*pathtarget).cost.per_tuple };

    let residual_list: *mut pg_sys::List = unsafe {
        let mut out: *mut pg_sys::List = ptr::null_mut();
        for &cell in residual {
            if cell.is_null() {
                continue;
            }
            out = pg_sys::lappend(out, cell.cast::<core::ffi::c_void>());
        }
        out
    };

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
    let per_tuple_cpu = (cpu_tuple_cost + qpqual_cost.per_tuple) * scanned_tuples;
    let projection_per_row = target_per_tuple * path_rows;

    let startup_cost = target_startup + qpqual_cost.startup + extra_startup_cost;
    let total_cost = startup_cost + disk_cost + per_tuple_cpu + projection_per_row;

    (startup_cost, total_cost)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::customscan::provider::CustomPathPlan;
    use core::ffi::CStr;

    use crate::customscan::provider::{
        BeginContext, CreateStateContext, CustomScanError, EndContext,
        NextSlotContext, PathContext, PlanTranslateContext, ReScanContext,
        RelationContext,
    };
    use crate::customscan::provider::{CustomScanPrivate, PrivateDataReader};
    use crate::expr::contract::QualPushdownDecision;

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

    macro_rules! impl_test_provider {
        ($ty:ty, $name:expr) => {
            impl LakebaseCustomScanProvider for $ty {
                const NAME: &'static CStr = $name;
                type PrivateData = TestPrivate;
                type State = ();

                fn supports_relation(_ctx: &RelationContext<'_>) -> bool {
                    false
                }

                fn classify_predicate(
                    _ctx: &PlanTranslateContext,
                    _predicate: &PlanPredicate,
                ) -> QualPushdownDecision {
                    QualPushdownDecision::Unsupported
                }

                fn create_path(
                    _ctx: &PathContext<'_>,
                    _variant: &PathVariant<'_>,
                    _builder: CustomPathBuilder<Self>,
                ) -> Option<CustomPathPlan<Self>> {
                    None
                }

                fn create_state(_ctx: CreateStateContext<Self>) -> Self::State {
                    unreachable!()
                }

                fn begin(
                    _ctx: BeginContext<'_, Self>,
                ) -> Result<(), CustomScanError> {
                    unreachable!()
                }

                fn next_slot(
                    _ctx: NextSlotContext<'_, Self>,
                ) -> Result<bool, CustomScanError> {
                    unreachable!()
                }

                fn rescan(
                    _ctx: ReScanContext<'_, Self>,
                ) -> Result<(), CustomScanError> {
                    unreachable!()
                }

                fn end(_ctx: EndContext<'_, Self>) -> Result<(), CustomScanError> {
                    unreachable!()
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
        assert!(plan.scanned_pages.is_none());
        assert!(plan.scanned_tuples.is_none());
        assert!(plan.extra_startup_cost.is_none());
    }

    /// The setters round-trip into the produced [`CustomPathPlan`].
    #[test]
    fn custom_path_builder_setters_round_trip() {
        let plan = CustomPathBuilder::<TestProviderA>::new()
            .scanned_pages(42.0)
            .scanned_tuples(100.0)
            .extra_startup_cost(1.5)
            .build(TestPrivate);
        assert_eq!(plan.scanned_pages, Some(42.0));
        assert_eq!(plan.scanned_tuples, Some(100.0));
        assert_eq!(plan.extra_startup_cost, Some(1.5));
    }
}
