//! `CustomPath` / `CustomScan` plan-tree builders: [`CustomPathBuilder`],
//! [`emit_custom_path`], and the `PlanCustomPath` trampoline.
//!
//! Path-stage `custom_private` is a provider-encoded `List*`; plan-stage
//! [`plan_custom_path_trampoline`] re-splits clauses and wraps it via
//! [`encode_split`].

use core::any::TypeId;
use core::ffi::c_void;
use core::marker::PhantomData;
use core::ptr;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use pgrx::pg_guard;
use pgrx::pg_sys;

use crate::customscan::codec::PrivateDataWriter;
use crate::customscan::custom_private::{
    CustomScanPrivate, encode_split_with_layout,
};
use crate::customscan::error::CustomScanError;
use crate::customscan::provider::{
    LakebaseCustomScanProvider, PathPushdownSummary, PathVariant, PathVariantKind,
    PlanTranslateContext,
};
use crate::customscan::state::scan_methods_for;
use crate::customscan::tuple_layout::BaseScanTuplePlanner;
use crate::diag::ReportableError;
use crate::expr::predicate::PlanPredicate;
use crate::expr::split::{
    PlanPushdownSplit, PlanPushdownSplitter, PlanRelationResolver, PushdownContract,
    QualPushdownDecision, ScanClauseSource,
};

/// Builder typestate: provider `PrivateData` not yet supplied.
pub struct NeedsPrivate;

/// Builder typestate: provider `PrivateData` supplied (or ZST escape hatch).
pub struct HasPrivate;

mod sealed {
    pub trait Sealed {}
}

/// Sealed marker for zero-sized `PrivateData`; allows `build` without `private_data`.
pub trait ZeroSizedPrivate: sealed::Sealed {}

/// Typed builder for [`LakebaseCustomScanProvider::create_path`]: cost overrides
/// and provider-private metadata. `path.rows` is set by the framework, not here.
pub struct CustomPathBuilder<P: LakebaseCustomScanProvider, State = NeedsPrivate> {
    scanned_pages: Option<f64>,
    scanned_tuples: Option<f64>,
    extra_startup_cost: Option<f64>,
    private_data: Option<P::PrivateData>,
    _state: PhantomData<State>,
    _marker: PhantomData<fn() -> P>,
}

impl<P: LakebaseCustomScanProvider, State> CustomPathBuilder<P, State> {
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
}

impl<P: LakebaseCustomScanProvider> CustomPathBuilder<P, NeedsPrivate> {
    fn new() -> Self {
        Self {
            scanned_pages: None,
            scanned_tuples: None,
            extra_startup_cost: None,
            private_data: None,
            _state: PhantomData,
            _marker: PhantomData,
        }
    }

    /// Attach provider-private metadata; transitions to [`HasPrivate`].
    pub fn private_data(
        self,
        data: P::PrivateData,
    ) -> CustomPathBuilder<P, HasPrivate> {
        CustomPathBuilder {
            scanned_pages: self.scanned_pages,
            scanned_tuples: self.scanned_tuples,
            extra_startup_cost: self.extra_startup_cost,
            private_data: Some(data),
            _state: PhantomData,
            _marker: PhantomData,
        }
    }
}

impl<P: LakebaseCustomScanProvider> CustomPathBuilder<P, HasPrivate> {
    pub fn build(self) -> CustomPathPlan<P> {
        CustomPathPlan {
            scanned_pages: self.scanned_pages,
            scanned_tuples: self.scanned_tuples,
            extra_startup_cost: self.extra_startup_cost,
            private_data: self.private_data,
            _marker: PhantomData,
        }
    }
}

impl<P> CustomPathBuilder<P, NeedsPrivate>
where
    P: LakebaseCustomScanProvider,
    P::PrivateData: ZeroSizedPrivate,
{
    /// ZST escape hatch: build without `private_data`.
    pub fn build(self) -> CustomPathPlan<P> {
        CustomPathPlan {
            scanned_pages: self.scanned_pages,
            scanned_tuples: self.scanned_tuples,
            extra_startup_cost: self.extra_startup_cost,
            private_data: None,
            _marker: PhantomData,
        }
    }
}

/// Output of `create_path`; consumed by [`emit_custom_path`].
pub struct CustomPathPlan<P: LakebaseCustomScanProvider> {
    pub(crate) scanned_pages: Option<f64>,
    pub(crate) scanned_tuples: Option<f64>,
    pub(crate) extra_startup_cost: Option<f64>,
    pub(crate) private_data: Option<P::PrivateData>,
    _marker: PhantomData<fn() -> P>,
}

/// `Send + Sync` shim for a leaked `CustomPathMethods` table (`CustomName` is `!Sync`).
#[derive(Clone, Copy)]
struct PathMethodsRef(&'static pg_sys::CustomPathMethods);

// SAFETY: `CustomName` points at immutable `'static` bytes for the process lifetime.
unsafe impl Send for PathMethodsRef {}
unsafe impl Sync for PathMethodsRef {}

static PATH_METHODS_CACHE: OnceLock<Mutex<HashMap<TypeId, PathMethodsRef>>> =
    OnceLock::new();

/// Cached per-provider `CustomPathMethods` (leaked once per process).
pub fn path_methods_for<P: LakebaseCustomScanProvider>()
-> &'static pg_sys::CustomPathMethods {
    let cache = PATH_METHODS_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = cache
        .lock()
        .expect("customscan path_methods_for: cache mutex poisoned");

    let key = TypeId::of::<P>();
    if let Some(&entry) = guard.get(&key) {
        return entry.0;
    }

    let methods = pg_sys::CustomPathMethods {
        // SAFETY: `P::NAME` is `&'static CStr`.
        CustomName: P::NAME.as_ptr(),
        PlanCustomPath: Some(plan_custom_path_trampoline::<P>),
        ReparameterizeCustomPathByChild: Some(
            reparameterize_custom_path_by_child_trampoline::<P>,
        ),
    };

    let leaked: &'static pg_sys::CustomPathMethods = Box::leak(Box::new(methods));
    guard.insert(key, PathMethodsRef(leaked));
    leaked
}

/// `PlanCustomPath`: re-split `scan_clauses`, build `custom_exprs` / `plan.qual`,
/// encode `custom_private`, and assemble the `CustomScan` node.
///
/// # Safety
///
/// Called from `create_customscan_plan`; all planner pointers are live in the
/// per-query memory context. `clauses` is `List<RestrictInfo>` (not extracted).
#[pg_guard]
unsafe extern "C-unwind" fn plan_custom_path_trampoline<
    P: LakebaseCustomScanProvider,
>(
    root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    best_path: *mut pg_sys::CustomPath,
    tlist: *mut pg_sys::List,
    clauses: *mut pg_sys::List,
    custom_plans: *mut pg_sys::List,
) -> *mut pg_sys::Plan {
    debug_assert!(
        !root.is_null(),
        "PlanCustomPath: root must be non-null at plan-stage",
    );
    debug_assert!(
        !rel.is_null(),
        "PlanCustomPath: rel must be non-null at plan-stage",
    );
    debug_assert!(
        !best_path.is_null(),
        "PlanCustomPath: best_path must be non-null at plan-stage",
    );

    let provider_metadata: *mut pg_sys::List = unsafe { (*best_path).custom_private };
    let final_clause_sources = unsafe { FinalScanClauseSources::new(rel) };

    let translate_ctx = unsafe { PlanTranslateContext::new(rel) };
    let mut classify_leaf = |predicate: &PlanPredicate<'_>| -> QualPushdownDecision {
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
    let pre_setrefs_scan_rti = unsafe { (*rel).relid } as i32;
    let scan_tuple = unsafe {
        BaseScanTuplePlanner::new((*rel).relid, relation_oid).plan(
            tlist,
            (*(*best_path).path.pathtarget).exprs,
            plan_qual,
            custom_exprs,
        )
    };
    let custom_private = unsafe {
        encode_split_with_layout(
            P::NAME,
            relation_oid,
            pushed_count,
            recheck_count,
            &pushed_contracts,
            &split.column_refs,
            provider_metadata,
            pre_setrefs_scan_rti,
            &scan_tuple.layout,
        )
    }
    .map_err(CustomScanError::encode_custom_private)
    .report_unwrap();

    let cscan = unsafe {
        pg_sys::palloc0(core::mem::size_of::<pg_sys::CustomScan>())
            as *mut pg_sys::CustomScan
    };
    debug_assert!(!cscan.is_null(), "palloc0(CustomScan) returned NULL",);

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
        (*cscan).methods = scan_methods_for::<P>();
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
unsafe extern "C-unwind" fn reparameterize_custom_path_by_child_trampoline<
    P: LakebaseCustomScanProvider,
>(
    root: *mut pg_sys::PlannerInfo,
    custom_private: *mut pg_sys::List,
    child_rel: *mut pg_sys::RelOptInfo,
) -> *mut pg_sys::List {
    unsafe { P::reparameterize_private_data(root, custom_private, child_rel) }
}

/// Context for [`emit_custom_path`].
pub struct EmitCustomPathContext<'a> {
    pub root: *mut pg_sys::PlannerInfo,
    pub baserel: *mut pg_sys::RelOptInfo,
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
pub unsafe fn emit_custom_path<P: LakebaseCustomScanProvider>(
    ctx: &EmitCustomPathContext<'_>,
) {
    let lateral_relids = unsafe { (*ctx.baserel).lateral_relids };
    let rel_relids = unsafe { (*ctx.baserel).relids };

    debug_assert!(
        unsafe { pg_sys::bms_is_subset(lateral_relids, ctx.required_outer) },
        "emit_custom_path: bms_is_subset(baserel->lateral_relids, required_outer) \
         must hold",
    );
    debug_assert!(
        !unsafe { pg_sys::bms_overlap(rel_relids, ctx.required_outer) },
        "emit_custom_path: !bms_overlap(baserel->relids, required_outer) \
         must hold",
    );

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

    debug_assert_eq!(
        param_info.is_null(),
        required_outer_is_empty,
        "emit_custom_path: param_info == NULL iff bms_is_empty(required_outer). \
         param_info.is_null()={}, bms_is_empty(required_outer)={}",
        param_info.is_null(),
        required_outer_is_empty,
    );

    let rel_path_ctx = unsafe {
        crate::customscan::provider::RelPathContext::with_planner(
            resolve_rte(ctx.root, ctx.baserel),
            ctx.root,
            ctx.baserel,
        )
    };
    let costed_pushed: Vec<_> = ctx.split.costed_pruning_exprs().collect();
    let pushdown = PathPushdownSummary::from_split(
        ctx.split,
        rel_path_ctx.clauselist_selectivity_for_exprs(&costed_pushed),
    );
    let variant = PathVariant {
        kind: ctx.kind,
        param_info: if param_info.is_null() {
            None
        } else {
            Some(unsafe { &*param_info })
        },
        required_outer: ctx.required_outer,
        pushdown,
    };
    let plan = match P::create_path(
        &rel_path_ctx,
        &variant,
        CustomPathBuilder::<P>::new(),
    ) {
        Some(plan) => plan,
        None => return, // provider declined this variant
    };

    let cpath_ptr = unsafe {
        pg_sys::palloc0(core::mem::size_of::<pg_sys::CustomPath>())
            as *mut pg_sys::CustomPath
    };
    debug_assert!(!cpath_ptr.is_null(), "palloc0(CustomPath) returned NULL");

    unsafe {
        let path = &mut (*cpath_ptr).path;
        path.type_ = pg_sys::NodeTag::T_CustomPath;
        path.pathtype = pg_sys::NodeTag::T_CustomScan;
        path.parent = ctx.baserel;
        path.pathtarget = (*ctx.baserel).reltarget;
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
        let (startup_cost, total_cost) = if crate::customscan::gucs::force_mode() {
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

        let provider_metadata: *mut pg_sys::List = match plan.private_data {
            Some(ref pd) => {
                let mut writer = PrivateDataWriter::new();
                pd.encode(&mut writer)
                    .and_then(|()| writer.finish())
                    .map_err(CustomScanError::encode_custom_private)
                    .report_unwrap()
            }
            None => ptr::null_mut(),
        };
        (*cpath_ptr).custom_private = provider_metadata;

        (*cpath_ptr).methods = path_methods_for::<P>();

        pg_sys::add_path(ctx.baserel, &mut (*cpath_ptr).path as *mut pg_sys::Path);
    }
}

/// `rt_fetch(rel->relid, root->parse->rtable)`.
///
/// # Safety
///
/// Live planner pointers; valid `parse->rtable`.
unsafe fn resolve_rte(
    root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
) -> *mut pg_sys::RangeTblEntry {
    let parse = unsafe { (*root).parse };
    debug_assert!(
        !parse.is_null(),
        "resolve_rte: root->parse must be non-null"
    );
    let rtable = unsafe { (*parse).rtable };
    debug_assert!(
        !rtable.is_null(),
        "resolve_rte: parse->rtable must be non-null"
    );
    let relid = unsafe { (*rel).relid };
    debug_assert!(
        relid > 0,
        "resolve_rte: rel->relid must be 1-based and positive"
    );
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
    debug_assert!(
        !pathtarget.is_null(),
        "compute_costs: baserel->reltarget must be non-null",
    );
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
    use core::ffi::CStr;

    use crate::customscan::codec::PrivateDataReader;
    use crate::customscan::custom_private::CustomScanPrivate;
    use crate::customscan::provider::{
        BeginContext, CreateStateContext, CustomScanError, EndContext,
        NextSlotContext, PlanTranslateContext, ReScanContext, RelPathContext,
    };
    use crate::expr::split::QualPushdownDecision;

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

    impl super::sealed::Sealed for TestPrivate {}
    impl ZeroSizedPrivate for TestPrivate {}

    struct TestProviderA;

    macro_rules! impl_test_provider {
        ($ty:ty, $name:expr) => {
            impl LakebaseCustomScanProvider for $ty {
                const NAME: &'static CStr = $name;
                type PrivateData = TestPrivate;
                type State = ();

                fn supports_relation(_ctx: &RelPathContext) -> bool {
                    false
                }

                fn classify_predicate(
                    _ctx: &PlanTranslateContext,
                    _predicate: &PlanPredicate<'_>,
                ) -> QualPushdownDecision {
                    QualPushdownDecision::Unsupported
                }

                fn create_path(
                    _ctx: &RelPathContext,
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

    /// `CustomPathBuilder::new()` defaults are `None` so [`build`]
    /// produces a [`CustomPathPlan`] that [`emit_custom_path`] will
    /// substitute with the relation's PG estimates.
    #[test]
    fn custom_path_builder_defaults_are_none() {
        let plan = CustomPathBuilder::<TestProviderA>::new().build();
        assert!(plan.scanned_pages.is_none());
        assert!(plan.scanned_tuples.is_none());
        assert!(plan.extra_startup_cost.is_none());
        assert!(plan.private_data.is_none());
    }

    #[test]
    fn zst_private_data_builds_without_private_data() {
        let plan = CustomPathBuilder::<TestProviderA>::new().build();
        assert!(
            plan.private_data.is_none(),
            "ZST escape-hatch build() must leave private_data unset",
        );
    }

    /// The setters round-trip into the produced [`CustomPathPlan`].
    #[test]
    fn custom_path_builder_setters_round_trip() {
        let plan = CustomPathBuilder::<TestProviderA>::new()
            .scanned_pages(42.0)
            .scanned_tuples(100.0)
            .extra_startup_cost(1.5)
            .private_data(TestPrivate)
            .build();
        assert_eq!(plan.scanned_pages, Some(42.0));
        assert_eq!(plan.scanned_tuples, Some(100.0));
        assert_eq!(plan.extra_startup_cost, Some(1.5));
        assert!(plan.private_data.is_some());
    }
}
